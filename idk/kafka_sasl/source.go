package kafka_sasl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

// Source implements the idk.Source interface using kafka as a data
// source. It is not threadsafe! Due to the way Kafka clients work, to
// achieve concurrency, create multiple Sources.
type Source struct {
	idk.ConfluentCommand
	Topics               []string
	Group                string
	Log                  logger.Logger
	Timeout              time.Duration
	SkipOld              bool
	Verbose              bool
	AllowMissingFields   bool
	consumerCloseTimeout time.Duration

	// Header is a file referencing a file containing JSON header configuration.
	Header string

	// HeaderFields can be provided instead of Header. It is a slice of
	// RawFields which will be marshalled and parsed the same way a JSON object
	// in Header would be. It is used only if a Header is not provided.
	HeaderFields []idk.RawField

	schema []idk.Field
	paths  idk.PathTable

	//	reader internal.KafkaReader

	spoolBase     uint64
	spool         []confluent.TopicPartition
	highmarks     []confluent.TopicPartition
	client        *confluent.Consumer
	recordChannel chan recordWithError
	ConfigMap     *confluent.ConfigMap

	// synchronize closing
	quit   chan struct{}
	wg     sync.WaitGroup
	opened bool
	mu     sync.Mutex
}

// NewSource gets a new Source
func NewSource() *Source {
	src := &Source{
		ConfluentCommand:     idk.ConfluentCommand{},
		Topics:               []string{"test"},
		Group:                "group0",
		Log:                  logger.NopLogger,
		recordChannel:        make(chan recordWithError),
		quit:                 make(chan struct{}),
		ConfigMap:            &confluent.ConfigMap{},
		highmarks:            make([]confluent.TopicPartition, 0),
		consumerCloseTimeout: 30,
	}
	src.KafkaBootstrapServers = []string{"localhost:9092"}

	return src
}

// Record returns the value of the next kafka message. The same Record
// object may be used by successive calls to Record, so it should not
// be retained.
func (s *Source) Record() (idk.Record, error) {
	rec := s.fetch()
	switch rec.Err {
	case nil:
	case io.EOF:
		return nil, io.EOF
	case context.DeadlineExceeded:
		return nil, idk.ErrFlush
	default:
		return nil, errors.Wrap(rec.Err, "failed to fetch record from Kafka")
	}

	if rec.Record == nil {
		return nil, idk.ErrFlush
	}

	data, err := s.decodeMessage(rec.Record.Value)
	if err != nil {
		return nil, errors.Wrap(err, "decoding with schema registry")
	}
	msg := rec.Record
	// with librdkafka, committing an offset means that offset is
	// where we should pick up from... we don't want to re-read this
	// message, so we add 1
	msg.TopicPartition.Offset++
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spool = append(s.spool, msg.TopicPartition)
	return &Record{
		src:       s,
		topic:     *msg.TopicPartition.Topic,
		partition: int(msg.TopicPartition.Partition),
		offset:    int64(msg.TopicPartition.Offset),
		idx:       s.spoolBase + uint64(len(s.spool)),
		data:      data,
	}, err
}

type recordWithError struct {
	Record *confluent.Message
	Err    error
}

func (s *Source) fetch() recordWithError {
	return <-s.recordChannel
}

func (s *Source) decodeMessage(buf []byte) ([]interface{}, error) {
	message := map[string]interface{}{}
	err := json.Unmarshal(buf, &message)
	if err != nil {
		jsonError, ok := err.(*json.SyntaxError)
		if ok {
			return nil, errors.Wrapf(err, "unmarshaling kafka message at character offset %v: %s", jsonError.Offset, string(buf))
		} else {
			return nil, errors.Wrapf(err, "unmarshaling kafka message at unknown character offset: %s", string(buf))
		}
	}

	return s.paths.Lookup(message, s.AllowMissingFields)
}

func (s *Source) Schema() []idk.Field {
	return s.schema
}

func (s *Source) CommitMessages(recs []confluent.TopicPartition) ([]confluent.TopicPartition, error) {
	return s.client.CommitOffsets(recs)
}

type Record struct {
	src       *Source
	topic     string
	partition int
	offset    int64
	idx       uint64
	data      []interface{}
}

func (r *Record) StreamOffset() (string, uint64) {
	return r.topic + ":" + strconv.Itoa(r.partition), uint64(r.offset)
}

var _ idk.OffsetStreamRecord = &Record{}

func (r *Record) Schema() interface{} { return nil }

func (r *Record) Commit(ctx context.Context) error {
	r.src.mu.Lock()
	defer r.src.mu.Unlock()
	idx, base := r.idx, r.src.spoolBase
	if idx < base {
		return errors.New("cannot commit a record that has already been committed")
	}
	section, remaining := r.src.spool[:idx-base], r.src.spool[idx-base:]
	sort.Slice(section, func(i, j int) bool {
		if *section[i].Topic != *section[j].Topic {
			return *section[i].Topic < *section[j].Topic
		} else if section[i].Partition != section[j].Partition {
			return section[i].Partition < section[j].Partition
		}
		return section[i].Offset > section[j].Offset
	})
	p := int32(-1)
	s := ""
	r.src.highmarks = r.src.highmarks[:0]

	// sort by increasing partition, decreasing offset
	for _, x := range section {
		if s != *x.Topic || p != x.Partition {
			r.src.highmarks = append(r.src.highmarks, x)
		}
		p = x.Partition
		s = *x.Topic

	}
	committedOffsets, err := r.src.CommitMessages(r.src.highmarks)
	if err != nil {
		return errors.Wrap(err, "failed to commit messages")
	}
	if r.src.Verbose {
		for _, o := range committedOffsets {
			r.src.Log.Debugf("t: %v p: %v o: %v", *o.Topic, o.Partition, o.Offset)
		}
	}

	r.src.spool = remaining
	r.src.spoolBase = idx

	return nil
}

func (r *Record) Data() []interface{} {
	return r.data
}

// Open initializes the kafka source.
func (s *Source) Open() error {
	if len(s.Header) == 0 && len(s.HeaderFields) == 0 {
		return errors.New("needs header specification (from file or from existing fields)")
	}

	var headerData []byte
	var err error
	if s.Header != "" {
		headerData, err = os.ReadFile(s.Header)
		if err != nil {
			return errors.Wrap(err, "reading header file")
		}
	} else {
		headerData, err = json.Marshal(s.HeaderFields)
		if err != nil {
			return errors.Wrap(err, "marshalling header fields")
		}
	}

	schema, paths, err := idk.ParseHeader(headerData)
	if err != nil {
		return errors.Wrap(err, "processing header")
	}
	s.schema = schema
	s.paths = paths

	// group
	if s.Group != "" {
		err = s.ConfigMap.SetKey("group.id", s.Group)
		if err != nil {
			return err
		}
	}

	// when there is no initial offset in Kafka or if the current offset does not exist any more,
	// use this as starting offset:
	//  "earliest": automatically reset the offset to the earliest offset
	//  "latest": automatically reset the offset to the latest offset
	offset := "earliest"
	if s.SkipOld {
		offset = "latest"
	}
	err = s.ConfigMap.SetKey("auto.offset.reset", offset)
	if err != nil {
		return err
	}
	if s.Verbose {
		buf := bytes.NewBufferString("Confluent Config Map:")
		encoder := json.NewEncoder(buf)
		err = encoder.Encode(s.ConfigMap)
		if err != nil {
			return err
		}
		s.Log.Debugf(buf.String())
		stv, iv := confluent.LibraryVersion()
		s.Log.Debugf("version:(%v) %v", iv, stv)
	}
	cl, err := confluent.NewConsumer(s.ConfigMap)
	if err != nil {
		return errors.Wrap(err, "new consumer")
	}

	// by default, Kafka will use the stored offset (the latest committed message) and continue on from there.
	// to skip old msgs, use rebalanceCbSkipOld to manually set offset to the end
	err = cl.SubscribeTopics(s.Topics, nil)
	if err != nil {
		return errors.Wrap(err, "subscribe topics")
	}

	if s.Verbose {
		s.Log.Debugf("subscribed to %v", s.Topics)
	}

	s.client = cl
	s.opened = true
	s.wg.Add(1)

	go func() {
		s.generator()
	}()

	return nil
}

func (c *Source) generator() {
	defer func() {
		close(c.recordChannel)
		c.wg.Done()
	}()

	for {
		select {

		case <-c.quit:
			if c.Verbose {
				c.Log.Debugf("source quit")
			}
			return
		default:
			ev := c.client.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {

			// If we received an `AssignedPartitions` event, we need to make sure we
			// assign the currently running consumer to the right partitions.
			case confluent.AssignedPartitions:
				err := c.client.Assign(e.Partitions)
				if err != nil {
					if c.Verbose {
						c.Log.Debugf("quit AssignedParitions")
					}
					return
				}
				// If we received an `RevokedPartitions` event, we need to revoke this
				// consumer from all partitions. This means this consumer won't pick up
				// any work anymore, until a new `AssignedPartitions` event is handled.
			case confluent.RevokedPartitions:
				err := c.client.Unassign()
				if err != nil {
					if c.Verbose {
						c.Log.Debugf("quit RevokeParkitions")
					}
					return
				}

			// If we receive an error, something happened on Kafka's side. We don't
			// know what happened or if we can recover gracefully, so we instead
			// terminate the running process.
			case confluent.Error:
				msg := recordWithError{Err: e}
				select {
				case c.recordChannel <- msg:
				case <-c.quit:
					if c.Verbose {
						c.Log.Debugf("source quit Error")
					}
					return
				}

			// On receiving a Kafka message, we process the received message and
			// prepare it for delivery to the consumer of the consumer.messages
			// channel.
			case *confluent.Message:
				msg := recordWithError{Record: e}

				// Once the message has been prepared, we offer it to the consumer of
				// the messages channel. Since this is a blocking channel, we also
				// listen for the quit signal, and stop delivering new messages
				// accordingly.
				select {
				case c.recordChannel <- msg:
				case <-c.quit:
					if c.Verbose {
						c.Log.Debugf("source quit Message")
					}
					return
				}
			case confluent.OffsetsCommitted:
				c.Log.Debugf("commited %s", e)
			default:
				if c.Verbose {
					c.Log.Debugf("ignored %#v", ev)
				}
				continue // consumer doesn't care about all event types (e.g. OffsetsCommitted)
			}
		}
	}
}

// Close closes the underlying kafka consumer.
func (s *Source) Close() error {
	if s.client != nil {
		if s.opened { // only close opened sources
			var err error
			closedReturned := make(chan error, 1)
			// send quit message to polling routine & wait for it to exit
			s.quit <- struct{}{}
			s.wg.Wait()
			s.Log.Debugf("Trying to close consumer %s...", s.client.String())
			go func() {
				closedReturned <- s.client.Close()
			}()
			start := time.Now()
			select {
			case err = <-closedReturned:
				if err == nil {
					s.Log.Debugf("Successfully closed consumer %s!", s.client.String())
					s.opened = false
				}
			case <-time.After(time.Duration(s.consumerCloseTimeout * 1000 * 1000 * 1000)):
				err = fmt.Errorf("unable to properly close consumer %s after %f seconds", s.client.String(), time.Since(start).Seconds())
			}

			return errors.Wrap(err, "closing kafka consumer")
		}
	}
	return nil
}
