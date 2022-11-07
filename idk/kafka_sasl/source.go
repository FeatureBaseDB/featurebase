package kafka_sasl

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"
)

// Source implements the idk.Source interface using kafka as a data
// source. It is not threadsafe! Due to the way Kafka clients work, to
// achieve concurrency, create multiple Sources.
type Source struct {
	idk.ConfluentCommand
	Topics             []string
	Group              string
	Log                logger.Logger
	Timeout            time.Duration
	SkipOld            bool
	Header             string
	AllowMissingFields bool

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
		Topics:        []string{"test"},
		Group:         "group0",
		Log:           logger.NopLogger,
		recordChannel: make(chan recordWithError),
		quit:          make(chan struct{}),
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
		return nil, errors.Wrap(rec.Err, "failed to fetch record from Kafka Confluent")
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

func (r *Record) Commit(ctx context.Context) error {
	r.src.mu.Lock()
	defer r.src.mu.Unlock()

	idx, base := r.idx, r.src.spoolBase
	if idx < base {
		return errors.New("cannot commit a record that has already been committed")
	}
	section, remaining := r.src.spool[:idx-base], r.src.spool[idx-base:]
	// sort by increasing partition, decreasing offset
	sort.Slice(section, func(i, j int) bool {
		if section[i].Partition != section[j].Partition {
			return section[i].Partition < section[j].Partition
		}
		return section[i].Offset > section[j].Offset
	})
	// calculate the high marks
	p := int32(-1)
	r.src.highmarks = r.src.highmarks[:0]
	for _, x := range section {
		if p != x.Partition {
			r.src.highmarks = append(r.src.highmarks, x)
		}
		p = x.Partition
	}
	_, err := r.src.CommitMessages(r.src.highmarks)
	if err != nil {
		return errors.Wrap(err, "failed to commit messages")
	}

	r.src.spool = remaining
	r.src.spoolBase = idx

	return nil
}

// Assuming committed msgs are in order
func calOffsetDiff(section, committed []confluent.TopicPartition) []confluent.TopicPartition {
	return section[len(committed):]
}

func (r *Record) Data() []interface{} {
	return r.data
}

// Open initializes the kafka source.
func (s *Source) Open() error {
	if len(s.Header) == 0 {
		return errors.New("needs header specification file")
	}

	headerData, err := os.ReadFile(s.Header)
	if err != nil {
		return errors.Wrap(err, "reading header file")
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
	err = s.ConfigMap.SetKey("auto.offset.reset", "earliest")
	if err != nil {
		return err
	}
	if s.SkipOld {
		err = s.ConfigMap.SetKey("auto.offset.reset", "latest")
		if err != nil {
			return err
		}
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
					return
				}
				// If we received an `RevokedPartitions` event, we need to revoke this
				// consumer from all partitions. This means this consumer won't pick up
				// any work anymore, until a new `AssignedPartitions` event is handled.
			case confluent.RevokedPartitions:
				err := c.client.Unassign()
				if err != nil {
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
					return
				}
			default:
				continue // consumer doesn't care about all event types (e.g. OffsetsCommitted)
			}
		}
	}
}

// Close closes the underlying kafka consumer.
func (s *Source) Close() error {
	if s.client != nil {
		if s.opened { // only close opened sources
			s.quit <- struct{}{}
			s.wg.Wait()
			err := s.client.Close()
			s.opened = false
			return errors.Wrap(err, "closing kafka consumer")
		}
	}
	return nil
}
