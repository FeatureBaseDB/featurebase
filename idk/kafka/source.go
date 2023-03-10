package kafka

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	pilosaclient "github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/common"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/go-avro/avro"
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
	schema               Schema
	TLS                  idk.TLSConfig
	consumerCloseTimeout int

	spoolBase     uint64
	spool         []confluent.TopicPartition
	highmarks     []confluent.TopicPartition
	client        *confluent.Consumer
	recordChannel chan recordWithError

	ConfigMap *confluent.ConfigMap

	// lastSchemaID and lastSchema keep track of the most recent
	// schema in use. We expect this not to change often, but when it
	// does, we need to notify the caller of Source.Record()
	lastSchemaID int32
	lastSchema   []idk.Field

	// cache is a schema cache so we don't have to look up the same
	// schema from the registry each time.
	cache      map[int32]avro.Schema
	httpClient *http.Client
	// synchronize closing
	quit   chan struct{}
	wg     sync.WaitGroup
	opened bool
	mu     sync.Mutex
}

const defaultRegistryHost = "localhost:8081"

// NewSource gets a new Source
func NewSource() *Source {
	src := &Source{
		ConfluentCommand: idk.ConfluentCommand{},
		Topics:           []string{"test"},
		Group:            "group0",
		Log:              logger.NopLogger,

		lastSchemaID:  -1,
		cache:         make(map[int32]avro.Schema),
		recordChannel: make(chan recordWithError),
		quit:          make(chan struct{}),
		ConfigMap:     &confluent.ConfigMap{},
		highmarks:     make([]confluent.TopicPartition, 0),
	}

	src.SchemaRegistryURL = "http://" + defaultRegistryHost
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

	val, avroSchema, err := s.decodeAvroValueWithSchemaRegistry(rec.Record.Value)
	if err != nil && err != idk.ErrSchemaChange {
		return nil, errors.Wrap(err, "decoding with schema registry")
	}
	data := s.toPDKRecord(val.(map[string]interface{})) // val must be map[string]interface{} because we only accept schemas which are Record type at the top level.

	msg := rec.Record
	// with librdkafka, committing an offset means that offset is
	// where we should pick up from... we don't want to re-read this
	// message, so we add 1
	msg.TopicPartition.Offset++
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spool = append(s.spool, msg.TopicPartition)
	return &Record{
		src:        s,
		topic:      *msg.TopicPartition.Topic,
		partition:  int(msg.TopicPartition.Partition),
		offset:     int64(msg.TopicPartition.Offset),
		idx:        s.spoolBase + uint64(len(s.spool)),
		data:       data,
		avroSchema: avroSchema,
	}, err
}

type recordWithError struct {
	Record *confluent.Message
	Err    error
}

func (s *Source) fetch() recordWithError {
	if s.Timeout != 0 {
		select {
		case msg := <-s.recordChannel:
			return msg
		case <-time.After(s.Timeout):
			return recordWithError{Err: context.DeadlineExceeded}
		}
	}
	return <-s.recordChannel
}

func (s *Source) Schema() []idk.Field {
	return s.lastSchema
}

func (s *Source) SchemaSubject() string {
	return s.schema.Subject
}

func (s *Source) SchemaSchema() string {
	return s.schema.Schema
}

func (s *Source) SchemaVersion() int {
	return s.schema.Version
}

func (s *Source) SchemaID() int {
	return s.schema.ID
}

func (s *Source) SchemaMetadata() string {
	var buf bytes.Buffer
	err := json.Compact(&buf, []byte(s.cache[s.lastSchemaID].String()))
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func (s *Source) toPDKRecord(vals map[string]interface{}) []interface{} {
	data := make([]interface{}, len(s.lastSchema))
	for i, field := range s.lastSchema {
		data[i] = vals[field.Name()]
	}

	return data
}

type Record struct {
	src        *Source
	topic      string
	partition  int
	offset     int64
	idx        uint64
	data       []interface{}
	avroSchema avro.Schema
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

func (r *Record) Schema() interface{} {
	return r.avroSchema
}

func (s *Source) CommitMessages(recs []confluent.TopicPartition) ([]confluent.TopicPartition, error) {
	return s.client.CommitOffsets(recs)
}

// Open initializes the kafka source. (i.e. creating and configuring a consumer)
// The configuration options for the confluentinc/confluent-kafka-go/kafka
// libarary are: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
func (s *Source) Open() error {
	cfg, err := common.SetupConfluent(&s.ConfluentCommand)
	if err != nil {
		return err
	}
	s.ConfigMap = cfg

	s.httpClient = http.DefaultClient
	if err := s.cleanRegistryURL(); err != nil {
		return errors.Wrap(err, "cleaning registry URL")
	}

	if strings.HasPrefix(s.SchemaRegistryURL, "https://") {
		tlsConfig, err := idk.GetTLSConfigFromConfluent(&s.ConfluentCommand, s.Log)
		if err != nil {
			return errors.Wrap(err, "getting TLS config")
		}
		s.httpClient = getHTTPClient(tlsConfig)
	}
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

func (s *Source) cleanRegistryURL() error {
	// We can't immediately url.Parse the RegistryURL because parsing
	// a host without a scheme is invalid. First we'll check for a
	// scheme and add the default http:// if needed.
	if !strings.Contains(s.SchemaRegistryURL, "://") {
		s.SchemaRegistryURL = "http://" + s.SchemaRegistryURL
	}

	SchemaRegistryURL, err := url.Parse(s.SchemaRegistryURL)
	if err != nil {
		return errors.Wrap(err, "parse registry URL")
	}

	s.SchemaRegistryURL = SchemaRegistryURL.String()
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
				err = fmt.Errorf("Unable to properly close consumer %s after %f seconds", s.client.String(), time.Since(start).Seconds())
			}

			return errors.Wrap(err, "closing kafka consumer")
		}
	}
	return nil
}

// TODO change name
func (s *Source) decodeAvroValueWithSchemaRegistry(val []byte) (interface{}, avro.Schema, error) {
	if len(val) < 6 || val[0] != 0 {
		return nil, nil, errors.Errorf("unexpected magic byte or length in avro kafka value, should be 0x00, but got %x", val)
	}
	id := int32(binary.BigEndian.Uint32(val[1:]))
	codec, err := s.getCodec(id)
	if err != nil {
		return nil, nil, errors.Wrap(err, "getting avro codec")
	}
	ret, err := avroDecode(codec, val[5:])
	if err != nil {
		return nil, codec, errors.Wrap(err, "decoding avro record")
	}
	if id != s.lastSchemaID {
		s.lastSchema, err = avroToPDKSchema(codec)
		if err != nil {
			return nil, codec, errors.Wrap(err, "converting to FeatureBase schema")
		}
		s.lastSchemaID = id
		return ret, codec, idk.ErrSchemaChange
	}

	return ret, codec, nil
}

// avroToPDKSchema converts a full avro schema to the much more
// constrained []idk.Field which maps pretty directly onto
// Pilosa. Many features of avro are unsupported and will cause this
// to return an error. The "codec" argument of this function must be
// an avro.Record.
func avroToPDKSchema(codec avro.Schema) ([]idk.Field, error) {
	switch codec.Type() {
	case avro.Record:
		recordSchema, ok := codec.(*avro.RecordSchema)
		if !ok {
			panic(fmt.Sprintf("Record isn't a *avro.RecordSchema, got %+v of %[1]T", codec))
		}
		pdkFields := make([]idk.Field, 0, len(recordSchema.Fields))
		for _, field := range recordSchema.Fields {
			pdkField, err := avroToPDKField(field)
			if err != nil {
				return nil, errors.Wrap(err, "converting avro field to pdk")
			}
			pdkFields = append(pdkFields, pdkField)
		}
		return pdkFields, nil
	default:
		return nil, errors.Errorf("unsupported Avro Schema type: %d", codec.Type()) // TODO error msg with int type is pretty opaque
	}
}

func avroToPDKField(aField *avro.SchemaField) (idk.Field, error) {
	switch aField.Type.Type() {
	case avro.Record:
		return nil, errors.Errorf("nested fields are not currently supported, so the field type cannot be record.")

	case avro.String:
		fld := idk.StringField{NameVal: aField.Name}

		if prop, ok := aField.Prop("mutex"); ok {
			if mtx, ok := prop.(bool); ok {
				fld.Mutex = mtx
			}
		}

		if quantum, err := stringProp(aField, "quantum"); err == nil {
			fld.Quantum = quantum
			if ttl, err := stringProp(aField, "ttl"); err == nil {
				fld.TTL = ttl
			}
		}

		if cacheConfig, err := cacheConfigProp(aField); err == nil {
			fld.CacheConfig = cacheConfig
		}
		return fld, nil

	case avro.Enum:
		return idk.StringField{
			NameVal: aField.Name,
			Mutex:   true,
		}, nil

	case avro.Bytes, avro.Fixed:
		var ft string

		// This is here to support the native avro logicalType.decimal,
		// but we have also implemented fieldType.decimal to be consistent
		// with the other fieldType options.
		if ft, _ = stringProp(aField, "logicalType"); ft != "decimal" {
			ft, _ = stringProp(aField, "fieldType")
		}

		switch ft {
		case "decimal":
			scale, err := intProp(aField, "scale")
			if scale > 18 || err == errWrongType {
				return nil, errors.Errorf("0<=scale<=18, got:%d err:%v", scale, err)
			}
			return idk.DecimalField{
				NameVal: aField.Name,
				Scale:   int64(scale),
			}, nil

		case "dateInt":
			layout, err := stringProp(aField, "layout")
			if err != nil {
				return nil, errors.Errorf("required property for DateIntField: layout, err:%v", err)
			}

			strEpoch, err := stringProp(aField, "epoch")
			if err != nil {
				return nil, errors.Errorf("required property for DateIntField: epoch, err:%v", err)
			}
			epoch, err := time.Parse(layout, strEpoch)
			if err != nil {
				return nil, errors.Wrapf(err, "parsing epoch: %s", strEpoch)
			}

			unit, err := stringProp(aField, "unit")
			if err != nil {
				return nil, errors.Errorf("required property for DateIntField: unit, err:%v", err)
			}

			customUnit, _ := stringProp(aField, "customUnit")

			return idk.DateIntField{
				NameVal:    aField.Name,
				Layout:     layout,
				Epoch:      epoch,
				Unit:       idk.Unit(unit),
				CustomUnit: customUnit,
			}, nil
		case "timestamp":
			layout, err := stringProp(aField, "layout")
			if err == errWrongType {
				return nil, errors.Errorf("property provided in wrong type for TimestampField: layout, err:%v", err)
			}

			unit, err := stringProp(aField, "unit")
			if err == errWrongType {
				return nil, errors.Errorf("property provided in wrong type for TimestampField: unit, err:%v", err)
			}

			if layout == "" && unit == "" {
				return nil, errors.New("either layout or unit required for TimestampField")
			}

			granularity, err := stringProp(aField, "granularity")
			if err == errWrongType {
				return nil, errors.Errorf("property provided in wrong type for TimestampField: unit, err:%v", err)
			}

			var epoch time.Time
			strEpoch, err := stringProp(aField, "epoch")
			if err == nil {
				epoch, err = time.Parse(layout, strEpoch)
				if err != nil {
					return nil, errors.Wrapf(err, "parsing epoch: %s", strEpoch)
				}
			}

			return idk.TimestampField{
				NameVal:     aField.Name,
				Granularity: granularity,
				Layout:      layout,
				Epoch:       epoch,
				Unit:        idk.Unit(unit),
			}, nil
		case "recordTime":
			layout, err := stringProp(aField, "layout")
			if err != nil {
				return nil, errors.Errorf("required property for RecordTimeField: layout, err:%v", err)
			}
			return idk.RecordTimeField{
				NameVal: aField.Name,
				Layout:  layout,
			}, nil
		}

		// If field type was not specified, then treat as string.
		var mutex bool
		if mtx, ok := aField.Prop("mutex"); ok {
			if mtxb, ok := mtx.(bool); ok {
				mutex = mtxb
			}
		}
		quantum, _ := stringProp(aField, "quantum")
		ttl := ""
		if quantum != "" {
			ttl, _ = stringProp(aField, "ttl")
		}
		return idk.StringField{
			NameVal: aField.Name,
			Mutex:   mutex,
			Quantum: quantum,
			TTL:     ttl,
		}, nil

	case avro.Union:
		return avroUnionToPDKField(aField)

	case avro.Array:
		itemSchema := aField.Type.(*avro.ArraySchema).Items

		quantum, _ := stringProp(itemSchema, "quantum")
		ttl := ""
		if quantum != "" {
			ttl, _ = stringProp(itemSchema, "ttl")
		}
		cacheConfig, _ := cacheConfigProp(itemSchema)

		switch typ := itemSchema.Type(); typ {
		case avro.String, avro.Bytes, avro.Fixed, avro.Enum:
			if ft, _ := stringProp(itemSchema, "fieldType"); ft == "decimal" {
				return nil, errors.New("arrays of decimal are not supported")
			}
			return idk.StringArrayField{
				NameVal:     aField.Name,
				Quantum:     quantum,
				TTL:         ttl,
				CacheConfig: cacheConfig,
			}, nil

		case avro.Long, avro.Int:
			if ft, _ := stringProp(itemSchema, "fieldType"); ft == "decimal" {
				return nil, errors.New("arrays of decimal are not supported")
			}
			return idk.IDArrayField{
				NameVal:     aField.Name,
				Quantum:     quantum,
				TTL:         ttl,
				CacheConfig: cacheConfig,
			}, nil

		default:
			return nil, errors.Errorf("array items type of %d is unsupported", itemSchema.Type())
		}

	case avro.Int, avro.Long:
		ft, _ := stringProp(aField, "fieldType")
		switch ft {
		case "id":
			fld := idk.IDField{NameVal: aField.Name}

			if prop, ok := aField.Prop("mutex"); ok {
				if mtx, ok := prop.(bool); ok {
					fld.Mutex = mtx
				}
			}

			if quantum, err := stringProp(aField, "quantum"); err == nil {
				fld.Quantum = quantum
				if ttl, err := stringProp(aField, "ttl"); err == nil {
					fld.TTL = ttl
				}
			}

			if cacheConfig, err := cacheConfigProp(aField); err == nil {
				fld.CacheConfig = cacheConfig
			}

			return fld, nil

		case "int":
			fld := idk.IntField{NameVal: aField.Name}

			if min, err := intProp(aField, "min"); err == nil {
				fld.Min = intptr(min)
			}
			if max, err := intProp(aField, "max"); err == nil {
				fld.Max = intptr(max)
			}
			return fld, nil

		case "signedIntBoolKey":
			return idk.SignedIntBoolKeyField{
				NameVal: aField.Name,
			}, nil

		case "dateInt":
			layout, err := stringProp(aField, "layout")
			if err != nil {
				return nil, errors.Errorf("required property for DateIntField: layout, err:%v", err)
			}

			strEpoch, err := stringProp(aField, "epoch")
			if err != nil {
				return nil, errors.Errorf("required property for DateIntField: epoch, err:%v", err)
			}
			epoch, err := time.Parse(layout, strEpoch)
			if err != nil {
				return nil, errors.Wrapf(err, "parsing epoch: %s", strEpoch)
			}

			unit, err := stringProp(aField, "unit")
			if err != nil {
				return nil, errors.Errorf("required property for DateIntField: unit, err:%v", err)
			}
			customUnit, _ := stringProp(aField, "customUnit")

			return idk.DateIntField{
				NameVal:    aField.Name,
				Layout:     layout,
				Epoch:      epoch,
				Unit:       idk.Unit(unit),
				CustomUnit: customUnit,
			}, nil
		}
		return idk.IntField{
			NameVal: aField.Name,
		}, nil

	case avro.Float, avro.Double:
		// TODO should probably require a logicalType if we're going
		// to treat a float as a decimal.
		field := idk.DecimalField{
			NameVal: aField.Name,
		}
		scale, err := intProp(aField, "scale")
		if err == errWrongType {
			return nil, errors.Wrap(err, "getting scale")
		} else if err == nil {
			field.Scale = scale
		}
		return field, nil

	case avro.Boolean:
		return idk.BoolField{
			NameVal: aField.Name,
		}, nil

	case avro.Null:
		return nil, errors.Errorf("null fields are not supported except inside Union")

	case avro.Map:
		return nil, errors.Errorf("nested fields are not currently supported, so the field type cannot be map.")

	case avro.Recursive:
		return nil, errors.Errorf("recursive schema fields are not currently supported.")

	default:
		return nil, errors.Errorf("unknown schema type: %+v", aField.Type)
	}
}

func stringProp(p propper, s string) (string, error) {
	ival, ok := p.Prop(s)
	if !ok {
		return "", errNotFound
	}
	sval, ok := ival.(string)
	if !ok {
		return "", errWrongType
	}
	return sval, nil
}

func cacheConfigProp(p propper) (*idk.CacheConfig, error) {
	cacheType, err := stringProp(p, "cacheType")
	if err != nil {
		return nil, err
	}

	cacheSizeStr, err := stringProp(p, "cacheSize")
	if err != nil {
		return nil, err
	}

	cacheSize, err := strconv.Atoi(cacheSizeStr)
	if err != nil {
		return nil, err
	}

	return &idk.CacheConfig{CacheType: pilosaclient.CacheType(cacheType), CacheSize: cacheSize}, nil
}

func intProp(p propper, s string) (int64, error) {
	ival, ok := p.Prop(s)
	if !ok {
		return 0, errNotFound
	}

	switch v := ival.(type) {
	case string:
		n, e := strconv.ParseInt(v, 10, 64)
		if e != nil {
			return 0, errors.Wrap(e, errWrongType.Error())
		}
		return n, nil

	case float64:
		return int64(v), nil

	case int64:
		return v, nil

	case int:
		return int64(v), nil
	}

	return 0, errWrongType
}

type propper interface {
	Prop(string) (interface{}, bool)
}

var (
	errNotFound  = errors.New("prop not found")
	errWrongType = errors.New("val is wrong type")
)

// avroUnionToPDKField takes an avro SchemaField with a Union type,
// and reduces it to a SchemaField with the type of one of the Types
// contained in the Union. It can only do this if the Union only has
// one type, or if it has multiple types and one is null.
func avroUnionToPDKField(field *avro.SchemaField) (idk.Field, error) {
	if field.Type.Type() != avro.Union {
		panic("it should be impossible to call avroUnionToPDKField with a non-union SchemaField")
	}
	uSchema := field.Type.(*avro.UnionSchema)
	nf := &avro.SchemaField{
		Name:    field.Name,
		Doc:     field.Doc,
		Default: field.Default,
	}

	for _, t := range uSchema.Types {
		nf.Type = t
		if nf.Type.Type() != avro.Null {
			break
		}
	}

	if nf.Type == nil {
		return nil, errors.New("unions are only supported when they are a single type plus optionally a Null")
	}

	if nf.Type.Type() == avro.Null && len(uSchema.Types) > 1 {
		return nil, errors.New("unions are only supported when one type is Null")
	}

	nf.Properties = propertiesFromSchema(nf.Type, field.Properties)
	return avroToPDKField(nf)
}

// ifEmptyThen returns `prefer` if it contains any data, otherwise it
// returns `backup`. This is used for backward compatibility.
//
// We originally supported field properties being specified at the
// root level (i.e. against the "Union" itself), like `mutex` here:
// { "name": "eighth", "type": ["null", "string"], "mutex": true, "default": null }
//
// Instead, we want to support properties being associated with the
// specific field type within the union:
// { "name": "eighth", "type": ["null", {"type": "string", "mutex": true}], "default": null }
//
// Using this function allows us to fall back to the former (backup)
// case, but prioritize the latter (prefer).
func ifEmptyThen(prefer, backup map[string]interface{}) map[string]interface{} {
	if len(prefer) == 0 {
		return backup
	}
	return prefer
}

// propertiesFromSchema (document and use!)
func propertiesFromSchema(sch avro.Schema, origProps map[string]interface{}) map[string]interface{} {
	switch schT := sch.(type) {
	case *avro.IntSchema, *avro.BooleanSchema, *avro.NullSchema, *avro.UnionSchema:
		// for string->mutex behavior, mutex:true must be propagated here
		return origProps
	case *avro.LongSchema:
		return ifEmptyThen(schT.Properties, origProps)
	case *avro.BytesSchema:
		return ifEmptyThen(schT.Properties, origProps)
	case *avro.StringSchema:
		return ifEmptyThen(schT.Properties, origProps)
	case *avro.DoubleSchema:
		return ifEmptyThen(schT.Properties, origProps)
	case *avro.FloatSchema:
		return ifEmptyThen(schT.Properties, origProps)
	case *avro.RecordSchema:
		return ifEmptyThen(schT.Properties, origProps)
	case *avro.RecursiveSchema:
		if schT.Actual != nil {
			return ifEmptyThen(schT.Actual.Properties, origProps)
		}
		return nil
	case *avro.EnumSchema:
		return ifEmptyThen(schT.Properties, origProps)
	case *avro.ArraySchema:
		return ifEmptyThen(schT.Properties, origProps)
	case *avro.MapSchema:
		return ifEmptyThen(schT.Properties, origProps)
	case *avro.FixedSchema:
		return ifEmptyThen(schT.Properties, origProps)
	default:
		// TODO handle logging properly (e.g. respect log path, use an interface logger, etc.)
		log.Printf("Source: unhandled avro.Schema concrete type %T in propertiesFromSchema, value: %+v", schT, schT)
		return nil
	}
}

// The Schema type is an object produced by the schema registry.
type Schema struct {
	Schema  string `json:"schema"`  // The actual AVRO schema
	Subject string `json:"subject"` // Subject where the schema is registered for
	Version int    `json:"version"` // Version within this subject
	ID      int    `json:"id"`      // Registry's unique id
}

func (s *Source) codecURL(id int32, urlPath string) (string, error) {
	url, err := url.Parse(s.SchemaRegistryURL)
	if err != nil {
		// this should be impossible since the Registry URL is created
		// from URL.String() in Source.Open
		return "", errors.Wrap(err, "parsing pre-validated registry url: "+s.SchemaRegistryURL)
	}
	url.Path = path.Join(url.Path, fmt.Sprintf(urlPath, id))

	return url.String(), nil
}

func (s *Source) getCodec(id int32) (avro.Schema, error) {
	if codec, ok := s.cache[id]; ok {
		return codec, nil
	}
	s.Log.Debugf("Source: new avro schema ID: %d", id)
	// r, err := s.httpClient.Get(s.codecURL___OLD(id))
	schemaUrl, err := s.codecURL(id, "schemas/ids/%d")
	if err != nil {
		return nil, errors.Wrap(err, "getting schema url")
	}

	//	schemaUrlResponse, err := s.httpClient.Get(schemaUrl)
	req, err := http.NewRequest(http.MethodGet, schemaUrl, http.NoBody)
	if err != nil {
		return nil, errors.Wrap(err, "building request for getting schema from registry")
	}
	if s.SchemaRegistryUsername != "" {
		req.SetBasicAuth(s.SchemaRegistryUsername, s.SchemaRegistryPassword)
	}

	schemaUrlResponse, err := s.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "getting schema from registry")
	}
	defer schemaUrlResponse.Body.Close()
	if schemaUrlResponse.StatusCode >= 300 {
		bod, err := io.ReadAll(schemaUrlResponse.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to get schema, code: %d, no body", schemaUrlResponse.StatusCode)
		}
		return nil, errors.Errorf("Failed to get schema, code: %d, resp: %s", schemaUrlResponse.StatusCode, bod)
	}
	dec := json.NewDecoder(schemaUrlResponse.Body)
	schema := &Schema{}
	err = dec.Decode(schema)
	if err != nil {
		return nil, errors.Wrap(err, "decoding schema from registry")
	}

	// get Subject and Version from url
	schemaSubVerUrl, err := s.codecURL(id, "schemas/ids/%d/versions")
	if err != nil {
		return nil, errors.Wrap(err, "getting schema subject & version url")
	}
	req, err = http.NewRequest(http.MethodGet, schemaSubVerUrl, http.NoBody)
	if err != nil {
		return nil, errors.Wrap(err, "building request for getting schema from registry")
	}
	if s.SchemaRegistryUsername != "" {
		req.SetBasicAuth(s.SchemaRegistryUsername, s.SchemaRegistryPassword)
	}

	// try to get subject/version info for schema. Does not work on
	// older confluent versions, so just log if it errors rather than
	// returning error
	subVerResponse, err := s.httpClient.Do(req)
	if err != nil {
		s.Log.Infof("Problem getting subject/version info for schema: %v", err)
	} else {
		if subVerResponse.StatusCode >= 300 {
			bod, err := io.ReadAll(subVerResponse.Body)
			s.Log.Infof("Problem getting subject/version info for schema, response: %s. Err reading body: %v", bod, err)
		}
		defer subVerResponse.Body.Close()

		var tempSchemaStruct []struct {
			Subject string `json:"subject"`
			Version int    `json:"version"`
		}

		if bod, err := io.ReadAll(subVerResponse.Body); err != nil {
			s.Log.Infof("decoding subj/version %s body: %v", schemaSubVerUrl, err)
		} else if err := json.Unmarshal(bod, &tempSchemaStruct); err != nil {
			s.Log.Infof("decoding schema subject & version from registry: %v", err)
		}

		if len(tempSchemaStruct) > 0 {
			// save version, subject
			schema.Version = tempSchemaStruct[0].Version
			schema.Subject = tempSchemaStruct[0].Subject
		}
	}

	schema.ID = int(id)
	// save schema object on s
	s.schema = *schema

	codec, err := avro.ParseSchema(schema.Schema)
	if err != nil {
		return nil, errors.Wrap(err, "parsing schema")
	}
	s.Log.Debugf("Source: successfully got new avro schema %d: %s", id, codec.String())

	s.cache[id] = codec
	return codec, nil
}

func avroDecode(codec avro.Schema, data []byte) (map[string]interface{}, error) {
	reader := avro.NewGenericDatumReader()
	// SetSchema must be called before calling Read
	reader.SetSchema(codec)

	// Create a new Decoder with a given buffer
	decoder := avro.NewBinaryDecoder(data)

	decodedRecord := avro.NewGenericRecord(codec)
	// Read data into given GenericRecord with a given Decoder. The first parameter to Read should be something to read into
	err := reader.Read(decodedRecord, decoder)
	if err != nil {
		return nil, errors.Wrap(err, "reading generic datum")
	}

	return decodedRecord.Map(), nil
}

func getHTTPClient(t *tls.Config) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 20 * time.Second,
			DualStack: true,
		}).DialContext,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if t != nil {
		transport.TLSClientConfig = t
	}
	return &http.Client{Transport: transport}
}

func intptr(i int64) *int64 {
	return &i
}
