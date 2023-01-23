package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/common"
	"github.com/featurebasedb/featurebase/v3/idk/kafka/csrc"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/go-avro/avro"
	liavro "github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// PutSource represents a kafka put process that operates on one
// or more idk.Source.
type PutSource struct {
	idk.ConfluentCommand
	Topic     string `help:"Kafka topic to post to."`
	Subject   string `help:"Kafka schema subject."`
	BatchSize int    `help:"Size of record batches to submit to Kafka."`

	// FB specific config for setting partition key efficiently
	FBPrimaryKeyFields []string
	FBIndexName        string
	FBIDField          string

	Concurrency int `help:"Number of concurrent sources and indexing routines to launch."`

	TrackProgress bool `help:"Periodically print status updates on how many records have been sourced." short:""`

	ReplicationFactor int `help:"set replication factor for kafka cluster"`
	NumPartitions     int `help:"set partition for kafka cluster"`
	// NewSource must be set by the user of Main before calling
	// Main.Run. Main.Run will call this function "Concurrency" times. It
	// is the job of this function to ensure that the concurrent
	// sources which are started partition work appropriately. This is
	// typically set up (by convention) in the Source's package in
	// cmd.go
	NewSource func() (idk.Source, error) `flag:"-"`

	Log logger.Logger

	progress *idk.ProgressTracker

	schemaClient *csrc.Client
	ConfigMap    *confluent.ConfigMap `flag:"-"`
	Target       string
}

// NewPutSource returns a new instance of PutSource.
func NewPutSource() (*PutSource, error) {
	var err error
	p := PutSource{}
	p.ConfluentCommand = idk.ConfluentCommand{}
	p.KafkaBootstrapServers = []string{"localhost:9092"}
	p.SchemaRegistryURL = "http://localhost:8081"
	p.Topic = "defaulttopic"
	p.Subject = "defaultsubject"
	p.BatchSize = 1000
	p.Log = logger.NewStandardLogger(os.Stderr)
	p.ConfigMap, err = common.SetupConfluent(&p.ConfluentCommand)
	return &p, err
}

// Run sends the source records to kafka based on concurrency.
func (p *PutSource) Run() error {
	// Set up the schema registry client.
	var auth *csrc.BasicAuth
	if p.SchemaRegistryUsername != "" {
		auth = &csrc.BasicAuth{
			KafkaSchemaApiKey:    p.SchemaRegistryUsername,
			KafkaSchemaApiSecret: p.SchemaRegistryPassword,
		}
	}
	p.schemaClient = csrc.NewClient(p.SchemaRegistryURL, nil, auth)

	if p.TrackProgress {
		p.progress = &idk.ProgressTracker{}
		startTime := time.Now()
		var wg sync.WaitGroup
		defer wg.Wait()
		doneCh := make(chan struct{})
		defer func() { close(doneCh) }()
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Set up a timer to check progress every 10 seconds.
			tick := time.NewTicker(10 * time.Second)
			defer tick.Stop()

			prev := uint64(0)
			stalled := true
			for {
				progress := p.progress.Check()
				switch {
				case progress != prev:
					// Forward progress continues.
					p.Log.Printf("sourced %d records (%.2f records/minute)", progress, float64(progress)/time.Since(startTime).Minutes())
					stalled = false
				case stalled:
					// We already told the user that it is stalled.
				default:
					// This is the start of a stall.
					// No records have been sourced in the past 5 seconds.
					p.Log.Printf("record sourcing stalled")
					stalled = true
				}
				prev = progress

				select {
				case <-tick.C:
				case <-doneCh:
					// Generate a final status update.
					p.Log.Printf("sourced %d records in %s", p.progress.Check(), time.Since(startTime))
					return
				}
			}
		}()
	}

	// Create Producer instance
	pr, err := confluent.NewProducer(p.ConfigMap)
	if err != nil {
		return errors.Wrap(err, "Failed to create producer")
	}
	// Create the topic if it does not exist.
	err = CreateKafkaTopic(context.Background(), p.Topic, pr, p.NumPartitions, p.ReplicationFactor)
	pr.Close()

	if err != nil {
		return errors.Wrap(err, "creating topic")
	}

	//Make producer channel buffered
	err = p.ConfigMap.SetKey("go.produce.channel.size", 1)
	if err != nil {
		return errors.Wrap(err, "setting produce channel")
	}

	// TODO: i don't think we actually want to run these concurrently for now
	// we can just iterate over them serially.
	eg := errgroup.Group{}
	for c := 0; c < p.Concurrency; c++ {
		c := c
		eg.Go(func() error {
			// targets allowed are defined in datagen target, make sure they match
			var err error
			if p.Target == "kafka" {
				err = p.runSource(c)
			} else { // this is for target kafkastatic
				err = p.runSourceJson(c)
			}
			if err != nil && err != io.EOF {
				return err
			}
			return nil
		})
	}
	err = eg.Wait()
	if err != nil && err != io.EOF {
		return errors.Wrap(err, "confluent.PutSource.Run")
	}
	return nil
}

func convertToJson(schema []idk.Field, record []interface{}) ([]byte, error) {
	if len(schema) != len(record) {
		return []byte{}, fmt.Errorf("Length of schema %v and record %v don't match", len(schema), len(record))
	}
	mesg := make(map[string]interface{})
	for i := range schema {
		field, val := schema[i], record[i]
		if valb, ok := val.([]byte); ok {
			val = string(valb)
		}
		mesg[field.Name()] = val
	}

	return json.Marshal(mesg)
}

func (p *PutSource) getKeyFunc(schema []idk.Field) (func(vals []interface{}) ([]byte, error), error) {
	if len(p.FBPrimaryKeyFields) > 0 {
		indices := make([]int, len(p.FBPrimaryKeyFields))
		for i, fieldName := range p.FBPrimaryKeyFields {
			for j, f := range schema {
				if f.Name() == fieldName {
					indices[i] = j
					break
				}
			}
		}
		return func(vals []interface{}) ([]byte, error) {
			h := fnv.New64a()
			h.Write([]byte(p.FBIndexName))
			for pos, ind := range indices {
				valb, ok := vals[ind].([]byte)
				if !ok {
					valS, ok := vals[ind].(string)
					if !ok {
						return nil, errors.Errorf("primary key value must be string or byte slice, but got %v of %[1]T", vals[ind])
					}
					valb = []byte(valS)
				}
				h.Write(valb)
				if pos < len(indices)-1 {
					h.Write([]byte{'|'})
				}
			}
			partition := h.Sum64() % 256
			ret := make([]byte, 8)
			binary.BigEndian.PutUint64(ret, partition)
			return ret, nil
		}, nil
	} else if p.FBIDField != "" {
		return nil, errors.New("getting partition key for IDField not implemented")
	}
	return nil, nil // autogen doesn't need to set the key at all
}

func (p *PutSource) runSourceJson(c int) error {
	p.Log.Printf("start source %d", c)
	source, err := p.NewSource()
	if err != nil {
		return errors.Wrap(err, "getting source")
	}
	if p.progress != nil {
		source = p.progress.Track(source)
	}

	// Create Producer instance
	producer, err := confluent.NewProducer(p.ConfigMap)
	if err != nil {
		return errors.Wrap(err, "Failed to create producer")
	}

	finished := int32(0)
	iter := int64(0)
	doneChan := common.LaunchKafkaEventConfirmer(producer, &finished, &iter)
	defer producer.Close()
	var schema []idk.Field
	var getKeyFunc func(vals []interface{}) ([]byte, error)
	rec, err := source.Record()
	if err == nil {
		err = idk.ErrSchemaChange // always need to fetch the schema the first time
	}
	for ; ; rec, err = source.Record() {
		if err == idk.ErrFlush {
			continue
		}
		if err != nil || iter == 0 {
			if err == idk.ErrSchemaChange || iter == 0 {
				p.Log.Debugf("handle kafka schema\n")
				schema = source.Schema()
				getKeyFunc, err = p.getKeyFunc(schema)
				if err != nil && err.Error() != "getting partition key for IDField not implemented" {
					return errors.Wrap(err, "getting key func")
				}
			} else {
				break
			}
		}
		// in custom.go, we return nil when it is EOF
		if rec == nil {
			break
		}

		// map schema and records to json
		data, err := convertToJson(schema, rec.Data())
		if err != nil {
			return errors.Wrap(err, "converting record to json")
		}
		// maybe we need a message key, but don't know what to do
		p.Log.Debugf("put kafka record json: %v, %s, %s, %v\n", p.Topic, rec.Data()[0], data, err)
		var key []byte
		if getKeyFunc != nil {
			key, err = getKeyFunc(rec.Data())
			if err != nil {
				return errors.Wrap(err, "trying to get partition key")
			}
		}
		producer.ProduceChannel() <- &confluent.Message{
			TopicPartition: confluent.TopicPartition{Topic: &p.Topic, Partition: confluent.PartitionAny},
			Key:            key,
			Value:          data,
		}
		atomic.AddInt64(&iter, 1)
	}
	atomic.AddInt32(&finished, 1)
	<-doneChan
	recNotFlushed := producer.Flush(15 * 10000) // 10ms
	count := 0
	for recNotFlushed != 0 {
		recNotFlushed = producer.Flush(15 * 10000) // 10ms
		count += 1
		if count > 4 {
			p.Log.Debugf("Tried more than 4 times to flush confluent producer")
			break
		}
	}

	if err != io.EOF {
		return errors.Wrap(err, "getting record")
	}

	p.Log.Printf("Put %d generated records\n", iter)

	return nil
}

func (p *PutSource) runSource(c int) error {
	p.Log.Printf("start source %d", c)
	source, err := p.NewSource()
	if err != nil {
		return errors.Wrap(err, "getting source")
	}
	if p.progress != nil {
		source = p.progress.Track(source)
	}

	// Create Producer instance
	producer, err := confluent.NewProducer(p.ConfigMap)
	if err != nil {
		return errors.Wrap(err, "Failed to create producer")
	}
	defer producer.Close()

	finished := int32(0)
	iter := int64(0)
	doneChan := common.LaunchKafkaEventConfirmer(producer, &finished, &iter)

	var licodec *liavro.Codec
	var resp *csrc.SchemaResponse
	var schema []idk.Field
	rec, err := source.Record()
	if err == nil {
		err = idk.ErrSchemaChange // always need to fetch the schema the first time
	}
	for ; ; rec, err = source.Record() {
		if err == idk.ErrFlush {
			continue
		}
		if err != nil || iter == 0 {
			if err == idk.ErrSchemaChange || iter == 0 {
				p.Log.Debugf("handle kafka schema\n")
				schema = source.Schema()

				recordSchema, err := idkSchemaToAvroRecordSchema(schema)
				if err != nil {
					return errors.Wrap(err, "converting schema to record schema")
				}

				b, err := json.Marshal(recordSchema)
				if err != nil {
					return errors.Wrap(err, "converting record schema to json")
				}

				schemaStr := string(b)
				resp, err = p.schemaClient.PostSubjects(p.Subject, schemaStr)
				if err != nil {
					return errors.Wrap(err, "posting schema")
				}
				p.Log.Printf("Posted schema ID: %d\n", resp.ID)

				licodec, err = liavro.NewCodec(schemaStr)
				if err != nil {
					return errors.Wrap(err, "li decoding schema")
				}
			} else {
				break
			}
		}

		// Convert record to a data structure suitable for kafka put.
		recordMap, err := p.recordToMap(rec, schema)
		if err != nil {
			return errors.Wrap(err, "converting record to map")
		}
		data, err := endcodeAvro(int(resp.ID), licodec, recordMap)
		if err != nil {
			return errors.Wrap(err, "encoding record")
		}
		producer.ProduceChannel() <- &confluent.Message{
			TopicPartition: confluent.TopicPartition{Topic: &p.Topic, Partition: confluent.PartitionAny},
			//Key:            []byte(strconv.Itoa(uid)),
			Value: data,
		}
		atomic.AddInt64(&iter, 1) //increment message produced
	}
	atomic.AddInt32(&finished, 1) //signal no more messages
	<-doneChan                    //wait till kafka acknowledges

	recNotFlushed := producer.Flush(15 * 10000) // 10ms
	count := 0
	for recNotFlushed != 0 {
		recNotFlushed = producer.Flush(10000) // 10ms
		count += 1
		if count > 4 {
			p.Log.Debugf("Tried more than 4 times to flush confluent producer")
			break
		}
	}
	if err != io.EOF {
		return errors.Wrap(err, "getting record")
	}
	return nil
}

// recordToMap converts an idk.Record to a map of interfaces based on the
// provided schema.
func (p *PutSource) recordToMap(rec idk.Record, schema []idk.Field) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	data := rec.Data()

	if len(schema) != len(data) {
		return nil, errors.Errorf("number of data fields (%d) does not match schema (%d)", len(data), len(schema))
	}

	// TODO: return errors for cases where casting isn't ok
	for i, field := range schema {
		v := data[i]
		if v == nil {
			m[field.Name()] = nil
			continue
		}
		switch fld := field.(type) {
		case idk.BoolField:
			b, err := toBool(v)
			if err != nil {
				return nil, errors.Wrap(err, "converting value to bool")
			}
			m[fld.Name()] = map[string]interface{}{"boolean": b}
		case idk.DateIntField:
			if n, err := toInt64(v); err == nil {
				m[fld.Name()] = map[string]interface{}{"long": n}
			} else {
				m[fld.Name()] = map[string]interface{}{"bytes": v}
			}

		case idk.DecimalField:
			d, err := toDecimal(v, fld.Scale)
			if err != nil {
				return nil, errors.Wrap(err, "converting value to decimal")
			}
			m[fld.Name()] = map[string]interface{}{"bytes": d}
		case idk.IDArrayField:
			switch vt := v.(type) {
			case []uint64:
				int64s := make([]int64, len(vt))
				for i := range vt {
					int64s[i] = int64(vt[i])
				}
				m[fld.Name()] = map[string]interface{}{"array": int64s}
			case []int64:
				m[fld.Name()] = map[string]interface{}{"array": vt}
			}
		case idk.IDField:
			l, err := toInt64(v)
			if err != nil {
				return nil, errors.Wrap(err, "converting value to int64")
			}
			m[fld.Name()] = map[string]interface{}{"long": l}
		case idk.IntField:
			l, err := toInt64(v)
			if err != nil {
				return nil, errors.Wrap(err, "converting value to int64")
			}
			m[fld.Name()] = map[string]interface{}{"long": l}
		case idk.RecordTimeField:
			m[fld.Name()] = map[string]interface{}{"bytes": v}
		case idk.TimestampField:
			buf := make([]byte, 8)
			u := uint64(v.(time.Time).Unix())
			binary.BigEndian.PutUint64(buf, u)
			m[fld.Name()] = map[string]interface{}{"bytes": buf}
		case idk.SignedIntBoolKeyField:
			l, err := toInt64(v)
			if err != nil {
				return nil, errors.Wrap(err, "converting value to int64")
			}
			m[fld.Name()] = map[string]interface{}{"long": l}
		case idk.StringArrayField:
			m[fld.Name()] = map[string]interface{}{"array": v}
		case idk.StringField:
			s, err := toString(v)
			if err != nil {
				return nil, errors.Wrap(err, "converting value to string")
			}
			m[fld.Name()] = map[string]interface{}{"string": s}
		default:
			m[fld.Name()] = v
		}
	}

	return m, nil
}

// idkSchemaToAvroRecordSchema converts an []idk.Field to a RecordSchema.
func idkSchemaToAvroRecordSchema(fields []idk.Field) (*avro.RecordSchema, error) {
	record := &avro.RecordSchema{
		Name:      "idk_datagen",
		Namespace: "idk.datagen",
		Doc:       "idk-datagen",
		Fields:    make([]*avro.SchemaField, len(fields)),
	}

	for n, fld := range fields {
		var schemaField = &avro.SchemaField{}
		quantum := idk.QuantumOf(fld)
		var ttl time.Duration = 0
		if quantum != "" {
			ttlTemp, err := idk.TTLOf(fld)
			if err != nil {
				return nil, err
			}
			ttl = ttlTemp
		}
		ttlString := ttl.String()
		cacheConfig := idk.CacheConfigOf(fld)
		hasMutex := idk.HasMutex(fld)

		switch typ := fld.(type) {
		case idk.BoolField:
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.BooleanSchema{},
				},
			}
			schemaField.Default = nil

		case idk.DateIntField:
			props := map[string]interface{}{
				"fieldType":  "dateInt", // TODO: namespace molecula custom properties?
				"epoch":      typ.Epoch.Format(typ.Layout),
				"unit":       typ.Unit,
				"customUnit": typ.CustomUnit,
				"layout":     typ.Layout,
			}
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.BytesSchema{Properties: props},
					&avro.LongSchema{Properties: props},
				},
			}
			schemaField.Default = nil

		case idk.TimestampField:
			props := map[string]interface{}{
				"fieldType":   "timestamp",
				"granularity": typ.Granularity,
				"layout":      typ.Layout,
				"epoch":       typ.Epoch.Format(typ.Layout),
				"unit":        typ.Unit,
			}
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.BytesSchema{Properties: props},
				},
			}
			schemaField.Default = nil

		case idk.DecimalField:
			props := map[string]interface{}{
				"fieldType": "decimal",
				"scale":     typ.Scale,
				"precision": idk.DecimalPrecision,
			}
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.BytesSchema{Properties: props},
				},
			}
			schemaField.Default = nil

		case idk.IDArrayField:
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.ArraySchema{
						Items: &avro.LongSchema{
							Properties: map[string]interface{}{
								"quantum":   quantum,
								"ttl":       ttlString,
								"cacheType": string(cacheConfig.CacheType),
								"cacheSize": strconv.Itoa(cacheConfig.CacheSize),
							},
						},
					},
				},
			}
			schemaField.Default = nil

		case idk.IDField:
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.LongSchema{
						Properties: map[string]interface{}{
							"fieldType": "id",
							"mutex":     hasMutex,
							"quantum":   quantum,
							"ttl":       ttlString,
							"cacheType": string(cacheConfig.CacheType),
							"cacheSize": strconv.Itoa(cacheConfig.CacheSize),
						},
					},
				},
			}
			schemaField.Default = nil

		case idk.IgnoreField:
			continue

		case idk.IntField:
			props := map[string]interface{}{
				"fieldType": "int",
			}

			// JSON decodes numeric values into float64,
			// so reading them back from kafka gives us float64 value,
			// which after convertion overflows int64.
			// That's why we convert Min and Max properties to strings,
			// so while unmarshaling (func intProp(p propper, s string)),
			// we can parse it as int value.
			if typ.Min != nil {
				props["min"] = strconv.FormatInt(*typ.Min, 10)
			}
			if typ.Max != nil {
				props["max"] = strconv.FormatInt(*typ.Max, 10)
			}
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.LongSchema{Properties: props},
				},
			}
			schemaField.Default = nil

		case idk.RecordTimeField:
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.BytesSchema{
						Properties: map[string]interface{}{
							"fieldType": "recordTime",
							"layout":    typ.Layout,
						},
					},
				},
			}
			schemaField.Default = nil

		case idk.SignedIntBoolKeyField:
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.LongSchema{
						Properties: map[string]interface{}{
							"fieldType": "signedIntBoolKey",
						},
					},
				},
			}
			schemaField.Default = nil

		case idk.StringArrayField:
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.ArraySchema{
						Items: &avro.StringSchema{
							Properties: map[string]interface{}{
								"quantum":   quantum,
								"ttl":       ttlString,
								"cacheType": string(cacheConfig.CacheType),
								"cacheSize": strconv.Itoa(cacheConfig.CacheSize),
							},
						},
					},
				},
			}
			schemaField.Default = nil

		case idk.StringField:
			schemaField.Type = &avro.UnionSchema{
				Types: []avro.Schema{
					&avro.NullSchema{},
					&avro.StringSchema{
						Properties: map[string]interface{}{
							"mutex":     hasMutex,
							"quantum":   quantum,
							"ttl":       ttlString,
							"cacheType": string(cacheConfig.CacheType),
							"cacheSize": strconv.Itoa(cacheConfig.CacheSize),
						},
					},
				},
			}
			schemaField.Default = nil

		default:
			return nil, errors.Errorf("unsupported idk fieldtype %T for schema", typ)
		}
		schemaField.Name = fields[n].Name()
		record.Fields[n] = schemaField
	}

	return record, nil
}

// helper functions below were all copied from idk/interfaces.go
func toInt64(val interface{}) (int64, error) {
	switch vt := val.(type) {
	case uint:
		return int64(vt), nil
	case uint8:
		return int64(vt), nil
	case uint16:
		return int64(vt), nil
	case uint32:
		return int64(vt), nil
	case uint64:
		return int64(vt), nil
	case int:
		return int64(vt), nil
	case int8:
		return int64(vt), nil
	case int16:
		return int64(vt), nil
	case int32:
		return int64(vt), nil
	case int64:
		return vt, nil
	case string: // added this case because of mysql driver sending the ids as strings
		v, err := strconv.ParseInt(strings.TrimSpace(vt), 10, 64)
		if err != nil {
			return 0, err
		}
		return v, nil
	default:
		return 0, errors.Errorf("couldn't convert %v of %[1]T to int64", vt)
	}
}

func toBool(val interface{}) (bool, error) {
	switch vt := val.(type) {
	case bool:
		return vt, nil
	case byte:
		if vt == '0' || vt == 'f' || vt == 'F' {
			return false, nil
		}
		return vt != 0, nil
	case string:
		switch strings.ToLower(vt) {
		case "", "0", "f", "false":
			return false, nil
		}
		return true, nil
	default:
		if vint, err := toInt64(val); err == nil {
			return vint != 0, nil
		}
		return false, errors.Errorf("couldn't convert %v of %[1]T to bool", vt)
	}
}

func toString(val interface{}) (string, error) {
	switch vt := val.(type) {
	case string:
		return vt, nil
	case []byte:
		return string(vt), nil
	default:
		if vt == nil {
			return "", nil
		}
		return fmt.Sprintf("%v", val), nil
	}
}

func toDecimal(val interface{}, scale int64) ([]byte, error) {
	var value uint64

	switch vt := val.(type) {
	case pql.Decimal:
		v := vt.Value()
		value, scale = uint64(v.Int64()), vt.Scale
	case float32:
		value = uint64(vt) * uint64(pql.Pow10(scale))
	case float64:
		value = uint64(vt) * uint64(pql.Pow10(scale))
	default:
		return nil, errors.Errorf("couldn't convert %v of %[1]T to decimal", vt)
	}

	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], value)
	binary.BigEndian.PutUint64(b[8:16], uint64(scale))
	return b, nil
}
