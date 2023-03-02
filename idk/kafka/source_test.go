//go:build !kafka_sasl
// +build !kafka_sasl

package kafka

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/common"
	"github.com/featurebasedb/featurebase/v3/idk/kafka/csrc"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/glycerine/vprint"
	"github.com/go-avro/avro"
	liavro "github.com/linkedin/goavro/v2"
)

func configureSourceTestFlags(source *Source) {
	source.KafkaBootstrapServers = []string{kafkaHost}
	source.SchemaRegistryURL = registryHost
}

func TestAvroToPDKSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		schemaFile string
		exp        []idk.Field
		expErr     string
	}{
		{
			schemaFile: "simple.json",
			exp:        expectedSchemas["simple.json"],
		},
		{
			schemaFile: "stringtypes.json",
			exp:        expectedSchemas["stringtypes.json"],
		},
		{
			schemaFile: "decimal.json",
			exp:        expectedSchemas["decimal.json"],
		},
		{
			schemaFile: "othertypes.json",
			exp:        expectedSchemas["othertypes.json"],
		},
		{
			schemaFile: "unions.json",
			exp:        expectedSchemas["unions.json"],
		},
		{
			schemaFile: "floatscale.json",
			exp:        expectedSchemas["floatscale.json"],
		},
		{
			schemaFile: "notarecord.json",
			expErr:     "unsupported Avro Schema type",
		},
		{
			schemaFile: "fieldisrecord.json",
			expErr:     "nested fields are not currently supported",
		},
		{
			schemaFile: "timestamp.json",
			exp:        expectedSchemas["timestamp.json"],
		},
	}

	// check that we've covered all the test schemas
	files, err := os.ReadDir("./testdata/schemas")
	if err != nil {
		t.Fatalf("reading directory: %v", err)
	}
	if len(files) != len(tests)+9 { // +9 because we aren't testing bigschema.json, the five delete ones, alltypes. timeQuantums or the ID allocation one here.
		t.Errorf("have different number of schemas and tests: %d and %d\n%+v", len(files), len(tests), files)
	}

	for _, test := range tests {
		test := test
		t.Run(test.schemaFile, func(t *testing.T) {
			t.Parallel()

			codec := decodeTestSchema(t, test.schemaFile)
			schema, err := avroToPDKSchema(codec)
			if err != nil && test.expErr == "" {
				t.Fatalf("unexpected error: %v", err)
			}
			if test.expErr != "" && err == nil {
				t.Fatalf("expected error")
			}
			if test.expErr != "" && !strings.Contains(err.Error(), test.expErr) {
				t.Fatalf("error expected/got\n%s\n%v", test.expErr, err.Error())
			}
			if !reflect.DeepEqual(test.exp, schema) {
				t.Fatalf("schema exp/got\n%+v\n%+v", test.exp, schema)
			}
		})
	}
}

func TestAvroToPDKField(t *testing.T) {
	tests := []struct {
		name        string
		schemaField *avro.SchemaField
		expField    idk.Field
		expErr      string
	}{
		{
			name: "string ttl",
			schemaField: &avro.SchemaField{
				Name: "string-ttl",
				Type: &avro.StringSchema{},
				Properties: map[string]interface{}{
					"ttl":     "30s",
					"quantum": "YMD",
				},
			},
			expField: idk.StringField{NameVal: "string-ttl", Quantum: "YMD", TTL: "30s"},
			expErr:   "nil",
		},
		{
			name: "string ttl without quantum",
			schemaField: &avro.SchemaField{
				Name: "string-ttl",
				Type: &avro.StringSchema{},
				Properties: map[string]interface{}{
					"ttl": "30s",
				},
			},
			expField: idk.StringField{NameVal: "string-ttl"},
			expErr:   "nil",
		},
		{
			name: "bytes ttl",
			schemaField: &avro.SchemaField{
				Name: "bytes-ttl",
				Type: &avro.BytesSchema{},
				Properties: map[string]interface{}{
					"ttl":     "30s",
					"quantum": "YMD",
				},
			},
			expField: idk.StringField{NameVal: "bytes-ttl", Quantum: "YMD", TTL: "30s"},
			expErr:   "nil",
		},
		{
			name: "bytes ttl without quantum",
			schemaField: &avro.SchemaField{
				Name: "bytes-ttl",
				Type: &avro.BytesSchema{},
				Properties: map[string]interface{}{
					"ttl": "30s",
				},
			},
			expField: idk.StringField{NameVal: "bytes-ttl"},
			expErr:   "nil",
		},
		{
			name: "string array ttl",
			schemaField: &avro.SchemaField{
				Name: "array-ttl",
				Type: &avro.ArraySchema{
					Items: &avro.StringSchema{
						Properties: map[string]interface{}{
							"quantum": "YMD",
							"ttl":     "60s",
						},
					},
				},
			},
			expField: idk.StringArrayField{NameVal: "array-ttl", Quantum: "YMD", TTL: "60s"},
			expErr:   "nil",
		},
		{
			name: "string array ttl without quantum",
			schemaField: &avro.SchemaField{
				Name: "array-ttl",
				Type: &avro.ArraySchema{
					Items: &avro.StringSchema{
						Properties: map[string]interface{}{
							"ttl": "60s",
						},
					},
				},
			},
			expField: idk.StringArrayField{NameVal: "array-ttl"},
			expErr:   "nil",
		},
		{
			name: "Int ttl",
			schemaField: &avro.SchemaField{
				Name: "int-ttl",
				Type: &avro.IntSchema{},
				Properties: map[string]interface{}{
					"fieldType": "id",
					"ttl":       "30s",
					"quantum":   "YMD",
				},
			},
			expField: idk.IDField{NameVal: "int-ttl", Quantum: "YMD", TTL: "30s"},
			expErr:   "nil",
		},
		{
			name: "Int ttl without quantum",
			schemaField: &avro.SchemaField{
				Name: "int-ttl",
				Type: &avro.IntSchema{},
				Properties: map[string]interface{}{
					"fieldType": "id",
					"ttl":       "30s",
				},
			},
			expField: idk.IDField{NameVal: "int-ttl"},
			expErr:   "nil",
		},
		{
			name: "recordTime",
			schemaField: &avro.SchemaField{
				Name: "record-time",
				Type: &avro.BytesSchema{},
				Properties: map[string]interface{}{
					"layout":    "2006-01-02 15:04:05",
					"fieldType": "recordTime",
				},
			},
			expField: idk.RecordTimeField{NameVal: "record-time", Layout: "2006-01-02 15:04:05"},
			expErr:   "nil",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field, err := avroToPDKField(test.schemaField)
			if !reflect.DeepEqual(field, test.expField) {
				t.Errorf("expected field: '%v', got: '%v'", test.expField, field)
			}
			if err != nil && !strings.Contains(err.Error(), test.expErr) {
				t.Errorf("expected error: '%s', got: '%s'", test.expErr, err.Error())
			}
		})
	}
}

func decodeTestSchema(t *testing.T, filename string) avro.Schema {
	codec, err := avro.ParseSchema(readTestSchema(t, filename))
	if err != nil {
		t.Fatalf("parsing schema: %v", err)
	}
	return codec
}

func readTestSchema(t *testing.T, filename string) string {
	bytes, err := os.ReadFile("./testdata/schemas/" + filename)
	if err != nil {
		t.Fatalf("reading schema file: %v", err)
	}
	return string(bytes)
}

func liDecodeTestSchema(t *testing.T, filename string) *liavro.Codec {
	codec, err := liavro.NewCodec(readTestSchema(t, filename))
	if err != nil {
		t.Fatalf("li parsing schema: %v", err)
	}
	return codec
}

var tests = []struct {
	data       []map[string]interface{}
	schemaFile string
	exp        [][]interface{}
}{
	{
		schemaFile: "simple.json",
		data:       []map[string]interface{}{{"first": "hello", "last": "goodbye"}, {"first": "one", "last": "two"}},
		exp:        [][]interface{}{{"hello", "goodbye"}, {"one", "two"}},
	},
	{
		schemaFile: "stringtypes.json",
		data:       []map[string]interface{}{{"first": "blah", "last": "goodbye", "middle": "123456789"}},
		exp:        [][]interface{}{{"blah", []byte("goodbye"), []byte("123456789")}},
	},
	{
		schemaFile: "decimal.json",
		data:       []map[string]interface{}{{"somenum": &big.Rat{}}, {"somenum": big.NewRat(10, 1)}, {"somenum": big.NewRat(1, 1)}, {"somenum": big.NewRat(5, 2)}, {"somenum": big.NewRat(1234567890, 1)}},
		exp:        [][]interface{}{{[]byte{0}}, {[]byte{0x3, 0xE8}}, {[]byte{100}}, {[]byte{0, 250}}, {[]byte{0x1C, 0xBE, 0x99, 0x1A, 0x08}}},
	},
	{
		schemaFile: "othertypes.json",
		data:       []map[string]interface{}{{"first": "a", "second": []string{"b", "c"}, "third": -8, "fourth": 99, "fifth": 99.9, "sixth": 101.1, "seventh": true, "eighth": map[string]interface{}{"string": "a"}, "ninth": map[string]interface{}{"string": "b"}}},
		exp:        [][]interface{}{{"a", []interface{}{"b", "c"}, int32(-8), int64(99), float32(99.9), float64(101.1), true, "a", "b"}},
	},
	{
		schemaFile: "unions.json",
		data: []map[string]interface{}{
			{"first": map[string]interface{}{"string": "a"}, "second": map[string]interface{}{"boolean": true}, "third": map[string]interface{}{"long": 101}, "fourth": map[string]interface{}{"bytes.decimal": big.NewRat(5, 2)}, "fifth": map[string]interface{}{"double": float64(9.4921)}},
			{"first": nil, "second": nil, "third": map[string]interface{}{"null": nil}, "fourth": nil, "fifth": nil},
		},
		exp: [][]interface{}{
			{"a", true, int64(101), []byte{9, 196}, float64(9.4921)},
			{nil, nil, nil, nil, nil},
		},
	},
	{
		schemaFile: "floatscale.json",
		data:       []map[string]interface{}{{"first": 23.12345}},
		exp:        [][]interface{}{{float32(23.12345)}},
	},
	{
		schemaFile: "timestamp.json",
		data:       []map[string]interface{}{{"time_samir": []byte("5000")}},
		exp:        [][]interface{}{{[]byte("5000")}},
	},
}

func TestKafkaSourceLocal(t *testing.T) {
	t.Parallel()

	// this is not an integration test, so we'll take steps to avoid
	// actually connecting to Kafka or Schema Registry.

	for i, test := range tests {
		i, test := i, test
		t.Run(test.schemaFile, func(t *testing.T) {
			t.Parallel()

			schema := liDecodeTestSchema(t, test.schemaFile)

			src := NewSource()
			defer src.Close()
			configureSourceTestFlags(src)
			cfg, err := common.SetupConfluent(&src.ConfluentCommand)
			if err != nil {
				t.Fatal(err)
			}
			src.ConfigMap = cfg
			// note: we will not call Open on the source which would connect
			// to Kafka. Instead, we'll set the reader manually so we
			// can inject messages.
			go func() {
				topic := string("test")
				for j, record := range test.data {
					buf := make([]byte, 5, 1000)
					buf[0] = 0
					binary.BigEndian.PutUint32(buf[1:], uint32(i))
					buf, err := schema.BinaryFromNative(buf, record)
					if err != nil {
						t.Errorf("encoding:\n%+v\nerr: %v", record, err)
					}
					e := &confluent.Message{
						TopicPartition: confluent.TopicPartition{
							Topic:     &topic,
							Partition: 0,
							Offset:    confluent.Offset(j),
						},
						Timestamp: time.Now(),
						Value:     buf,
					}

					src.recordChannel <- recordWithError{Record: e}
				}
				close(src.recordChannel)
			}()

			// prefill the schema cache so the registry isn't contacted.
			src.cache[int32(i)] = decodeTestSchema(t, test.schemaFile)
			for j, expect := range test.exp {
				pdkRec, err := src.Record()
				if j == 0 {
					if err != idk.ErrSchemaChange {
						t.Errorf("expected schema changed signal, got: %v", err)
					}
					gotSchema := src.Schema()
					if !reflect.DeepEqual(gotSchema, expectedSchemas[test.schemaFile]) {
						t.Errorf("unexpected schema exp/got:\n%+v\n%+v", expectedSchemas[test.schemaFile], gotSchema)
					}
				} else if err != nil {
					t.Fatalf("unexpected error getting record: %v", err)
				}
				if pdkRec == nil {
					t.Fatalf("should have a record")
				}
				data := pdkRec.Data()
				if !reflect.DeepEqual(data, expect) {
					t.Errorf("data mismatch exp/got:\n%+v\n%+v", expect, data)
					if len(data) != len(expect) {
						t.Fatalf("mismatched lengths exp/got %d/%d", len(expect), len(data))
					}
					for k, exp := range expect {
						got := data[k]
						if !reflect.DeepEqual(exp, got) {
							t.Errorf("Mismatch at %d, exp/got\n%v of %[2]T\n%v of %[3]T", k, exp, got)
						}
					}
				}
			}

			err = src.Close()
			if err != nil {
				t.Fatalf("failed to close source: %v", err)
			}
		})
	}
}

// TestKafkaSourceSchemaChangeCommitRegresion is a regression test for a bug where a failure during a schema change could cause a record to be lost.
func TestKafkaSourceSchemaChangeCommitRegression(t *testing.T) {
	t.Parallel()

	src := NewSource()
	defer src.Close()
	configureSourceTestFlags(src)
	cfg, err := common.SetupConfluent(&src.ConfluentCommand)
	if err != nil {
		t.Fatal(err)
	}
	src.ConfigMap = cfg

	// prefill the schema cache so the registry isn't contacted.
	t1, t2 := tests[0], tests[1]
	schema1 := liDecodeTestSchema(t, t1.schemaFile)
	schema2 := liDecodeTestSchema(t, t2.schemaFile)
	schemaID1, schemaID2 := int32(0), int32(1)
	src.cache[schemaID1] = decodeTestSchema(t, t1.schemaFile)
	src.cache[schemaID2] = decodeTestSchema(t, t2.schemaFile)

	// synthesize fake Kakfa messages
	data1, err := endcodeAvro(int(schemaID1), schema1, t1.data[0])
	if err != nil {
		t.Fatalf("encoding record: %v", err)
	}
	data2, err := endcodeAvro(int(schemaID2), schema2, t2.data[0])
	if err != nil {
		t.Fatalf("encoding record: %v", err)
	}

	go func() {
		topic := "xyzzy"
		src.recordChannel <- recordWithError{
			Record: &confluent.Message{
				TopicPartition: confluent.TopicPartition{
					Topic:     &topic,
					Partition: 0,
					Offset:    confluent.Offset(0),
				},
				Timestamp: time.Now(),
				Value:     data1,
			},
		}
		src.recordChannel <- recordWithError{
			Record: &confluent.Message{
				TopicPartition: confluent.TopicPartition{
					Topic:     &topic,
					Partition: 0,
					Offset:    confluent.Offset(1),
				},
				Timestamp: time.Now(),
				Value:     data2,
			},
		}
		close(src.recordChannel)
	}()

	rec1, err := src.Record()
	if err != idk.ErrSchemaChange {
		t.Fatalf("expected schema change but got: %v", err)
	}
	if rec1 == nil {
		t.Fatal("first record is missing")
	}
	rec2, err := src.Record()
	if err != idk.ErrSchemaChange {
		t.Fatalf("expected schema change but got: %v", err)
	}
	if rec2 == nil {
		t.Fatal("second record is missing")
	}
}

func TestKafkaSourceTimeout(t *testing.T) {
	t.Parallel()

	src := NewSource()
	defer src.Close()
	cfg, err := common.SetupConfluent(&src.ConfluentCommand)
	if err != nil {
		t.Fatal(err)
	}

	src.Timeout = time.Millisecond
	src.KafkaSocketTimeoutMs = 10 // lowest value you can set librdkafka

	src.ConfigMap = cfg

	schema := liDecodeTestSchema(t, "simple.json")

	// prefill the schema cache so the registry isn't contacted.
	schemaID := int32(0)
	src.cache[schemaID] = decodeTestSchema(t, "simple.json")

	// synthesize fake Kakfa message
	record := map[string]interface{}{"first": "hello", "last": "goodbye"}
	buf := make([]byte, 5, 1000)
	buf[0] = 0
	binary.BigEndian.PutUint32(buf[1:], uint32(schemaID))
	buf, err = schema.BinaryFromNative(buf, record)
	if err != nil {
		t.Errorf("encoding:\n%+v\nerr: %v", record, err)
	}

	go func() {
		topic := "test"
		src.recordChannel <- recordWithError{
			Record: &confluent.Message{
				TopicPartition: confluent.TopicPartition{
					Topic: &topic,
				},
				Value: buf,
			},
		}
	}()

	// ensure we can get a message if one is available
	if rec, err := src.Record(); err != nil && err != idk.ErrSchemaChange {
		t.Fatalf("expected record, but got error %v", err)
	} else if rec.Data()[0].(string) != "hello" || rec.Data()[1].(string) != "goodbye" {
		t.Fatalf("unexpected record: %v", rec.Data())
	}

	// ensure timeout works and we get ErrFlush when no message is available
	start := time.Now()
	if rec, err := src.Record(); err != idk.ErrFlush {
		t.Fatalf("expected Flush, but got %+v err: %v", rec, err)
	} else if dur := time.Since(start); dur < time.Millisecond {
		t.Fatalf("expected to wait at least 1ms before getting Flush, but only waited %v", dur)
	}
	close(src.recordChannel)
}

// TestKafkaSource uses a real Kafka and Schema Registry. I downloaded
// the tar archive of the Confluent Platform (self managed software)
// from confluent.io/download (I got version 5.3.1). I ran `tar xzf`
// on the file, changed into the directory, ran `curl -L
// https://cnfl.io/cli | sh -s -- -b /Users/jaffee/bin` (that
// directory is on my PATH), then ran `confluent local start
// schema-registry`.
//
// I find that this test runs much faster after a `confluent local
// destroy` followed by `confluent local start schema-registry`. The
// difference is stark—10s of seconds—and I don't know why this should
// be, but I think it has something to do with kafka rebalancing
// itself when a new client joins.
func TestKafkaSourceIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	src := NewSource()
	defer src.Close()
	configureSourceTestFlags(src)
	cfg, err := common.SetupConfluent(&src.ConfluentCommand)
	if err != nil {
		t.Fatal(err)
	}
	src.ConfigMap = cfg

	src.Topics = []string{"testKafkaSourceIntegration"}
	src.Group = "group0"
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Create Producer instance
	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers": kafkaHost,
	})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()
	tCreateTopic(t, src.Topics[0], p)
	/*	"sasl.mechanisms":   conf["sasl.mechanisms"],
		"security.protocol": conf["security.protocol"],
		"sasl.username":     conf["sasl.username"],
		"sasl.password":     conf["sasl.password"]})*/
	err = src.Open()
	if err != nil {
		t.Fatalf("Failed to open src:%v", err)
	}

	key := fmt.Sprintf("%d", rnd.Int())
	for i, test := range tests {
		schemaID := postSchema(t, test.schemaFile, fmt.Sprintf("schema%d", i), registryHost, nil)
		schema := liDecodeTestSchema(t, test.schemaFile)
		t.Run(test.schemaFile, func(t *testing.T) {
			for j, record := range test.data {
				tPutRecordsKafka(t, p, src.Topics[0], schemaID, schema, key, record)
				pdkRec, err := src.Record()
				if j == 0 {
					if err != idk.ErrSchemaChange {
						t.Errorf("expected schema changed signal, got: %v", err)
					}
					gotSchema := src.Schema()
					if !reflect.DeepEqual(gotSchema, expectedSchemas[test.schemaFile]) {
						t.Errorf("unexpected schema got/exp:\n%+v\n%+v", gotSchema, expectedSchemas[test.schemaFile])
					}
				} else if err != nil {
					t.Fatalf("unexpected error getting record: %v", err)
				}
				if pdkRec == nil {
					t.Fatalf("should have a record")
				}
				data := pdkRec.Data()
				if !reflect.DeepEqual(data, test.exp[j]) {
					t.Errorf("data mismatch exp/got:\n%+v\n%+v", test.exp[j], data)
					if len(data) != len(test.exp[j]) {
						t.Fatalf("mismatched lengths exp/got %d/%d", len(test.exp[j]), len(data))
					}
					for k := range test.exp[j] {
						if !reflect.DeepEqual(test.exp[j][k], data[k]) {
							t.Errorf("Mismatch at %d, exp/got\n%v of %[2]T\n%v of %[3]T", k, test.exp[j][k], data[k])
						}
					}

				}
			}
		})
	}
}

func mustNewProducer(t *testing.T, kafkaHost string) *confluent.Producer {
	t.Helper()
	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers": kafkaHost,
	})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	return p
}

func TestKafkaSourceSkipOld(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	src := NewSource()
	defer src.Close()
	configureSourceTestFlags(src)
	src.Topics = []string{"testKafkaSourceSkipOld"}
	src.Group = "group1"
	src.SkipOld = true
	src.Timeout = 10 * time.Second
	src.Verbose = true
	src.KafkaSocketTimeoutMs = int(src.Timeout / 2 / time.Millisecond)
	src.Log = logger.NewVerboseLogger(os.Stdout)

	// Create Producer instance
	p := mustNewProducer(t, kafkaHost)
	defer p.Close()
	tCreateTopic(t, src.Topics[0], p)

	schemaID := postSchema(t, "simple.json", "skipOldSchema", registryHost, nil)
	schema := liDecodeTestSchema(t, "simple.json")

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	key := fmt.Sprintf("%d", rnd.Int())

	// 1. put a record into kafka
	tPutRecordsKafka(t, p, src.Topics[0], schemaID, schema, key, map[string]interface{}{"first": "hello", "last": "goodbye"})

	// src.Open... this starts actually reading from Kafka asynchronously. With skipOld, it should not see the record.
	if err := src.Open(); err != nil {
		t.Fatalf("opening source: %v", err)
	}

	// grab the record which has been read from Kafka by the goroutine started in src.Open.... or timeout (we're expecting timeout)
	if rec, err := src.Record(); err == nil {
		t.Fatalf("expected error, got record: %v", rec)
	} else if err != idk.ErrFlush {
		t.Fatalf("expected flush error, got %v", err)
	}

	// put another record into Kafka
	tPutRecordsKafka(t, p, src.Topics[0], schemaID, schema, key, map[string]interface{}{"first": "a", "last": "b"})

	// grab that record or timeout (we're expecting to get it this
	// time)... but we've seen weird issues where if src.Timeout is
	// too low (like 5s) we don't see this record (on confluent 1.4.2)
	rec, err := src.Record()
	switch err {
	case nil:
		t.Fatal("expected schema change")
	case idk.ErrSchemaChange:
	default:
		t.Fatalf("expected schema change, got %v", err)
	}
	expect := []interface{}{"a", "b"}
	if got := rec.Data(); !reflect.DeepEqual(got, []interface{}{"a", "b"}) {
		t.Fatalf("expected %v but got %v", expect, got)
	}
}

func TestKafkaSourceNotAutoCommitting(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip()
	}
	rand.Seed(time.Now().UnixNano())
	src := NewSource()
	defer src.Close()
	configureSourceTestFlags(src)
	src.Topics = []string{"testKafkaSourceNotAutoCommit"}
	src.Group = fmt.Sprintf("group%d", rand.Int())

	// Create Producer instance
	p := mustNewProducer(t, kafkaHost)
	defer p.Close()
	tCreateTopic(t, src.Topics[0], p)

	schemaID := postSchema(t, "simple.json", "skipOldSchema", registryHost, nil)
	schema := liDecodeTestSchema(t, "simple.json")

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	key := fmt.Sprintf("%d", rnd.Int())

	if err := src.Open(); err != nil {
		t.Fatalf("opening source: %v", err)
	}
	partition := int32(1)

	// 1. put records into Kafka
	numRecords := int64(7)
	for i := int64(0); i < numRecords; i++ {
		tPutRecordsKafkaPartition(t, p, src.Topics[0], schemaID, schema, key, partition, map[string]interface{}{"first": fmt.Sprintf("%d", i), "last": fmt.Sprintf("%d", i+1)})
	}

	// 2. read records from Kafka (but don't commit!)
	var rec idk.Record
	var err error
	for i := 0; i < 7; i++ {
		if rec, err = src.Record(); err != nil && err != idk.ErrSchemaChange {
			t.Fatalf("expected schema change or no error, got: %v", err)
		}
	}

	// 3. wait for it...
	time.Sleep(time.Second * 6) // default autocommit interval is 5s, so we have to wait if we're going to catch that

	// 4. check to see if any offsets got committed. If
	// enable.auto.commit somehow gets set to true (which is the
	// default), then this should fail.
	if offsets, err := src.client.Committed([]confluent.TopicPartition{{Topic: &src.Topics[0], Partition: partition}}, 100); err != nil {
		t.Fatalf("getting committed offsets: %v", err)
	} else if len(offsets) != 1 {
		t.Fatalf("unexpected number of offsets in response: %v", offsets)
	} else if off := offsets[0]; off.Offset > 0 {
		t.Fatalf("initial committed offset is greater than 0: %d", off.Offset)
	}

	// 5. now commit
	err = rec.Commit(context.Background())
	if err != nil {
		t.Fatalf("committing: %v", err)
	}

	// 6. now verify commit worked
	if offsets, err := src.client.Committed([]confluent.TopicPartition{{Topic: &src.Topics[0], Partition: partition}}, 100); err != nil {
		t.Fatalf("getting committed offsets: %v", err)
	} else if len(offsets) != 1 {
		t.Fatalf("unexpected number of offsets in response: %v", offsets)
	} else if off := offsets[0]; int64(off.Offset) != numRecords {
		t.Fatalf("after commit, offset is not %d: %d", numRecords-1, off.Offset)
	}
}

func TestRegistryURLParsing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		registryURL      string
		expectedCodecURL string
	}{
		{
			name:             "host:port",
			registryURL:      "localhost:8081",
			expectedCodecURL: "http://localhost:8081/schemas/ids/1",
		},
		{
			name:             "host",
			registryURL:      "localhost",
			expectedCodecURL: "http://localhost/schemas/ids/1",
		},
		{
			name:             "scheme:host:port",
			registryURL:      "http://localhost:8081",
			expectedCodecURL: "http://localhost:8081/schemas/ids/1",
		},
		{
			name:             "scheme:host",
			registryURL:      "http://localhost",
			expectedCodecURL: "http://localhost/schemas/ids/1",
		},
		{
			name:             "scheme:host",
			registryURL:      "http://localhost",
			expectedCodecURL: "http://localhost/schemas/ids/1",
		},
		{
			name:             "scheme:host/path",
			registryURL:      "http://subsub.subdomain.domain/kafka-schema-registry",
			expectedCodecURL: "http://subsub.subdomain.domain/kafka-schema-registry/schemas/ids/1",
		},
		{
			name:             "scheme:host:port/path",
			registryURL:      "http://subsub.subdomain.domain:8081/kafka-schema-registry",
			expectedCodecURL: "http://subsub.subdomain.domain:8081/kafka-schema-registry/schemas/ids/1",
		},
		{
			name:             "https:host:port/path",
			registryURL:      "https://subsub.subdomain.domain:8081/kafka-schema-registry",
			expectedCodecURL: "https://subsub.subdomain.domain:8081/kafka-schema-registry/schemas/ids/1",
		},
		{
			name:             "https:host:port",
			registryURL:      "https://localhost:8081",
			expectedCodecURL: "https://localhost:8081/schemas/ids/1",
		},
	}

	for i, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%s-%d", test.name, i), func(t *testing.T) {
			t.Parallel()

			src := &Source{}
			src.SchemaRegistryURL = test.registryURL

			err := src.cleanRegistryURL()
			if err != nil {
				t.Fatalf("cleaning registry URL: %v", err)
			}

			codecURL, err := src.codecURL(1, "schemas/ids/%d")
			if err != nil {
				t.Fatalf("codec URL: %v", err)
			}
			if codecURL != test.expectedCodecURL {
				t.Errorf("codec URL exp:\n%s\ngot:\n%s", test.expectedCodecURL, codecURL)
			}
		})
	}
}

func postSchema(t *testing.T, schemaFile, subj, regURL string, tlsConfig *tls.Config) (schemaID int) {
	schemaClient := csrc.NewClient(regURL, tlsConfig, nil)
	schemaStr := readTestSchema(t, schemaFile)
	resp, err := schemaClient.PostSubjects(subj, schemaStr)
	if err != nil {
		t.Fatalf("posting schema: %v", err)
	}
	return resp.ID
}

func tCreateTopic(t *testing.T, topic string, p *confluent.Producer) {
	t.Helper()
	a, err := confluent.NewAdminClientFromProducer(p)
	if err != nil {
		t.Fatalf("Failed to create new admin client from producer: %s", err)
	}
	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create topics on cluster.
	// Set Admin options to wait up to 60s for the operation to finish on the remote cluster
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		t.Fatalf("ParseDuration(60s): %s", err)
	}
	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]confluent.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     64,
			ReplicationFactor: 1,
		}},
		// Admin options
		confluent.SetAdminOperationTimeout(maxDur))
	if err != nil {
		t.Fatalf("Admin Client request error: %v\n", err)
	}
	for _, result := range results {
		if result.Error.Code() != confluent.ErrNoError && result.Error.Code() != confluent.ErrTopicAlreadyExists {
			t.Fatalf("Failed to create topic: %v\n", result.Error)
		}
	}
	a.Close()
}

func tPutRecordsKafka(t *testing.T, p *confluent.Producer, topic string, schemaID int, schema *liavro.Codec, key string, records ...map[string]interface{}) {
	tPutRecordsKafkaPartition(t, p, topic, schemaID, schema, key, confluent.PartitionAny, records...)
}

func tPutRecordsKafkaPartition(t *testing.T, p *confluent.Producer, topic string, schemaID int, schema *liavro.Codec, key string, partition int32, records ...map[string]interface{}) {
	t.Helper()
	delivery_chan := make(chan kafka.Event, 10000)
	for _, record := range records {
		data, err := endcodeAvro(schemaID, schema, record)
		if err != nil {
			t.Fatalf("encoding record: %v", err)
		}
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
			Key:            []byte(key),
			Value:          data,
		}, delivery_chan)
		if err != nil {
			t.Fatalf("producing record: %v", err)
		}
		e := <-delivery_chan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			t.Fatalf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			vprint.VV("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}
	close(delivery_chan)
}

var expectedSchemas = map[string][]idk.Field{
	"simple.json": {
		idk.StringField{NameVal: "first"},
		idk.StringField{NameVal: "last"},
	},
	"stringtypes.json": {
		idk.StringField{NameVal: "first", Mutex: true},
		idk.StringField{NameVal: "last"},
		idk.StringField{NameVal: "middle"},
	},
	"decimal.json": {
		idk.DecimalField{NameVal: "somenum", Scale: 2},
	},
	"unions.json": {
		idk.StringField{NameVal: "first"},
		idk.BoolField{NameVal: "second"},
		idk.IntField{NameVal: "third"},
		idk.DecimalField{NameVal: "fourth", Scale: 3},
		idk.DecimalField{NameVal: "fifth", Scale: 2},
	},
	"othertypes.json": {
		idk.StringField{NameVal: "first", Mutex: true},
		idk.StringArrayField{NameVal: "second"},
		idk.IntField{NameVal: "third"},
		idk.IntField{NameVal: "fourth"},
		idk.DecimalField{NameVal: "fifth"},
		idk.DecimalField{NameVal: "sixth"},
		idk.BoolField{NameVal: "seventh"},
		idk.StringField{NameVal: "eighth", Mutex: true},
		idk.StringField{NameVal: "ninth", Mutex: true},
	},
	"floatscale.json": {
		idk.DecimalField{NameVal: "first", Scale: 4},
	},
	"timestamp.json": {
		idk.TimestampField{NameVal: "time_samir", Granularity: "s", Unit: "ms"},
	},
}

// Ensure the avro schemas produce the exptected JSON.
func TestAvroSchemaFieldToJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		schemaField *avro.SchemaField
		exp         string
	}{
		{
			schemaField: &avro.SchemaField{
				Type: &avro.BytesSchema{},
			},
			exp: `{"type":"bytes"}`,
		},
		{
			schemaField: &avro.SchemaField{
				Name: "union-null-bytes",
				Type: &avro.UnionSchema{
					Types: []avro.Schema{
						&avro.NullSchema{},
						&avro.BytesSchema{},
					},
				},
				Default: nil,
			},
			exp: `{"name":"union-null-bytes","default":null,"type":["null","bytes"]}`,
		},
		{
			schemaField: &avro.SchemaField{
				Name: "decimal",
				Type: &avro.BytesSchema{
					Properties: map[string]interface{}{
						"logicalType": "decimal",
						"scale":       2,
						"precision":   5,
					},
				},
			},
			exp: `{"name":"decimal","type":{"type":"bytes","logicalType":"decimal","scale":2,"precision":5}}`,
		},
		{
			schemaField: &avro.SchemaField{
				Name: "union-null-decimal",
				Type: &avro.UnionSchema{
					Types: []avro.Schema{
						&avro.NullSchema{},
						&avro.BytesSchema{
							Properties: map[string]interface{}{
								"logicalType": "decimal",
								"scale":       2,
								"precision":   5,
							},
						},
					},
				},
			},
			exp: `{"name":"union-null-decimal","type":["null",{"type":"bytes","logicalType":"decimal","scale":2,"precision":5}],"default":null}`,
		},
		{
			schemaField: &avro.SchemaField{
				Name: "float",
				Type: &avro.FloatSchema{},
			},
			exp: `{"name":"float","type":"float"}`,
		},
		{
			schemaField: &avro.SchemaField{
				Name: "float-props",
				Type: &avro.FloatSchema{
					Properties: map[string]interface{}{
						"scale": 2,
					},
				},
			},
			exp: `{"name":"float-props","type":{"type":"float","scale":2}}`,
		},
		{
			schemaField: &avro.SchemaField{
				Name: "union-null-float",
				Type: &avro.UnionSchema{
					Types: []avro.Schema{
						&avro.NullSchema{},
						&avro.FloatSchema{
							Properties: map[string]interface{}{
								"scale": 2,
							},
						},
					},
				},
			},
			exp: `{"name":"union-null-float","type":["null",{"type":"float","scale":2}],"default":null}`,
		},
		{
			schemaField: &avro.SchemaField{
				Name: "timestamp",
				Type: &avro.BytesSchema{
					Properties: map[string]interface{}{
						"logicalType": "timestamp",
						"granularity": "s",
						"unit":        "ms",
					},
				},
			},
			exp: `{"name":"timestamp","type":{"granularity":"s","logicalType":"timestamp","type":"bytes","unit":"ms"}}`,
		},
	}

	for i, test := range tests {
		j, err := json.Marshal(test.schemaField)
		if err != nil {
			t.Fatal(err)
		}
		if eq, err := equalJSON(string(j), test.exp); err != nil {
			t.Fatal(err)
		} else if !eq {
			t.Fatalf("test %d: \nexp: %s, \ngot: %s", i, test.exp, j)
		}
	}
}

func equalJSON(s1, s2 string) (bool, error) {
	var o1 interface{}
	var o2 interface{}

	var err error
	err = json.Unmarshal([]byte(s1), &o1)
	if err != nil {
		return false, fmt.Errorf("Error mashalling string 1 :: %s", err.Error())
	}
	err = json.Unmarshal([]byte(s2), &o2)
	if err != nil {
		return false, fmt.Errorf("Error mashalling string 2 :: %s", err.Error())
	}

	return reflect.DeepEqual(o1, o2), nil
}

func TestGetKeyFunc(t *testing.T) {
	tests := []struct {
		rec    []interface{}
		expKey uint64
	}{
		{
			rec:    []interface{}{"aaa"},
			expKey: uint64(162),
		},
		{
			rec:    []interface{}{"aaaaaaaaaaaaaaa"},
			expKey: uint64(86),
		},
	}

	p, err := NewPutSource()
	if err != nil {
		t.Fatalf("failed to return new putsource: %v", err)
	}

	p.FBPrimaryKeyFields = []string{"uuid"}
	p.FBIDField = "uuid"
	p.FBIndexName = "test"

	schema := []idk.Field{
		idk.StringField{NameVal: "uuid"},
	}

	getKeyFunc, err := p.getKeyFunc(schema)
	if err != nil {
		t.Fatalf("failed to getKeyFunc: %v", err)
	}

	for i, test := range tests {
		tc := test
		t.Run(fmt.Sprintf("test%d", i), func(t *testing.T) {
			key, err := getKeyFunc(tc.rec)
			if err != nil {
				t.Fatalf("failed to get key for record: %v", err)
			}
			keyInt := uint64(binary.BigEndian.Uint64(key))
			if tc.expKey != keyInt {
				t.Fatalf("expected %d, got %d", tc.expKey, keyInt)
			}
		})
	}
}
