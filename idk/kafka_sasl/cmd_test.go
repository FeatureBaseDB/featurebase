//go:build kafka_sasl
// +build kafka_sasl

package kafka_sasl

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	pilosaclient "github.com/molecula/featurebase/v3/client"
	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/common"
	"github.com/molecula/featurebase/v3/idk/idktest"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var pilosaHost string
var pilosaTLSHost string
var pilosaGrpcHost string
var kafkaHost string
var certPath string
var configMap *confluent.ConfigMap // configMap is used to connect to ssl port to create topics for testing
var configMapSource idk.ConfluentCommand

func init() {
	var ok bool
	if pilosaHost, ok = os.LookupEnv("IDK_TEST_PILOSA_HOST"); !ok {
		pilosaHost = "pilosa:10101"
	}
	if pilosaTLSHost, ok = os.LookupEnv("IDK_TEST_PILOSA_TLS_HOST"); !ok {
		pilosaTLSHost = "https://pilosa-tls:10111"
	}
	if pilosaGrpcHost, ok = os.LookupEnv("IDK_TEST_PILOSA_GRPC_HOST"); !ok {
		pilosaGrpcHost = "pilosa:20101"
	}
	if kafkaHost, ok = os.LookupEnv("IDK_TEST_KAFKA_HOST"); !ok {
		kafkaHost = "kafka:9094"
	}
	if certPath, ok = os.LookupEnv("IDK_TEST_CERT_PATH"); !ok {
		certPath = "/certs"
	}

	configMapSource = idk.ConfluentCommand{
		KafkaBootstrapServers: []string{kafkaHost},
		KafkaSaslUsername:     "kafkaClient1",
		KafkaSaslPassword:     "kafkaClient1pw",
		KafkaSaslMechanism:    "PLAIN",
		KafkaSecurityProtocol: "SASL_SSL",
		KafkaSslKeyPassword:   "123456",

		// for localhost test
		// SslCaLocation:          "../docker-sasl/ssl_keys/ca-cert",
		// SslCertificateLocation: "../docker-sasl/ssl_keys/client_kafkaClient_client.pem",
		// SslKeyLocation:         "../docker-sasl/ssl_keys/client_kafkaClient_client.key",

		// for circleci test
		KafkaSslCaLocation:          "/ssl_keys/ca-cert",
		KafkaSslCertificateLocation: "/ssl_keys/client_kafkaClient_client.pem",
		KafkaSslKeyLocation:         "/ssl_keys/client_kafkaClient_client.key",
	}

	configMap, _ = common.SetupConfluent(&configMapSource)

}

func configureTestFlags(main *Main) {
	main.PilosaHosts = []string{pilosaHost}
	main.PilosaGRPCHosts = []string{pilosaGrpcHost}
	main.ConfluentCommand = configMapSource
	main.KafkaBootstrapServers = []string{kafkaHost}
	// intentionally low timeout — if this gets triggered it shouldn't
	// have any negative effects
	main.Timeout = time.Millisecond * 20
	main.Stats = ""
	_, main.Verbose = os.LookupEnv("IDK_TEST_VERBOSE")
}

func tCreateConfluentAdmin(t *testing.T) *confluent.AdminClient {
	t.Helper()
	adminClient, err := confluent.NewAdminClient(configMap)

	if err != nil {
		t.Fatalf("Failed to create Admin client: %s\n", err)
	}
	return adminClient
}

func tCreateConfluentTopic(t *testing.T, adminClient *confluent.AdminClient, topic string) {
	t.Helper()

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create topics on cluster.
	// Set Admin options to wait for the operation to finish (or at most 60s)
	maxDuration, err := time.ParseDuration("60s")
	if err != nil {
		t.Fatalf("time.ParseDuration(60s)")
	}

	_, err = adminClient.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]confluent.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     64,
			ReplicationFactor: 1}},
		// Admin options
		confluent.SetAdminOperationTimeout(maxDuration))

	if err != nil {
		t.Fatalf("Problem during the topic creation: %v\n", err)
	}
}

func tCreateConfluentProducer(t *testing.T) *confluent.Producer {
	t.Helper()
	producer, err := confluent.NewProducer(configMap)

	if err != nil {
		t.Fatalf("Failed to create producer: %s\n", err)
	}
	return producer
}
func tWriteConfluentMessage(t *testing.T, producer *confluent.Producer, topic string, key string, value string) {
	t.Helper()

	err := producer.Produce(
		&confluent.Message{
			TopicPartition: confluent.TopicPartition{
				Topic:     &topic,
				Offset:    confluent.OffsetEnd,
				Partition: confluent.PartitionAny},
			Key:   []byte(key),
			Value: []byte(value)}, nil)

	if err != nil {
		t.Fatalf("err %v", err)
	}
	// Wait for delivery report
	e := <-producer.Events()

	message := e.(*confluent.Message)
	if message.TopicPartition.Error != nil {
		t.Fatalf("failed to deliver message: %v\n",
			message.TopicPartition)
	} else {
		t.Logf("%s:%s delivered to topic %s [%d] at offset %v\n",
			key,
			value,
			*message.TopicPartition.Topic,
			message.TopicPartition.Partition,
			message.TopicPartition.Offset)
	}
}

func makeRecordString(t *testing.T, fields []string, vals []interface{}) string {
	if len(fields) != len(vals) {
		t.Fatalf("have %d fields and %d vals", len(fields), len(vals))
	}
	rec := make(map[string]interface{})
	for i, field := range fields {
		rec[field] = vals[i]
	}
	ret, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("error marshaling record to json")
	}
	return string(ret)
}

func TestSaslFieldTypes(t *testing.T) {
	t.Parallel()

	fieldNames := []string{"i", "d", "t", "@s", "@st", "unixtime", "e"}
	records := [][]interface{}{
		{100, 34.0404, "2021-02-01", "apple", "egg", 1617246530, "Lorem ipsum dolor sit amet"}, // Thu Apr 01 03:08:50 2021 UTC
	}
	sPilosaName := "s"

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()
	topic := "sasl_xyz" + strconv.Itoa(a)

	// use admin to create topic
	adminClient := tCreateConfluentAdmin(t)
	tCreateConfluentTopic(t, adminClient, topic)
	defer adminClient.Close()

	// create Main and run with MaxMsgs
	m, err := NewMain()
	if err != nil {
		t.Fatal(err)
	}
	configureTestFlags(m)
	m.Index = fmt.Sprintf("sasl_cmd_test_index223ij%s", topic)
	m.AutoGenerate = true
	m.Header = "../kafka_static/testdata/TestFieldTypes.json"
	m.PackBools = "bools"
	m.BatchSize = 1
	m.Topics = []string{topic}
	m.MaxMsgs = uint64(len(records))
	m.PilosaHosts = []string{pilosaHost}
	m.Timeout = time.Minute
	m.LookupDBDSN = "postgresql://postgres:password@postgres:5432/postgres?sslmode=disable"
	m.LookupBatchSize = 1

	// put records from all testcases into kafka
	producer := tCreateConfluentProducer(t)
	for _, vals := range records {
		rec := makeRecordString(t, fieldNames, vals)
		tWriteConfluentMessage(t, producer, topic, "akey", rec)
	}
	defer producer.Close()

	err = m.Run()
	if err != nil {
		t.Fatalf("running main: %v", err)
	}

	client := m.PilosaClient()
	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}

	index := schema.Index(m.Index)
	defer func() {
		err := client.DeleteIndex(index)
		if err != nil {
			t.Logf("deleting index: %v", err)
		}
	}()

	if status, body, err := client.HTTPRequest("POST", "/recalculate-caches", nil, nil); err != nil {
		t.Fatalf("recalculating cache: status: %d, response: %s, error: %s", status, body, err)
	}

	// check data in Pilosa
	if !index.HasField(sPilosaName) {
		t.Fatalf("don't have field '%s'", sPilosaName)
	}

	fields := index.Field(sPilosaName)
	stPilosaName := "st"
	fieldSt := index.Field(stPilosaName)

	qr, err := client.Query(index.Count(fields.Row("apple")))
	if err != nil {
		t.Errorf("querying: %v", err)
	}
	if qr.Result().Count() != 1 {
		t.Errorf("wrong count for field '%s', %d is not 1", sPilosaName, qr.Result().Count())
	}

	qr, err = client.Query(index.Count(fieldSt.Row("egg")))
	if err != nil {
		t.Fatalf("querying time range for egg: %v", err)
	}
	if qr.Result().Count() != 1 {
		t.Errorf("wrong count for field '%s', %d is not 1", stPilosaName, qr.Result().Count())
	}
	qr, err = client.Query(index.Count(fieldSt.Range("egg", time.Unix(1617145530, 0), time.Unix(1617348530, 0))))
	if err != nil {
		t.Fatalf("querying time range for egg: %v", err)
	}
	if qr.Result().Count() != 1 {
		t.Errorf("wrong count for field '%s' with time range, %d is not 1", stPilosaName, qr.Result().Count())
	}

	if !index.HasField("i") {
		t.Fatal("don't have field 'i'")
	}
	fieldi := index.Field("i")

	qr, err = client.Query(index.Count(fieldi.Row(100)))
	if err != nil {
		t.Errorf("querying: %v", err)
	}
	if qr.Result().Count() != 1 {
		t.Errorf("wrong count for field 'i', %d is not 1", qr.Result().Count())
	}

	if !index.HasField("d") {
		t.Fatal("don't have field 'd'")
	}
	fieldd := index.Field("d")

	qr, err = client.Query(index.Count(fieldd.Row(34.0404)))
	if err != nil {
		t.Errorf("querying: %v", err)
	}
	if qr.Result().Count() != 1 {
		t.Errorf("wrong count for field 'd', %d is not 1", qr.Result().Count())
	}

}

func TestSaslLookupFieldIdNameDisallowed(t *testing.T) {
	t.Parallel()
	tcs := []lookupTestCase{
		{name: "junk", text: "foo", expText: "foo"},
	}

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()
	topic := "sasl_xyz" + strconv.Itoa(a)

	// use admin to create topic
	adminClient := tCreateConfluentAdmin(t)
	tCreateConfluentTopic(t, adminClient, topic)
	defer adminClient.Close()

	// create Main and run with MaxMsgs
	m, err := NewMain()
	if err != nil {
		t.Fatal(err)
	}
	configureTestFlags(m)
	m.Index = fmt.Sprintf("sasl_cmd_test_index223ij%s", topic)
	m.AutoGenerate = true
	m.PackBools = "bools"
	m.BatchSize = 1
	m.Topics = []string{topic}
	m.PilosaHosts = []string{pilosaHost}
	m.Timeout = time.Minute
	m.LookupDBDSN = "postgresql://postgres:password@postgres:5432/postgres?sslmode=disable"
	m.Header = "../kafka_static/testdata/LookupId.json"

	// lookupClient.Setup() can't get called until after initialFetch, so need to insert
	// some junk into kafka to test this at this level.
	m.MaxMsgs = uint64(len(tcs)) // make source wait for TimeOut after last message
	m.LookupBatchSize = 1

	ingesterErrs := make(chan error, 1)
	go func() {
		err := m.Run()
		ingesterErrs <- err
	}()

	// put records from all testcases into kafka
	producer := tCreateConfluentProducer(t)
	for _, tc := range tcs {
		msg := messageStringFromTestcase(t, tc)
		tWriteConfluentMessage(t, producer, topic, "bkey", msg)
	}
	defer producer.Close()

	// Wait for ingestion to finish.
	err = <-ingesterErrs

	if !strings.Contains(err.Error(), "field name 'id' not allowed for LookupText fields") {
		t.Fatalf("invalid field name 'id' not detected: %s", err)
	}
}

// lookupTestCase consolidates definitions for:
// - messages sent to Kafka
// - record IDs generated by Pilosa and retrieved within a test
// - values to check against Postgres
// - values to check against Pilosa
// name: testcase name
// uniquePilosaVal: unqiue integer value sent to Pilosa. Corresponds to `int` field in Lookup.json
// externalId: Pilosa record ID, allocated by Pilosa, looked up by test, used as Postgres ID as well
// text: raw text sent to Postgres. Corresponds to `text` field in Lookup.josn
// expText: text after retrieving from Postges (distinct from `text` due to escape characters)
// missing: true if `text` should NOT be present in the Kafka message
type lookupTestCase struct {
	name            string
	uniquePilosaVal uint64
	externalId      uint64
	text            string
	expText         string
	missing         bool
}

// messageStringFromTestCase defines a Kafka json message string, to be
// sent to Kafka. Matches Lookup.json.
func messageStringFromTestcase(t *testing.T, tc lookupTestCase) string {
	if tc.missing {
		return fmt.Sprintf(`{"int":%d}`, tc.uniquePilosaVal)
	} else {
		return fmt.Sprintf(`{"int":%d,"text":"%s"}`, tc.uniquePilosaVal, tc.text)
	}

}

// TestLookupFieldWithExternalId checks that the external
// lookup (postgres) feature works as expected,
// - using ExternalGenerate to use pilosa to generate IDs
// - with bad string input
// - with missing data
// This test is intended to match THR's use case.
func TestSaslLookupFieldWithExternalId(t *testing.T) {
	t.Parallel()
	tcs := []lookupTestCase{
		// When using ExternalGenerate (pilosa Nexter), then the only kafka setting that
		// makes sense is at-most-once delivery.
		// That means this postgres client/batcher should only be used in its current state
		// for this very specific use case.

		// Testing for duplicate records and overwriting behavior would be done here by defining
		// multiple testcases with overlapping ID values.
		// This is not done because that doesn't make sense when using ExternalGenerate:
		// - key is not present in Kafka message, so it is generated by Pilosa
		// - identifying duplicates is not possible without a primary key in the message
		{name: "missing-data", missing: true},
		{name: "normal-write", text: "D", expText: "D"},
		{name: "weirdstring-1", text: "Æ�漢д ☮♬ ♞🜻💣", expText: "Æ�漢д ☮♬ ♞🜻💣"},
		{name: "weirdstring-2", text: "", expText: ""},
		{name: "weirdstring-3", text: "'", expText: "'"},
		{name: "doublequotes-1", text: `\"`, expText: `"`}, // is this sensible?
		{name: "doublequotes-2", text: `{\"log\": \"message\", 'with': 'whatever', ` + "`weird`: `syntax`}", expText: `{"log": "message", 'with': 'whatever', ` + "`weird`: `syntax`}"},
	}

	// lookupTestCase.uniquePilosaVal is required to be unique,
	// so it can be used to correlate testcases with IDs allocated by Pilosa,
	// in retrieveTestCaseIds. Set it automatically here.
	lookupRecordCount := 0
	for n := range tcs {
		tcs[n].uniquePilosaVal = uint64(100 * (n + 1))
		if !tcs[n].missing {
			lookupRecordCount++
		}
	}

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()
	topic := "sasl_xyz" + strconv.Itoa(a)

	// put records from all testcases into kafka
	adminClient := tCreateConfluentAdmin(t)
	tCreateConfluentTopic(t, adminClient, topic)
	defer adminClient.Close()

	// create Main and run with MaxMsgs
	m, err := NewMain()
	if err != nil {
		t.Fatal(err)
	}
	configureTestFlags(m)
	m.Index = fmt.Sprintf("sasl_cmd_test_index223ij%s", topic)
	m.AutoGenerate = true
	m.PackBools = "bools"
	m.BatchSize = 1
	m.Topics = []string{topic}
	m.PilosaHosts = []string{pilosaHost}
	m.Timeout = time.Minute
	m.Header = "../kafka_static/testdata/Lookup.json"
	pilosaFieldName := "int"     // matches Lookup.json
	postgresColumnName := "text" // matches Lookup.json

	// lookup+external specific settings
	m.ExternalGenerate = true
	m.LookupDBDSN = "postgresql://postgres:password@postgres:5432/postgres?sslmode=disable"
	m.AllowMissingFields = true  // needed for missing data testcases
	m.MaxMsgs = uint64(len(tcs)) // make source wait for TimeOut after last message
	m.LookupBatchSize = lookupRecordCount

	// put records from all testcases into kafka
	producer := tCreateConfluentProducer(t)
	for _, tc := range tcs {
		msg := messageStringFromTestcase(t, tc)
		tWriteConfluentMessage(t, producer, topic, "bkey", msg)
	}
	defer producer.Close()

	ingesterErrs := make(chan error, 1)
	go func() {
		err := m.Run()
		ingesterErrs <- err
	}()

	// Wait for ingestion to finish.
	if err := <-ingesterErrs; err != nil {
		t.Fatalf("running main: %v", err)
	}

	err = retrieveTestCaseIds(tcs, m.Index, pilosaFieldName)
	if err != nil {
		t.Fatalf("looking up ExternalIds: %v", err)
	}

	lookupClient, err := m.NewLookupClient()
	if err != nil {
		t.Fatal("creating lookup client")
	}
	defer lookupClient.Close()

	// check final postgres values
	for _, tc := range tcs {
		t.Run(tc.name+"-postgres", func(t *testing.T) {
			if tc.missing {
				if present, err := lookupClient.RowExists(tc.externalId); err != nil {
					t.Fatalf("querying postgres: %s", err)
				} else if present {
					t.Fatalf("present and shouldn't be")
				}
			} else {
				if got, err := lookupClient.ReadString(tc.externalId, postgresColumnName); err != nil {
					t.Fatalf("querying postgres: %s", err)
				} else if tc.expText != got {
					t.Errorf("wrong value from postgres, expected\n%s\n  got\n%s\n", tc.expText, got)
				}
			}
		})
	}

	// check postgres values via pilosa
	for _, tc := range tcs {
		t.Run(tc.name+"-pilosa", func(t *testing.T) {
			pilosaCount, pilosaVal, err := lookupViaPilosa(tc.externalId, postgresColumnName, m.Index)
			if err != nil {
				t.Fatalf("querying pilosa: %v", err)
			}
			if tc.missing {
				if pilosaCount > 0 {
					t.Errorf("present and shouldn't be")
				}
			} else {
				if pilosaVal != tc.expText {
					t.Errorf("wrong value from pilosa, expected\n%s\n  got\n%s\n", tc.expText, pilosaVal)
				}
			}
		})
	}

	// // delete data from pilosa
	// schema, err := m.PilosaClient().Schema()
	// if err != nil {
	// 	t.Errorf("getting client: %v", err)
	// }
	// index := schema.Index(m.Index)

	// err = m.PilosaClient().DeleteIndex(index)
	// if err != nil {
	// 	t.Errorf("deleting index: %v", err)
	// }

	// // delete data from postgres
	// err = lookupClient.DropTable()
	// if err != nil {
	// 	t.Errorf("dropping table: %v", err)
	// }
}

// lookupViaPilosa retrieves lookupText values from Postgres via the ExternalLookup
// Pilosa query.
// This was created as a helper function for TestLookupFieldWithExternalId.
func lookupViaPilosa(id uint64, column, index string) (int, string, error) {
	pql := fmt.Sprintf(`ExternalLookup(ConstRow(columns=[%d]), query="select id, %s from %s where id = ANY($1)")`, id, "text", index)

	eResp, err := idktest.DoExtractQuery(pql, index)
	if err != nil {
		return 0, "", err
	}

	if eResp.Results[0].Columns == nil {
		// This itself is not an error condition; it is expected for the
		// missing-data testcase.
		return 0, "", nil
	}

	rows := eResp.Results[0].Columns[0].Rows
	count := len(rows)
	text := rows[0].(string)
	return count, text, nil
}

// retrieveTestCaseIds populates the externalId field of each testcase
// by comparing the uniquePilosaValue in the testcase with the results of
// an Extract query which correlates uniquePilosaValue with its
// pilosa-allocated ID.
// This was created as a helper function for TestLookupFieldWithExternalId.
func retrieveTestCaseIds(tcs []lookupTestCase, index, field string) error {
	pql := fmt.Sprintf("Extract(All(), Rows(%s))", field)

	eResp, err := idktest.DoExtractQuery(pql, index)
	if err != nil {
		return err
	}

	if eResp.Results[0].Columns == nil {
		return errors.Errorf("no data in Extract response")
	}

	// Correlate PilosaVal to assign corresponding IDs.
	for n, tc := range tcs {
		for _, col := range eResp.Results[0].Columns {
			// ?? panic: interface conversion: interface {} is float64, not uint64
			if uint64(col.Rows[0].(float64)) == tc.uniquePilosaVal {
				tcs[n].externalId = uint64(col.ColumnID)
				break
			}
		}
		if tcs[n].externalId == 0 {
			// NOTE This assumes an ID of 0 will not be used by the Nexter.
			return errors.Errorf("no externalID found for test case with uniquePilosaVal=%d", tc.uniquePilosaVal)
		}
	}

	return nil
}

func TestSaslDuplicateFieldNameDisallowed(t *testing.T) {
	t.Parallel()

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()
	topic := "sasl_xyz" + strconv.Itoa(a)

	// create Main and run with MaxMsgs
	m, err := NewMain()
	if err != nil {
		t.Fatal(err)
	}

	configureTestFlags(m)
	m.Index = fmt.Sprintf("sasl_cmd_test_index223ij%s", topic)
	m.AutoGenerate = true
	m.PackBools = "bools"
	m.BatchSize = 1
	m.Topics = []string{topic}
	m.PilosaHosts = []string{pilosaHost}
	m.Timeout = time.Minute
	m.LookupDBDSN = "postgresql://postgres:password@postgres:5432/postgres?sslmode=disable"
	m.Header = "../kafka_static/testdata/LookupDuplicate.json"
	m.MaxMsgs = uint64(0)

	err = m.Run()
	if !strings.Contains(err.Error(), "schema field 2 duplicates name of field 1 (text)") {
		t.Fatalf("duplicate field name not detected: %s", err)
	}
}

func TestSaslPrimaryKeyFieldsMissing(t *testing.T) {
	t.Parallel()

	fieldNames := []string{"i", "d", "t", "@s"}
	records := [][]interface{}{
		{2, 5.5, "2021-02-01", nil},
	}

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()
	topic := "sasl_xyz" + strconv.Itoa(a)

	// use admin to create topic
	adminClient := tCreateConfluentAdmin(t)
	tCreateConfluentTopic(t, adminClient, topic)
	defer adminClient.Close()

	// create Main and run with MaxMsgs
	m, err := NewMain()
	if err != nil {
		t.Fatal(err)
	}

	configureTestFlags(m)
	m.Index = fmt.Sprintf("sasl_cmd_test_index223ij%s", topic)
	m.PrimaryKeyFields = []string{"s"}
	m.AllowMissingFields = true
	m.Header = "../kafka_static/testdata/TestFieldTypes.json"
	m.PackBools = "bools"
	m.BatchSize = 1
	m.Topics = []string{topic}
	m.MaxMsgs = uint64(len(records))
	m.PilosaHosts = []string{pilosaHost}
	m.Timeout = time.Minute
	m.Verbose = true

	// put records from all testcases into kafka
	producer := tCreateConfluentProducer(t)
	for _, vals := range records {
		msg := makeRecordString(t, fieldNames, vals)
		tWriteConfluentMessage(t, producer, topic, "akey", msg)
	}
	defer producer.Close()

	err = m.Run()
	if err == nil {
		t.Fatal("running main should have failed")
	}

	client := m.PilosaClient()
	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	index := schema.Index(m.Index)

	err = client.DeleteIndex(index)
	if err != nil {
		t.Logf("deleting index: %v", err)
	}

}

func TestSaslIDFieldMissing(t *testing.T) {
	t.Parallel()

	fieldNames := []string{"i", "d", "t", "@s"}
	records := [][]interface{}{
		{nil, 5.5, "2021-02-01", "apple"},
	}

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()
	topic := "sasl_xyz" + strconv.Itoa(a)

	// use admin to create topic
	adminClient := tCreateConfluentAdmin(t)
	tCreateConfluentTopic(t, adminClient, topic)
	defer adminClient.Close()

	// create Main and run with MaxMsgs
	m, err := NewMain()
	if err != nil {
		t.Fatal(err)
	}
	configureTestFlags(m)
	m.Index = fmt.Sprintf("sasl_cmd_test_index223ij%s", topic)
	m.IDField = "i"
	m.AllowMissingFields = true
	m.Header = "../kafka_static/testdata/TestFieldTypes.json"
	m.PackBools = "bools"
	m.BatchSize = 1
	m.Topics = []string{topic}
	m.MaxMsgs = uint64(len(records))
	m.PilosaHosts = []string{pilosaHost}
	m.Timeout = time.Minute
	m.Verbose = true

	// put records from all testcases into kafka
	producer := tCreateConfluentProducer(t)
	for _, vals := range records {
		msg := makeRecordString(t, fieldNames, vals)
		tWriteConfluentMessage(t, producer, topic, "akey", msg)
	}
	defer producer.Close()

	err = m.Run()
	if err == nil {
		t.Fatal("running main should have failed")
	}

	client := m.PilosaClient()
	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	index := schema.Index(m.Index)

	err = client.DeleteIndex(index)
	if err != nil {
		t.Logf("deleting index: %v", err)
	}
}

func TestSaslCmdAutoID(t *testing.T) {
	t.Parallel()

	fieldNames := []string{"first"}
	records := [][]interface{}{
		{"a"},
		{"b"},
		{"c"},
	}

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()
	topic := "sasl_xyz" + strconv.Itoa(a)

	// use admin to create topic
	adminClient := tCreateConfluentAdmin(t)
	tCreateConfluentTopic(t, adminClient, topic)
	defer adminClient.Close()

	// create Main and run with MaxMsgs
	m, err := NewMain()
	if err != nil {
		t.Fatal(err)
	}
	configureTestFlags(m)
	m.Index = fmt.Sprintf("sasl_cmd_test_auto_id223ij%s", topic)
	m.AutoGenerate = true
	m.ExternalGenerate = true
	m.Header = "../kafka_static/testdata/Flat.json"
	m.BatchSize = 1
	m.Topics = []string{topic}
	m.MaxMsgs = uint64(len(records))
	m.PilosaHosts = []string{pilosaHost}
	m.Timeout = time.Minute
	m.Verbose = true

	// put records from all testcases into kafka
	producer := tCreateConfluentProducer(t)
	for _, vals := range records {
		msg := makeRecordString(t, fieldNames, vals)
		tWriteConfluentMessage(t, producer, topic, "a", msg)
	}
	defer producer.Close()

	err = m.Run()
	if err != nil {
		t.Fatalf("running main: %v", err)
	}

	client := m.PilosaClient()
	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	index := schema.Index(m.Index)
	defer func() {
		err := client.DeleteIndex(index)
		if err != nil {
			t.Logf("deleting index: %v", err)
		}
	}()

	qr, err := client.Query(index.Count(index.All()))
	if err != nil {
		t.Errorf("querying: %v", err)
	}
	if qr.Result().Count() != 3 {
		t.Errorf("wrong count for columns, %d is not 3", qr.Result().Count())
	}

	qr, err = client.Query(pilosaclient.NewPQLBaseQuery(`Count(Distinct(All(), field="first"))`, index, nil))
	if err != nil {
		t.Errorf("querying: %v", err)
	}
	if qr.Result().Count() != 3 {
		t.Errorf("wrong count for val, %d is not 3", qr.Result().Count())
	}
}
func TestSaslSaslConfig(t *testing.T) {
	t.Parallel()

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()
	topic := "sasl_xyz" + strconv.Itoa(a)

	// create Main and run with MaxMsgs
	m, err := NewMain()
	if err != nil {
		t.Fatal(err)
	}
	m.Index = fmt.Sprintf("sasl_cmd_test_index223ij%s", topic)
	m.KafkaBootstrapServers = []string{kafkaHost}

	m.PilosaHosts = []string{pilosaHost}
	// m.LookupDBDSN = "postgresql://postgres:password@postgres:5432/postgres?sslmode=disable"
	m.AutoGenerate = true
	m.Topics = []string{topic}
	m.Header = "../kafka_static/testdata/LookupId.json"

	type testCase struct {
		name          string
		config        idk.ConfluentCommand
		expectedError string
	}

	tests := []testCase{
		{
			name: "incorrect username",
			config: idk.ConfluentCommand{
				KafkaSaslUsername:           "wrong-username",
				KafkaSaslPassword:           configMapSource.KafkaSaslPassword,
				KafkaSaslMechanism:          configMapSource.KafkaSaslMechanism,
				KafkaSecurityProtocol:       configMapSource.KafkaSecurityProtocol,
				KafkaSslKeyPassword:         configMapSource.KafkaSslKeyPassword,
				KafkaSslCaLocation:          configMapSource.KafkaSslCaLocation,
				KafkaSslCertificateLocation: configMapSource.KafkaSslCertificateLocation,
				KafkaSslKeyLocation:         configMapSource.KafkaSslKeyLocation,
			},
			expectedError: "Authentication failed: Invalid username or password",
		},
		{
			name: "incorrect password",
			config: idk.ConfluentCommand{
				KafkaSaslUsername:           configMapSource.KafkaSaslUsername,
				KafkaSaslPassword:           "wrong-password",
				KafkaSaslMechanism:          configMapSource.KafkaSaslMechanism,
				KafkaSecurityProtocol:       configMapSource.KafkaSecurityProtocol,
				KafkaSslKeyPassword:         configMapSource.KafkaSslKeyPassword,
				KafkaSslCaLocation:          configMapSource.KafkaSslCaLocation,
				KafkaSslCertificateLocation: configMapSource.KafkaSslCertificateLocation,
				KafkaSslKeyLocation:         configMapSource.KafkaSslKeyLocation,
			},
			expectedError: "Authentication failed: Invalid username or password",
		},
		{
			name: "unsupported sasl mechanism",
			config: idk.ConfluentCommand{
				KafkaSaslUsername:           configMapSource.KafkaSaslUsername,
				KafkaSaslPassword:           configMapSource.KafkaSaslPassword,
				KafkaSaslMechanism:          "random-mechanism",
				KafkaSecurityProtocol:       configMapSource.KafkaSecurityProtocol,
				KafkaSslKeyPassword:         configMapSource.KafkaSslKeyPassword,
				KafkaSslCaLocation:          configMapSource.KafkaSslCaLocation,
				KafkaSslCertificateLocation: configMapSource.KafkaSslCertificateLocation,
				KafkaSslKeyLocation:         configMapSource.KafkaSslKeyLocation,
			},
			expectedError: "Unsupported SASL mechanism",
		},
		{
			name: "invalid value for security protocol",
			config: idk.ConfluentCommand{
				KafkaSaslUsername:           configMapSource.KafkaSaslUsername,
				KafkaSaslPassword:           configMapSource.KafkaSaslPassword,
				KafkaSaslMechanism:          configMapSource.KafkaSaslMechanism,
				KafkaSecurityProtocol:       "random-protocol",
				KafkaSslKeyPassword:         configMapSource.KafkaSslKeyPassword,
				KafkaSslCaLocation:          configMapSource.KafkaSslCaLocation,
				KafkaSslCertificateLocation: configMapSource.KafkaSslCertificateLocation,
				KafkaSslKeyLocation:         configMapSource.KafkaSslKeyLocation,
			},
			expectedError: "Invalid value " + "\"random-protocol\"" + " for configuration property \"security.protocol\"",
		},
	}

	runTest := func(t *testing.T, main *Main, config idk.ConfluentCommand, expectedError string) {
		main.CopyIn(config)
		err := m.Run()
		if err != nil {
			assert.Contains(t, err.Error(), expectedError)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runTest(t, m, test.config, test.expectedError)
		})
	}
}

func TestMaxMsgs(t *testing.T) {
	tests := []struct {
		rec   []string
		count int
	}{
		{
			rec:   []string{`{"protocol": "TCP", "port": 22, "client": "127.0.0.1", "server": "127.0.0.1", "metadata": {"suspicious": true, "region": "North America"}}`},
			count: 1,
		},
		{
			rec: []string{`{"protocol": "TCP", "port": 22, "client": "127.0.0.1", "server": "127.0.0.1", "metadata": {"suspicious": true, "region": "North America"}}`,
				`{"protocol": "TCP", "port": 22, "client": "127.0.0.1", "server": "127.0.0.1", "metadata": {"suspicious": true, "region": "North America"}}`,
				`{"protocol": "SSH", "port": 22, "client": "127.0.0.1", "server": "127.0.0.1", "metadata": {"suspicious": false, "region": "South America"}}`},
			count: 2,
		},
	}

	collector := &collector{counter: 0}

	for i, test := range tests {
		t.Run(fmt.Sprintf("Test%d", i), func(t *testing.T) {
			topic := fmt.Sprintf("topic-max-msgs-%d", i)
			// create Main and run with MaxMsgs
			m, err := NewMain()
			m.Header = "testdata/Tree.json"
			if err != nil {
				t.Fatalf("opps %v", err)
			}
			m.NewSource = func() (idk.Source, error) {
				source := NewSource()
				source.Group = m.Group
				source.Topics = []string{topic}
				source.Log = m.Main.Log()
				source.Timeout = m.Timeout
				source.SkipOld = m.SkipOld
				source.Header = m.Header
				source.AllowMissingFields = m.AllowMissingFields
				cfg, err := common.SetupConfluent(&m.ConfluentCommand)
				if err != nil {
					return nil, err
				}

				source.ConfigMap = cfg
				err = source.Open()
				if err != nil {
					return nil, errors.Wrap(err, "opening source")
				}
				return NewWrapSource(source, collector), nil
			}

			configureTestFlags(m)
			m.Index = fmt.Sprintf("max-msgs-%d", i)
			m.AutoGenerate = true
			m.ExternalGenerate = true
			m.Header = "../kafka_static/testdata/Tree.json"
			m.BatchSize = 1
			m.Topics = []string{fmt.Sprintf("test-topic-%d", i)}
			m.MaxMsgs = uint64(test.count)
			m.PilosaHosts = []string{pilosaHost}

			// use admin to create topic
			adminClient := tCreateConfluentAdmin(t)
			tCreateConfluentTopic(t, adminClient, topic)
			adminClient.Close()

			// put records from all testcases into kafka
			producer := tCreateConfluentProducer(t)
			for _, vals := range test.rec {
				tWriteConfluentMessage(t, producer, topic, "a", vals)
			}
			producer.Close()

			collector.reset()
			err = m.Run()
			if err != nil {
				t.Fatalf("running main: %v", err)
			}

			if collector.counter != uint64(test.count) {
				t.Fatalf("expected counter %d, got counter %d", test.count, collector.counter)
			}
		})
	}
}

type wrapSource struct {
	source    *Source
	collector *collector
}

type collector struct {
	counter uint64
}

func (c *collector) reset() {
	c.counter = uint64(0)
}

func NewWrapSource(s *Source, c *collector) idk.Source {
	return &wrapSource{source: s, collector: c}
}

func (w *wrapSource) Record() (idk.Record, error) {
	a, err := w.source.Record()
	if err == nil {
		w.collector.counter++
	}
	return a, err
}

func (w *wrapSource) Schema() []idk.Field {
	return w.source.Schema()
}

func (w *wrapSource) Close() error {
	return w.source.Close()
}
