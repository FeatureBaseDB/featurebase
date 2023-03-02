//go:build !kafka_sasl
// +build !kafka_sasl

package kafka

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	pilosaclient "github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/kafka/csrc"
	"github.com/featurebasedb/featurebase/v3/logger"
	liavro "github.com/linkedin/goavro/v2"
)

var (
	pilosaHost     string
	pilosaTLSHost  string
	pilosaGrpcHost string
	kafkaHost      string
	registryHost   string
	certPath       string
)

func init() {
	local := false
	var ok bool
	if pilosaHost, ok = os.LookupEnv("IDK_TEST_PILOSA_HOST"); !ok {
		if local {
			pilosaHost = "localhost:10101"
		} else {
			pilosaHost = "pilosa:10101"
		}
	}
	if pilosaTLSHost, ok = os.LookupEnv("IDK_TEST_PILOSA_TLS_HOST"); !ok {
		pilosaTLSHost = "https://pilosa-tls:10111"
	}
	if pilosaGrpcHost, ok = os.LookupEnv("IDK_TEST_PILOSA_GRPC_HOST"); !ok {
		if local {
			pilosaGrpcHost = "localhost:20101"
		} else {
			pilosaGrpcHost = "pilosa:20101"
		}
	}
	if kafkaHost, ok = os.LookupEnv("IDK_TEST_KAFKA_HOST"); !ok {
		if local {
			kafkaHost = "localhost:9092"
		} else {
			kafkaHost = "kafka:9092"
		}
	}
	if registryHost, ok = os.LookupEnv("IDK_TEST_REGISTRY_HOST"); !ok {
		if local {
			registryHost = "localhost:8081"
		} else {
			registryHost = "schema-registry:8081"
		}
	}
	if certPath, ok = os.LookupEnv("IDK_TEST_CERT_PATH"); !ok {
		certPath = "/certs"
	}
}

func configureTestFlags(main *Main) {
	main.PilosaHosts = []string{pilosaHost}
	main.PilosaGRPCHosts = []string{pilosaGrpcHost}
	main.KafkaBootstrapServers = []string{kafkaHost}
	main.SchemaRegistryURL = registryHost
	main.Verbose = true
	// intentionally low timeout — if this gets triggered it shouldn't
	// have any negative effects
	main.Timeout = time.Millisecond * 20
	main.Stats = ""
	_, main.Verbose = os.LookupEnv("IDK_TEST_VERBOSE")
}

func TestCmdAutoID(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	rand.Seed(time.Now().UnixNano())
	topic := strconv.Itoa(rand.Int())

	fields := []string{"val"}
	records := [][]interface{}{
		{2},
		{3},
		{4},
		{6},
	}

	m, err := NewMain()
	if err != nil {
		t.Fatalf("creating main %v", err)
	}
	configureTestFlags(m)
	m.Index = fmt.Sprintf("cmd_test_autoid239ij%s", topic)
	m.BatchSize = 2 // need to test at a batch size less than the # of records, greater than, and equal to
	m.Topics = []string{topic}
	m.MaxMsgs = uint64(len(records))
	m.AutoGenerate = true
	m.ExternalGenerate = true

	// load schema
	licodec := liDecodeTestSchema(t, "ids.json")
	schemaID := postSchema(t, "ids.json", "ids", m.SchemaRegistryURL, nil)
	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers": kafkaHost,
	})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()
	// put records in kafka
	tCreateTopic(t, topic, p)

	msgs := make([]map[string]interface{}, len(records))
	for i, vals := range records {
		msgs[i] = makeRecord(t, fields, vals)
	}
	tPutRecordsKafka(t, p, topic, schemaID, licodec, "a", msgs...)

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
	if qr.Result().Count() != 4 {
		t.Errorf("wrong count for columns, %d is not 4", qr.Result().Count())
	}

	qr, err = client.Query(pilosaclient.NewPQLBaseQuery(`Count(Distinct(All(), field="val"))`, index, nil))
	if err != nil {
		t.Errorf("querying: %v", err)
	}
	if qr.Result().Count() != 4 {
		t.Errorf("wrong count for val, %d is not 4", qr.Result().Count())
	}
}

func TestConfigOptions(t *testing.T) {
	m, err := NewMain()
	if err != nil {
		t.Fatalf("getting main: %v", err)
	}

	m.KafkaDebug = "consumer"
	m.KafkaClientId = "blah"
	m.SkipOld = true

	// call NewSource to get and open source which calls some setup
	// stuff that we want. We don't expect it to open successfully
	// though.
	src, err := m.NewSource()
	if err != nil {
		t.Fatalf("error calling new source: %v", err)
	}
	defer src.Close()

	cfg := src.(*Source).ConfigMap

	if val, err := cfg.Get("debug", nil); err != nil || val.(string) != "consumer" {
		t.Fatalf("unexpected val for debug val: %v, err: %v", val, err)
	}
	if val, err := cfg.Get("client.id", nil); err != nil || val.(string) != "blah" {
		t.Fatalf("unexpected val for client.id val: %v, err: %v", val, err)
	}

	if val, err := cfg.Get("auto.offset.reset", nil); err != nil || val.(string) != "latest" {
		t.Fatalf("unexpected val for auto.offset.reset val: %v, err: %v", val, err)
	}
}

func TestCmdMainOne(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip()
	}

	tests := []struct {
		name             string
		PrimaryKeyFields []string
		IDField          string
		PilosaHosts      []string
		RegistryURL      string
		TLS              *idk.TLSConfig
		expRhinoKeys     []string
		expRhinoCols     []uint64
	}{
		{
			name:             "3 primary keys str/str/int",
			PrimaryKeyFields: []string{"abc", "db", "user_id"},
			expRhinoKeys:     []string{"2|1|159", "4|3|44", "123456789|q2db_1234|432"}, // "2" + "1" + uint32(159)

		},
		{
			name:             "3 primary keys str/str/int TLS",
			PrimaryKeyFields: []string{"abc", "db", "user_id"},
			PilosaHosts:      []string{pilosaTLSHost},
			TLS: &idk.TLSConfig{
				CertificatePath:          certPath + "/theclient.crt",
				CertificateKeyPath:       certPath + "/theclient.key",
				CACertPath:               certPath + "/ca.crt",
				EnableClientVerification: true,
			},
			expRhinoKeys: []string{"2|1|159", "4|3|44", "123456789|q2db_1234|432"}, // "2" + "1" + uint32(159)

		},
		{
			name:         "IDField int",
			IDField:      "user_id",
			expRhinoCols: []uint64{44, 159, 432},
		},
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))

	for _, test := range tests {
		test := test
		a := rand.Int()
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fields := []string{"abc", "db", "user_id", "all_users", "has_deleted_date", "central_group", "custom_audiences", "desktop_boolean", "desktop_frequency", "desktop_recency", "product_boolean_historical_forestry_cravings_or_bugles", "ddd_category_total_current_rhinocerous_checking", "ddd_category_total_current_rhinocerous_thedog_cheetah", "survey1234", "days_since_last_logon", "elephant_added_for_account"}

			records := [][]interface{}{
				{"2", "1", 159, map[string]interface{}{"boolean": true}, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.4}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil},
				{"4", "3", 44, map[string]interface{}{"boolean": true}, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.4}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil},
				{"123456789", "q2db_1234", 432, map[string]interface{}{"boolean": false}, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.9}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil},
				{"123456789", "q2db_1234", 432, map[string]interface{}{"boolean": false}, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.4}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil},
				{"2", "1", 159, map[string]interface{}{"boolean": false}, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.4}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil},
				{"4", "3", 44, map[string]interface{}{"boolean": false}, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.4}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil},
				{"4", "3", 44, map[string]interface{}{"boolean": true}, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.4}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil},
				{"4", "3", 44, nil, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.4}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil}, // send all_users nil - regression test to make sure it isn't cleared by the nil
			}

			topic := strconv.Itoa(a)

			// create Main and run with MaxMsgs
			m, err := NewMain()
			if err != nil {
				t.Fatalf("creating main %v", err)
			}
			configureTestFlags(m)
			m.Index = fmt.Sprintf("cmd_test_index239ij%s", topic)
			m.PrimaryKeyFields = test.PrimaryKeyFields
			m.IDField = test.IDField
			m.PackBools = "bools"
			m.BatchSize = 3 // need to test at a batch size less than the # of records, greater than, and equal to
			m.Topics = []string{topic}
			m.MaxMsgs = uint64(len(records))
			if test.PilosaHosts != nil {
				m.PilosaHosts = test.PilosaHosts
			}
			if test.TLS != nil {
				m.TLS = *test.TLS
			}
			if test.RegistryURL != "" {
				m.SchemaRegistryURL = test.RegistryURL
			}

			// load big schema
			licodec := liDecodeTestSchema(t, "bigschema.json")
			tlsConf, err := idk.GetTLSConfig(test.TLS, logger.NopLogger)
			if err != nil {
				t.Fatalf("getting tls config: %v", err)
			}
			schemaID := postSchema(t, "bigschema.json", "bigschema2", m.SchemaRegistryURL, tlsConf)

			p, err := confluent.NewProducer(&confluent.ConfigMap{
				"bootstrap.servers": kafkaHost,
			})
			if err != nil {
				t.Fatalf("Failed to create producer: %s", err)
			}
			defer p.Close()
			// put records in kafka
			tCreateTopic(t, topic, p)

			msgs := make([]map[string]interface{}, len(records))
			for i, vals := range records {
				msgs[i] = makeRecord(t, fields, vals)
			}
			tPutRecordsKafka(t, p, topic, schemaID, licodec, "akey", msgs...)
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
			if !index.HasField("abc") {
				t.Fatal("don't have abc")
			}
			abc := index.Field("abc")
			qr, err := client.Query(index.Count(abc.Row("2")))
			if err != nil {
				t.Errorf("querying: %v", err)
			}
			if qr.Result().Count() != 1 {
				t.Errorf("wrong count for abc, %d is not 1", qr.Result().Count())
			}

			bools := index.Field("bools")
			qr, err = client.Query(bools.TopN(10))
			if err != nil {
				t.Fatalf("querying: %v", err)
			}
			ci := sortableCRI(qr.Result().CountItems())
			exp := sortableCRI{{Count: 1, Key: "all_users"}}
			sort.Sort(ci)
			sort.Sort(exp)
			if !reflect.DeepEqual(ci, exp) {
				qr, err = client.Query(bools.Row("all_users"))
				if err != nil {
					t.Fatalf("querying: %v", err)
				}
				t.Errorf("unexpected result exp/got\n%+v\n%+v\nall_users:%+v", exp, ci, qr.Result().Row().Keys)
			}

			qr, err = client.Query(bools.Row("all_users"))
			if err != nil {
				t.Fatalf("querying: %v", err)
			}
			if test.IDField == "" {
				if keys := qr.Result().Row().Keys; !reflect.DeepEqual(keys, []string{"4|3|44"}) {
					t.Errorf("unexpected keys %v", keys)
				}
			} else if cols := qr.Result().Row().Columns; !reflect.DeepEqual(cols, []uint64{44}) {
				t.Errorf("unexpected cols %v", cols)
			}

			bools = index.Field("bools-exists")
			qr, err = client.Query(bools.TopN(10))
			if err != nil {
				t.Fatalf("querying: %v", err)
			}
			ci = sortableCRI(qr.Result().CountItems())
			exp = sortableCRI{{Count: 3, Key: "all_users"}, {Count: 3, Key: "has_deleted_date"}}
			sort.Sort(ci)
			sort.Sort(exp)
			if !reflect.DeepEqual(ci, exp) {
				t.Errorf("unexpected result exp/got\n%+v\n%+v", exp, ci)
			}

			rhino := index.Field("ddd_category_total_current_rhinocerous_checking")
			qr, err = client.Query(rhino.Between(5, 6))
			if err != nil {
				t.Fatalf("querying: %v", err)
			}
			keys := qr.Result().Row().Keys
			sort.Strings(keys)
			sort.Strings(test.expRhinoKeys)
			if test.expRhinoKeys != nil {
				if !reflect.DeepEqual(keys, test.expRhinoKeys) {
					t.Errorf("wrong keys: %v, exp: %v", keys, test.expRhinoKeys)
				}
			}
			if test.expRhinoCols != nil {
				bod := tDoHTTPPost(t, fmt.Sprintf("http://"+pilosaHost+"/index/%s/query", m.Index), "application/pql", "Row(ddd_category_total_current_rhinocerous_checking==5.4)")
				if !strings.Contains(bod, "44,159,432") {
					t.Errorf("unexpected result with float query: '%s'", bod)
				}
				if cols := qr.Result().Row().Columns; !reflect.DeepEqual(cols, test.expRhinoCols) {
					t.Errorf("wrong cols: %v, exp: %v", cols, test.expRhinoCols)
				}
			}

			if !index.HasField("survey1234") {
				t.Fatalf("don't have survey1234")
			}
			survey1234 := index.Field("survey1234")
			if typ := survey1234.Opts().Type(); typ != pilosaclient.FieldTypeMutex {
				t.Errorf("survey1234 of unexpected type: %v", typ)
			}
		})
	}
}

func TestCmdSourceTimeout(t *testing.T) {
	//	t.Parallel()

	rand.Seed(time.Now().UnixNano())
	topic := strconv.Itoa(rand.Int())

	// create Main and run with MaxMsgs
	m, err := NewMain()
	if err != nil {
		t.Fatalf("creating main %v", err)
	}
	configureTestFlags(m)
	m.Index = fmt.Sprintf("cmd_test_schemachange239ij%s", topic)
	m.IDField = "id"
	m.PackBools = "bools"
	m.BatchSize = 5
	m.Topics = []string{topic}
	m.Timeout = time.Millisecond * 20
	m.MaxMsgs = uint64(3)

	schemaStr1 := `{"type": "record","namespace": "cmdSrcTimeout","name": "basic","fields": [{"name":"id","type":"long"},{"name":"a","type": "string"}]}`
	licodec1, err := liavro.NewCodec(schemaStr1)
	if err != nil {
		t.Fatalf("li parsing schema: %v", err)
	}

	schemaClient := csrc.NewClient(m.SchemaRegistryURL, nil, nil)
	resp, err := schemaClient.PostSubjects("cmdSourceTimeout", schemaStr1)
	if err != nil {
		t.Fatalf("posting schema: %v", err)
	}
	schemaID1 := resp.ID

	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers": kafkaHost,
	})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	rec := makeRecord(t, []string{"id", "a"}, []interface{}{1, "v"})
	tPutRecordsKafka(t, p, topic, schemaID1, licodec1, "akey", rec)
	rec = makeRecord(t, []string{"id", "a"}, []interface{}{2, "v"})
	tPutRecordsKafka(t, p, topic, schemaID1, licodec1, "akey", rec)

	// create client manually and then create schema. We have to do
	// this ahead of time because we're calling m.Run concurrently.
	client, err := pilosaclient.NewClient(m.PilosaHosts)
	if err != nil {
		t.Fatalf("creating new client: %v", err)
	}
	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	index := schema.Index(m.Index)
	af := index.Field("a", pilosaclient.OptFieldKeys(true))
	if err := client.SyncIndex(index); err != nil {
		t.Fatalf("syncing index: %v", err)
	}
	defer func() {
		if err := client.DeleteIndexByName(m.Index); err != nil {
			t.Fatalf("deleting index: %s", m.Index)
		}
	}()

	// Run ingest in a goroutine so we can see the effect of the
	// timeout flush.
	signal := make(chan struct{})
	go func() {
		err = m.Run()
		if err != nil {
			t.Logf("running main: %v", err)
		}
		close(signal)
	}()

	// query Pilosa repeatedly until we see the two records reflected there.
	for {
		qr, err := client.Query(index.Count(af.Row("v")))
		if err != nil {
			t.Fatalf("querying: %v", err)
		}
		if cnt := qr.Result().Count(); cnt == 2 {
			break
		} else if cnt == 0 {
			time.Sleep(time.Millisecond * 50)
			continue
		} else {
			t.Fatalf("unexpected count: %v", cnt)
		}
	}

	// put another message to make m.Run finish (MaxMsgs = 3)
	tPutRecordsKafka(t, p, topic, schemaID1, licodec1, "akey", rec)

	// make sure the Run goroutine actually finished
	<-signal
}

func TestCmdSchemaChange(t *testing.T) {
	t.Parallel()

	rand.Seed(time.Now().UnixNano())
	topic := strconv.Itoa(rand.Int())

	// create Main and run with MaxMsgs
	m, err := NewMain()
	if err != nil {
		t.Fatalf("creating main %v", err)
	}
	configureTestFlags(m)
	m.Index = fmt.Sprintf("cmd_test_schemachange239ij%s", topic)
	m.IDField = "id"
	m.PackBools = "bools"
	m.BatchSize = 1
	m.Topics = []string{topic}
	m.MaxMsgs = uint64(3)

	// load two schemas
	schemaStr1 := `{"type": "record","namespace": "c.e","name": "F","fields": [{"name":"id","type":"long"},{"name":"a","type": "boolean"},{"name": "c", "type": "int"}]}`
	licodec1, err := liavro.NewCodec(schemaStr1)
	if err != nil {
		t.Fatalf("li parsing schema: %v", err)
	}
	schemaStr2 := `{"type": "record","namespace": "c.e","name": "F","fields": [{"name":"id","type":"long"},{"name":"a","type": "boolean"},{"name":"b","type":"long"},{"name": "c", "type": "int"}]}`
	licodec2, err := liavro.NewCodec(schemaStr2)
	if err != nil {
		t.Fatalf("li parsing schema: %v", err)
	}

	// have to change schema name to change field from int to long
	schemaStr3 := `{"type": "record","namespace": "c.e","name": "F2","fields": [{"name":"id","type":"long"},{"name":"a","type": "boolean"},{"name":"b","type":"long"},{"name": "c", "type": "long"}]}`
	licodec3, err := liavro.NewCodec(schemaStr3)
	if err != nil {
		t.Fatalf("li parsing schema: %v", err)
	}

	schemaClient := csrc.NewClient(m.SchemaRegistryURL, nil, nil)
	resp, err := schemaClient.PostSubjects("subj", schemaStr1)
	if err != nil {
		t.Fatalf("posting schema: %v", err)
	}
	schemaID1 := resp.ID
	resp, err = schemaClient.PostSubjects("subj2", schemaStr2)
	if err != nil {
		t.Fatalf("posting schema: %v", err)
	}
	schemaID2 := resp.ID
	resp, err = schemaClient.PostSubjects("subj3", schemaStr3)
	if err != nil {
		t.Fatalf("posting schema: %v", err)
	}
	schemaID3 := resp.ID

	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers": kafkaHost,
	})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	tCreateTopic(t, topic, p)

	rec := makeRecord(t, []string{"id", "a", "c"}, []interface{}{1, true, 7})
	tPutRecordsKafka(t, p, topic, schemaID1, licodec1, "akey", rec)
	rec = makeRecord(t, []string{"id", "a", "b", "c"}, []interface{}{2, true, 22, 8})
	tPutRecordsKafka(t, p, topic, schemaID2, licodec2, "akey", rec)
	rec = makeRecord(t, []string{"id", "a", "b", "c"}, []interface{}{2, true, 22, 9_876_543_210})
	tPutRecordsKafka(t, p, topic, schemaID3, licodec3, "akey", rec)

	// run ingest
	err = m.Run()
	if err != nil {
		t.Logf("running main: %v", err)
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

	fieldA := index.Field("bools")
	fieldB := index.Field("b")

	qresp, err := client.Query(index.BatchQuery(fieldA.Row("a"), fieldB.GTE(0)))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	aRow := qresp.Results()[0].Row().Columns
	if !reflect.DeepEqual(aRow, []uint64{1, 2}) {
		t.Errorf("unexpected columns for a: %v", aRow)
	}
	bRow := qresp.Results()[1].Row().Columns
	if !reflect.DeepEqual(bRow, []uint64{2}) {
		t.Errorf("unexpected columns for a: %v", bRow)
	}

	qresp, err = client.Query(index.RawQuery("Row(c==7)"))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	sevenRow := qresp.Results()[0].Row().Columns
	if !reflect.DeepEqual(sevenRow, []uint64{1}) {
		t.Errorf("unexpected columns for 8: %v", sevenRow)
	}

	qresp, err = client.Query(index.RawQuery("Row(c==9876543210)"))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	nineBRow := qresp.Results()[0].Row().Columns
	if !reflect.DeepEqual(nineBRow, []uint64{2}) {
		t.Errorf("unexpected columns for 9876543210: %v", nineBRow)
	}
}

type ConsumerTestConfig struct {
	idType    string   // must be "generated", "id", or "string"
	keyFields []string // field used for "id" or "string" record keys
	topic     string
	delete    bool
}

/*
Struct that captures data required to run a test that meets the following conditions:
  - There is a JSON file will plain text records to write to kafka
  - There is an Avro Schema that will be used to record records
    above before writing them to kafka
  - The data written above will be consumed and set to FeatureBase
  - There is a list of PQL queries that can be used to determine if
    above went correctly or incorrectly

For test that require multiple iterations of " consumer data from kafka, write it to
FeatureBase, and run queries against it, "
*/
type ConsumerTest struct {
	name              string
	pathsToAvroSchema []string // code currently prepends values with ./testdata/schema/
	pathsToRecords    []string
	consumerConfigs   []ConsumerTestConfig
	index             string
	queries           [][]string
	expectedResults   [][]string
	pilosaHosts       string
	kafkaHost         string
	registryURL       string
}

func TestAddingRemovingData(t *testing.T) {
	//t.Parallel()
	/*
		at a high level, a test here represents
		  - an avro schema
		  - a set of records to ingest to kafka
		  - an ingest configuration
		  - query to run to confirm the data was ingest properly

		see test
	*/
	tests := []ConsumerTest{

		{ // confirm time quantums are being ingested
			name:              "time quantums exist",
			pathsToAvroSchema: []string{"timeQuantum.json"},
			pathsToRecords:    []string{"./testdata/records/timeQuantum.json"},
			pilosaHosts:       pilosaHost,
			registryURL:       registryHost,
			kafkaHost:         kafkaHost,
			consumerConfigs: []ConsumerTestConfig{
				{
					idType:    "string",
					keyFields: []string{"device"}, //can be multiple for idType: string but a single value otherwise
					topic:     "timequantums",
					delete:    false,
				},
			},
			index: "timequantums",
			queries: [][]string{
				{
					"Row(segment_ts='7R83')",
					"Row(segment_ts='7R83', from=\"2023-02-17T00:00\", to=\"2023-02-18T00:00\")",
					"Row(segment_ts='7R83', from=\"2023-02-16T00:00\", to=\"2023-02-17T00:00\")",
				},
			},
			expectedResults: [][]string{
				{
					"{\"results\":[{\"columns\":[],\"keys\":[\"0QKtSTqJYXMZWvVe\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"0QKtSTqJYXMZWvVe\"]}]}\n",
					"{\"results\":[{\"columns\":[]}]}\n",
				},
			},
		},
		{ // confirm all types are being ingest as they should
			name:              "all values can be inserted",
			pathsToAvroSchema: []string{"alltypes.json"},
			pathsToRecords:    []string{"./testdata/records/alltypes.json"},
			pilosaHosts:       pilosaHost,
			registryURL:       registryHost,
			kafkaHost:         kafkaHost,
			consumerConfigs: []ConsumerTestConfig{
				{
					idType:    "string",
					keyFields: []string{"pk0", "pk1", "pk2"},
					topic:     "alltypes",
					delete:    false,
				},
			},
			index: "alltypes",
			queries: [][]string{
				{
					"Count(All())",
					"Count(ConstRow(columns=['u2Yr4|sHaUv|x5z8P', 'DY2Ui|kUbdU|pjxqm']))",
					"Count(Row(stringset_string='58KIR'))",
					"Count(Row(string_string='8MGwy'))",
					"Count(Row(stringtq_string='ivWWb'))",
					"Count(Row(stringtq_string='ivWWb', to=\"2023-02-03\"))",
					"Count(Row(stringtq_string='ivWWb', from=\"2023-02-03\", to=\"2023-02-04\"))",
					"Count(Union(Row(stringset_bytes='eNKWF'),Row(stringset_bytes='5ptDx')))",
					"Row(string_bytes='vTwn4')",
					"Row(stringsettq_bytes='798ka')",
					"Row(stringsettq_bytes='798ka', from=\"2023-02-18\")",
					"Row(stringsettq_bytes='798ka', from=\"2023-02-16\", to=\"2023-02-18\")",
					"Intersect(Row(stringset_stringarray='u2Yr4'), Row(stringset_stringarray='PYE8V'), Row(stringset_stringarray='VBcyJ'), Row(stringset_stringarray='Chgzr'), Row(stringset_stringarray='DY2Ui'))",
					"Row(stringtq_stringarray='oxjI0', from=\"2023-01-29\", to=\"2023-01-31\")",
					"Intersect(Row(stringset_bytesarray='wNZ7o'), Row(stringset_bytesarray='OKNV2'),Row(stringset_bytesarray='F0uC4'),Row(stringset_bytesarray='VBcyJ'),Row(stringset_bytesarray='KMZnH'))",
					"Count(Row(idset_long=839))",
					"Count(Row(id_long=809))",
					"Count(Row(idtq_long=533))",
					"Count(Row(idtq_long=533, from=\"2020-01-01\"))",
					"Count(Row(idset_int=533))",
					"Count(Row(id_int=168))",
					"Count(Row(idsettq_int=113))",
					"Row(idsettq_int=113, to=\"2024-01-01\")",
					"Count(Intersect(Row(idset_longarray=399),Row(idset_longarray=322), Row(idset_longarray=975), Row(idset_longarray=730), Row(idset_longarray=969)))",
					"Count(Intersect(Row(idtq_longarray=172),Row(idtq_longarray=388), Row(idtq_longarray=731), Row(idtq_longarray=429), Row(idtq_longarray=730)))",
					"Count(Intersect(Row(idtq_longarray=172, from=\"2022-01-01\"),Row(idtq_longarray=388, from=\"2022-01-01\"), Row(idtq_longarray=731, from=\"2022-01-01\"), Row(idtq_longarray=429, from=\"2022-01-01\"), Row(idtq_longarray=730, from=\"2022-01-01\")))",
					"Count(Intersect(Row(idset_intarray=958),Row(idset_intarray=242), Row(idset_intarray=778), Row(idset_intarray=289), Row(idset_intarray=797)))",
					"Count(Row(int_long > 500))",
					"Count(Row(int_int > 500))",
					"Count(Row(decimal_bytes > 1000.00))",
					"Count(Row(decimal_float > 3.05))",
					"Count(Row(decimal_double > 4.11))",
					"Count(Row(dateint_bytes_ts > 1675163490))",
					"Count(Row(bools=bool_bool))",
					"Count(Not(Row(bools=bool_bool)))",
					"Count(Row(timestamp_bytes_ts > \"2023-02-20T00:00:00Z\"))",
					"Count(Row(timestamp_bytes_int > \"2023-02-20T00:00:00Z\"))",
				},
			},
			expectedResults: [][]string{
				{
					"{\"results\":[10]}\n",
					"{\"results\":[2]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[0]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[2]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"DY2Ui|kUbdU|pjxqm\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"9z4aw|5ptDx|CKs1F\"]}]}\n",
					"{\"results\":[{\"columns\":[]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"9z4aw|5ptDx|CKs1F\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"u2Yr4|sHaUv|x5z8P\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"yg8hY|tvNOB|byHh9\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"yg8hY|tvNOB|byHh9\"]}]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"6TKzc|YKLk9|h1iqc\"]}]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[4]}\n",
					"{\"results\":[5]}\n",
					"{\"results\":[4]}\n",
					"{\"results\":[3]}\n",
					"{\"results\":[3]}\n",
					"{\"results\":[8]}\n",
					"{\"results\":[4]}\n",
					"{\"results\":[6]}\n",
					"{\"results\":[3]}\n",
					"{\"results\":[3]}\n",
				},
			},
		},
		{ // confirm behavior of nulls.. when all null values passed, nothing should change
			name:              "all values can be inserted",
			pathsToAvroSchema: []string{"alltypes.json"},
			pathsToRecords:    []string{"./testdata/records/alltypes_null.json"},
			pilosaHosts:       pilosaHost,
			registryURL:       registryHost,
			kafkaHost:         kafkaHost,
			consumerConfigs: []ConsumerTestConfig{
				{
					idType:    "string",
					keyFields: []string{"pk0", "pk1", "pk2"},
					topic:     "alltypes_null",
					delete:    false,
				},
			},
			index: "alltypes_null",
			queries: [][]string{
				{
					//"Extract(All(), Rows(stringset_string), Rows(string_string), Rows(stringtq_string), Rows(stringset_bytes), Rows(string_bytes), Rows(stringsettq_bytes), Rows(stringset_stringarray), Rows(stringtq_stringarray), Rows(stringset_bytesarray), Rows(stringtq_bytesarray), Rows(idset_long), Rows(id_long), Rows(idtq_long), Rows(idset_int), Rows(id_int), Rows(idsettq_int), Rows(idset_longarray), Rows(idtq_longarray), Rows(idset_intarray), Rows(stringtq_stringarray), Rows(stringset_bytesarray), Rows(stringtq_bytesarray), Rows(idset_long), Rows(id_long), Rows(idtq_long), Rows(idset_int), Rows(id_int), Rows(idsettq_int), Rows(idset_longarray), Rows(idtq_longarray), Rows(idset_intarray), Rows(int_long), Rows(int_int), Rows(decimal_bytes), Rows(decimal_float), Rows(decimal_double), Rows(dateint_bytes_ts), Rows(bools), Rows(timestamp_bytes_ts), Rows(timestamp_bytes_int))",
					"Count(All())",
					"Count(Row(stringset_string='7EYSp'))",
					"Count(Row(string_string='uirDR'))",
					"Count(Row(stringtq_string='Qylqq'))",
					"Count(Row(stringset_bytes='gL2Hg'))",
					"Count(Row(string_bytes='BmvHF'))",
					"Count(Row(stringsettq_bytes='798ka'))",
					"Count(Intersect(Row(stringset_stringarray='vbbuf'), Row(stringset_stringarray='VQs7y'), Row(stringset_stringarray='9z4aw'), Row(stringset_stringarray='h1iqc'), Row(stringset_stringarray='aQQxr')))",
					"Count(Intersect(Row(stringtq_stringarray='x5z8P'), Row(stringtq_stringarray='0UGJQ'), Row(stringtq_stringarray='58KIR'), Row(stringtq_stringarray='7EYSp'), Row(stringtq_stringarray='CKs1F')))",
					"Count(Intersect(Row(stringset_bytesarray='u2Yr4'), Row(stringset_bytesarray='tvNOB'), Row(stringset_bytesarray='iYeOV'), Row(stringset_bytesarray='ZgkOB'), Row(stringset_bytesarray='RPGAm')))",
					"Count(Intersect(Row(stringtq_bytesarray='BwqU2'), Row(stringtq_bytesarray='6iGIm'), Row(stringtq_bytesarray='fjQK2'), Row(stringtq_bytesarray='LBTEU'), Row(stringtq_bytesarray='C6xxn')))",
					"Count(Row(idset_long=647))",
					"Count(Row(id_long=792))",
					"Count(Row(idtq_long=676))",
					"Count(Row(idset_int=898))",
					"Count(Row(id_int=63))",
					"Count(Row(idsettq_int=890))",
					"Count(Intersect(Row(idset_longarray=442), Row(idset_longarray=167), Row(idset_longarray=230), Row(idset_longarray=344), Row(idset_longarray=733)))",
					"Count(Intersect(Row(idtq_longarray=385), Row(idtq_longarray=931), Row(idtq_longarray=157), Row(idtq_longarray=865), Row(idtq_longarray=394)))",
					"Count(Intersect(Row(idset_intarray=442), Row(idset_intarray=614), Row(idset_intarray=394), Row(idset_intarray=284), Row(idset_intarray=344)))",
					"Count(Row(int_long=584))",
					"Count(Row(int_int=344))",
					"Count(Row(decimal_bytes=1155.95))",
					"Count(Row(decimal_float=3.23))",
					"Count(Row(decimal_double=0.95))",
					"Count(Row(dateint_bytes_ts=1676534039))",
					"Count(Row(bools=bool_bool))",
					"Count(Row(timestamp_bytes_ts='2023-02-16T07:53:59Z'))",
					"Count(Row(timestamp_bytes_int=1676555639))",
				},
			},
			expectedResults: [][]string{
				{
					//"{\"results\":[{\"fields\":[{\"name\":\"stringset_string\",\"type\":\"[]string\"},{\"name\":\"string_string\",\"type\":\"string\"},{\"name\":\"stringtq_string\",\"type\":\"[]string\"},{\"name\":\"stringset_bytes\",\"type\":\"[]string\"},{\"name\":\"string_bytes\",\"type\":\"string\"},{\"name\":\"stringsettq_bytes\",\"type\":\"[]string\"},{\"name\":\"stringset_stringarray\",\"type\":\"[]string\"},{\"name\":\"stringtq_stringarray\",\"type\":\"[]string\"},{\"name\":\"stringset_bytesarray\",\"type\":\"[]string\"},{\"name\":\"stringtq_bytesarray\",\"type\":\"[]string\"},{\"name\":\"idset_long\",\"type\":\"[]uint64\"},{\"name\":\"id_long\",\"type\":\"uint64\"},{\"name\":\"idtq_long\",\"type\":\"[]uint64\"},{\"name\":\"idset_int\",\"type\":\"[]uint64\"},{\"name\":\"id_int\",\"type\":\"uint64\"},{\"name\":\"idsettq_int\",\"type\":\"[]uint64\"},{\"name\":\"idset_longarray\",\"type\":\"[]uint64\"},{\"name\":\"idtq_longarray\",\"type\":\"[]uint64\"},{\"name\":\"idset_intarray\",\"type\":\"[]uint64\"},{\"name\":\"stringtq_stringarray\",\"type\":\"[]string\"},{\"name\":\"stringset_bytesarray\",\"type\":\"[]string\"},{\"name\":\"stringtq_bytesarray\",\"type\":\"[]string\"},{\"name\":\"idset_long\",\"type\":\"[]uint64\"},{\"name\":\"id_long\",\"type\":\"uint64\"},{\"name\":\"idtq_long\",\"type\":\"[]uint64\"},{\"name\":\"idset_int\",\"type\":\"[]uint64\"},{\"name\":\"id_int\",\"type\":\"uint64\"},{\"name\":\"idsettq_int\",\"type\":\"[]uint64\"},{\"name\":\"idset_longarray\",\"type\":\"[]uint64\"},{\"name\":\"idtq_longarray\",\"type\":\"[]uint64\"},{\"name\":\"idset_intarray\",\"type\":\"[]uint64\"}],\"columns\":[{\"column\":\"9z4aw|5ptDx|CKs1F\",\"rows\":[[\"7EYSp\"],\"uirDR\",[\"Qylqq\"],[\"gL2Hg\"],\"BmvHF\",[\"798ka\"],[\"9z4aw\",\"h1iqc\",\"aQQxr\",\"vbbuf\",\"VQs7y\"],[\"7EYSp\",\"CKs1F\",\"x5z8P\",\"0UGJQ\",\"58KIR\"],[\"u2Yr4\",\"tvNOB\",\"iYeOV\",\"ZgkOB\",\"RPGAm\"],[\"BwqU2\",\"6iGIm\",\"fjQK2\",\"LBTEU\",\"C6xxn\"],[647],792,[676],[898],63,[890],[167,230,344,442,733],[157,385,394,865,931],[284,344,394,442,614],[\"7EYSp\",\"CKs1F\",\"x5z8P\",\"0UGJQ\",\"58KIR\"],[\"u2Yr4\",\"tvNOB\",\"iYeOV\",\"ZgkOB\",\"RPGAm\"],[\"BwqU2\",\"6iGIm\",\"fjQK2\",\"LBTEU\",\"C6xxn\"],[647],792,[676],[898],63,[890],[167,230,344,442,733],[157,385,394,865,931],[284,344,394,442,614]]}]}]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
				},
			},
		}, /*
			{
				name: "all values can be deleted",
				pathsToAvroSchema: []string{
					"alltypes.json",
					"alltypes_delete_fields.json",
				},
				pathsToRecords: []string{
					"./testdata/records/alltypes.json",
					"./testdata/records/alltypes_delete_fields.json",
				},
				pilosaHosts: pilosaHost,
				registryURL: registryHost,
				kafkaHost:   kafkaHost,
				consumerConfigs: []ConsumerTestConfig{
					{
						idType:    "string",
						keyFields: []string{"pk0", "pk1", "pk2"},
						topic:     "alltypes",
						delete:    false,
					},
					{
						idType:    "string",
						keyFields: []string{"pk0", "pk1", "pk2"},
						topic:     "alltypes_delete_fields",
						delete:    true,
					},
				},
				index: "alltypes_delete",
				queries: [][]string{
					[]string{
						"Count(All())",
						"Count(ConstRow(columns=['u2Yr4|sHaUv|x5z8P', 'DY2Ui|kUbdU|pjxqm']))",
						"Count(Row(stringset_string='58KIR'))",
						"Count(Row(string_string='8MGwy'))",
						"Count(Row(stringtq_string='ivWWb'))",
						"Count(Row(stringtq_string='ivWWb', to=\"2023-02-03\"))",
						"Count(Row(stringtq_string='ivWWb', from=\"2023-02-03\", to=\"2023-02-04\"))",
						"Count(Union(Row(stringset_bytes='eNKWF'),Row(stringset_bytes='5ptDx')))",
						"Row(string_bytes='vTwn4')",
						"Row(stringsettq_bytes='798ka')",
						"Row(stringsettq_bytes='798ka', from=\"2023-02-18\")",
						"Row(stringsettq_bytes='798ka', from=\"2023-02-16\", to=\"2023-02-18\")",
						"Intersect(Row(stringset_stringarray='u2Yr4'), Row(stringset_stringarray='PYE8V'), Row(stringset_stringarray='VBcyJ'), Row(stringset_stringarray='Chgzr'), Row(stringset_stringarray='DY2Ui'))",
						"Row(stringtq_stringarray='oxjI0', from=\"2023-01-29\", to=\"2023-01-31\")",
						"Intersect(Row(stringset_bytesarray='wNZ7o'), Row(stringset_bytesarray='OKNV2'),Row(stringset_bytesarray='F0uC4'),Row(stringset_bytesarray='VBcyJ'),Row(stringset_bytesarray='KMZnH'))",
						"Count(Row(idset_long=839))",
						"Count(Row(id_long=809))",
						"Count(Row(idtq_long=533))",
						"Count(Row(idtq_long=533, from=\"2020-01-01\"))",
						"Count(Row(idset_int=533))",
						"Count(Row(id_int=168))",
						"Count(Row(idsettq_int=113))",
						"Row(idsettq_int=113, to=\"2024-01-01\")",
						"Count(Intersect(Row(idset_longarray=399),Row(idset_longarray=322), Row(idset_longarray=975), Row(idset_longarray=730), Row(idset_longarray=969)))",
						"Count(Intersect(Row(idtq_longarray=172),Row(idtq_longarray=388), Row(idtq_longarray=731), Row(idtq_longarray=429), Row(idtq_longarray=730)))",
						"Count(Intersect(Row(idtq_longarray=172, from=\"2022-01-01\"),Row(idtq_longarray=388, from=\"2022-01-01\"), Row(idtq_longarray=731, from=\"2022-01-01\"), Row(idtq_longarray=429, from=\"2022-01-01\"), Row(idtq_longarray=730, from=\"2022-01-01\")))",
						"Count(Intersect(Row(idset_intarray=958),Row(idset_intarray=242), Row(idset_intarray=778), Row(idset_intarray=289), Row(idset_intarray=797)))",
						"Count(Row(int_long > 500))",
						"Count(Row(int_int > 500))",
						"Count(Row(decimal_bytes > 1000.00))",
						"Count(Row(decimal_float > 3.05))",
						"Count(Row(decimal_double > 4.11))",
						"Count(Row(dateint_bytes_ts > 1675163490))",
						"Count(Row(bools=bool_bool))",
						"Count(Not(Row(bools=bool_bool)))",
						"Count(Row(timestamp_bytes_ts > \"2023-02-20T00:00:00Z\"))",
						"Count(Row(timestamp_bytes_int > \"2023-02-20T00:00:00Z\"))",
					},
					{
						// TODO: missing tests for timestamps and time fields, they don't see supported
						"Count(All())",
						"Row(int_long=null)",
						"Count(UnionRows(Rows(stringset_stringarray)))",
						"Not(UnionRows(Rows(stringset_stringarray), Rows(string_string),Rows(stringset_bytes),Rows(string_bytes),Rows(stringset_stringarray),Rows(stringset_bytesarray),Rows(idset_long),Rows(id_long),Rows(idset_int),Rows(id_int),Rows(idset_longarray),Rows(idset_intarray)))",
						"Row(int_int=null)",
						"Row(decimal_bytes=null)",
						"Row(decimal_float=null)",
						"Count(Row(bools=bool_bool))",
						"Count(Not(Row(bools-exists=bool_bool)))",
						"Count(Row(dateint_bytes_ts=null))",
					},
				},
				expectedResults: [][]string{
					[]string{
						"{\"results\":[10]}\n",
						"{\"results\":[2]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[0]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[2]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"DY2Ui|kUbdU|pjxqm\"]}]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"9z4aw|5ptDx|CKs1F\"]}]}\n",
						"{\"results\":[{\"columns\":[]}]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"9z4aw|5ptDx|CKs1F\"]}]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"u2Yr4|sHaUv|x5z8P\"]}]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"yg8hY|tvNOB|byHh9\"]}]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"yg8hY|tvNOB|byHh9\"]}]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"6TKzc|YKLk9|h1iqc\"]}]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[4]}\n",
						"{\"results\":[5]}\n",
						"{\"results\":[4]}\n",
						"{\"results\":[3]}\n",
						"{\"results\":[3]}\n",
						"{\"results\":[8]}\n",
						"{\"results\":[4]}\n",
						"{\"results\":[6]}\n",
						"{\"results\":[3]}\n",
						"{\"results\":[3]}\n",
					},
					[]string{
						"{\"results\":[10]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"9z4aw|5ptDx|CKs1F\"]}]}\n",
						"{\"results\":[9]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"6TKzc|YKLk9|h1iqc\"]}]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"9z4aw|5ptDx|CKs1F\"]}]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"RKE3c|6TKzc|RKE3c\"]}]}\n",
						"{\"results\":[{\"columns\":[],\"keys\":[\"RKE3c|6TKzc|RKE3c\"]}]}\n",
						"{\"results\":[3]}\n",
						"{\"results\":[1]}\n",
						"{\"results\":[1]}\n",
					},
				},
			},*/
		{
			name: "all values can be deleted",
			pathsToAvroSchema: []string{
				"alltypes.json",
				"alltypes_delete_fields.json",
				"alltypes_delete_records.json",
				"alltypes_delete_value.json",
			},
			pathsToRecords: []string{
				"./testdata/records/alltypes.json",
				"./testdata/records/alltypes_delete_fields.json",
				"./testdata/records/alltypes_delete_records.json",
				"./testdata/records/alltypes_delete_value.json",
			},
			pilosaHosts: pilosaHost,
			registryURL: registryHost,
			kafkaHost:   kafkaHost,
			//pilosaHosts: "localhost:10101",
			//registryURL: "localhost:8081",
			//kafkaHost:   "localhost:9092",
			consumerConfigs: []ConsumerTestConfig{
				{
					idType:    "string",
					keyFields: []string{"pk0", "pk1", "pk2"},
					topic:     "alltypes",
					delete:    false,
				},
				{
					idType:    "string",
					keyFields: []string{"pk0", "pk1", "pk2"},
					topic:     "alltypes_delete_fields",
					delete:    true,
				},
				{
					idType:    "string",
					keyFields: []string{"pk0", "pk1", "pk2"},
					topic:     "alltypes_delete_record",
					delete:    true,
				},
				{
					idType:    "string",
					keyFields: []string{"pk0", "pk1", "pk2"},
					topic:     "alltypes_delete_values",
					delete:    true,
				},
			},
			index: "alltypes_delete",
			queries: [][]string{
				{
					"Count(All())",
					"Count(ConstRow(columns=['u2Yr4|sHaUv|x5z8P', 'DY2Ui|kUbdU|pjxqm']))",
					"Count(Row(stringset_string='58KIR'))",
					"Count(Row(string_string='8MGwy'))",
					"Count(Row(stringtq_string='ivWWb'))",
					"Count(Row(stringtq_string='ivWWb', to=\"2023-02-03\"))",
					"Count(Row(stringtq_string='ivWWb', from=\"2023-02-03\", to=\"2023-02-04\"))",
					"Count(Union(Row(stringset_bytes='eNKWF'),Row(stringset_bytes='5ptDx')))",
					"Row(string_bytes='vTwn4')",
					"Row(stringsettq_bytes='798ka')",
					"Row(stringsettq_bytes='798ka', from=\"2023-02-18\")",
					"Row(stringsettq_bytes='798ka', from=\"2023-02-16\", to=\"2023-02-18\")",
					"Intersect(Row(stringset_stringarray='u2Yr4'), Row(stringset_stringarray='PYE8V'), Row(stringset_stringarray='VBcyJ'), Row(stringset_stringarray='Chgzr'), Row(stringset_stringarray='DY2Ui'))",
					"Row(stringtq_stringarray='oxjI0', from=\"2023-01-29\", to=\"2023-01-31\")",
					"Intersect(Row(stringset_bytesarray='wNZ7o'), Row(stringset_bytesarray='OKNV2'),Row(stringset_bytesarray='F0uC4'),Row(stringset_bytesarray='VBcyJ'),Row(stringset_bytesarray='KMZnH'))",
					"Count(Row(idset_long=839))",
					"Count(Row(id_long=809))",
					"Count(Row(idtq_long=533))",
					"Count(Row(idtq_long=533, from=\"2020-01-01\"))",
					"Count(Row(idset_int=533))",
					"Count(Row(id_int=168))",
					"Count(Row(idsettq_int=113))",
					"Row(idsettq_int=113, to=\"2024-01-01\")",
					"Count(Intersect(Row(idset_longarray=399),Row(idset_longarray=322), Row(idset_longarray=975), Row(idset_longarray=730), Row(idset_longarray=969)))",
					"Count(Intersect(Row(idtq_longarray=172),Row(idtq_longarray=388), Row(idtq_longarray=731), Row(idtq_longarray=429), Row(idtq_longarray=730)))",
					"Count(Intersect(Row(idtq_longarray=172, from=\"2022-01-01\"),Row(idtq_longarray=388, from=\"2022-01-01\"), Row(idtq_longarray=731, from=\"2022-01-01\"), Row(idtq_longarray=429, from=\"2022-01-01\"), Row(idtq_longarray=730, from=\"2022-01-01\")))",
					"Count(Intersect(Row(idset_intarray=958),Row(idset_intarray=242), Row(idset_intarray=778), Row(idset_intarray=289), Row(idset_intarray=797)))",
					"Count(Row(int_long > 500))",
					"Count(Row(int_int > 500))",
					"Count(Row(decimal_bytes > 1000.00))",
					"Count(Row(decimal_float > 3.05))",
					"Count(Row(decimal_double > 4.11))",
					"Count(Row(dateint_bytes_ts > 1675163490))",
					"Count(Row(bools=bool_bool))",
					"Count(Not(Row(bools=bool_bool)))",
					"Count(Row(timestamp_bytes_ts > \"2023-02-20T00:00:00Z\"))",
					"Count(Row(timestamp_bytes_int > \"2023-02-20T00:00:00Z\"))",
				},
				{
					// TODO: missing tests for timestamps and time fields, they don't see supported
					"Count(All())",
					"Row(int_long=null)",
					"Count(UnionRows(Rows(stringset_stringarray)))",
					"Not(UnionRows(Rows(stringset_stringarray), Rows(string_string),Rows(stringset_bytes),Rows(string_bytes),Rows(stringset_stringarray),Rows(stringset_bytesarray),Rows(idset_long),Rows(id_long),Rows(idset_int),Rows(id_int),Rows(idset_longarray),Rows(idset_intarray)))",
					"Row(int_int=null)",
					"Row(decimal_bytes=null)",
					"Row(decimal_float=null)",
					"Count(Row(bools=bool_bool))",
					"Count(Not(Row(bools-exists=bool_bool)))",
					"Count(Row(dateint_bytes_ts=null))",
				},
				{
					"Count(All())",
				},
				{
					"Row(string_string=\"ZgkOB\")",
					"Row(stringset_string =\"7EYSp\")",
					"Count(Not(UnionRows(Rows(stringset_string))))",
					"Intersect(Not(Intersect(Row(stringset_stringarray=\"u2Yr4\"),Row(stringset_stringarray=\"PYE8V\"), Row(stringset_stringarray=\"VBcyJ\"))), Intersect(Row(stringset_string=\"Chgzr\"), Row(stringset_stringarray=\"DY2Ui\")))",
					"Count(Intersect(ConstRow(columns=[\"u2Yr4|sHaUv|x5z8P\"]), Not(UnionRows(Rows(idset_int)))))",
					"Count(Intersect(ConstRow(columns=[\"u2Yr4|sHaUv|x5z8P\"]), Not(UnionRows(Rows(id_int)))))",
					"Row(int_int=969)",
					"Count(Intersect(ConstRow(columns=[\"u2Yr4|sHaUv|x5z8P\"]), Not(UnionRows(Rows(bools-exists)))))",
					"Count(Intersect(ConstRow(columns=[\"u2Yr4|sHaUv|x5z8P\"]), Row(decimal_double=null)))",
					"Count(Intersect(ConstRow(columns=[\"u2Yr4|sHaUv|x5z8P\"]), Not(Row(dateint_bytes_ts=null))))",
					"Count(Intersect(ConstRow(columns=[\"u2Yr4|sHaUv|x5z8P\"]), Row(timestamp_bytes_int=null)))",
				},
			},
			expectedResults: [][]string{
				{
					"{\"results\":[10]}\n",
					"{\"results\":[2]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[0]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[2]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"DY2Ui|kUbdU|pjxqm\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"9z4aw|5ptDx|CKs1F\"]}]}\n",
					"{\"results\":[{\"columns\":[]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"9z4aw|5ptDx|CKs1F\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"u2Yr4|sHaUv|x5z8P\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"yg8hY|tvNOB|byHh9\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"yg8hY|tvNOB|byHh9\"]}]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"6TKzc|YKLk9|h1iqc\"]}]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[4]}\n",
					"{\"results\":[5]}\n",
					"{\"results\":[4]}\n",
					"{\"results\":[3]}\n",
					"{\"results\":[3]}\n",
					"{\"results\":[8]}\n",
					"{\"results\":[4]}\n",
					"{\"results\":[6]}\n",
					"{\"results\":[3]}\n",
					"{\"results\":[3]}\n",
				},
				{
					"{\"results\":[10]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"9z4aw|5ptDx|CKs1F\"]}]}\n",
					"{\"results\":[9]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"6TKzc|YKLk9|h1iqc\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"9z4aw|5ptDx|CKs1F\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"RKE3c|6TKzc|RKE3c\"]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"RKE3c|6TKzc|RKE3c\"]}]}\n",
					"{\"results\":[3]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
				},
				{
					"{\"results\":[7]}\n",
				},
				{
					"{\"results\":[{\"columns\":[]}]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"u2Yr4|sHaUv|x5z8P\"]}]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[{\"columns\":[]}]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[{\"columns\":[],\"keys\":[\"u2Yr4|sHaUv|x5z8P\"]}]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
					"{\"results\":[1]}\n",
				},
			},
		},
	}

	for _, test := range tests {

		// define some vars
		now := time.Now().UnixNano()
		//index := fmt.Sprintf("%s_%d", test.index, now)
		index := test.index

		// so topics of the same name end up being the same name after
		// adding a timestamp to make sure they're unique
		var topics map[string]string
		for _, iteration := range test.consumerConfigs {
			if topicName, ok := topics[iteration.topic]; ok {
				iteration.topic = topicName
			} else {
				newTopicName := fmt.Sprintf("%s_%d", iteration.topic, now)
				iteration.topic = newTopicName

			}
		}

		// iterate through tests (one per consumer config)
		for i, iteration := range test.consumerConfigs {
			// read in records
			records, err := kafkaRecordFromFile(test.pathsToRecords[i])
			if err != nil {
				t.Errorf("%s", err)
			}

			// load schema registry, create and produce to the topic
			writeRecordsToKafka(t, test.pathsToAvroSchema[i], iteration.topic, test.registryURL, test.kafkaHost, records)

			// configure the consumer
			consumer, err := NewMain()
			if err != nil {
				t.Fatalf("creating main %v", err)
			}
			configureTestFlags(consumer)
			consumer.Index = index
			consumer.Topics = []string{iteration.topic}
			consumer.KafkaBootstrapServers = []string{test.kafkaHost}
			consumer.SchemaRegistryURL = test.registryURL
			consumer.Delete = iteration.delete
			switch iteration.idType {
			case "id":
				consumer.IDField = iteration.keyFields[0]
			case "string":
				consumer.PrimaryKeyFields = iteration.keyFields
			case "generate":
				consumer.AutoGenerate = true
				consumer.ExternalGenerate = true
			default:
				t.Errorf("incorrect idType supplied")
			}
			consumer.MaxMsgs = uint64(len(records))
			pilosaHostsNew := strings.Split(test.pilosaHosts, ",")
			consumer.PilosaHosts = pilosaHostsNew
			//remove after flipping back to docker
			host := strings.Split(pilosaHostsNew[0], ":")
			consumer.PilosaGRPCHosts = []string{fmt.Sprintf("%s:20101", host[0])}

			// consumer records
			err = consumer.Run()
			if err != nil {
				t.Fatalf("running consumer: %v", err)
			}

			// now run queries and confirm the data is as expected
			client := consumer.PilosaClient()
			runTestQueries(t, index, test.queries[i], test.expectedResults[i], client)
		}

	}
}

func runTestQueries(t *testing.T, index string, queries, expectedResults []string, client *pilosaclient.Client) {
	for i, q := range queries {
		status, body, err := client.HTTPRequest("POST", fmt.Sprintf("/index/%s/query", index), []byte(q), nil)
		if err != nil {
			t.Fatalf("querying featurebase: status: %d, response: %s, error: %s: on query: %s", status, body, err, q)
		}
		if string(body[:]) != expectedResults[i] {
			t.Fatalf("running query %s against index %s. expected %s but got %s", q, index, expectedResults[i], body)
		}
	}
}

func kafkaRecordFromFile(pathToRecords string) (records []map[string]interface{}, err error) {
	var data map[string]interface{}

	recordsFile, err := os.Open(pathToRecords)
	if err != nil {
		return nil, fmt.Errorf("opening records file")
	}
	defer recordsFile.Close()

	s := bufio.NewScanner(recordsFile)
	for s.Scan() {
		err := json.Unmarshal(s.Bytes(), &data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal json: %s", err)
		}
		records = append(records, data)
		data = make(map[string]interface{})
	}
	return records, nil
}

func writeRecordsToKafka(t *testing.T, pathToAvroSchema, topic, schemaRegistryURL, kafkaHost string, records []map[string]interface{}) {
	licodec := liDecodeTestSchema(t, pathToAvroSchema)
	schemaID := postSchema(t, pathToAvroSchema, fmt.Sprintf("%s_id", topic), schemaRegistryURL, nil)
	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers": kafkaHost,
	})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()
	tCreateTopic(t, topic, p)

	tPutRecordsKafka(t, p, topic, schemaID, licodec, "akey", records...)
}

type sortableCRI []pilosaclient.CountResultItem

func (s sortableCRI) Len() int { return len(s) }
func (s sortableCRI) Less(i, j int) bool {
	if s[i].Count != s[j].Count {
		return s[i].Count > s[j].Count
	}
	if s[i].ID != s[j].ID {
		return s[i].ID < s[j].ID
	}
	if s[i].Key != s[j].Key {
		return s[i].Key < s[j].Key
	}
	return true
}

func (s sortableCRI) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func makeRecord(t *testing.T, fields []string, vals []interface{}) map[string]interface{} {
	if len(fields) != len(vals) {
		t.Fatalf("have %d fields and %d vals", len(fields), len(vals))
	}
	ret := make(map[string]interface{})
	for i, field := range fields {
		ret[field] = vals[i]
	}
	return ret
}

func tDoHTTPPost(t *testing.T, url, contentType, body string) string {
	resp, err := http.Post(url, contentType, strings.NewReader(body))
	if err != nil {
		t.Fatalf("making POST request: %v", err)
	}

	bod, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading POST response bdoy: %v", err)
	}

	return string(bod)
}
