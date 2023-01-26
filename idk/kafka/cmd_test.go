//go:build !kafka_sasl
// +build !kafka_sasl

package kafka

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
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
		kafkaHost = "kafka:9092"
	}
	if registryHost, ok = os.LookupEnv("IDK_TEST_REGISTRY_HOST"); !ok {
		registryHost = "schema-registry:8081"
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
	m.KafkaGroupInstanceId = "instId"
	m.KafkaMaxPollInterval = "30"
	m.KafkaSessionTimeout = "20"
	m.KafkaSocketKeepaliveEnable = "true"

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
	if val, err := cfg.Get("group.instance.id", nil); err != nil || val.(string) != "isntId" {
		t.Fatalf("unexpected group.instance.id val: %v, err: %v", val, err)
	}
	if val, err := cfg.Get("max.poll.interval.ms", nil); err != nil || val.(string) != "30" {
		t.Fatalf("unexpected val for max.poll.interval.ms val: %v, err: %v", val, err)
	}
	if val, err := cfg.Get("session.timeout.ms", nil); err != nil || val.(string) != "20" {
		t.Fatalf("unexpected val for session.timeout.ms val: %v, err: %v", val, err)
	}
	if val, err := cfg.Get("socket.keepalive.enable", nil); err != nil || val.(string) != "true" {
		t.Fatalf("unexpected val for socket.keepalive.enable val: %v, err: %v", val, err)
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

/*
In some cases, when we see max.poll.interval.ms exceeded on the kafka consumer,
we also see the consumer hang when it's trying to close properly. See SUP-288.

This test:
 1. configures the consumer such that it will hang if max.poll.interval.ms is exceeded.
 2. checks that
    a. the consumer did leave the group due to exceeding max.poll.interval.ms
    b. we close the consumer and log that we weren't able to confirm it closed properly

It could be possible this test fails even though there isn't necessarily. This would be
the case if a fix comes along to the kafka-go lib that prevents the consumer from hanging
during the call it's consumer.Close() func.
*/
func TestCloseTimeout(t *testing.T) {

	// produce messages
	// wait for completion
	// start consumer
	// sleep 10
	// take lock
	// produce messages

	//define some vars
	source := "bank"
	target := "kafka"
	now := time.Now().UnixNano()
	index := fmt.Sprintf("close_timeout_%d", now)
	topic := fmt.Sprintf("close_timeout_%d", now)
	subject := fmt.Sprintf("close_timeout_%d_subject", now)
	log_path := fmt.Sprintf("./consumer_%d.log", now)

	// configure the consumer
	consumer, err := NewMain()
	if err != nil {
		t.Fatalf("issues creating a consumer")
	}
	configureTestFlags(consumer)
	consumer.Topics = []string{topic}
	consumer.BatchSize = 1000
	consumer.IDField = "user_id"
	consumer.KafkaGroupInstanceId = "some_id"
	consumer.ConsumerCloseTimeout = 3
	consumer.KafkaMaxPollInterval = "15000"
	consumer.MaxMsgs = 10000
	consumer.Index = index
	consumer.KafkaSessionTimeout = "10000"
	consumer.LogPath = log_path

	// run datagen and caputre stdout and stderr in case of error
	var datagen_0_stdout bytes.Buffer
	var datagen_0_stderr bytes.Buffer
	cmdString := fmt.Sprintf("go run ../cmd/datagen/main.go --source %s --target %s --kafka.topic %s --kafka.subject %s --end-at 5000 --kafka.confluent-command.schema-registry-url %s --kafka.confluent-command.kafka-bootstrap-servers %s", source, target, topic, subject, consumer.SchemaRegistryURL, consumer.KafkaBootstrapServers[0])
	cmd := exec.Command("bash", "-c", cmdString)
	cmd.Stdout = &datagen_0_stdout
	cmd.Stderr = &datagen_0_stderr
	err = cmd.Run()
	if err != nil {
		t.Errorf("stdout: %s", datagen_0_stdout.String())
		t.Errorf("stderr: %s", datagen_0_stderr.String())
		t.Fatalf("issue generating records for kafka: %s", err)
	}

	consumerError := make(chan error)
	go func() {
		consumerErr := consumer.Run()
		consumerError <- consumerErr
	}()

	//wait some time for ingest - takes ~5 seconds locally
	time.Sleep(5 * time.Second)

	// take a transaction lock on the database which drives
	// max.poll.interval.ms timeout to occur
	client := consumer.PilosaClient()
	status, body, err := client.HTTPRequest("POST", "/transaction", []byte("{\"timeout\": 20, \"exclusive\": true, \"active\": true}"), nil)
	if err != nil {
		t.Fatalf("error taking transaction lock")
	}
	if status != 200 || !strings.Contains(string(body), "active\":true") {
		t.Fatalf("was unable to successfully take transaction lock: %s", string(body))
	}

	// produce to kafka again capturing stdout and stderr in case of error
	cmd = exec.Command("bash", "-c", cmdString)

	var datagen_1_stdout bytes.Buffer
	var datagen_1_stderr bytes.Buffer

	cmd.Stdout = &datagen_1_stdout
	cmd.Stderr = &datagen_1_stderr
	err = cmd.Run()
	if err != nil {
		t.Errorf("stdout: %s", datagen_1_stdout.String())
		t.Errorf("stderr: %s", datagen_1_stderr.String())
		t.Fatalf("issue generating records for kafka: %s", err)
	}

	select {
	case <-consumerError:
		buf, err := ioutil.ReadFile(log_path)
		if err != nil {
			t.Errorf("issue reading consumer log: %s", err)
		}
		s := string(buf)
		if !strings.Contains(s, "Application maximum poll interval") && !strings.Contains(s, "Unable to properly close consumer") {
			t.Errorf("the consumer did not exceed max.poll.interval.ms or the consumer was closed properly when it shouldn't have")
		}
	case <-time.After(30 * time.Second):
		// wait an extra 10 seconds
		t.Errorf("the consumer is hanging for longer that it should")
	}
}
