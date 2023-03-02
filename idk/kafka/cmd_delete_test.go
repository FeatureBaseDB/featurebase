//go:build !kafka_sasl
// +build !kafka_sasl

package kafka

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	pilosaclient "github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/jaffee/commandeer/pflag"
	"github.com/pkg/errors"
)

func TestDeleteConsumerParsesMultipleGRPCHosts(t *testing.T) {
	t.Parallel()

	m, err := NewMain()
	if err != nil {
		t.Fatalf("Creating Main: %v", err)

	}
	os.Setenv("IDK_TEST_PILOSA_GRPC_HOSTS", "pilosa:20103,pilosa:20102")

	if err := pflag.LoadEnv(m, "IDK_TEST_", nil); err != nil {
		t.Fatalf("loading environment: %v", err)
	}

	if len(m.PilosaGRPCHosts) != 2 {
		t.Errorf("expected to parse 2 values but got %v\n", len(m.PilosaGRPCHosts))
	}

}

func TestDeleteConsumerCompoundStringKey(t *testing.T) {
	t.Parallel()

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()
	topic := strconv.Itoa(a)

	m, err := NewMain()
	if err != nil {
		t.Fatalf("loading environment: %v", err)
	}
	configureTestFlags(m)
	m.Delete = true
	m.Index = fmt.Sprintf("cmd_del_comp_indexij%s", topic)
	m.PrimaryKeyFields = []string{"abc", "db", "user_id"}
	m.Topics = []string{topic}
	m.PackBools = "bools"
	//m.KafkaBootstrapServers = []string{"localhost:9092"}
	//m.PilosaHosts = []string{"localhost:10101"}
	//m.PilosaGRPCHosts = []string{"localhost:20101"}
	//m.SchemaRegistryURL = "localhost:8081"

	defer func() {
		// TODO: for some reason (which I didn't dig into),
		// this panics because m.PilosaClient() is nil. But
		// it seems to only happen when running the full test
		// (i.e. `make test`). If you just run this one test
		// via `go test -v ./kafka -run TestDeleteConsumerCompoundStringKey`
		// then the m.PilosaClient() is not nil.
		if pclient := m.PilosaClient(); pclient != nil {
			if err := pclient.DeleteIndexByName(m.Index); err != nil {
				t.Logf("deleting test index: %v", err)
			}
		}
	}()

	tlsConf, err := idk.GetTLSConfig(nil, logger.NopLogger)
	if err != nil {
		t.Fatalf("getting tls config: %v", err)
	}
	licodec := liDecodeTestSchema(t, "delete_string.json")
	schemaID := postSchema(t, "delete_string.json", "delete_string", m.SchemaRegistryURL, tlsConf)
	// Create Producer instance
	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers": kafkaHost,
	})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	fields := []string{"abc", "db", "user_id", "fields"}
	records := [][]interface{}{
		{"aba", "db", 9, []string{"a", "bint", "cmutex", "mutstr", "bools|bf1", "bools|bf2", "setkeys", "bools-exists", "dec"}},
	}
	m.MaxMsgs = uint64(len(records))

	for _, vals := range records {
		rec := makeRecord(t, fields, vals)
		tPutRecordsKafka(t, p, topic, schemaID, licodec, "akey", rec)
	}

	client, err := pilosaclient.NewClient(m.PilosaHosts)
	if err != nil {
		t.Fatalf("getting new client: %v", err)
	}
	sch, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	idx := sch.Index(m.Index, pilosaclient.OptIndexKeys(true))
	fld := idx.Field("a", pilosaclient.OptFieldTypeSet(pilosaclient.CacheTypeRanked, 100))
	bfld := idx.Field("bint", pilosaclient.OptFieldTypeInt())
	cfld := idx.Field("cmutex", pilosaclient.OptFieldTypeMutex(pilosaclient.CacheTypeNone, 0))
	mutStr := idx.Field("mutstr", pilosaclient.OptFieldTypeMutex(pilosaclient.CacheTypeNone, 0), pilosaclient.OptFieldKeys(true))
	bools := idx.Field("bools", pilosaclient.OptFieldTypeSet(pilosaclient.CacheTypeRanked, 100), pilosaclient.OptFieldKeys(true))
	boolsExists := idx.Field("bools-exists", pilosaclient.OptFieldTypeSet(pilosaclient.CacheTypeRanked, 100), pilosaclient.OptFieldKeys(true))
	setKeys := idx.Field("setkeys", pilosaclient.OptFieldTypeSet(pilosaclient.CacheTypeRanked, 100), pilosaclient.OptFieldKeys(true))
	decFld := idx.Field("dec", pilosaclient.OptFieldTypeDecimal(2))
	err = client.SyncSchema(sch)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	_, err = client.Query(idx.BatchQuery(
		fld.Set(2, "aba|db|9"),
		fld.Set(5, "aba|db|9"),
		bfld.Set(15, "aba|db|9"),
		cfld.Set(3, "aba|db|9"),
		mutStr.Set("aval", "aba|db|9"),
		bools.Set("bf1", "aba|db|9"), // bf1 is true
		boolsExists.Set("bf1", "aba|db|9"),
		boolsExists.Set("bf2", "aba|db|9"), // bf2 is false
		setKeys.Set("2", "aba|db|9"),
		setKeys.Set("5", "aba|db|9"),
		decFld.Set(2222, "aba|db|9"), // TODO: Set doesn't do the decimal scale conversion — Pilosa doesn't handle decimals in Set call.
	))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	query := idx.BatchQuery(
		fld.Row(2),
		fld.Row(5),
		bfld.GT(0),
		cfld.Row(3),
		mutStr.Row("aval"),
		bools.Row("bf1"),
		boolsExists.Row("bf1"),
		boolsExists.Row("bf2"),
		setKeys.Row("2"),
		setKeys.Row("5"),
		decFld.GT(22.21),
	)
	resp, err := client.Query(query)
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	for i, res := range resp.Results() {
		row := res.Row().Keys
		if len(row) != 1 {
			t.Errorf("expected to get a single result after setting for %d, got %v", i, row)
		}
	}

	err = m.Run()
	if err != nil {
		t.Fatalf("running main: %v", err)
	}

	resp, err = client.Query(query)
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	for i, res := range resp.Results() {
		row := res.Row().Keys
		if len(row) != 0 {
			t.Errorf("expected no results after clearing for %d, got %v", i, row)
		}
	}

}

func TestDeleteConsumer(t *testing.T) {
	t.Parallel()

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()
	topic := strconv.Itoa(a)

	m, err := NewMain()
	if err != nil {
		t.Fatalf("creating main %v", err)
	}
	configureTestFlags(m)
	m.Timeout = 0
	m.Delete = true
	m.Index = fmt.Sprintf("cmd_del_index239ij%s", topic)
	m.IDField = "pk"
	m.Topics = []string{topic}
	//m.KafkaBootstrapServers = []string{"localhost:9092"}
	//m.PilosaHosts = []string{"localhost:10101"}
	//m.PilosaGRPCHosts = []string{"localhost:20101"}
	//m.SchemaRegistryURL = "localhost:8081"
	defer func() {
		client := m.PilosaClient()
		if client == nil {
			return
		}
		if err := client.DeleteIndexByName(m.Index); err != nil {
			t.Logf("deleting test index: %v", err)
		}
	}()

	tlsConf, err := idk.GetTLSConfig(nil, logger.NopLogger)
	if err != nil {
		t.Fatalf("getting tls config: %v", err)
	}
	licodec := liDecodeTestSchema(t, "delete.json")
	schemaID := postSchema(t, "delete.json", "delete", m.SchemaRegistryURL, tlsConf)
	// Create Producer instance
	p, err := confluent.NewProducer(&confluent.ConfigMap{
		"bootstrap.servers": kafkaHost,
	})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()
	// put records in kafka
	tCreateTopic(t, topic, p)

	fields := []string{"pk", "fields"}
	records := [][]interface{}{
		{1, []string{"a"}},
		{2, []string{"a"}},
	}
	m.MaxMsgs = uint64(len(records))

	for _, vals := range records {
		rec := makeRecord(t, fields, vals)
		tPutRecordsKafka(t, p, topic, schemaID, licodec, "akey", rec)
	}

	client, err := pilosaclient.NewClient(m.PilosaHosts)
	if err != nil {
		t.Fatalf("getting new client: %v", err)
	}
	sch, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	idx := sch.Index(m.Index)
	fld := idx.Field("a")
	err = client.SyncIndex(idx)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	_, err = client.Query(fld.Set(2, 1))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	err = m.Run()
	if err != nil && errors.Cause(err) != io.EOF {
		t.Fatalf("running main: %v", err)
	}

	resp, err := client.Query(fld.Set(2, 1))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	if !resp.Result().Changed() {
		t.Fatalf("expected that bit should change after deletion called")
	}
}
