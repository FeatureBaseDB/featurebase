package datagen

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/common"
)

var sources = []string{
	"bank",
	"claim",
	"custom",
	"customer",
	"dell",
	"dwarranty",
	"equipment",
	"hughes",
	"item",
	"kitchensink",
	"kitchensink_keyed",
	"merck",
	"network_ts",
	"power_scenario_1",
	"power_scenario_1_2",
	"power_ts",
	"site",
	"sizing",
	"stringpk",
	"texas_health",
	"transactions_scenario_1",
	"transactions_ts",
	"warranty",
}

var keyedIndices = map[string]bool{
	"stringpk":          true,
	"kitchensink_keyed": true,
	"bank":              true,
}

var duplicateIndices = map[string]bool{
	"bank":                           true,
	"customer_segmentation_linkedin": true,
	"custom":                         true,
}

// TestConfig tests that all default configurations load.
func TestConfig(t *testing.T) {
	t.Parallel()

	for _, src := range sources {
		src := src
		t.Run(src, func(t *testing.T) {
			t.Parallel()

			main := NewMain()
			main.Source = src
			main.Concurrency = 2
			if src == "custom" {
				main.CustomConfig = "testdata/custom.yaml"
			}
			main.Target = TargetFeaturebase
			err := main.Preload()
			if err != nil {
				t.Errorf("failed to preload: %v", err)
				return
			}
			main.NoStats()
			main.PrintPlan()
		})
	}
}

// TestGen tests record generation.
// It ensures that all generated IDs are unique, and that the right number of records are generated.
// If the IDs are numerical, it also ensures that they occupy a single contiguous range.
func TestGen(t *testing.T) {
	t.Parallel()

	for _, src := range sources {
		src := src
		t.Run(src, func(t *testing.T) {
			t.Parallel()

			const conc = 16
			const n = 100

			main := NewMain()
			main.Source = src
			main.Concurrency = conc
			main.EndAt = n - 1
			if src == "custom" {
				main.CustomConfig = "testdata/custom.yaml"
			}
			if err := main.Preload(); err != nil {
				t.Errorf("failed to preload: %v", err)
				return
			}
			main.NoStats()

			var wg sync.WaitGroup
			var out [conc]map[interface{}]struct{}
			var errs [conc]error
			srcs := main.Sources()
			for i := range out {
				i := i
				wg.Add(1)
				go func() {
					defer wg.Done()

					ids := map[interface{}]struct{}{}
					out[i] = ids

					for {
						rec, err := srcs[i].Record()
						if err != nil {
							if err == io.EOF {
								return
							}

							errs[i] = err
							return
						}

						id := rec.Data()[0]
						if idbytes, ok := id.([]byte); ok {
							id = string(idbytes)
						}
						if _, ok := ids[id]; ok {
							errs[i] = fmt.Errorf("duplicated ID: %v", id)
							return
						}
						ids[id] = struct{}{}
					}
				}()
			}
			wg.Wait()

			for i, v := range errs {
				if v != nil {
					t.Errorf("source %d failed: %v", i, v)
				}
			}

			total := 0
			ids := map[interface{}][]int{}
			var idlist []interface{}
			for i, s := range out {
				total += len(s)
				for id := range s {
					if _, ok := ids[id]; !ok {
						idlist = append(idlist, id)
					}
					ids[id] = append(ids[id], i)
				}
			}
			// some sources are expected to have duplicate records, so skip checking for duplicates.
			if !duplicateIndices[src] {
				sort.Slice(idlist, func(i, j int) bool {
					switch idlist[i].(type) {
					case uint64:
						return idlist[i].(uint64) < idlist[j].(uint64)
					case string:
						return idlist[i].(string) < idlist[j].(string)
					default:
						panic(fmt.Errorf("unexpected type: %T", idlist[i]))
					}
				})
				for _, id := range idlist {
					srcs := ids[id]
					if len(srcs) > 1 {
						sort.Ints(srcs)
						t.Errorf("duplicates of id %v found in sources: %v", id, srcs)
					}
				}
				if base, ok := idlist[0].(uint64); ok {
					for i, v := range idlist {
						if base+uint64(i) != v {
							t.Errorf("found gap in ID range at %d", base+uint64(i))
							break
						}
					}
				}
			}

			if total != n {
				t.Errorf("expected %d records; got %d records", n, total)
			}

		})
	}
}

func configureTestFlags(main *Main) {
	if pilosaHost, ok := os.LookupEnv("IDK_TEST_PILOSA_HOST"); ok {
		main.Pilosa.Hosts = []string{pilosaHost}
	} else {
		main.Pilosa.Hosts = []string{"pilosa:10101"}
	}
}

func TestThousandFormating(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input  uint64
		output string
	}{
		{input: 0, output: "0"},
		{input: 1000, output: "1,000"},
		{input: 111222333444, output: "111,222,333,444"},
	}
	for _, tc := range tests {
		tc := tc
		if tc.output != AddThousandSep(tc.input) {
			t.Errorf("expected %v but got %v", tc.output, AddThousandSep(tc.input))
		}
	}
}

// TestIntegration is an intergration test that ingests each source into Pilosa.
// It currently only verifies that the correct number of rows are ingested into each index.
func TestIntegration(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration tests in short mode")
		return
	}

	for _, src := range sources {
		src := src
		t.Run(src, func(t *testing.T) {
			t.Parallel()

			const n = 100

			main := NewMain()
			configureTestFlags(main)
			main.Pilosa.Index = fmt.Sprintf("test-%s-%d", src, rand.New(rand.NewSource(time.Now().Unix())).Uint64())
			main.Source = src
			main.Target = TargetFeaturebase
			main.EndAt = n - 1
			main.Concurrency = 3
			main.Pilosa.BatchSize = 13
			if src == "custom" {
				main.CustomConfig = "testdata/custom.yaml"
			}
			if err := main.Preload(); err != nil {
				t.Errorf("failed to preload: %v", err)
				return
			}
			main.NoStats()

			defer func() {
				client := main.PilosaClient()
				if client == nil {
					return
				}
				if err := client.DeleteIndexByName(main.Pilosa.Index); err != nil {
					t.Errorf("deleting test index: %v", err)
				}
			}()

			if err := main.Run(); err != nil {
				t.Errorf("failed to ingest: %v", err)
				return
			}

			client := main.PilosaClient()

			schema, err := client.Schema()
			if err != nil {
				t.Errorf("getting schema: %v", err)
				return
			}

			index := schema.Indexes()[main.Pilosa.Index]
			if index == nil {
				t.Error("index dissapeared")
				return
			}

			expectKeys := keyedIndices[src]
			indexKeys := index.Opts().Keys()
			if indexKeys != expectKeys {
				t.Errorf("expected keys=%v; got keys=%v", expectKeys, indexKeys)
			}

			resp, err := client.Query(index.Count(index.All()))
			if err != nil {
				t.Errorf("validation query failed: %v", err)
				return
			}

			recordCount := resp.Result().Count()
			// some sources have duplicate records with same id.
			// record count in featurebase won't match the number of ingested records
			if duplicateIndices[src] {
				if recordCount == 0 {
					t.Errorf("expected %d records; found %d records", 0, recordCount)
				}
			} else {
				if recordCount != n {
					t.Errorf("expected %d records; found %d records", n, recordCount)
				}
			}
		})
	}
}

func TestConfigKafka(t *testing.T) {
	t.Parallel()
	tests := []struct {
		src               string
		conc              int
		numpartitions     int
		replicationfactor int
		topic             string
		batchsize         int
		target            string
	}{
		{
			src:               sources[0],
			conc:              2,
			numpartitions:     4,
			replicationfactor: 2,
			topic:             fmt.Sprintf("topic-%d", rand.New(rand.NewSource(time.Now().Unix())).Uint64()),
			batchsize:         1000000,
			target:            TargetKafka,
		},
		{
			src:               sources[0],
			conc:              1,
			numpartitions:     1,
			replicationfactor: 1,
			topic:             fmt.Sprintf("topic-%d", rand.New(rand.NewSource(time.Now().Unix())).Uint64()),
			batchsize:         10000,
			target:            TargetKafkaStatic,
		},
		{
			src:               sources[0],
			conc:              3,
			numpartitions:     1,
			replicationfactor: 1,
			topic:             fmt.Sprintf("topic-%d", rand.New(rand.NewSource(time.Now().Unix())).Uint64()),
			batchsize:         10000,
			target:            TargetKafkaStatic,
		},
	}

	for i, test := range tests {
		tc := test
		t.Run(fmt.Sprintf("Test%d", i), func(t *testing.T) {
			t.Parallel()

			main := NewMain()
			main.Source = tc.src
			main.Concurrency = tc.conc
			main.Target = tc.target
			main.Concurrency = tc.conc
			main.Kafka.BatchSize = tc.batchsize
			main.Kafka.Topic = tc.topic
			main.Kafka.ReplicationFactor = tc.replicationfactor
			main.Kafka.NumPartitions = tc.numpartitions
			err := main.Preload()
			if err != nil {
				t.Fatalf("failed to preload: %v", err)
			}
			if main.KafkaPut.Concurrency != tc.conc {
				t.Fatalf("expected %d for concurrency, got %d", tc.conc, main.KafkaPut.Concurrency)
			}
			if main.KafkaPut.BatchSize != tc.batchsize {
				t.Fatalf("expected %d for batch-size, got %d", tc.batchsize, main.KafkaPut.BatchSize)
			}
			if main.KafkaPut.NumPartitions != tc.numpartitions {
				t.Fatalf("expected %d for num partitions, got %d", tc.numpartitions, main.KafkaPut.NumPartitions)
			}
			if main.KafkaPut.ReplicationFactor != tc.replicationfactor {
				t.Fatalf("expected %d for replication factor, got %d", tc.replicationfactor, main.KafkaPut.ReplicationFactor)
			}
			if main.KafkaPut.Target != tc.target {
				t.Fatalf("expected %s for target, got %s", tc.target, main.KafkaPut.Target)
			}
			if main.KafkaPut.Topic != tc.topic {
				t.Fatalf("expected %s for topic, got %s", tc.topic, main.KafkaPut.Topic)
			}
		})
	}
}

func configureKafkaTestFlags(main *Main) {
	main.Pilosa.Hosts = []string{"pilosa:10101"}
	main.Kafka.KafkaBootstrapServers = []string{"kafka:9092"}
}

func TestIntegrationWithKafka(t *testing.T) {
	tests := []struct {
		target           string
		numPartitions    int
		primaryKeyFields []string
		idField          string
	}{
		{
			target:           TargetKafka,
			numPartitions:    6,
			primaryKeyFields: []string{"uuid"},
			idField:          "uuid",
		},
		{
			target:           TargetKafka,
			numPartitions:    1,
			primaryKeyFields: []string{},
			idField:          "",
		},
		{
			target:           TargetKafkaStatic,
			numPartitions:    4,
			primaryKeyFields: []string{},
			idField:          "",
		},
		{
			target:           TargetKafkaStatic,
			numPartitions:    2,
			primaryKeyFields: []string{"uuid"},
			idField:          "uuid",
		},
	}

	for i, test := range tests {
		tc := test
		ti := i
		t.Run(tc.target, func(t *testing.T) {
			const n = 100
			src := "custom"

			main := NewMain()
			main.Pilosa.Index = fmt.Sprintf("test-%s-%d-%d", tc.target, ti, rand.New(rand.NewSource(time.Now().Unix())).Uint64())
			main.Source = src
			main.CustomConfig = "testdata/tremor_keys.yaml"
			main.Target = tc.target
			main.EndAt = n - 1
			main.Pilosa.BatchSize = 13
			configureKafkaTestFlags(main)
			main.Kafka.Topic = fmt.Sprintf("topic-%s-%d-%d", tc.target, ti, rand.New(rand.NewSource(time.Now().Unix())).Uint64())
			main.Kafka.NumPartitions = tc.numPartitions
			main.Concurrency = tc.numPartitions // make concurrency and partitions equal
			main.Kafka.ReplicationFactor = 1
			if tc.target == TargetKafka {
				main.Kafka.SchemaRegistryURL = "http://schema-registry:8081"
			}
			if len(tc.primaryKeyFields) != 0 {
				main.idkMain.PrimaryKeyFields = tc.primaryKeyFields
			}
			if tc.idField != "" {
				main.idkMain.IDField = tc.idField
			}
			if err := main.Preload(); err != nil {
				t.Fatalf("failed to preload: %v", err)
			}

			if err := main.KafkaPut.Run(); err != nil {
				t.Fatalf("failed to ingest records to kafka: %v", err)
			}

			if err := main.KafkaPut.ConfigMap.SetKey("go.produce.channel.size", 1); err != nil {
				t.Fatalf("failed to st produce channel size: %v", err)
			}

			confluentCommand := idk.ConfluentCommand{
				KafkaBootstrapServers: []string{"kafka:9092"},
				KafkaSocketTimeoutMs:  10,
			}

			configMap, err := common.SetupConfluent(&confluentCommand)
			if err != nil {
				t.Fatalf("failed to create kafka confluent config map: %v", err)
			}

			kafkaClient, err := kafka.NewAdminClient(configMap)
			if err != nil {
				t.Fatalf("failed to get kafka client: %v", err)
			}
			defer kafkaClient.Close()

			metadata, err := kafkaClient.GetMetadata(&main.KafkaPut.Topic, false, 100)
			if err != nil {
				t.Fatalf("failed to get kafka metadata: %v", err)
			}

			topic, ok := metadata.Topics[main.KafkaPut.Topic]
			if !ok {
				t.Fatalf("topic %s not found in metadata", main.KafkaPut.Topic)
			}

			if len(topic.Partitions) != tc.numPartitions {
				t.Fatalf("expected %d for num partitions, got %d", tc.numPartitions, len(topic.Partitions))
			}
		})
	}
}

func TestStartEnds(t *testing.T) {
	tests := []struct {
		config SourceGeneratorConfig
		conc   int
		ses    []startEnd
		total  uint64
	}{
		{
			config: SourceGeneratorConfig{
				StartFrom:   0,
				EndAt:       100,
				Concurrency: 5},
			conc:  5,
			ses:   []startEnd{{0, 20}, {20, 40}, {40, 60}, {60, 80}, {80, 100}},
			total: 100,
		},
		{
			config: SourceGeneratorConfig{
				StartFrom:   0,
				EndAt:       100,
				Concurrency: 1},
			conc:  1,
			ses:   []startEnd{{0, 100}},
			total: 100,
		},
	}

	for i, test := range tests {
		tc := test
		t.Run(fmt.Sprintf("Test_%d", i), func(t *testing.T) {
			conc, ses, total := startEnds(tc.config)
			if conc != tc.conc {
				t.Fatalf("expected %d for concurrency, got %d", tc.conc, conc)
			}
			if total != tc.total {
				t.Fatalf("expected %d for total records, got %d", tc.total, total)
			}
			if len(ses) != len(tc.ses) {
				t.Fatalf("expected %d of boundaries, got %d", len(tc.ses), len(ses))
			}
			for i, exp := range tc.ses {
				got := ses[i]
				if exp.start != got.start || exp.end != got.end {
					t.Fatalf("expected boundaries to start %d and end at %d, but got start %d and end at %d", exp.start, exp.end, got.start, got.end)
				}
			}
		})
	}
}
