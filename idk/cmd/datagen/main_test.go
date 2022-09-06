package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/jaffee/commandeer"
	"github.com/jaffee/commandeer/pflag"
	"github.com/featurebasedb/featurebase/v3/idk/datagen"
	pflag13 "github.com/spf13/pflag"
)

func TestDatagenArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string

		Source      string
		Target      string
		Concurrency int
		StartFrom   uint64
		EndAt       uint64

		PilosaHosts     []string
		PilosaIndex     string
		PilosaBatchSize int

		KafkaHosts             []string
		KafkaRegistryURL       string
		KafkaTopic             string
		KafkaSubject           string
		KafkaBatchSize         int
		KafkaReplicationFactor int
		KafkaNumPartitions     int
		Datadog                bool
	}{
		{
			name: "empty",
			args: []string{
				"", // os.Args[0] can be ignored
				"--source", "example",
			},
			Source:      "example",
			Target:      "featurebase",
			Concurrency: 1,
			StartFrom:   uint64(0),
			EndAt:       uint64(0),

			PilosaHosts:     nil,
			PilosaIndex:     "",
			PilosaBatchSize: 0,

			KafkaHosts:             nil,
			KafkaRegistryURL:       "",
			KafkaTopic:             "",
			KafkaSubject:           "",
			KafkaReplicationFactor: 0,
			KafkaNumPartitions:     0,
			Datadog:                false,
		},
		{
			name: "long",
			args: []string{
				"datagen",
				"--source", "customer",
				"--target", "kafka",
				"--concurrency", "10",
				"--start-from", "1",
				"--end-at", "11",
				"--pilosa.hosts", "pilosa:1,pilosa:2,pilosa:3",
				"--pilosa.index", "idx",
				"--pilosa.batch-size", "12345",
				"--kafka.confluent-command.kafka-bootstrap-servers", "kafka:1,kafka:2,kafka:3",
				"--kafka.confluent-command.schema-registry-url", "registry:1",
				"--kafka.subject", "subj",
				"--kafka.topic", "top",
				"--kafka.batch-size", "2",
				"--kafka.replication-factor", "3",
				"--kafka.num-partitions", "3",
				"--datadog",
			},
			Source:      "customer",
			Target:      "kafka",
			Concurrency: 10,
			StartFrom:   uint64(1),
			EndAt:       uint64(11),

			PilosaHosts:     []string{"pilosa:1", "pilosa:2", "pilosa:3"},
			PilosaIndex:     "idx",
			PilosaBatchSize: 12345,

			KafkaHosts:             []string{"kafka:1", "kafka:2", "kafka:3"},
			KafkaRegistryURL:       "registry:1",
			KafkaSubject:           "subj",
			KafkaTopic:             "top",
			KafkaBatchSize:         2,
			KafkaReplicationFactor: 3,
			KafkaNumPartitions:     3,
			Datadog:                true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &pflag.FlagSet{FlagSet: pflag13.NewFlagSet(tc.args[0], pflag13.ExitOnError)}
			m := datagen.NewMain()

			if err := commandeer.LoadArgsEnv(fs, m, tc.args[1:], "GEN_", nil); err != nil {
				t.Fatal(err)
			}

			if err := m.Preload(); err != nil {
				t.Fatalf("failed to load: %v", err)
			}

			if tc.Source != m.Source {
				t.Fatalf("--source expected: %v got: %v", tc.Source, m.Source)
			}
			if tc.Target != m.Target {
				t.Fatalf("--target expected: %v got: %v", tc.Target, m.Target)
			}
			if tc.Concurrency != m.Concurrency {
				t.Fatalf("--concurrency expected: %v got: %v", tc.Concurrency, m.Concurrency)
			}

			if !reflect.DeepEqual(tc.PilosaHosts, m.Pilosa.Hosts) {
				t.Fatalf("--pilosa.hosts expected: %v got: %v", tc.PilosaHosts, m.Pilosa.Hosts)
			}
			if tc.PilosaIndex != m.Pilosa.Index {
				t.Fatalf("--pilosa.index expected: %v got: %v", tc.PilosaIndex, m.Pilosa.Index)
			}
			if tc.PilosaBatchSize != m.Pilosa.BatchSize {
				t.Fatalf("--pilosa.batch-size expected: %v got: %v", tc.PilosaBatchSize, m.Pilosa.BatchSize)
			}

			if tc.KafkaHosts != nil {
				bs, err := m.KafkaPut.ConfigMap.Get("bootstrap.servers", nil)
				if err != nil || bs == nil {
					t.Fatalf("kafka servers not set properly: %v", err)
				}
				bootstrapServers := bs.(string)

				if !reflect.DeepEqual(strings.Join(tc.KafkaHosts, ","), bootstrapServers) {
					t.Fatalf("--kafka.hosts expected: %v got: %v", strings.Join(tc.KafkaHosts, ","), bootstrapServers)
				}
			}
			if tc.KafkaRegistryURL != m.Kafka.SchemaRegistryURL {
				t.Fatalf("--kafka.registry-url expected: %v got: %v", tc.KafkaRegistryURL, m.Kafka.SchemaRegistryURL)
			}
			if tc.KafkaSubject != m.Kafka.Subject {
				t.Fatalf("--kafka.subject expected: %v got: %v", tc.KafkaSubject, m.Kafka.Subject)
			}
			if tc.KafkaTopic != m.Kafka.Topic {
				t.Fatalf("--kafka.topic expected: %v got: %v", tc.KafkaTopic, m.Kafka.Topic)
			}
			if tc.KafkaBatchSize != 0 {
				if tc.KafkaBatchSize != m.Kafka.BatchSize {
					t.Fatalf("--kafka.batch-size expected: %v got: %v", tc.KafkaBatchSize, m.Kafka.BatchSize)
				}
			}
			if tc.KafkaReplicationFactor != 0 {
				if tc.KafkaReplicationFactor != m.Kafka.ReplicationFactor {
					t.Fatalf("--kafka.replication-factor expected: %v got: %v", tc.KafkaReplicationFactor, m.Kafka.ReplicationFactor)
				}
			}
			if tc.KafkaNumPartitions != 0 {
				if tc.KafkaNumPartitions != m.Kafka.NumPartitions {
					t.Fatalf("--kafka.num-partitions expected: %v got: %v", tc.KafkaNumPartitions, m.Kafka.NumPartitions)
				}
			}
		})
	}
}
