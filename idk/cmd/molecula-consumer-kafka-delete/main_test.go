package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/jaffee/commandeer"
	"github.com/jaffee/commandeer/pflag"
	"github.com/molecula/featurebase/v3/idk/kafka"
	pflag13 "github.com/spf13/pflag"
)

func TestConsumerKafkaDeleteArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string

		AssumeEmptyPilosa bool
		AutoGenerate      bool
		BatchSize         int
		Concurrency       int
		IDField           string
		PilosaHosts       []string
		PilosaGRPCHosts   []string
		Stats             string
		Verbose           bool
		ExpSplitBatchMode bool
		WriteCSV          string
		Index             string
		PrimaryKeyFields  []string
		Pprof             string
		KafkaHosts        []string
		RegistryURL       string
		MaxMsgs           uint64
		Group             string
		Topics            []string
		Timeout           time.Duration
	}{
		{
			name: "empty",
			args: []string{
				"", // os.Args[0] can be ignored
			},
			AssumeEmptyPilosa: false,
			AutoGenerate:      false,
			BatchSize:         1,
			Concurrency:       1,
			IDField:           "",
			PilosaHosts:       []string{"localhost:10101"},
			PilosaGRPCHosts:   []string{"localhost:20101"},
			Stats:             "localhost:9093",
			Verbose:           false,
			ExpSplitBatchMode: false,
			WriteCSV:          "",
			Index:             "",
			Pprof:             "localhost:6062",
			KafkaHosts:        []string{"localhost:9092"},
			RegistryURL:       "http://localhost:8081",
			MaxMsgs:           uint64(0),
			Group:             "defaultgroup",
			Topics:            []string{"defaulttopic"},
			Timeout:           time.Second,
		},
		{
			name: "long",
			args: []string{
				"molecula-consumer-kafka-delete",
				"--assume-empty-pilosa", "true",
				"--auto-generate", "true",
				"--batch-size", "12345",
				"--concurrency", "1",
				"--exp-split-batch-mode", "true",
				"--id-field", "id",
				"--index", "index_name",
				"--log-path", "/tmp/file.log",
				"--pack-bools", "true",
				"--pilosa-grpc-hosts", "grpc:1,grpc:2,grpc:3",
				"--pilosa-hosts", "pilosa:1,pilosa:2,pilosa:3",
				"--stats", "localhost:9093",
				"--tls.ca-certificate", "/tmp/file.ca",
				"--tls.certificate", "/tmp/file.certificate",
				"--tls.enable-client-verification", "true",
				"--tls.key", "/tmp/file.key",
				"--tls.skip-verify", "false",
				"--verbose", "true",
				"--write-csv", "/tmp/file.csv",
				"--index", "index-name",
				"--primary-key-fields", "k1",
				"--pprof", "localhost:6666",
				"--kafka-bootstrap-servers", "kafka:1,kafka:2,kafka:3",
				"--schema-registry-url", "registry.molecula.com",
				"--max-msgs", "1234",
				"--group", "molecula",
				"--topics", "t1,t2,t3,t4",
				"--timeout", "123ms",
			},
			AssumeEmptyPilosa: true,
			AutoGenerate:      true,
			BatchSize:         12345,
			Concurrency:       1,
			IDField:           "id",
			PilosaHosts:       []string{"pilosa:1", "pilosa:2", "pilosa:3"},
			PilosaGRPCHosts:   []string{"grpc:1", "grpc:2", "grpc:3"},
			Stats:             "localhost:9093",
			Verbose:           true,
			ExpSplitBatchMode: true,
			WriteCSV:          "/tmp/file.csv",
			Index:             "index-name",
			PrimaryKeyFields:  []string{"k1"},
			KafkaHosts:        []string{"kafka:1", "kafka:2", "kafka:3"},
			RegistryURL:       "registry.molecula.com",
			MaxMsgs:           uint64(1234),
			Group:             "molecula",
			Topics:            []string{"t1", "t2", "t3", "t4"},
			Timeout:           123 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &pflag.FlagSet{FlagSet: pflag13.NewFlagSet(tc.args[0], pflag13.ExitOnError)}
			m, err := kafka.NewMain()
			if err != nil {
				t.Fatal(err)
			}
			m.Delete = true

			if err := commandeer.LoadArgsEnv(fs, m, tc.args[1:], "CONSUMER_DEL_", nil); err != nil {
				t.Fatal(err)
			}

			if tc.AssumeEmptyPilosa != m.AssumeEmptyPilosa {
				t.Fatalf("--assume-empty-pilosa expected: %v got: %v", tc.AssumeEmptyPilosa, m.AssumeEmptyPilosa)
			}
			if tc.AutoGenerate != m.AutoGenerate {
				t.Fatalf("--auto-generate expected: %v got: %v", tc.AutoGenerate, m.AutoGenerate)
			}
			if tc.BatchSize != m.BatchSize {
				t.Fatalf("--batch-size expected: %v got: %v", tc.BatchSize, m.BatchSize)
			}
			if tc.Concurrency != m.Concurrency {
				t.Fatalf("--concurrency expected: %v got: %v", tc.Concurrency, m.Concurrency)
			}
			if tc.IDField != m.IDField {
				t.Fatalf("--id-field expected: %v got: %v", tc.IDField, m.IDField)
			}
			if !reflect.DeepEqual(tc.PilosaHosts, m.PilosaHosts) {
				t.Fatalf("--pilosa-hosts expected: %v got: %v", tc.PilosaHosts, m.PilosaHosts)
			}
			if !reflect.DeepEqual(tc.PilosaGRPCHosts, m.PilosaGRPCHosts) {
				t.Fatalf("--pilosa-grpc-hosts expected: %v got: %v", tc.PilosaGRPCHosts, m.PilosaGRPCHosts)
			}
			if tc.Stats != m.Stats {
				t.Fatalf("--stats expected: %v got: %v", tc.Stats, m.Stats)
			}
			if tc.Verbose != m.Verbose {
				t.Fatalf("--verbose expected: %v got: %v", tc.Verbose, m.Verbose)
			}
			if tc.ExpSplitBatchMode != m.ExpSplitBatchMode {
				t.Fatalf("--exp-split-batch-mode expected: %v got: %v", tc.ExpSplitBatchMode, m.ExpSplitBatchMode)
			}
			if tc.WriteCSV != m.WriteCSV {
				t.Fatalf("--write-csv expected: %v got: %v", tc.WriteCSV, m.WriteCSV)
			}
			if tc.Index != m.Index {
				t.Fatalf("--index expected: %v got: %v", tc.Index, m.Index)
			}
			if !reflect.DeepEqual(tc.PrimaryKeyFields, m.PrimaryKeyFields) {
				t.Fatalf("--primary-key-fields expected: %v got: %v", tc.PrimaryKeyFields, m.PrimaryKeyFields)
			}
			if !reflect.DeepEqual(tc.KafkaHosts, m.KafkaBootstrapServers) {
				t.Fatalf("--kafka-hosts expected: %v got: %v", tc.KafkaHosts, m.KafkaBootstrapServers)
			}
			if tc.RegistryURL != m.SchemaRegistryURL {
				t.Fatalf("--registry-url expected: %v got: %v", tc.RegistryURL, m.SchemaRegistryURL)
			}
			if tc.MaxMsgs != m.MaxMsgs {
				t.Fatalf("--max-msgs expected: %v got: %v", tc.MaxMsgs, m.MaxMsgs)
			}
			if tc.Group != m.Group {
				t.Fatalf("--group expected: %v got: %v", tc.Group, m.Group)
			}
			if !reflect.DeepEqual(tc.Topics, m.Topics) {
				t.Fatalf("--topics expected: %v got: %v", tc.Topics, m.Topics)
			}
			if tc.Timeout != m.Timeout {
				t.Fatalf("--timeout expected: %v got: %v", tc.Timeout, m.Timeout)
			}
		})
	}
}
