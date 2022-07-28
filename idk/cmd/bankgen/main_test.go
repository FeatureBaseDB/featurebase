package main

import (
	"reflect"
	"testing"

	"github.com/jaffee/commandeer"
	"github.com/jaffee/commandeer/pflag"
	"github.com/molecula/featurebase/v3/idk/bankgen"
	pflag13 "github.com/spf13/pflag"
)

func TestBankgenArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string

		KafkaHosts  []string
		RegistryURL string
		Topic       string
		Subject     string
		Save        string
		NumRecords  int
		Seed        int64
		BatchSize   int
	}{
		{
			name: "empty",
			args: []string{
				"", // os.Args[0] can be ignored
			},
			KafkaHosts:  []string{"localhost:9092"},
			RegistryURL: "localhost:8081",
			Topic:       "banktest",
			Subject:     "test",
			Save:        "",
			NumRecords:  10,
		},
		{
			name: "many-kafka-hosts",
			args: []string{
				"bankgen", // os.Args[0] can be ignored
				"--kafka-bootstrap-servers", "localhost:0,localhost:80,localhost:9092",
				"--num-records", "123",
				"--schema-registry-url", "https://localhost:8081",
				"--save", "-",
				"--subject", "test",
				"--topic", "test",
				"--batch-size", "30",
			},
			KafkaHosts:  []string{"localhost:0", "localhost:80", "localhost:9092"},
			RegistryURL: "https://localhost:8081",
			Topic:       "test",
			Subject:     "test",
			Save:        "-",
			NumRecords:  123,
			BatchSize:   30,
		},
		{
			name: "with-seed",
			args: []string{
				"bankgen", // os.Args[0] can be ignored
				"--kafka-bootstrap-servers", "kafka:9092",
				"--num-records", "10",
				"--schema-registry-url", "localhost:8081",
				"--save", "/tmp/file.log",
				"--seed", "1234567890",
				"--subject", "subject",
				"--topic", "banktest",
			},
			KafkaHosts:  []string{"kafka:9092"},
			RegistryURL: "localhost:8081",
			Topic:       "banktest",
			Subject:     "subject",
			Save:        "/tmp/file.log",
			NumRecords:  10,
			Seed:        int64(1234567890),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &pflag.FlagSet{FlagSet: pflag13.NewFlagSet(tc.args[0], pflag13.ExitOnError)}
			m, err := bankgen.NewPutCmd()
			if err != nil {
				t.Fatal(err)
			}

			if err := commandeer.LoadArgsEnv(fs, m, tc.args[1:], "BANKGEN_", nil); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(tc.KafkaHosts, m.KafkaBootstrapServers) {
				t.Fatalf("--kafka-bootstrap-servers expected: %v got: %v", tc.KafkaHosts, m.KafkaBootstrapServers)
			}
			if !reflect.DeepEqual(tc.RegistryURL, m.SchemaRegistryURL) {
				t.Fatalf("--schema-registry-url expected: %v got: %v", tc.RegistryURL, m.SchemaRegistryURL)
			}

			if !reflect.DeepEqual(tc.Topic, m.Topic) {
				t.Fatalf("--topic expected: %v got: %v", tc.Topic, m.Topic)
			}

			if !reflect.DeepEqual(tc.Subject, m.Subject) {
				t.Fatalf("--subject expected: %v got: %v", tc.Subject, m.Subject)
			}

			if !reflect.DeepEqual(tc.Save, m.Save) {
				t.Fatalf("--save expected: %v got: %v", tc.Save, m.Save)
			}

			if !reflect.DeepEqual(tc.NumRecords, m.NumRecords) {
				t.Fatalf("--num-records expected: %v got: %v", tc.NumRecords, m.NumRecords)
			}

			if tc.BatchSize != 0 {
				if !reflect.DeepEqual(tc.BatchSize, m.BatchSize) {
					t.Fatalf("--batch-size expected: %v got: %v", tc.BatchSize, m.BatchSize)
				}
			}

			if tc.Seed != 0 {
				if !reflect.DeepEqual(tc.Seed, m.Seed) {
					t.Fatalf("--seed expected: %v got: %v", tc.Seed, m.Seed)
				}
			}
		})
	}
}
