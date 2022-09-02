package main

import (
	"reflect"
	"testing"

	"github.com/jaffee/commandeer"
	"github.com/jaffee/commandeer/pflag"
	"github.com/molecula/featurebase/v3/idk/kafka"
	pflag13 "github.com/spf13/pflag"
)

func TestKafkaputArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string

		KafkaHosts  []string
		RegistryURL string
		Topic       string
		Schema      string
		SchemaFile  string
		Subject     string
		Data        string
	}{
		{
			name: "empty",
			args: []string{
				"",
			},
			KafkaHosts:  []string{"localhost:9092"},
			RegistryURL: "localhost:8081",
			Topic:       "defaulttopic",
			Subject:     "test",
			SchemaFile:  "",
			Schema:      "{\"type\": \"record\",\"namespace\": \"c.e\",\"name\": \"F\",\"fields\": [{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"a\",\"type\": \"boolean\"},{\"name\": \"b\", \"type\": \"float\", \"scale\": 2}]}",
			Data:        "{\"id\": 1, \"a\": true, \"b\": 1.43}",
		},
		{
			name: "all-set",
			args: []string{
				"kafkaput",
				"--kafka-bootstrap-servers", "localhost:0,localhost:80,localhost:9092",
				"--schema-registry-url", "https://localhost:8081",
				"--topic", "my-topic",
				"--schema", "{}",
				"--schema-file", "/tmp/schema.json",
				"--data", "{}",
				"--subject", "my-subject",
			},
			KafkaHosts:  []string{"localhost:0", "localhost:80", "localhost:9092"},
			RegistryURL: "https://localhost:8081",
			Topic:       "my-topic",
			Subject:     "my-subject",
			SchemaFile:  "/tmp/schema.json",
			Schema:      "{}",
			Data:        "{}",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &pflag.FlagSet{FlagSet: pflag13.NewFlagSet(tc.args[0], pflag13.ExitOnError)}
			m, err := kafka.NewPutCmd()
			if err != nil {
				t.Fatal(err)
			}
			if err := commandeer.LoadArgsEnv(fs, m, tc.args[1:], "KPUT_", nil); err != nil {
				t.Fatal(err)
			}

			if len(tc.KafkaHosts) > 0 {
				if !reflect.DeepEqual(tc.KafkaHosts, m.KafkaBootstrapServers) {
					t.Fatalf("--kafka-hosts expected: %v got: %v", tc.KafkaHosts, m.KafkaBootstrapServers)
				}
			}

			if tc.RegistryURL != m.SchemaRegistryURL {
				t.Fatalf("--registry-url expected: %v got: %v", tc.RegistryURL, m.SchemaRegistryURL)
			}

			if tc.Topic != m.Topic {
				t.Fatalf("--topic expected: %v got: %v", tc.Topic, m.Topic)
			}

			if tc.Subject != m.Subject {
				t.Fatalf("--subject expected: %v got: %v", tc.Subject, m.Subject)
			}

			if tc.SchemaFile != m.SchemaFile {
				t.Fatalf("--schema-file expected: %v got: %v", tc.SchemaFile, m.SchemaFile)
			}

			if tc.Schema != m.Schema {
				t.Fatalf("--schema expected: %v got: %v", tc.Schema, m.Schema)
			}

			if tc.Data != m.Data {
				t.Fatalf("--data expected: %v got: %v", tc.Data, m.Data)
			}
		})
	}
}
