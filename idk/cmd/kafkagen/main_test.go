package main

import (
	"reflect"
	"testing"

	"github.com/featurebasedb/featurebase/v3/idk/kafkagen"
	"github.com/jaffee/commandeer"
	"github.com/jaffee/commandeer/pflag"
	pflag13 "github.com/spf13/pflag"
)

func TestKafkagenArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string

		KafkaHosts  []string
		RegistryURL string
		Topic       string
		Subject     string
		SchemaFile  string
	}{
		{
			name: "empty",
			args: []string{
				"",
			},

			KafkaHosts:  []string{"localhost:9092"},
			RegistryURL: "localhost:8081",
			Topic:       "defaulttopic",
			Subject:     "bigschema",
			SchemaFile:  "bigschema.json",
		},
		{
			name: "all-set",
			args: []string{
				"kafkagen",
				"--kafka-bootstrap-servers", "kafka:1,kafka:2,kafka:3,kafka4",
				"--schema-registry-url", "idk.registry.com",
				"--schema-file", "/tmp/schema.json",
				"--subject", "whatever",
				"--topic", "whatever",
			},
			KafkaHosts:  []string{"kafka:1", "kafka:2", "kafka:3", "kafka4"},
			RegistryURL: "idk.registry.com",
			Topic:       "whatever",
			Subject:     "whatever",
			SchemaFile:  "/tmp/schema.json",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &pflag.FlagSet{FlagSet: pflag13.NewFlagSet(tc.args[0], pflag13.ExitOnError)}
			m, err := kafkagen.NewMain()
			if err != nil {
				t.Fatal(err)
			}
			if err := commandeer.LoadArgsEnv(fs, m, tc.args[1:], "KGEN_", nil); err != nil {
				t.Fatal(err)
			}

			if len(tc.KafkaHosts) > 0 {
				if !reflect.DeepEqual(tc.KafkaHosts, m.KafkaBootstrapServers) {
					t.Fatalf("--kafka-bootstrap-servers expected: %v got: %v", tc.KafkaHosts, m.KafkaBootstrapServers)
				}
			}

			if tc.RegistryURL != "" {
				if !reflect.DeepEqual(tc.RegistryURL, m.SchemaRegistryURL) {
					t.Fatalf("--registry-url expected: %v got: %v", tc.RegistryURL, m.SchemaRegistryURL)
				}
			}

			if tc.Topic != "" {
				if !reflect.DeepEqual(tc.Topic, m.Topic) {
					t.Fatalf("--topic expected: %v got: %v", tc.Topic, m.Topic)
				}
			}

			if tc.Subject != "" {
				if !reflect.DeepEqual(tc.Subject, m.Subject) {
					t.Fatalf("--subject expected: %v got: %v", tc.Subject, m.Subject)
				}
			}
		})
	}
}
