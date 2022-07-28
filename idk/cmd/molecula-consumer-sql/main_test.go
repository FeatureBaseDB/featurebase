package main

import (
	"reflect"
	"testing"

	"github.com/jaffee/commandeer"
	"github.com/jaffee/commandeer/pflag"
	"github.com/molecula/featurebase/v3/idk/sql"
	pflag13 "github.com/spf13/pflag"
)

func TestConsumerSQLArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string

		AssumeEmptyPilosa    bool
		AutoGenerate         bool
		BatchSize            int
		Concurrency          int
		IDField              string
		PilosaHosts          []string
		PilosaGRPCHosts      []string
		Stats                string
		Verbose              bool
		ExpSplitBatchMode    bool
		WriteCSV             string
		Driver               string
		ConnectionString     string
		RowExpr              string
		StringArraySeparator string
	}{
		{
			name: "empty",
			args: []string{
				"", // os.Args[0] can be ignored
			},
			BatchSize:            1,
			Concurrency:          1,
			PilosaHosts:          []string{"localhost:10101"},
			PilosaGRPCHosts:      []string{"localhost:20101"},
			Stats:                "localhost:9093",
			Driver:               "postgres",
			ConnectionString:     "postgres://user:password@localhost:5432/defaultindex?sslmode=disable",
			StringArraySeparator: ",",
		},
		{
			name: "long",
			args: []string{
				"molecula-consumer-sql",
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
				"--primary-key-fields", "primary_key",
				"--stats", "localhost:9093",
				"--tls.ca-certificate", "/tmp/file.ca",
				"--tls.certificate", "/tmp/file.certificate",
				"--tls.enable-client-verification", "true",
				"--tls.key", "/tmp/file.key",
				"--tls.skip-verify", "false",
				"--verbose", "true",
				"--write-csv", "/tmp/file.csv",
				"--driver", "mysql",
				"--connection-string", "mysql://user:password@localhost:3306/index_name",
				"--string-array-separator", ";",
				"--row-expr", "select 1;",
			},
			AssumeEmptyPilosa:    true,
			AutoGenerate:         true,
			BatchSize:            12345,
			Concurrency:          1,
			IDField:              "id",
			PilosaHosts:          []string{"pilosa:1", "pilosa:2", "pilosa:3"},
			PilosaGRPCHosts:      []string{"grpc:1", "grpc:2", "grpc:3"},
			Stats:                "localhost:9093",
			Verbose:              true,
			ExpSplitBatchMode:    true,
			WriteCSV:             "/tmp/file.csv",
			Driver:               "mysql",
			ConnectionString:     "mysql://user:password@localhost:3306/index_name",
			StringArraySeparator: ";",
			RowExpr:              "select 1;",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &pflag.FlagSet{FlagSet: pflag13.NewFlagSet(tc.args[0], pflag13.ExitOnError)}
			m := sql.NewMain()

			if err := commandeer.LoadArgsEnv(fs, m, tc.args[1:], "IDK_", nil); err != nil {
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
			if tc.Driver != m.Driver {
				t.Fatalf("--driver expected: %v got: %v", tc.Driver, m.Driver)
			}
			if tc.ConnectionString != m.ConnectionString {
				t.Fatalf("--connection-string expected: %v got: %v", tc.ConnectionString, m.ConnectionString)
			}
			if tc.StringArraySeparator != m.StringArraySeparator {
				t.Fatalf("--string-array-separator expected: %v got: %v", tc.StringArraySeparator, m.StringArraySeparator)
			}
			if tc.RowExpr != m.RowExpr {
				t.Fatalf("--row-expr expected: %v got: %v", tc.RowExpr, m.RowExpr)
			}
		})
	}
}
