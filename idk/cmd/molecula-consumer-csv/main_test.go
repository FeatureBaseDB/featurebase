package main

import (
	"reflect"
	"testing"

	"github.com/jaffee/commandeer"
	"github.com/jaffee/commandeer/pflag"
	"github.com/featurebasedb/featurebase/v3/idk/csv"
	pflag13 "github.com/spf13/pflag"
)

func TestConsumerCSVArgs(t *testing.T) {
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
		Files             []string
		Header            []string
		Index             string
		JustDoIt          bool
		IgnoreHeader      bool
		FutureRename      bool
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
			JustDoIt:          false,
			IgnoreHeader:      false,
			FutureRename:      false,
		},
		{
			name: "long",
			args: []string{
				"molecula-consumer-csv",
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
				"--files", "f1,f2,f3",
				"--header", "h1,h2,h3",
				"--index", "index-name",
				"--just-do-it", "true",
				"--ignore-header", "true",
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
			Files:             []string{"f1", "f2", "f3"},
			Header:            []string{"h1", "h2", "h3"},
			Index:             "index-name",
			JustDoIt:          true,
			IgnoreHeader:      true,
			FutureRename:      false,
		},
		{
			name: "empty",
			args: []string{
				"molecula-consumer-csv",
				"--future.rename", "true",
				"--featurebase-hosts", "localhost:50101",
				"--featurebase-grpc-hosts", "localhost:60101",
			},
			AssumeEmptyPilosa: false,
			AutoGenerate:      false,
			BatchSize:         1,
			Concurrency:       1,
			IDField:           "",
			PilosaHosts:       []string{"localhost:50101"},
			PilosaGRPCHosts:   []string{"localhost:60101"},
			Stats:             "localhost:9093",
			Verbose:           false,
			ExpSplitBatchMode: false,
			WriteCSV:          "",
			Index:             "",
			JustDoIt:          false,
			IgnoreHeader:      false,
			FutureRename:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &pflag.FlagSet{FlagSet: pflag13.NewFlagSet(tc.args[0], pflag13.ExitOnError)}
			m := csv.NewMain()

			if err := commandeer.LoadArgsEnv(fs, m, tc.args[1:], "IDKCSV_", nil); err != nil {
				t.Fatal(err)
			}

			m.Rename()

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
			if !reflect.DeepEqual(tc.Files, m.Files) {
				t.Fatalf("--files expected: %+v got: %+v", tc.Files, m.Files)
			}
			if !reflect.DeepEqual(tc.Header, m.Header) {
				t.Fatalf("--header expected: %v got: %v", tc.Header, m.Header)
			}
			if tc.Index != m.Index {
				t.Fatalf("--index expected: %v got: %v", tc.Index, m.Index)
			}
			if tc.JustDoIt != m.JustDoIt {
				t.Fatalf("--just-do-it expected: %v got: %v", tc.JustDoIt, m.JustDoIt)
			}
			if tc.IgnoreHeader != m.IgnoreHeader {
				t.Fatalf("--ignore-header expected: %v got: %v", tc.IgnoreHeader, m.IgnoreHeader)
			}
			if tc.FutureRename != m.Future.Rename {
				t.Fatalf("--future.rename expected: %v got: %v", tc.FutureRename, m.Future.Rename)
			}

			source, err := m.NewSource()
			// check for err as empty testcase fails to create Source
			if err == nil {
				csvSource := source.(*csv.Source)
				if csvSource.JustDoIt != m.JustDoIt {
					t.Fatalf("Source.JustDoIt does not match Main.JustDoIt")
				}
				if csvSource.IgnoreHeader != m.IgnoreHeader {
					t.Fatalf("Source.IgnoreHeader does not match Main.IgnoreHeader")
				}
			}
		})
	}
}
