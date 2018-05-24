// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Bencher *ctl.BenchCommand

func NewBenchCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Bencher = ctl.NewBenchCommand(os.Stdin, os.Stdout, os.Stderr)
	benchCmd := &cobra.Command{
		Use:   "bench",
		Short: "Benchmark operations.",
		Long: `
Executes a benchmark for a given operation against the index.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := Bencher.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	flags := benchCmd.Flags()
	flags.StringVarP(&Bencher.Host, "host", "", "localhost:10101", "host:port of Pilosa.")
	flags.StringVarP(&Bencher.Index, "index", "i", "", "Pilosa index to benchmark.")
	flags.StringVarP(&Bencher.Frame, "frame", "f", "", "Frame to benchmark.")
	flags.StringVarP(&Bencher.Op, "operation", "o", "set-bit", "Operation to perform: choose from [set-bit]")
	flags.IntVarP(&Bencher.N, "num", "n", 0, "Number of operations to perform.")
	ctl.SetTLSConfig(flags, &Bencher.TLS.CertificatePath, &Bencher.TLS.CertificateKeyPath, &Bencher.TLS.SkipVerify)

	return benchCmd
}

func init() {
	subcommandFns["bench"] = NewBenchCommand
}
