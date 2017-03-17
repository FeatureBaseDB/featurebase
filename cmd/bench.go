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
Executes a benchmark for a given operation against the database.
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
	flags.StringVarP(&Bencher.Database, "database", "d", "", "Pilosa database to benchmark.")
	flags.StringVarP(&Bencher.Frame, "frame", "f", "", "Frame to benchmark.")
	flags.StringVarP(&Bencher.Op, "operation", "o", "set-bit", "Operation to perform: choose from [set-bit]")
	flags.IntVarP(&Bencher.N, "num", "n", 0, "Number of operations to perform.")

	return benchCmd
}

func init() {
	subcommandFns["bench"] = NewBenchCommand
}
