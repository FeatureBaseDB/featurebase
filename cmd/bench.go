package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

func NewBenchCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	bencher := ctl.NewBenchCommand(os.Stdin, os.Stdout, os.Stderr)
	benchCmd := &cobra.Command{
		Use:   "bench",
		Short: "Benchmark operations.",
		Long: `
Executes a benchmark for a given operation against the database.
`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := bencher.Run(context.Background()); err != nil {
				fmt.Println(err)
			}
		},
	}
	flags := benchCmd.Flags()
	flags.StringVarP(&bencher.Host, "host", "", "localhost:15000", "host:port of Pilosa.")
	flags.StringVarP(&bencher.Database, "database", "d", "", "Pilosa database to benchmark.")
	flags.StringVarP(&bencher.Frame, "frame", "f", "", "Frame to benchmark.")
	flags.StringVarP(&bencher.Op, "operation", "o", "set-bit", "Operation to perform: choose from [set-bit]")
	flags.IntVarP(&bencher.N, "num", "n", 0, "Number of operations to perform.")

	return benchCmd
}

func init() {
	subcommandFns["bench"] = NewBenchCommand
}
