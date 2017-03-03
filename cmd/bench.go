package cmd

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/pilosa/pilosa/ctl"
)

var bencher = ctl.NewBenchCommand(os.Stdin, os.Stdout, os.Stderr)

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "bench - benchmark operations",
	Long: `
Executes a benchmark for a given operation against the database.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := bencher.Run(context.Background()); err != nil {
			fmt.Println(err)
		}
	},
}

func init() {
	benchCmd.Flags().StringVarP(&bencher.Host, "host", "", "localhost:15000", "host:port of Pilosa.")
	benchCmd.Flags().StringVarP(&bencher.Database, "database", "d", "", "Pilosa database to benchmark.")
	benchCmd.Flags().StringVarP(&bencher.Frame, "frame", "f", "", "Frame to benchmark.")
	benchCmd.Flags().StringVarP(&bencher.Op, "operation", "o", "set-bit", "Operation to perform: choose from [set-bit]")
	benchCmd.Flags().IntVarP(&bencher.N, "num", "n", 0, "Number of operations to perform.")

	err := viper.BindPFlags(benchCmd.Flags())
	if err != nil {
		log.Fatalf("Error binding bench flags: %v", err)
	}

	RootCmd.AddCommand(benchCmd)
}
