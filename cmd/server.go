package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/server"
)

var serve = server.NewCommand()

var serveCmd = &cobra.Command{
	Use:   "server",
	Short: "Run Pilosa.",
	Long: `pilosa server runs Pilosa.

It will load existing data from the configured
directory, and start listening client connections
on the configured port.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		serve.Server.Handler.Version = Version
		fmt.Fprintf(serve.Stderr, "Pilosa %s, build time %s\n", Version, BuildTime)

		// Start CPU profiling.
		if serve.CPUProfile != "" {
			f, err := os.Create(serve.CPUProfile)
			if err != nil {
				return fmt.Errorf("create cpu profile: %v", err)
			}
			defer f.Close()

			fmt.Fprintln(serve.Stderr, "Starting cpu profile")
			pprof.StartCPUProfile(f)
			time.AfterFunc(serve.CPUTime, func() {
				fmt.Fprintln(serve.Stderr, "Stopping cpu profile")
				pprof.StopCPUProfile()
				f.Close()
			})
		}

		// Execute the program.
		if err := serve.Run(); err != nil {
			return err
		}

		// First SIGKILL causes server to shut down gracefully.
		c := make(chan os.Signal, 2)
		signal.Notify(c, os.Interrupt)
		sig := <-c
		fmt.Fprintf(serve.Stderr, "Received %s; gracefully shutting down...\n", sig.String())

		// Second signal causes a hard shutdown.
		go func() { <-c; os.Exit(1) }()

		if err := serve.Close(); err != nil {
			return err
		}
		return nil
	},
}

func init() {
	flags := serveCmd.Flags()

	flags.StringVarP(&serve.ConfigPath, "config", "c", "", "Configuration file to read from.")
	flags.StringVarP(&serve.Config.DataDir, "data-dir", "d", "~/.pilosa", "Directory to store pilosa data files.")
	flags.StringVarP(&serve.CPUProfile, "cpu-profile", "", "", "Where to store CPU profile.")
	flags.DurationVarP(&serve.CPUTime, "cpu-time", "", 30*time.Second, "CPU profile duration.")

	RootCmd.AddCommand(serveCmd)
}
