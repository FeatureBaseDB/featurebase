package cmd

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/server"
)

// Serve is global so that tests can control and verify it.
var Serve *server.Command

func NewServeCmd(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Serve = server.NewCommand()
	Serve.Stdin, Serve.Stdout, Serve.Stderr = stdin, stdout, stderr
	serveCmd := &cobra.Command{
		Use:   "server",
		Short: "Run Pilosa.",
		Long: `pilosa server runs Pilosa.

It will load existing data from the configured
directory, and start listening client connections
on the configured port.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			Serve.Server.Handler.Version = Version
			fmt.Fprintf(Serve.Stderr, "Pilosa %s, build time %s\n", Version, BuildTime)

			// Start CPU profiling.
			if Serve.CPUProfile != "" {
				f, err := os.Create(Serve.CPUProfile)
				if err != nil {
					return fmt.Errorf("create cpu profile: %v", err)
				}
				defer f.Close()

				fmt.Fprintln(Serve.Stderr, "Starting cpu profile")
				pprof.StartCPUProfile(f)
				time.AfterFunc(Serve.CPUTime, func() {
					fmt.Fprintln(Serve.Stderr, "Stopping cpu profile")
					pprof.StopCPUProfile()
					f.Close()
				})
			}

			// Execute the program.
			if err := Serve.Run(); err != nil {
				return err
			}

			// First SIGKILL causes server to shut down gracefully.
			c := make(chan os.Signal, 2)
			signal.Notify(c, os.Interrupt)
			select {
			case sig := <-c:
				fmt.Fprintf(Serve.Stderr, "Received %s; gracefully shutting down...\n", sig.String())

				// Second signal causes a hard shutdown.
				go func() { <-c; os.Exit(1) }()

				if err := Serve.Close(); err != nil {
					return err
				}
			case <-Serve.Done:
				fmt.Fprintf(Serve.Stderr, "Server closed externally")
			}
			return nil
		},
	}
	flags := serveCmd.Flags()

	flags.StringVarP(&Serve.ConfigPath, "config", "c", "", "Configuration file to read from.")
	flags.StringVarP(&Serve.Config.DataDir, "data-dir", "d", "~/.pilosa", "Directory to store pilosa data files.")
	flags.StringVarP(&Serve.Config.Host, "bind", "", ":10101", "Default URI on which pilosa should listen.")
	flags.IntVarP(&Serve.Config.Cluster.ReplicaN, "cluster.replicas", "", 1, "Number hosts each piece of data should be stored on.")
	flags.StringSliceVarP(&Serve.Config.Cluster.Nodes, "cluster.hosts", "", []string{}, "Comma separated list of hosts in cluster.")
	flags.DurationVarP((*time.Duration)(&Serve.Config.Cluster.PollingInterval), "cluster.poll-interval", "", time.Minute, "Polling interval for cluster.") // TODO what actually is this?
	flags.StringVarP(&Serve.Config.Plugins.Path, "plugins.path", "", "", "Path to plugin directory.")
	flags.StringVarP(&Serve.CPUProfile, "cpu-profile", "", "", "Where to store CPU profile.")
	flags.DurationVarP(&Serve.CPUTime, "cpu-time", "", 30*time.Second, "CPU profile duration.")

	return serveCmd
}

func init() {
	subcommandFns["server"] = NewServeCmd
}
