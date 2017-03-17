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
var Server *server.Command

func NewServeCmd(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Server = server.NewCommand()
	Server.Stdin, Server.Stdout, Server.Stderr = stdin, stdout, stderr
	serveCmd := &cobra.Command{
		Use:   "server",
		Short: "Run Pilosa.",
		Long: `pilosa server runs Pilosa.

It will load existing data from the configured
directory, and start listening client connections
on the configured port.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			Server.Server.Handler.Version = Version
			fmt.Fprintf(Server.Stderr, "Pilosa %s, build time %s\n", Version, BuildTime)

			// Start CPU profiling.
			if Server.CPUProfile != "" {
				f, err := os.Create(Server.CPUProfile)
				if err != nil {
					return fmt.Errorf("create cpu profile: %v", err)
				}
				defer f.Close()

				fmt.Fprintln(Server.Stderr, "Starting cpu profile")
				pprof.StartCPUProfile(f)
				time.AfterFunc(Server.CPUTime, func() {
					fmt.Fprintln(Server.Stderr, "Stopping cpu profile")
					pprof.StopCPUProfile()
					f.Close()
				})
			}

			// Execute the program.
			if err := Server.Run(); err != nil {
				return fmt.Errorf("error running server: %v", err)
			}

			// First SIGKILL causes server to shut down gracefully.
			c := make(chan os.Signal, 2)
			signal.Notify(c, os.Interrupt)
			select {
			case sig := <-c:
				fmt.Fprintf(Server.Stderr, "Received %s; gracefully shutting down...\n", sig.String())

				// Second signal causes a hard shutdown.
				go func() { <-c; os.Exit(1) }()

				if err := Server.Close(); err != nil {
					return err
				}
			case <-Server.Done:
				fmt.Fprintf(Server.Stderr, "Server closed externally")
			}
			return nil
		},
	}
	flags := serveCmd.Flags()

	flags.StringVarP(&Server.Config.DataDir, "data-dir", "d", "~/.pilosa", "Directory to store pilosa data files.")
	flags.StringVarP(&Server.Config.Host, "bind", "b", ":10101", "Default URI on which pilosa should listen.")
	flags.IntVarP(&Server.Config.Cluster.ReplicaN, "cluster.replicas", "", 1, "Number hosts each piece of data should be stored on.")
	flags.StringSliceVarP(&Server.Config.Cluster.Nodes, "cluster.hosts", "", []string{}, "Comma separated list of hosts in cluster.")
	flags.DurationVarP((*time.Duration)(&Server.Config.Cluster.PollingInterval), "cluster.poll-interval", "", time.Minute, "Polling interval for cluster.") // TODO what actually is this?
	flags.StringVarP(&Server.Config.Plugins.Path, "plugins.path", "", "", "Path to plugin directory.")
	flags.DurationVarP((*time.Duration)(&Server.Config.AntiEntropy.Interval), "anti-entropy.interval", "", time.Minute*10, "Interval at which to run anti-entropy routine.")
	flags.StringVarP(&Server.CPUProfile, "profile.cpu", "", "", "Where to store CPU profile.")
	flags.DurationVarP(&Server.CPUTime, "profile.cpu-time", "", 30*time.Second, "CPU profile duration.")

	return serveCmd
}

func init() {
	subcommandFns["server"] = NewServeCmd
}
