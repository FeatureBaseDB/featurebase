package cmd

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/pilosa/pilosa/server"
)

var serve = server.NewMain()

var serveCmd = &cobra.Command{
	Use:   "server",
	Short: "Run Pilosa.",
	Long: `pilosa server runs Pilosa.

It will load existing data from the configured
directory, and start listening client connections
on the configured port.`,
	Run: func(cmd *cobra.Command, args []string) {
		serve.Server.Handler.Version = server.Version
		fmt.Fprintf(serve.Stderr, "Pilosa %s, build time %s\n", server.Version, server.BuildTime)

		// Parse command line arguments.
		if err := serve.SetupConfig(args); err != nil {
			fmt.Fprintln(serve.Stderr, err)
			os.Exit(2)
		}

		// Start CPU profiling.
		if serve.CPUProfile != "" {
			f, err := os.Create(serve.CPUProfile)
			if err != nil {
				fmt.Fprintf(serve.Stderr, "create cpu profile: %v", err)
				os.Exit(1)
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
			fmt.Fprintln(serve.Stderr, err)
			fmt.Fprintln(serve.Stderr, "stopping profile")
			os.Exit(1)
		}

		// First SIGKILL causes server to shut down gracefully.
		c := make(chan os.Signal, 2)
		signal.Notify(c, os.Interrupt)
		sig := <-c
		fmt.Fprintf(serve.Stderr, "Received %s; gracefully shutting down...\n", sig.String())

		// Second signal causes a hard shutdown.
		go func() { <-c; os.Exit(1) }()

		if err := serve.Close(); err != nil {
			fmt.Fprintln(serve.Stderr, err)
			os.Exit(1)
		}

	},
}

func init() {
	serveCmd.Flags().StringVarP(&serve.ConfigPath, "config", "c", "", "Configuration file to read from")
	serveCmd.Flags().StringVarP(&serve.CPUProfile, "cpuprofile", "", "", "Where to store CPU profile")
	serveCmd.Flags().DurationVarP(&serve.CPUTime, "cputime", "", 30*time.Second, "CPU profile duration")

	err := viper.BindPFlags(serveCmd.Flags())
	if err != nil {
		log.Fatalf("Error binding server flags: %v", err)
	}

	RootCmd.AddCommand(serveCmd)
}
