package cmd

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"

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
		setupViper(cmd.Flags())
		fmt.Println(viper.AllSettings())
		serve.Server.Handler.Version = Version
		fmt.Fprintf(serve.Stderr, "Pilosa %s, build time %s\n", Version, BuildTime)

		// Parse command line arguments.
		if err := serve.SetupConfig(args); err != nil {
			return err
		}

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

	flags.StringVarP(&serve.ConfigPath, "config", "c", "", "Configuration file to read from")
	flags.StringVarP(&serve.CPUProfile, "cpu-profile", "", "", "Where to store CPU profile")
	flags.DurationVarP(&serve.CPUTime, "cpu-time", "", 30*time.Second, "CPU profile duration")

	RootCmd.AddCommand(serveCmd)
}

func setupViper(flags *flag.FlagSet) {
	// add cmd line flag def to viper
	err := viper.BindPFlags(flags)
	if err != nil {
		log.Fatalf("Error binding server flags: %v", err)
	}
	// add env to viper
	viper.SetEnvPrefix("PILOSA")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	c := viper.GetString("config")

	// add config file to viper
	if c != "" {
		viper.AddConfigPath(c)
		err := viper.ReadInConfig()
		if err != nil {
			log.Printf("Couldn't read config from '%s'", c)
		}
	}
	flags.VisitAll(func(f *flag.Flag) {
		log.Printf("Now visiting: %v with value '%s'", f.Name, f.Value)
		value := viper.GetString(f.Name)
		log.Printf("Setting to value: '%v'", value)
		err := f.Value.Set(value)
		if err != nil {
			log.Printf("Error setting %s to '%s': %v", f.Name, value, err)
		}
	})
}
