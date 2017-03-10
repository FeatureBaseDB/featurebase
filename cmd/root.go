package cmd

import (
	"fmt"
	"log"
	"strings"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	Version   string
	BuildTime string
)

var RootCmd = &cobra.Command{
	Use:   "pilosa",
	Short: "Pilosa - A Distributed In-memory Binary Bitmap Index.",
	// TODO - is documentation actually there?
	Long: `Pilosa is a fast index to turbocharge your database.

This binary contains Pilosa itself, as well as common
tools for administering pilosa, importing/exporting data,
backing up, and more. Complete documentation is available
at http://pilosa.com/docs

`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		err := setAllConfig(cmd.Flags(), "PILOSA")
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	if Version == "" {
		Version = "v0.0.0"
	}
	if BuildTime == "" {
		BuildTime = "not recorded"
	}

	RootCmd.Long = RootCmd.Long + "Version: " + Version + "\nBuild Time: " + BuildTime + "\n"
}

// setAllConfig takes a FlagSet to be the definition of all configuration
// options, as well as their defaults. It then reads from the command line, the
// environment, and a config file (if specified), and applies the configuration
// in that priority order. Since each flag in the set contains a pointer to
// where its value should be stored, setAllConfig can directly modify the value
// of each config variable.
//
// setAllConfig looks for environment variables which are capitalized versions
// of the flag names with dashes replaced by underscores, and prefixed with
// envPrefix plus an underscore.
func setAllConfig(flags *flag.FlagSet, envPrefix string) error {
	// add cmd line flag def to viper
	err := viper.BindPFlags(flags)
	if err != nil {
		return err
	}

	// add env to viper
	viper.SetEnvPrefix(envPrefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	c := viper.GetString("config")

	// add config file to viper
	if c != "" {
		viper.AddConfigPath(c)
		err := viper.ReadInConfig()
		if err != nil {
			return fmt.Errorf("error reading configuration file '%s': %v", c, err)
		}
	}

	// set all values from viper
	var flagErr error
	flags.VisitAll(func(f *flag.Flag) {
		if flagErr != nil {
			return
		}
		log.Printf("Now visiting: %v with value '%s'", f.Name, f.Value)
		value := viper.GetString(f.Name)
		log.Printf("Setting to value: '%v'", value)
		flagErr = f.Value.Set(value)
	})
	if flagErr == nil {
		fmt.Println(viper.AllSettings())
	}
	return flagErr
}
