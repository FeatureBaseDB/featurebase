package cmd

import (
	"fmt"
	"io"
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

// TODO maybe give this an Add method which will ensure two command
// with same name aren't added
var subcommandFns = map[string]func(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command{}

func NewRootCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	setupVersionBuild()
	rc := &cobra.Command{
		Use:   "pilosa",
		Short: "Pilosa - A Distributed In-memory Binary Bitmap Index.",
		// TODO - is documentation actually there?
		Long: `Pilosa is a fast index to turbocharge your database.

This binary contains Pilosa itself, as well as common
tools for administering pilosa, importing/exporting data,
backing up, and more. Complete documentation is available
at http://pilosa.com/docs

Version: ` + Version + `
Build Time: ` + BuildTime + "\n",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			v := viper.New()
			err := setAllConfig(v, cmd.Flags(), "PILOSA")
			if err != nil {
				return err
			}
			return nil
		},
	}
	for _, subcomFn := range subcommandFns {
		rc.AddCommand(subcomFn(stdin, stdout, stderr))
	}
	rc.SetOutput(stderr)
	return rc
}

func setupVersionBuild() {
	if Version == "" {
		Version = "v0.0.0"
	}
	if BuildTime == "" {
		BuildTime = "not recorded"
	}
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
func setAllConfig(v *viper.Viper, flags *flag.FlagSet, envPrefix string) error {
	// add cmd line flag def to viper
	err := v.BindPFlags(flags)
	if err != nil {
		return err
	}

	// add env to viper
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	c := v.GetString("config")

	// add config file to viper
	if c != "" {
		v.AddConfigPath(c)
		err := v.ReadInConfig()
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
		value := v.GetString(f.Name)
		log.Printf("Setting to value: '%v'", value)
		flagErr = f.Value.Set(value)
	})
	if flagErr == nil {
		fmt.Println(v.AllSettings())
	}
	return flagErr
}
