package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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

			// return "dry run" error if "dry-run" flag is set
			if ret, err := cmd.Flags().GetBool("dry-run"); ret && err == nil {
				if cmd.Parent() != nil {
					return fmt.Errorf("dry run")
				} else if err != nil {
					return fmt.Errorf("problem getting dry-run flag: %v", err)
				}
			}

			return nil
		},
	}
	rc.PersistentFlags().Bool("dry-run", false, "stop before executing")
	_ = rc.PersistentFlags().MarkHidden("dry-run")
	rc.PersistentFlags().StringP("config", "c", "", "Configuration file to read from.")
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
func setAllConfig(v *viper.Viper, flags *pflag.FlagSet, envPrefix string) error {
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
		v.SetConfigFile(c)
		v.SetConfigType("toml")
		err := v.ReadInConfig()
		if err != nil {
			return fmt.Errorf("error reading configuration file '%s': %v", c, err)
		}
	}

	// set all values from viper
	var flagErr error
	flags.VisitAll(func(f *pflag.Flag) {
		if flagErr != nil {
			return
		}
		var value string
		if f.Value.Type() == "stringSlice" {
			// special handling is needed for stringSlice as v.GetString will
			// always return "" in the case that the value is an actual string
			// slice from a config file rather than a comma separated string
			// from a flag or env var.
			vss := v.GetStringSlice(f.Name)
			value = strings.Join(vss, ",")
		} else {
			value = v.GetString(f.Name)
		}

		if f.Changed {
			// If f.Changed is true, that means the value has already been set
			// by a flag, and we don't need to ask viper for it since the flag
			// is the highest priority. This works around a problem with string
			// slices where f.Value.Set(csvString) would cause the elements of
			// csvString to be appended to the existing value rather than
			// replacing it.
			return
		}
		flagErr = f.Value.Set(value)
	})
	return flagErr
}
