// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/ctl"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// runner represents a thing, like any of the NewFooCommand we produce
// in ctl/*.go, which has a Run(context) error method, which is used
// to implement a command. We use this so we can just specify the
// object, rather than its run method, in calling usageErrorWrapper.
type runner interface {
	Run(context.Context) error
}

// UsageErrorWrapper takes a thing with a Run(context) error, and produces
// a func(*cobra.Command, []string) error from it which will run that
// command, and then set Cobra's SilenceUsage flag unless the returned
// error errors.Is() a ctl.UsageError.
func UsageErrorWrapper(inner runner) func(*cobra.Command, []string) error {
	return func(c *cobra.Command, args []string) error {
		return considerUsageError(c, inner.Run(context.Background()))
	}
}

// considerUsageError sets a command to silence usage errors if
// the given error is not a ctl.UsageError, then returns the
// unmodified error. It's here to let us write one-liner Run
// wrappers.
func considerUsageError(cmd *cobra.Command, err error) error {
	cmd.SilenceErrors = true
	if !errors.Is(err, ctl.ErrUsage) {
		cmd.SilenceUsage = true
	}
	return err
}

func NewRootCommand(stderr io.Writer) *cobra.Command {
	logdest := logger.NewStandardLogger(stderr)
	rc := &cobra.Command{
		Use: "featurebase",
		// TODO: These short/long descriptions could use some updating.
		Short: "FeatureBase is a feature extraction and storage technology that enables real-time analytics.",
		Long: `FeatureBase is a feature extraction and storage technology that enables real-time analytics.

This binary contains FeatureBase itself, as well as common
tools for administering FeatureBase, importing/exporting data,
backing up, and more. Complete documentation is available
at https://docs.featurebase.com/.

` + pilosa.VersionInfo(true) + "\n",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			v := viper.New()

			switch cmd.Use {
			case "dax":
				v.Set("future.rename", true) // always use FEATUREBASE env for dax
			}
			if err := SetAllConfig(v, cmd.Flags(), ""); err != nil {
				return err
			}

			// return "dry run" error if "dry-run" flag is set
			ret, err := cmd.Flags().GetBool("dry-run")
			if err != nil {
				return fmt.Errorf("problem getting dry-run flag: %v", err)
			}
			if ret {
				if cmd.Parent() != nil {
					return fmt.Errorf("dry run")
				}
			}

			return nil
		},
		SilenceErrors: true,
	}
	rc.PersistentFlags().Bool("dry-run", false, "stop before executing")
	_ = rc.PersistentFlags().MarkHidden("dry-run")
	rc.PersistentFlags().StringP("config", "c", "", "Configuration file to read from.")

	rc.AddCommand(newChkSumCommand(logdest))
	rc.AddCommand(newBackupCommand(logdest))
	rc.AddCommand(newRestoreCommand(logdest))
	rc.AddCommand(newBackupTarCommand(stderr))
	rc.AddCommand(newRestoreTarCommand(logdest))
	rc.AddCommand(newConfigCommand(stderr))
	rc.AddCommand(newExportCommand(logdest))
	rc.AddCommand(newGenerateConfigCommand(logdest))
	rc.AddCommand(newImportCommand(logdest))
	rc.AddCommand(newAuthTokenCommand(logdest))
	rc.AddCommand(newRBFCommand(logdest))
	rc.AddCommand(newServeCmd(stderr))
	rc.AddCommand(newHolderCmd(stderr))
	rc.AddCommand(newKeygenCommand(logdest))
	rc.AddCommand(newDAXCommand(stderr))
	rc.AddCommand(newDataframeCsvLoaderCommand(logdest))
	rc.AddCommand(newPreSortCommand(logdest))
	rc.AddCommand(newParquetInfoCommand(logdest))
	rc.AddCommand(newBoltCommand(logdest))

	rc.SetOutput(stderr)
	return rc
}

// SetAllConfig takes a FlagSet to be the definition of all configuration
// options, as well as their defaults. It then reads from the command line, the
// environment, and a config file (if specified), and applies the configuration
// in that priority order. Since each flag in the set contains a pointer to
// where its value should be stored, SetAllConfig can directly modify the value
// of each config variable.
//
// SetAllConfig looks for environment variables which are capitalized versions
// of the flag names with dashes replaced by underscores, and prefixed with
// envPrefix plus an underscore.
func SetAllConfig(v *viper.Viper, flags *pflag.FlagSet, envPrefix string) error { // nolint: unparam
	// add cmd line flag def to viper
	err := v.BindPFlags(flags)
	if err != nil {
		return err
	}

	if envPrefix == "" {
		envPrefix = "PILOSA"
		if v.GetBool("future.rename") {
			envPrefix = "FEATUREBASE"
		}
	}

	// add env to viper
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	v.AutomaticEnv()

	c := v.GetString("config")
	var flagErr error
	validTags := make(map[string]bool)
	flags.VisitAll(func(f *pflag.Flag) {
		validTags[f.Name] = true
	})

	// add config file to viper
	if c != "" {
		v.SetConfigFile(c)
		v.SetConfigType("toml")
		err := v.ReadInConfig()
		if err != nil {
			return fmt.Errorf("error reading configuration file '%s': %v", c, err)
		}

		for _, key := range v.AllKeys() {
			if _, ok := validTags[key]; !ok {
				if key == "future.rename" {
					continue
				}
				return fmt.Errorf("invalid option in configuration file: %v", key)
			}
		}
	}

	// set all values from viper
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
