package cli

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/cli/batch"
	"github.com/featurebasedb/featurebase/v3/cli/kafka"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/spf13/viper"
)

func (cmd *Command) newKafkaRunner(cfgFile string) (*kafka.Runner, error) {
	// Read the kafka config file.
	v := viper.New()
	v.SetConfigFile(cfgFile)
	v.SetConfigType("toml")
	err := v.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("error reading configuration file '%s': %v", cfgFile, err)
	}

	cfg := kafka.Config{}
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, errors.Wrap(err, "unmarshalling config")
	}

	if err := kafka.ValidateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "validating config")
	}

	// Create a new config with defaults.

	// Look up fields based on table provided in the config.
	wqr, err := cmd.executeQuery(newRawQuery("SHOW COLUMNS FROM " + cfg.Table))
	if err != nil {
		return nil, errors.Wrap(err, "executing query")
	}

	scr, err := wqr.ShowColumnsResponse()
	if err != nil {
		return nil, errors.Wrap(err, "getting show columns from wire query response")
	}

	// If no fields were provided in the config, use the fields defined on the
	// table and assume a 1-to-1 mapping of source to destination.
	if len(cfg.Fields) == 0 {
		cfg.Fields = kafka.FieldsToConfig(scr.Fields)
	} else {
		cfg.Fields, err = kafka.CheckFieldCompatibility(cfg.Fields, scr)
		if err != nil {
			return nil, errors.Wrap(err, "validating config fields")
		}
	}

	idkCfg, err := kafka.ConvertConfig(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "cleaning config")
	}

	flds, err := kafka.ConfigToFields(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "getting fields from config")
	}

	return kafka.NewRunner(
		idkCfg,
		batch.NewSQLBatcher(cmd, flds),
		cmd.stderr,
	), nil
}
