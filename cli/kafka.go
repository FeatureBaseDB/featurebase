package cli

import (
	"github.com/featurebasedb/featurebase/v3/cli/batch"
	"github.com/featurebasedb/featurebase/v3/cli/kafka"
	"github.com/featurebasedb/featurebase/v3/errors"
)

func (cmd *Command) newKafkaRunner(cfgFile string) (*kafka.Runner, error) {

	cfg, err := kafka.ConfigFromFile(cfgFile)
	if err != nil {
		return nil, errors.Wrap(err, "getting config from file")
	}

	if err := kafka.ValidateConfig(cfg); err != nil {
		return nil, errors.Wrap(err, "validating config")
	}

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

	flds, err := kafka.ConfigToFields(cfg, idkCfg.PrimaryKeys)
	if err != nil {
		return nil, errors.Wrap(err, "getting fields from config")
	}

	return kafka.NewRunner(
		idkCfg,
		batch.NewSQLBatcher(cmd, flds),
		cmd.stderr,
	), nil
}
