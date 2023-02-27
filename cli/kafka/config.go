package kafka

import (
	"fmt"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/pkg/errors"
)

type Config struct {
	Hosts  []string `mapstructure:"hosts" help: "Kafka hosts."`
	Group  string   `mapstructure:"group" help:"Kafka group."`
	Topics []string `mapstructure:"topics" help:"Kafka topics to read from."`

	BatchSize         int           `mapstructure:"batch-size" help:"Batch size."`
	BatchMaxStaleness time.Duration `mapstructure:"batch-max-staleness" help:"Maximum length of time that the oldest record in a batch can exist before flushing the batch. Note that this can potentially stack with timeouts waiting for the source."`
	Timeout           time.Duration `mapstructure:"timeout" help:"Time to wait for more records from Kafka before flushing a batch. 0 to disable."`

	Table  string  `mapstructure:"table" help:"Destination table name."`
	Fields []Field `mapstructure:"fields"`
}

type ConfigForIDK struct {
	Hosts  []string
	Group  string
	Topics []string

	BatchSize         int
	BatchMaxStaleness time.Duration
	Timeout           time.Duration

	Table   string
	IDField string
	Fields  []idk.RawField
}

type Field struct {
	Name       string   `mapstructure:"name"`
	Type       string   `mapstructure:"type"`
	SourcePath []string `mapstructure:"source-path"`
	PrimaryKey bool     `mapstructure:"primary-key"`

	Options FieldOptions `mapstructure:"options"`
}

type FieldOptions struct {
	Scale int64 `mapstructure:"scale"`
}

// ValidateConfig validates the config is usable.
func ValidateConfig(c Config) error {
	if c.Table == "" {
		return errors.Errorf("table is required")
	} else if len(c.Topics) == 0 {
		return errors.Errorf("at least one topic is required")
	} else if len(c.Fields) < 2 {
		return errors.Errorf("at least two fields are required (one should be a primary key)")
	} else {
		var found int
		for i := range c.Fields {
			if c.Fields[i].PrimaryKey {
				found++
			}
		}
		if found != 1 {
			return errors.Errorf("exactly one primary key field is required")
		}
	}
	return nil
}

// ConvertConfig converts a Config to one that suitable for IDK.
func ConvertConfig(c Config) (ConfigForIDK, error) {
	// Set a default kafka host in case one isn't provided.
	hosts := []string{"localhost:9092"}
	if len(c.Hosts) > 0 {
		hosts = c.Hosts
	}

	// Copy all the shared members from Config to ConfigForIDK.
	out := ConfigForIDK{
		Hosts:             hosts,
		Group:             c.Group,
		Topics:            c.Topics,
		BatchSize:         c.BatchSize,
		BatchMaxStaleness: c.BatchMaxStaleness,
		Timeout:           c.Timeout,
		Table:             c.Table,
	}

	if len(c.Fields) == 0 {
		return out, errors.New("fields cannot be empty")
	}

	// rawFields wil be the same as c.Fields, but possibly enhanced.
	rawFields := make([]idk.RawField, 0, len(c.Fields))

	var foundPK bool
	for _, fld := range c.Fields {
		if fld.PrimaryKey {
			out.IDField = fld.Name
			foundPK = true
		}

		typ, err := dax.BaseTypeFromString(fld.Type)
		if err != nil {
			return out, errors.Wrap(err, "getting base type")
		}

		rawFld := idk.RawField{
			Name: fld.Name,
			Type: string(typ),
			Path: fld.SourcePath,
		}
		// If a SourcePath wasn't provided, default to using the field name.
		if len(rawFld.Path) == 0 {
			rawFld.Path = []string{fld.Name}
		}

		switch typ {
		case dax.BaseTypeInt:
			// We don't have to handle min/max because we don't create the table.
		case dax.BaseTypeDecimal:
			rawFld.Config = []byte(fmt.Sprintf(`{"scale":%d}`, fld.Options.Scale))
		case dax.BaseTypeID:
			rawFld.Config = []byte("{\"mutex\":true}")
		case dax.BaseTypeIDSet:
			rawFld.Type = "ids"
		case dax.BaseTypeString:
			rawFld.Config = []byte("{\"mutex\":true}")
		case dax.BaseTypeStringSet:
			rawFld.Type = "strings"
		case dax.BaseTypeTimestamp:
			// No timestamp options are handled for now.
		}

		rawFields = append(rawFields, rawFld)
	}
	if !foundPK {
		return out, errors.New("primary-key not found in fields")
	}

	out.Fields = rawFields

	return out, nil
}

// ConfigToFields returns a list of *dax.Field based on the IDField and Fields
// in the Config.
func ConfigToFields(c Config) []*dax.Field {
	// We don't know if a primary key will be found, so we can't set the
	// capacity to `len(c.Fields)-1`.
	out := make([]*dax.Field, 0, len(c.Fields))

	for _, fld := range c.Fields {
		if fld.PrimaryKey {
			continue
		}
		dfld := &dax.Field{
			Name: dax.FieldName(fld.Name),
			Type: dax.BaseType(fld.Type),
			Options: dax.FieldOptions{
				Scale: fld.Options.Scale,
			},
		}
		out = append(out, dfld)
	}

	return out
}
