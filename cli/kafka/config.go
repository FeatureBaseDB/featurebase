package kafka

import (
	"fmt"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/pkg/errors"
)

// Config is the user-facing configuration for kafka support in the CLI. This is
// unmarshalled from the the toml config file supplied by the user.
type Config struct {
	Hosts  []string `mapstructure:"hosts" help:"Kafka hosts."`
	Group  string   `mapstructure:"group" help:"Kafka group."`
	Topics []string `mapstructure:"topics" help:"Kafka topics to read from."`

	BatchSize         int           `mapstructure:"batch-size" help:"Batch size."`
	BatchMaxStaleness time.Duration `mapstructure:"batch-max-staleness" help:"Maximum length of time that the oldest record in a batch can exist before flushing the batch. Note that this can potentially stack with timeouts waiting for the source."`
	Timeout           time.Duration `mapstructure:"timeout" help:"Time to wait for more records from Kafka before flushing a batch. 0 to disable."`

	Table  string  `mapstructure:"table" help:"Destination table name."`
	Fields []Field `mapstructure:"fields"`
}

// Field is a user-facing configuration field.
type Field struct {
	Name       string   `mapstructure:"name"`
	SourceType string   `mapstructure:"source-type"`
	SourcePath []string `mapstructure:"source-path"`
	PrimaryKey bool     `mapstructure:"primary-key"`
}

// ConfigForIDK represents Config converted to values suitable for IDK. In
// particular, the idk.RawField is used in parsing the schema in IDK.
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

// ValidateConfig validates the config is usable.
func ValidateConfig(c Config) error {
	if c.Table == "" {
		return errors.Errorf("table is required")
	} else if len(c.Topics) == 0 {
		return errors.Errorf("at least one topic is required")
	} else if len(c.Fields) > 0 {
		// We only need to do these checks if any fields are specified at all.
		// If no fields are specified, that's ok because then we default to
		// using fields based off the existing table.
		if len(c.Fields) < 2 {
			return errors.Errorf("at least two fields are required (one should be a primary key)")
		} else {
			var found int
			for i := range c.Fields {
				if c.Fields[i].PrimaryKey {
					found++
				}
				if c.Fields[i].Name == "" {
					return errors.Errorf("a name attribute (which isn't equal to \"\") should exist for all fields")
				}
			}
			if found != 1 {
				return errors.Errorf("exactly one primary key field is required")
			}
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

		typ, quals, err := dax.SplitFieldType(fld.SourceType)
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
			if len(quals) != 1 {
				return out, errors.Errorf("expected decimal scale")
			}
			rawFld.Config = []byte(fmt.Sprintf(`{"scale":%d}`, quals[0]))
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
func ConfigToFields(c Config) ([]*dax.Field, error) {
	// We don't know if a primary key will be found, so we can't set the
	// capacity to `len(c.Fields)-1`.
	out := make([]*dax.Field, 0, len(c.Fields))

	for _, fld := range c.Fields {
		if fld.PrimaryKey {
			continue
		}
		typ, quals, err := dax.SplitFieldType(fld.SourceType)
		if err != nil {
			return nil, errors.Wrap(err, "splitting field type")
		}
		dfld := &dax.Field{
			Name: dax.FieldName(fld.Name),
			Type: typ,
		}
		switch typ {
		case dax.BaseTypeDecimal:
			if len(quals) != 1 {
				return nil, errors.Errorf("expected decimal scale")
			}
			scale, ok := quals[0].(int64)
			if !ok {
				return nil, errors.Errorf("invalid decimal scale: %v", quals[0])
			}
			dfld.Options.Scale = scale
		}
		out = append(out, dfld)
	}

	return out, nil
}

// FieldsToConfig returns a Config.Fields based on a list of *dax.Field.
func FieldsToConfig(flds []*dax.Field) []Field {
	out := make([]Field, 0, len(flds))
	for _, fld := range flds {
		out = append(out, Field{
			Name:       string(fld.Name),
			SourceType: fld.FullType(),
			PrimaryKey: fld.IsPrimaryKey(),
		})
	}
	return out
}

// CheckFieldCompatibility ensures that the fields provided in the kafka config
// are compatible with the fields in the existing table. It returns a copy of
// the kafka config fields with empty values defaulted to the table field
// configuration.
func CheckFieldCompatibility(cflds []Field, scr *featurebase.ShowColumnsResponse) ([]Field, error) {
	out := make([]Field, len(cflds))
	for i, cfld := range cflds {
		out[i] = cfld
		cfldName := dax.FieldName(cfld.Name)

		// Primary key field.
		if cfld.PrimaryKey {
			f := scr.Field(dax.PrimaryKeyFieldName)
			if f == nil {
				return nil, dax.NewErrFieldDoesNotExist(dax.PrimaryKeyFieldName) // It should be impossible to hit this.
			}
			if out[i].SourceType == "" {
				if f.StringKeys() {
					out[i].SourceType = dax.BaseTypeString
				} else {
					out[i].SourceType = dax.BaseTypeID
				}
			}
			continue
		}

		// Non primary key fields.
		if cfldName == dax.PrimaryKeyFieldName {
			return nil, errors.Errorf("field named '%s' must be a primary key", dax.PrimaryKeyFieldName)
		}

		f := scr.Field(cfldName)
		if f == nil {
			return nil, dax.NewErrFieldDoesNotExist(cfldName)
		}

		if out[i].SourceType == "" {
			out[i].SourceType = f.FullType()
		}
	}
	return out, nil
}
