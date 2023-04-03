package kafka

import (
	"fmt"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// Kafka message encoding types supported - changes here should be
// propogated to the Config struct and ValidateConfig error message
const (
	encodingTypeJSON = "json"
	encodingTypeAvro = "avro"
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

	Encode             string `mapstructure:"encode" help:"Encoding format (currently supported formats: avro, json)"`
	AllowMissingFields bool   `mapstructure:"allow-missing-fields" help:"allow missing fields in messages from kafka"`
	MaxMessages        int    `mapstructure:"max-messages" help:"max messages read from kakfka"`
	ConfluentConfig    string `mapstructure:"confluent-config" help:"max messages read from kakfka"`
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

	Table       string
	IDField     string
	PrimaryKeys []string
	Fields      []idk.RawField

	Encode             string
	AllowMissingFields bool
	MaxMessages        int
	ConfluentConfig    string
}

// ConfigFromFile returns a Config struct based on a configuration file Default
// values for Config struct are defined here
func ConfigFromFile(cfgFile string) (cfg Config, err error) {
	// configure viper
	v := viper.New()
	v.SetConfigFile(cfgFile)
	v.SetConfigType("toml")

	// set defaults
	v.SetDefault("hosts", []string{"localhost:9092"})
	v.SetDefault("group", "default-featurebase-group")
	v.SetDefault("batch-size", 1)
	v.SetDefault("batch-max-staleness", 5*time.Second)
	v.SetDefault("timeout", 5*time.Second)
	v.SetDefault("encode", encodingTypeJSON)

	// Read the kafka config file.
	err = v.ReadInConfig()
	if err != nil {
		return cfg, fmt.Errorf("error reading configuration file '%s': %v", cfgFile, err)
	}

	if err := v.Unmarshal(&cfg); err != nil {
		return cfg, errors.Wrap(err, "unmarshalling config")
	}

	return

}

// ValidateConfig validates the config is usable. Note that different encoding
// methods require different configurations
func ValidateConfig(c Config) error {

	// validate common
	if c.Table == "" {
		return errors.Errorf("table is required")
	}

	if len(c.Topics) == 0 {
		return errors.Errorf("at least one topic is required")
	}

	// validate on a by encoding basis
	switch c.Encode {
	case encodingTypeJSON:
		return validateConfigJSON(c)
	case encodingTypeAvro:
		return validateConfigAvro(c)
	}

	return nil

}

func validateConfigJSON(c Config) error {

	if len(c.Fields) > 0 {
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
				if c.Fields[i].SourceType == "" {
					return errors.Errorf("a source-type attribute (which isn't equal to \"\") should exist for all fields")
				}
			}
			if found < 1 {
				return errors.Errorf("at least one primary key field is required")
			}
		}
	}

	return nil
}

// Only primary key fields required
func validateConfigAvro(c Config) error {

	// for avro encoded messages, we just need to know what avro fields are
	// going to be used for the primary key. We'll check that there is at least
	// one field. For every field, we'll check that it has a name attribute and
	// has primary-key set.
	if len(c.Fields) < 1 {
		return errors.New("at least one field is required for avro encoded messages")
	}
	for i := range c.Fields {
		if !c.Fields[i].PrimaryKey {
			return errors.New("each field must be a primary key for avro encoded messages")
		}
		if c.Fields[i].Name == "" {
			return errors.New("a name attribute (which isn't equal to \"\") should exist for all fields")
		}

	}

	return nil
}

// ConvertConfig converts a Config to one that suitable for IDK.
func ConvertConfig(c Config) (ConfigForIDK, error) {

	// Copy all the shared members from Config to ConfigForIDK.
	out := ConfigForIDK{
		Hosts:              c.Hosts,
		Group:              c.Group,
		Topics:             c.Topics,
		BatchSize:          c.BatchSize,
		BatchMaxStaleness:  c.BatchMaxStaleness,
		Timeout:            c.Timeout,
		Table:              c.Table,
		Encode:             c.Encode,
		AllowMissingFields: c.AllowMissingFields,
		MaxMessages:        c.MaxMessages,
		ConfluentConfig:    c.ConfluentConfig,
	}

	if len(c.Fields) == 0 {
		return out, errors.New("fields cannot be empty")
	}

	// rawFields will be the same as c.Fields, but possibly enhanced.
	rawFields := make([]idk.RawField, 0, len(c.Fields))

	var stringKeys bool
	primaryKeys := []string{}
	for _, fld := range c.Fields {

		// handle primary key
		if fld.PrimaryKey {
			switch keyType := fld.SourceType; keyType {
			case dax.BaseTypeID:
			case dax.BaseTypeString, dax.BaseTypeInt:
				stringKeys = true
			default:
				// IDK can handle other field types as primary keys but limiting
				// here to the ones above for now. ID and string are the ones
				// that make sense and existing users also use int fields so I'm
				// including that as well.
				return out, errors.Errorf("primary-key fields must be \"id\", \"string\", or \"int\": got field %s which is type %s", fld.Name, keyType)
			}
			primaryKeys = append(primaryKeys, fld.Name)
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

	// Should have at least one primary key.  If there is more than one primary key OR
	// using a string field as the key then use string keys (i.e. table will be keyed)
	// Else, use ids (i.e. table will not be keyed)
	if len(primaryKeys) < 1 {
		return out, errors.New("primary-key not found in fields")
	} else if stringKeys || len(primaryKeys) > 1 {
		out.PrimaryKeys = primaryKeys
	} else {
		out.IDField = primaryKeys[0]
	}

	out.Fields = rawFields

	return out, nil
}

// ConfigToFields returns a list of *dax.Field based on the IDField and Fields
// in the Config.
func ConfigToFields(c Config, primaryKeys []string) ([]*dax.Field, error) {
	// We don't know if a primary key will be found, so we can't set the
	// capacity to `len(c.Fields)-1`.
	out := make([]*dax.Field, 0, len(c.Fields))

	for _, fld := range c.Fields {
		// When we have a single primary key, don't also store that value as a
		// field in FeatureBase. However, when we have more than one primary
		// key, store all the values used for the compound key as fields in
		// FeatureBase.
		if fld.PrimaryKey && len(primaryKeys) < 2 {
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
