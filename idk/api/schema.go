package api

import (
	"encoding/json"
	"io"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	pilosaclient "github.com/molecula/featurebase/v3/client"
	"github.com/pkg/errors"
)

type schema struct {
	IndexName      string `json:"index-name"`
	IfNotExists    bool   `json:"if-not-exists,omitempty"`
	PrimaryKeyType string `json:"primary-key-type"`

	Fields []schemaField `json:"fields"`
}

type schemaField struct {
	FieldName    string `json:"field-name"`
	FieldType    string `json:"field-type"`
	FieldOptions struct {
		TimeQuantum            string `json:"time-quantum,omitempty"`
		CacheSize              int    `json:"cache-size,omitempty"`
		CacheType              string `json:"cache-type,omitempty"`
		EnforceMutualExclusion bool   `json:"enforce-mutual-exclusion,omitempty"`
		Scale                  int    `json:"scale,omitempty"`
		Epoch                  string `json:"epoch,omitempty"`
		Unit                   string `json:"unit,omitempty"`
		TTL                    string `json:"ttl,omitempty"`
	} `json:"field-options,omitempty"`
}

// DecodeSchema takes JSON stream (io.Reader - mainly http request body)
// and tries to decode it into pilosa.Schema.
// The function returns also a map: index-name -> if-not-exists option,
// so we can swallow "conflict" error in case of creating an index with
// the name as existing one.
func DecodeSchema(r io.Reader) (*pilosaclient.Schema, map[string]bool, error) {
	pilosaSchema := pilosaclient.NewSchema()

	// map: index-name -> if-not-exists option
	ifNotExists := make(map[string]bool)

	// Create a JSON decoder.
	d := json.NewDecoder(r)

	// This is just a generic loop-approach where we can iterate over JSON objects
	// (stream of indexes), but on daily-bases we should just decode one index.
	for d.More() {
		var apiSchema schema
		if err := d.Decode(&apiSchema); err != nil {
			return nil, nil, errors.Wrap(err, "decoding schema")
		}

		err := apiSchema.applyToPilosa(pilosaSchema)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "applying schema of index %q", apiSchema.IndexName)
		}

		ifNotExists[apiSchema.IndexName] = apiSchema.IfNotExists
	}

	return pilosaSchema, ifNotExists, nil
}

// applyToPilosa validates an index schema and adds it to a Pilosa schema.
func (s schema) applyToPilosa(ps *pilosaclient.Schema) error {
	if err := pilosa.ValidateName(s.IndexName); err != nil {
		return errors.Wrap(err, "validating index name")
	}

	var keyed bool
	switch s.PrimaryKeyType {
	case "string":
		keyed = true
	case "uint":
		keyed = false
	case "auto":
		return errors.New("auto primary keys not yet implemented")
	default:
		return errors.Errorf(`invalid primary-key-type %q, it must be "string", "uint" or "auto"`, s.PrimaryKeyType)
	}

	idx := ps.Index(s.IndexName,
		pilosaclient.OptIndexTrackExistence(true),
		pilosaclient.OptIndexKeys(keyed),
	)

	for _, f := range s.Fields {
		err := f.applyToPilosa(idx)
		if err != nil {
			return errors.Wrapf(err, "applying field %q from schema", f.FieldName)
		}
	}

	return nil
}

// applyToPilosa validates a field schema and adds it to an index schema.
func (f schemaField) applyToPilosa(idx *pilosaclient.Index) error {
	if err := pilosa.ValidateName(f.FieldName); err != nil {
		return errors.Wrap(err, "validating field name")
	}

	if idx.HasField(f.FieldName) {
		return errors.New("field defined multiple times")
	}

	var opts []pilosaclient.FieldOption
	switch f.FieldType {
	case "id":
		switch {
		case f.FieldOptions.EnforceMutualExclusion:
			opts = []pilosaclient.FieldOption{pilosaclient.OptFieldTypeMutex(pilosaclient.CacheType(f.FieldOptions.CacheType), f.FieldOptions.CacheSize)}
		case f.FieldOptions.TimeQuantum != "":
			opts = []pilosaclient.FieldOption{pilosaclient.OptFieldTypeTime(pilosaclient.TimeQuantum(f.FieldOptions.TimeQuantum))}
			if f.FieldOptions.TTL != "" {
				ttl, err := time.ParseDuration(f.FieldOptions.TTL)
				if err != nil {
					return errors.Wrapf(err, "unable to parse TTL from field %s", f.FieldName)
				} else {
					opts = append(opts, pilosaclient.OptFieldTTL(ttl))
				}
			}
		default:
			opts = []pilosaclient.FieldOption{pilosaclient.OptFieldTypeSet(pilosaclient.CacheType(f.FieldOptions.CacheType), f.FieldOptions.CacheSize)}
		}

	case "string":
		switch {
		case f.FieldOptions.EnforceMutualExclusion:
			opts = []pilosaclient.FieldOption{pilosaclient.OptFieldTypeMutex(pilosaclient.CacheType(f.FieldOptions.CacheType), f.FieldOptions.CacheSize)}
		case f.FieldOptions.TimeQuantum != "":
			opts = []pilosaclient.FieldOption{pilosaclient.OptFieldTypeTime(pilosaclient.TimeQuantum(f.FieldOptions.TimeQuantum))}
			if f.FieldOptions.TTL != "" {
				ttl, err := time.ParseDuration(f.FieldOptions.TTL)
				if err != nil {
					return errors.Wrapf(err, "unable to parse TTL from field %s", f.FieldName)
				} else {
					opts = append(opts, pilosaclient.OptFieldTTL(ttl))
				}
			}
		default:
			opts = []pilosaclient.FieldOption{pilosaclient.OptFieldTypeSet(pilosaclient.CacheType(f.FieldOptions.CacheType), f.FieldOptions.CacheSize)}
		}

	case "bool":
		opts = []pilosaclient.FieldOption{pilosaclient.OptFieldTypeBool()}

	case "int":
		opts = []pilosaclient.FieldOption{pilosaclient.OptFieldTypeInt()}

	case "decimal":
		opts = []pilosaclient.FieldOption{pilosaclient.OptFieldTypeDecimal(int64(f.FieldOptions.Scale))}

	case "timestamp":
		epoch := time.Unix(0, 0)
		if f.FieldOptions.Epoch != "" {
			var err error
			epoch, err = time.Parse(time.RFC3339, f.FieldOptions.Epoch)
			if err != nil {
				return errors.Errorf("invalid epoch time %q for layout %q", f.FieldOptions.Epoch, time.RFC3339)
			}
		}

		unit := f.FieldOptions.Unit
		if !pilosa.IsValidTimeUnit(unit) {
			return errors.Errorf("invalid time unit %q", f.FieldOptions.Unit)
		}

		opts = []pilosaclient.FieldOption{pilosaclient.OptFieldTypeTimestamp(epoch, unit)}

	default:
		return errors.Errorf(`invalid field-type %q, it must be "id", "string", "bool", "int", "decimal" or "timestamp"`, f.FieldType)
	}

	idx.Field(f.FieldName, opts...)

	return nil
}
