package idk

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

type FieldType string

const (
	IDType               FieldType = "id"
	BoolType             FieldType = "bool"
	StringType           FieldType = "string"
	LookupTextType       FieldType = "lookuptext"
	IntType              FieldType = "int"
	ForeignKeyType       FieldType = "foreignkey"
	DecimalType          FieldType = "decimal"
	StringArrayType      FieldType = "stringarray"
	IDArrayType          FieldType = "idarray"
	DateIntType          FieldType = "dateint"
	RecordTimeType       FieldType = "recordtime"
	SignedIntBoolKeyType FieldType = "signedintboolkey"
	IgnoreType           FieldType = "ignore"
	TimestampType        FieldType = "timestamp"
)

var (
	ErrNoFieldSpec      = errors.New("no field spec in this header")
	ErrInvalidFieldName = errors.New("field name must match [a-z][a-z0-9_-]{0,229}")
	ErrParsingEpoch     = "parsing epoch for "
	ErrDecodingConfig   = "decoding config for field "
)

// HeaderToField takes a header specification which looks like
// sourcename___destname__FieldType_Arg_Arg2 (note that sourcename and
// destname are separated by "___", triple underscore) and converts it
// to an idk Field like:
//
//	FieldTypeField {
//	    NameVal: sourcename,
//	    DestNameVal: destname,
//	    Thing1: Arg,
//	    Thing2: Arg2,
//	}
//
// It does this using a variety of reflective magic. The unwritten
// rules are that all idk Fields must be structs and have their first
// member be `NameVal string`. The arguments are applied in order to
// exported fields.
func HeaderToField(headerField string, log logger.Logger) (field Field, _ error) {
	if log == nil {
		log = logger.NopLogger
	}

	sourceName, destName, fieldspecstr, err := splitHeader(headerField)
	if err != nil {
		return nil, err
	}

	fieldspec := strings.Split(fieldspecstr, "_")
	if len(fieldspec) == 0 {
		return nil, errors.Errorf("no fieldspec-impossible? headerfield: %s, fieldspec: %s", headerField, fieldspec)
	}

	fieldType := FieldType(strings.ToLower(fieldspec[0]))
	if sourceName == "" && fieldType != RecordTimeType {
		return nil, errors.Errorf("field '%s' has no sourceName", headerField)
	}

	switch fieldType {
	case IDType:
		field, err = headerToIDField(headerField, sourceName, destName, fieldspec, log)
	case BoolType:
		field, err = headerToBoolField(headerField, sourceName, destName, fieldspec, log)
	case StringType:
		field, err = headerToStringField(headerField, sourceName, destName, fieldspec, log)
	case LookupTextType:
		field, err = headerToLookupTextField(headerField, sourceName, destName, fieldspec, log)
	case IntType:
		field, err = headerToIntField(headerField, sourceName, destName, fieldspec, log)
	case ForeignKeyType:
		field, err = headerToForeignKeyField(headerField, sourceName, destName, fieldspec, log)
	case DecimalType:
		field, err = headerToDecimalField(headerField, sourceName, destName, fieldspec, log)
	case StringArrayType:
		field, err = headerToStringArrayField(headerField, sourceName, destName, fieldspec, log)
	case IDArrayType:
		field, err = headerToIDArrayField(headerField, sourceName, destName, fieldspec, log)
	case DateIntType:
		field, err = headerToDateIntField(headerField, sourceName, destName, fieldspec, log)
	case TimestampType:
		field, err = headerToTimestampField(headerField, sourceName, destName, fieldspec, log)
	case RecordTimeType:
		field, err = headerToRecordTimeField(headerField, sourceName, destName, fieldspec, log)
	case SignedIntBoolKeyType:
		field, err = headerToSignedIntBoolKeyField(headerField, sourceName, destName, fieldspec, log)
	case IgnoreType:
		field = IgnoreField{}
		if len(fieldspec) > 1 {
			log.Printf("ignoring extra arguments to IgnoreField %s: %v", headerField, fieldspec[1:])
		}
	default:
		return nil, errors.Errorf("unknown field '%s' for '%s'", fieldspec[0], headerField)
	}

	return field, err
}

func headerToIDField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	idField := IDField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	if len(fieldspec) > 1 {
		if fieldspec[1] == "T" {
			idField.Mutex = true
		} else if fieldspec[1] != "F" {
			return nil, errors.Errorf("can't interpret '%s' for IDField.Mutex for field '%s'", fieldspec[1], sourceName)
		}
	}
	if len(fieldspec) > 2 {
		idField.Quantum = fieldspec[2]
	}
	if len(fieldspec) > 3 {
		idField.TTL = fieldspec[3]
	}
	if len(fieldspec) > 4 {
		log.Printf("ignoring extra arguments to IDField %s: %v", headerField, fieldspec[4:])
	}
	return idField, nil
}

func headerToBoolField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	field := BoolField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	if len(fieldspec) > 1 {
		log.Printf("ignoring extra arguments to BoolField %s: %v", headerField, fieldspec[1:])
	}
	return field, nil
}

func headerToStringField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	strField := StringField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	if len(fieldspec) > 1 {
		if fieldspec[1] == "T" {
			strField.Mutex = true
		} else if fieldspec[1] != "F" {
			return nil, errors.Errorf("can't interpret '%s' for StringField.Mutex for field '%s'", fieldspec[1], sourceName)
		}
	}
	if len(fieldspec) > 2 {
		strField.Quantum = fieldspec[2]
	}
	if len(fieldspec) > 3 {
		strField.TTL = fieldspec[3]
	}
	if len(fieldspec) > 4 {
		log.Printf("ignoring extra arguments to StringField %s: %v", headerField, fieldspec[4:])
	}
	return strField, nil
}

func headerToLookupTextField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	lTextField := LookupTextField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	if len(fieldspec) > 1 {
		log.Printf("ignoring extra arguments to LookupTextField %s: %v", headerField, fieldspec[1:])
	}
	return lTextField, nil
}

func headerToIntField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	intField := IntField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	if len(fieldspec) > 1 {
		min, err := strconv.ParseInt(fieldspec[1], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing min for %s", sourceName)
		}
		intField.Min = &min
	}
	if len(fieldspec) > 2 {
		max, err := strconv.ParseInt(fieldspec[2], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing max for %s", sourceName)
		}
		intField.Max = &max
	}
	if len(fieldspec) > 3 {
		intField.ForeignIndex = fieldspec[3]
	}
	if len(fieldspec) > 4 {
		log.Printf("ignoring extra arguments to IntField %s: %v", headerField, fieldspec[4:])
	}
	return intField, nil
}

func headerToForeignKeyField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	fkField := IntField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	if len(fieldspec) > 1 {
		fkField.ForeignIndex = fieldspec[1]
	} else {
		return nil, errors.Errorf("need foreign index for foreign key field: %s", headerField)
	}
	if len(fieldspec) > 2 {
		log.Printf("ignoring extra arguments to ForeignKey Field %s: %v", headerField, fieldspec[2:])
	}
	return fkField, nil
}

func headerToDecimalField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	decField := DecimalField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	if len(fieldspec) > 1 {
		scale, err := strconv.ParseInt(fieldspec[1], 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing scale for %s", sourceName)
		}
		decField.Scale = scale
	}
	if len(fieldspec) > 2 {
		log.Printf("ignoring extra arguments to DecimalField %s: %v", headerField, fieldspec[2:])
	}
	return decField, nil
}

func headerToStringArrayField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	strArrField := StringArrayField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	if len(fieldspec) > 1 {
		strArrField.Quantum = fieldspec[1]
	}
	if len(fieldspec) > 2 {
		strArrField.TTL = fieldspec[2]
	}
	if len(fieldspec) > 3 {
		log.Printf("ignoring extra arguments to StringArrayField %s: %v", headerField, fieldspec[3:])
	}
	return strArrField, nil
}

func headerToIDArrayField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	idArrField := IDArrayField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	if len(fieldspec) > 1 {
		idArrField.Quantum = fieldspec[1]
	}
	if len(fieldspec) > 2 {
		idArrField.TTL = fieldspec[2]
	}
	if len(fieldspec) > 3 {
		log.Printf("ignoring extra arguments to IDArrayField %s: %v", headerField, fieldspec[3:])
	}
	return idArrField, nil
}

func headerToDateIntField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	dateField := DateIntField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	layout := time.RFC3339
	if len(fieldspec) > 1 {
		layout = fieldspec[1]
	}
	dateField.Layout = layout
	if len(fieldspec) > 2 {
		epoch, err := time.Parse(layout, fieldspec[2])
		if err != nil {
			return nil, errors.Wrapf(err, ErrParsingEpoch, headerField)
		}
		dateField.Epoch = epoch
	}
	if len(fieldspec) > 3 {
		dateField.Unit = Unit(fieldspec[3]).unit()

		if len(fieldspec) > 4 && (dateField.Unit.IsCustom()) {
			if _, err := time.ParseDuration(fieldspec[4]); err != nil {
				return nil, errors.Wrapf(err, "parsing custom unit %s", fieldspec[4])
			}
			dateField.CustomUnit = fieldspec[4]
		} else {
			if _, err := dateField.Unit.Duration(); err != nil {
				return nil, err
			}
		}
	}

	if len(fieldspec) > 5 {
		log.Printf("ignoring extra arguments to DateIntField %s: %v", headerField, fieldspec[5:])
	}
	return dateField, nil
}

func headerToTimestampField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	tsField := TimestampField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	granularity := "s"
	layout := time.RFC3339Nano
	if len(fieldspec) > 1 {
		granularity = fieldspec[1]
	}
	tsField.Granularity = granularity
	if len(fieldspec) > 2 {
		layout = fieldspec[2]
	}
	tsField.Layout = layout
	if len(fieldspec) > 3 {
		epoch, err := time.Parse(layout, fieldspec[3])
		if err != nil {
			return nil, errors.Wrapf(err, ErrParsingEpoch, headerField)
		}
		if epoch.IsZero() {
			tsField.Epoch = time.Unix(0, 0)
		} else {
			tsField.Epoch = epoch
		}
	}
	if len(fieldspec) > 4 {
		unit := Unit(fieldspec[4]).unit()
		if _, err := unit.Duration(); err != nil {
			return nil, errors.Wrapf(err, "invalid unit for TimestampField %s", headerField)
		}
		tsField.Unit = unit
	}
	if len(fieldspec) > 5 {
		log.Printf("ignoring extra arguments to TimestampField %s: %v", headerField, fieldspec[5:])
	}
	return tsField, nil
}

func headerToRecordTimeField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	rtField := RecordTimeField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	// We used to use a default of "" here, and then
	// (RecordTimeType).layout() would treat that as RFC3339.
	// This is now more parallel to the handling for DateIntType.
	layout := time.RFC3339
	if len(fieldspec) > 1 {
		layout = fieldspec[1]
	}
	rtField.Layout = layout
	if len(fieldspec) > 2 {
		epoch, err := time.Parse(layout, fieldspec[2])
		if err != nil {
			return nil, errors.Wrapf(err, ErrParsingEpoch, headerField)
		}
		rtField.Epoch = epoch
	}
	if len(fieldspec) > 3 {
		unit := Unit(fieldspec[3]).unit()
		_, err := unit.Duration()
		if err != nil {
			return nil, err
		}
		rtField.Unit = unit
	}
	if len(fieldspec) > 4 {
		log.Printf("ignoring extra arguments to RecordTimeField %s: %v", headerField, fieldspec[4:])
	}
	return rtField, nil
}

func headerToSignedIntBoolKeyField(headerField string, sourceName string, destName string, fieldspec []string, log logger.Logger) (Field, error) {
	field := SignedIntBoolKeyField{
		NameVal:     sourceName,
		DestNameVal: destName,
	}
	if len(fieldspec) > 1 {
		log.Printf("ignoring extra arguments to SignedIntBoolKeyField %s: %v", headerField, fieldspec[1:])
	}
	return field, nil
}

var nameSpecDelimiter = "___" // Triple Underscore

// splitHeader splits a header string.
// Allowed formats:
// - SourceName__FieldType_Arg_Arg2
// - SourceName___DestName__FieldType_Arg_Arg2
// where
// - SourceName may be an arbitrary string
// - DestName may be specified as a valid pilosa fieldname
// - DestName MUST be specified if SourceName is NOT a valid pilosa field name
// - DestName MUST NOT include any triple-underscores
// - FieldType MUST be specified
// - Args may or may not be required, depending on the FieldType
func splitHeader(s string) (sourceName, destName, typeSpec string, err error) {
	// One dunder is required, to separate name from fieldspec.
	// One trunder is optional, to separate sourceName from destName.

	var idx2 int
	idx3 := strings.LastIndex(s, nameSpecDelimiter)
	if idx3 == -1 {
		// simple namespec: SourceName__FieldType_Arg_Arg2
		s := s
		idx2 = strings.LastIndex(s, "__")
		if idx2 == -1 {
			return "", "", "", ErrNoFieldSpec
		}
		destName = s[:idx2]
		sourceName = destName
		typeSpec = s[idx2+2:]

	} else {
		// DestName namespec: SourceName___DestName__FieldType_Arg_Arg2
		sourceName = s[:idx3]
		rest := s[idx3+len(nameSpecDelimiter):]

		idx2 = strings.LastIndex(rest, "__")

		if idx2 == -1 {
			return "", "", "", ErrNoFieldSpec
		}
		destName = rest[:idx2]
		typeSpec = rest[idx2+2:]
	}

	err = pilosa.ValidateName(destName)
	if destName != "" && err != nil {
		// "" is valid for RecordTimeField and IgnoreField
		return "", "", "", err
	}
	return sourceName, destName, typeSpec, nil

}

type PathTable [][]string
type DeleteSentinel int

const (
	DELETE_SENTINEL DeleteSentinel = 1
)

func (t PathTable) Lookup(root interface{}, allowMissingFields bool) ([]interface{}, error) {
	data := make([]interface{}, len(t))
	for i, path := range t {
		node := root
		for j, branch := range path {
			obj, ok := node.(map[string]interface{})
			if !ok {
				return nil, errors.Errorf("failed to lookup path %v: element at %v is not an object", path, path[:j])
			}

			next, ok := obj[branch]
			if !ok {
				if allowMissingFields {
					node = nil
					break
				} else {
					return nil, errors.Errorf("failed to lookup path %v: element %q at %v is missing", path, branch, path[:j])
				}
			} else {
				if next == nil {
					node = DELETE_SENTINEL
					break
				}
				node = next
			}
		}

		data[i] = node
	}

	return data, nil
}

func (t PathTable) FlatMap() map[string]int {
	m, i := make(map[string]int), 0
	for _, arr := range t {
		for _, str := range arr {
			if _, ok := m[str]; !ok {
				m[str] = i
			}
			i++
		}
	}
	return m
}

func ParseHeader(raw []byte) ([]Field, PathTable, error) {
	var rawSchema []struct {
		Name   string   `json:"name"`
		Path   []string `json:"path"`
		Type   string   `json:"type"`
		Config json.RawMessage
	}
	err := json.Unmarshal(raw, &rawSchema)
	if err != nil {
		return nil, nil, errors.Wrap(err, "parsing schema")
	}

	fields, paths := make([]Field, len(rawSchema)), make(PathTable, len(rawSchema))
	fieldNameSet := make(map[string]int)
	for i, s := range rawSchema {
		if s.Path == nil {
			return nil, nil, errors.Errorf("field %q is missing a path", s.Name)
		}
		paths[i] = s.Path
		switch s.Type {
		case "id":
			var field IDField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		case "ids":
			var field IDArrayField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		case "string":
			var field StringField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		case "strings":
			var field StringArrayField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		case "bool":
			var field BoolField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		case "recordTime":
			var field RecordTimeField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		case "int":
			var field IntField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		case "decimal":
			var field DecimalField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		case "signedIntBoolKey":
			var field SignedIntBoolKeyField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		case "dateInt":
			var field DateIntField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		case "timestamp":
			var field TimestampField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field
		case "lookupText":
			var field LookupTextField
			if s.Config != nil {
				err := json.Unmarshal(s.Config, &field)
				if err != nil {
					return nil, nil, errors.Wrapf(err, ErrDecodingConfig, s.Name)
				}
			}
			field.NameVal = s.Name
			fields[i] = field

		default:
			return nil, nil, errors.Errorf("failed to initialize schema for field %q: unsupported type %q", s.Name, s.Type)
		}
		fieldName := fields[i].Name()
		if prev, ok := fieldNameSet[fieldName]; ok {
			return nil, nil, errors.Errorf("schema field %d duplicates name of field %d (%s)", i, prev, fieldName)
		}
		fieldNameSet[fieldName] = i
	}

	return fields, paths, nil
}
