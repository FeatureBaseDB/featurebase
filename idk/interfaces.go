package idk

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	pilosacore "github.com/featurebasedb/featurebase/v3"
	pilosaclient "github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/pkg/errors"
)

var (
	// ErrSchemaChange is returned from Source.Record when the returned
	// record has a different schema from the previous record.
	ErrSchemaChange = errors.New("this record has a different schema from the previous record (or is the first one delivered). Please call Source.Schema() to fetch the schema in order to properly decode this record")

	// ErrFlush is returned from Source.Record when the Source wants to
	// signal that there may not be data for a while, so it's a good time
	// to make sure all data which has been received is ingested. The
	// record must be nil when ErrFlush is returned.
	ErrFlush = errors.New("the Source is requesting the batch be flushed")

	ErrFmtUnknownUnit      = "unknown unit %q, please choose from d/h/m/s/ms/us/ns"
	ErrIntOutOfRange       = errors.New("value provided for int field is out of range")
	ErrDecimalOutOfRange   = errors.New("value provided for decimal field is out of range")
	ErrTimestampOutOfRange = errors.New("value provided for timestamp field is out of range")
)

type (
	// Source is an interface implemented by sources of data which can be
	// ingested into Pilosa. Each Record returned from Record is described
	// by the slice of Fields returned from Source.Schema directly after
	// the call to Source.Record. If the error returned from Source.Record
	// is nil, then the call to Schema which applied to the previous
	// Record also applies to this Record. Source implementations are
	// fundamentally not threadsafe (due to the interplay between Record
	// and Schema).
	Source interface {
		// Record returns a data record, and an optional error. If the
		// error is ErrSchemaChange, then the record is valid, but one
		// should call Source.Schema to understand how each of its fields
		// should be interpreted.
		Record() (Record, error)

		// Schema returns a slice of Fields which applies to the most
		// recent Record returned from Source.Record. Every Field has a
		// name and a type, and depending on the concrete type of the
		// Field, may have other information which is relevant to how it
		// should be indexed.
		Schema() []Field

		Close() error
	}

	Record interface {
		// Commit notifies the Source which produced this record that it
		// and any record which came before it have been completely
		// processed. The Source can then take any necessary action to
		// record which records have been processed, and restart from the
		// earliest unprocessed record in the event of a failure.
		Commit(ctx context.Context) error

		Data() []interface{}
		Schema() interface{}
	}

	// OffsetStreamRecord is an extension of the record type which also tracks offsets within streams.
	OffsetStreamRecord interface {
		Record

		// StreamOffset returns the stream from which the record originated, and the offset of the record within that stream.
		StreamOffset() (key string, offset uint64)
	}

	Metadata interface {
		// SchemaMetadata returns a string representation of source-specific details
		// about the schema.
		SchemaMetadata() string
		SchemaSubject() string
		SchemaSchema() string
		SchemaVersion() int
		SchemaID() int
	}

	// Field knows how to interpret values of different types and tells
	// how they get indexed in Pilosa. Every field implementation should
	// be a struct named like <something>Field, and have as members
	// `NameVal string` and `DestNameVal string`, where NameVal contains
	// the name of the field at the source, and DestNameVal contains the
	// name of the field at the destination (pilosa)
	//
	// Many Field implementations have a Quantum field which can be any
	// valid Pilosa time quantum, e.g. "Y", "YMDH", "DH", etc. If Quantum
	// is set to a valid quantum, the Pilosa field created for this field
	// will be of type "time". Other fields which control field type will
	// be ignored until/if Pilosa supports time+(othertype) fields.
	Field interface {
		Name() string
		DestName() string
		PilosafyVal(val interface{}) (interface{}, error) // TODO rename this
	}
)

// FieldsEqual is used in testing to compare Fields. The pointers make it a bit tricky for IntField.
func FieldsEqual(f1, f2 Field) bool {
	if reflect.TypeOf(f1) != reflect.TypeOf(f2) {
		return false
	}
	switch f1t := f1.(type) {
	case IgnoreField, IDField, BoolField, RecordTimeField, StringField, LookupTextField, DecimalField, SignedIntBoolKeyField, StringArrayField, IDArrayField, TimestampField, DateIntField:
		return f1 == f2
	case IntField:
		f2t := f2.(IntField)
		if f1t.NameVal == f2t.NameVal && f1t.DestNameVal == f2t.DestNameVal && f1t.ForeignIndex == f2t.ForeignIndex {
			if !(f1t.Min == nil && f2t.Min == nil) {
				if f1t.Min == nil || f2t.Min == nil {
					return false
				}
				if *f1t.Min != *f2t.Min {
					return false
				}
			}
			if !(f1t.Max == nil && f2t.Max == nil) {
				if f1t.Max == nil || f2t.Max == nil {
					return false
				}
				if *f1t.Max != *f2t.Max {
					return false
				}
			}
			return true
		}
		return false
	default:
		panic(fmt.Sprintf("unknown field type %T when comparing fields", f1))
	}
}

// CacheConfigOf returns CacheConfig of the Field.
func CacheConfigOf(f Field) CacheConfig {
	switch f := f.(type) {
	case StringField:
		if f.CacheConfig == nil {
			if f.Quantum != "" {
				return noneCacheConfig
			}
			return defaultCacheConfig
		}
		return *f.CacheConfig
	case StringArrayField:
		if f.CacheConfig == nil {
			if f.Quantum != "" {
				return noneCacheConfig
			}
			return defaultCacheConfig
		}
		return *f.CacheConfig
	case IDField:
		if f.CacheConfig == nil {
			if f.Quantum != "" {
				return noneCacheConfig
			}
			return defaultCacheConfig
		}
		return *f.CacheConfig
	case IDArrayField:
		if f.CacheConfig == nil {
			if f.Quantum != "" {
				return noneCacheConfig
			}
			return defaultCacheConfig
		}
		return *f.CacheConfig
	default:
		return defaultCacheConfig
	}
}

// QuantumOf returns Quantum of the Field.
func QuantumOf(fld Field) string {
	switch ft := fld.(type) {
	case IDField:
		return ft.Quantum
	case StringField:
		return ft.Quantum
	case StringArrayField:
		return ft.Quantum
	case IDArrayField:
		return ft.Quantum
	default:
		return ""
	}
}

func TTLOf(fld Field) (time.Duration, error) {
	var ttl time.Duration
	var err error

	switch ft := fld.(type) {
	case IDField:
		if ft.TTL != "" {
			ttl, err = time.ParseDuration(ft.TTL)
		} else {
			return 0, nil
		}
	case StringField:
		if ft.TTL != "" {
			ttl, err = time.ParseDuration(ft.TTL)
		} else {
			return 0, nil
		}
	case StringArrayField:
		if ft.TTL != "" {
			ttl, err = time.ParseDuration(ft.TTL)
		} else {
			return 0, nil
		}
	case IDArrayField:
		if ft.TTL != "" {
			ttl, err = time.ParseDuration(ft.TTL)
		} else {
			return 0, nil
		}
	default:
		return 0, nil
	}
	if err != nil {
		return ttl, errors.Wrapf(err, "unable to parse TTL from field %s", fld.Name())
	} else {
		return ttl, nil
	}
}

// HasMutex returns Mutex value of StringField or IDField, otherwise false
func HasMutex(fld Field) bool {
	if sfld, ok := fld.(StringField); ok && sfld.Quantum == "" {
		return sfld.Mutex
	}
	if sfld, ok := fld.(IDField); ok && sfld.Quantum == "" {
		return sfld.Mutex
	}
	return false
}

// IgnoreField can be used when you wish not to process one of the
// input fields, but it is inconvenient to remove it ahead of time.
type IgnoreField struct{}

func (IgnoreField) Name() string                                 { return "" }
func (IgnoreField) DestName() string                             { return "" }
func (IgnoreField) PilosafyVal(interface{}) (interface{}, error) { return nil, nil }

type IDField struct {
	NameVal     string
	DestNameVal string

	// Mutex denotes whether we need to enforce that each record only
	// has a single value for this field. Put another way, says
	// whether a new value for this field be treated as adding an
	// additional value, or replacing the existing value (if there is
	// one).
	Mutex bool

	// Quantum — see note about Quantum on "Field" interface.
	Quantum string

	TTL string

	*CacheConfig
}

func (id IDField) Name() string { return id.NameVal }
func (id IDField) DestName() string {
	if id.DestNameVal == "" {
		return id.NameVal
	}

	return id.DestNameVal
}
func (id IDField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	} else if vs, ok := val.(string); ok && vs == "" {
		return nil, nil
	}
	return toUint64(val)
}

type BoolField struct {
	NameVal     string
	DestNameVal string
}

func (b BoolField) Name() string { return b.NameVal }
func (b BoolField) DestName() string {
	if b.DestNameVal == "" {
		return b.NameVal
	}

	return b.DestNameVal
}
func (b BoolField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	} else if vs, ok := val.(string); ok && vs == "" {
		return nil, nil
	}
	return toBool(val)
}

var (
	MinTimestampNano = time.Unix(-1<<32, 0).UTC()       // 1833-11-24T17:31:44Z
	MaxTimestampNano = time.Unix(1<<32, 0).UTC()        // 2106-02-07T06:28:16Z
	MinTimestamp     = time.Unix(-62135596799, 0).UTC() // 0001-01-01T00:00:01Z
	MaxTimestamp     = time.Unix(253402300799, 0).UTC() // 9999-12-31T23:59:59Z
)

const (
	Custom      = Unit("c")
	Day         = Unit("d")
	Hour        = Unit("h")
	Minute      = Unit("m")
	Second      = Unit("s")
	Millisecond = Unit("ms")
	Microsecond = Unit("us")
	Nanosecond  = Unit("ns")

	DefaultUnit = Second
)

type Unit string

func (u Unit) unit() Unit {
	s := strings.ToLower(string(u))
	if s == "" {
		return DefaultUnit
	}
	return Unit(s)
}

func (u Unit) IsCustom() bool {
	return u.unit() == Custom
}

func (u Unit) Duration() (time.Duration, error) {
	duration := time.Duration(1)
	switch u.unit() {
	case Day:
		duration *= 24
		fallthrough
	case Hour:
		duration *= 60
		fallthrough
	case Minute:
		duration *= 60
		fallthrough
	case Second:
		duration *= 1000
		fallthrough
	case Millisecond:
		duration *= 1000
		fallthrough
	case Microsecond:
		duration *= 1000
		fallthrough
	case Nanosecond:
		return duration, nil
	}
	return 0, errors.Errorf(ErrFmtUnknownUnit, u)
}

// ToNanos returns the number of Nanoseconds per given Unit
func (u Unit) ToNanos() (int64, error) {
	duration := int64(1)
	switch u.unit() {
	case Day:
		duration *= 24
		fallthrough
	case Hour:
		duration *= 60
		fallthrough
	case Minute:
		duration *= 60
		fallthrough
	case Second:
		duration *= 1000
		fallthrough
	case Millisecond:
		duration *= 1000
		fallthrough
	case Microsecond:
		duration *= 1000
		fallthrough
	case Nanosecond:
		return duration, nil
	}
	return 0, errors.Errorf(ErrFmtUnknownUnit, u)
}

func (u Unit) DurationFromValue(val int64) (time.Duration, error) {
	scale, err := u.Duration()
	if err != nil {
		return 0, err
	}
	if max, min := int64(math.MaxInt64/scale), int64(math.MinInt64/scale); val > max || val < min {
		return 0, errors.Errorf("%d%s is outside representable time scales, must be between %d and %d", val, u, min, max)
	}
	return time.Duration(val) * scale, nil
}

func (r RecordTimeField) epoch() time.Time {
	if r.Epoch.IsZero() {
		return time.Unix(0, 0)
	}
	return r.Epoch
}

// RecordTimeField applies to whole record, but doesn't have a name
// (or quantum) of its own since it applies to any other time fields
// in the record.
type RecordTimeField struct {
	NameVal     string
	DestNameVal string
	Layout      string // Layout tells how the time should be parsed. Defaults to RFC3339.
	// need a way to create other time fields in the record (add time/quantum to String, StringArray, ID, IDArray?)
	// do we need a way to have timefields in a record with independent times/values
	Epoch time.Time
	Unit  Unit
}

func (r RecordTimeField) Name() string { return r.NameVal }
func (r RecordTimeField) DestName() string {
	if r.DestNameVal == "" {
		return r.NameVal
	}

	return r.DestNameVal
}

// PilosafyVal for RecordTimeField always returns a time.Time or nil.
func (r RecordTimeField) PilosafyVal(val interface{}) (interface{}, error) {
	if valt, ok := val.(time.Time); ok {
		return valt, nil
	}
	if !r.Epoch.IsZero() || r.Unit != "" {
		result, err := timeFromEpoch(val, r.epoch(), r.Unit)
		if err != nil {
			err = errors.Wrap(err, "converting RecordTimeField from epoch")
		}
		return result, err
	}

	result, err := timeFromTimestring(val, r.layout())
	if err != nil {
		err = errors.Wrap(err, "converting RecordTimeField from layout")
	}
	if result.IsZero() {
		return nil, err
	}
	return result, err
}

func (r RecordTimeField) layout() string {
	if r.Layout == "" {
		return time.RFC3339
	}
	return r.Layout
}

// CacheConfig - type (ranked, lru, none) and size.
type CacheConfig struct {
	CacheType pilosaclient.CacheType
	CacheSize int
}

var defaultCacheConfig = CacheConfig{CacheType: pilosaclient.CacheTypeRanked, CacheSize: pilosacore.DefaultCacheSize}
var noneCacheConfig = CacheConfig{CacheType: "", CacheSize: 0}

func (cfg CacheConfig) setOption() pilosaclient.FieldOption {
	if cfg == (CacheConfig{}) {
		cfg = defaultCacheConfig
	}

	return pilosaclient.OptFieldTypeSet(pilosaclient.CacheType(cfg.CacheType), cfg.CacheSize)
}

func (cfg CacheConfig) mutexOption() pilosaclient.FieldOption {
	if cfg == (CacheConfig{}) {
		cfg = defaultCacheConfig
	}

	return pilosaclient.OptFieldTypeMutex(pilosaclient.CacheType(cfg.CacheType), cfg.CacheSize)
}

type StringField struct {
	NameVal     string
	DestNameVal string

	// Mutex denotes whether we need to enforce that each record only
	// has a single value for this field. Put another way, says
	// whether a new value for this field be treated as adding an
	// additional value, or replacing the existing value (if there is
	// one).
	Mutex bool

	// Quantum — see note about Quantum on "Field" interface.
	Quantum string

	TTL string

	*CacheConfig
}

func (s StringField) Name() string { return s.NameVal }
func (s StringField) DestName() string {
	if s.DestNameVal == "" {
		return s.NameVal
	}

	return s.DestNameVal
}
func (s StringField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	return toString(val)
}

type LookupTextField struct {
	// NOTE this implements the Field interface for simplicity of implementation/API, but that interface is intended for data going into pilosa, while this is not.
	NameVal     string
	DestNameVal string
	// TODO this might should reference the lookupDB
}

func (s LookupTextField) Name() string { return s.NameVal }
func (s LookupTextField) DestName() string {
	if s.DestNameVal == "" {
		return s.NameVal
	}

	return s.DestNameVal
}
func (s LookupTextField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	return toString(val)
}

// IntField - if you add any new fields to this struct, please update the FieldsEqual function to accomodate.
type IntField struct {
	NameVal      string
	DestNameVal  string
	Min          *int64
	Max          *int64
	ForeignIndex string
}

func (i IntField) Name() string { return i.NameVal }
func (i IntField) DestName() string {
	if i.DestNameVal == "" {
		return i.NameVal
	}

	return i.DestNameVal
}
func (i IntField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	if valS, ok := val.(string); ok && i.ForeignIndex != "" {
		return valS, nil
	} else if ok && valS == "" {
		return nil, nil
	}
	asInt, err := toInt64(val)
	if err != nil {
		return nil, err
	}

	if i.Min != nil && asInt < *i.Min {
		return nil, errors.Wrapf(ErrIntOutOfRange, "field = %v, value %v is smaller than min allowed %v", i.Name(), asInt, *i.Min)
	}
	if i.Max != nil && asInt > *i.Max {
		return nil, errors.Wrapf(ErrIntOutOfRange, "field = %v, value %v is bigger than max allowed %v", i.Name(), asInt, *i.Max)
	}
	return asInt, nil
}

const DecimalPrecision = 18

type DecimalField struct {
	NameVal     string
	DestNameVal string
	Scale       int64
}

func (d DecimalField) Name() string { return d.NameVal }
func (d DecimalField) DestName() string {
	if d.DestNameVal == "" {
		return d.NameVal
	}

	return d.DestNameVal
}

// PilosafyVal for DecimalField always returns an int64. If the
// incoming value is anything but a float or string we attempt to
// convert to int64 and then scale it. Strings are attempted to be
// parsed into floats, and all values are scaled by the 10^scale
// before being returned. Byte slices are assumed to represent the
// already scaled value and are interpreted as int64.
func (d DecimalField) PilosafyVal(val interface{}) (interface{}, error) {
	// Make sure the provided scale is supported before proceeding.
	// Note: the "1" value below doesn't matter; we're just
	// validating the scale.
	// Also, the existing tests don't exercise this check because
	// they error creating the field prior to getting here.
	// TODO: let's move scale validation to the pql package so
	// that it can control the specific error message.
	if !pql.NewDecimal(1, d.Scale).IsValid() {
		return nil, errors.Errorf("scale values outside the range [0,19] are not supported: %d", d.Scale)
	}

	if val == nil {
		return nil, nil
	}
	if vs, ok := val.(string); ok {
		if vs == "" {
			return nil, nil
		}
		asInt, err := scaledStringToInt(d.Scale, vs)
		if err != nil {
			// scaledStringToInt returns 0 on error. If there is a problem parsing the string,
			// we want to import nil.
			return nil, errors.Wrap(err, ErrDecimalOutOfRange.Error())
		}
		return asInt, nil
	}
	switch vt := val.(type) {
	case pql.Decimal:
		return vt.ToInt64(d.Scale), nil

	case float32:
		v := vt * float32(math.Pow10(int(d.Scale)))
		return int64(v), nil
	case float64:
		vt = vt * math.Pow10(int(d.Scale))
		return int64(vt), nil
	case []byte:
		// 16: int64(value)+int64(scale)
		//  8: int64(value)
		var tmp [8]byte
		if len(vt) == 16 {
			value := int64(binary.BigEndian.Uint64(vt[0:8]))
			scale := int64(binary.BigEndian.Uint64(vt[8:16]))
			return pql.NewDecimal(value, scale).ToInt64(d.Scale), nil
		} else if len(vt) == 8 {
			return int64(binary.BigEndian.Uint64(vt)), nil
		} else if len(vt) < 8 {
			copy(tmp[8-len(vt):], vt)
			return int64(binary.BigEndian.Uint64(tmp[:])), nil
		} else {
			return nil, errors.Errorf("can only support decimal value up to 8 bytes, or 16 bytes containing value and scale, got %d for %s", len(vt), d.Name())
		}
	default:
		v, err := toInt64(val)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't convert %v to int64 for decimal field", val)
		}
		return pql.NewDecimal(v, 0).ToInt64(d.Scale), nil
	}
}

// SignedIntBoolKeyField translates a signed integer value to a (rowID, bool)
// pair corresponding to the magnitude and sign of the original value. This
// may be used to specify whether a bool value is to be set (positive/true)
// or cleared (negative/false).
type SignedIntBoolKeyField struct {
	NameVal     string
	DestNameVal string
}

func (b SignedIntBoolKeyField) Name() string { return b.NameVal }
func (b SignedIntBoolKeyField) DestName() string {
	if b.DestNameVal == "" {
		return b.NameVal
	}

	return b.DestNameVal
}

func (SignedIntBoolKeyField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	} else if vs, ok := val.(string); ok && vs == "" {
		return nil, nil
	}

	return toInt64(val)
}

type StringArrayField struct {
	NameVal     string
	DestNameVal string

	// Quantum — see note about Quantum on "Field" interface.
	Quantum string

	TTL string

	*CacheConfig
}

func (s StringArrayField) Name() string { return s.NameVal }
func (s StringArrayField) DestName() string {
	if s.DestNameVal == "" {
		return s.NameVal
	}

	return s.DestNameVal
}
func (StringArrayField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	return toStringArray(val)
}

type IDArrayField struct {
	NameVal     string
	DestNameVal string

	// Quantum — see note about Quantum on "Field" interface.
	Quantum string

	TTL string

	*CacheConfig
}

func (i IDArrayField) Name() string { return i.NameVal }
func (i IDArrayField) DestName() string {
	if i.DestNameVal == "" {
		return i.NameVal
	}

	return i.DestNameVal
}
func (IDArrayField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	return toUint64Array(val)
}

type TimestampField struct {
	NameVal     string
	DestNameVal string
	Layout      string
	Epoch       time.Time
	Unit        Unit
	Granularity string
}

func (d TimestampField) Name() string { return d.NameVal }

func (d TimestampField) DestName() string {
	if d.DestNameVal == "" {
		return d.NameVal
	}
	return d.DestNameVal
}

// ValToTimestamp takes a timeunit and an integer value and converts it to time.Time
func ValToTimestamp(unit string, val int64) (time.Time, error) {
	switch unit {
	case string(Second):
		return time.Unix(val, 0).UTC(), nil
	case string(Millisecond):
		return time.UnixMilli(val).UTC(), nil
	case string(Microsecond):
		return time.UnixMicro(val).UTC(), nil
	case string(Nanosecond):
		return time.Unix(0, val).UTC(), nil
	default:
		return time.Time{}, errors.Errorf("Unknown time unit: '%v'", unit)
	}
}

// TimestampToVal takes a time unit and a time.Time and converts it to an integer value
func TimestampToVal(unit Unit, ts time.Time) int64 {
	switch unit {
	case Second:
		return ts.Unix()
	case Millisecond:
		return ts.UnixMilli()
	case Microsecond:
		return ts.UnixMicro()
	case Nanosecond:
		return ts.UnixNano()
	}
	return 0

}

/*
// PilosafyVal for TimestampField always returns an int or nil.
func (t TimestampField) PilosafyVal(val interface{}) (interface{}, error) {

}
*/

// PilosafyVal for TimestampField always returns an int or nil.
func (t TimestampField) PilosafyVal(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	var dur int64
	// Check if the epoch alone is out-of-range. If so, ingest should halt, regardless
	// of state of the timestamp out-of-range CLI option.
	if err := validateTimestamp(t.granularity(), t.epoch()); err != nil {
		return nil, errors.Wrap(err, "validating epoch")
	}

	epochAsVal := TimestampToVal(t.granularity(), t.epoch())
	if _, ok := val.(time.Time); ok || (t.Epoch.IsZero() && t.Unit == "") {
		ts, err := timeFromTimestring(val, t.layout())
		if err != nil {
			if strings.Contains(err.Error(), "out of range") {
				return nil, errors.Wrap(err, ErrTimestampOutOfRange.Error())
			}
			return nil, errors.Wrap(err, "converting TimestampField from layout")
		}
		if err := validateTimestamp(t.granularity(), ts); err != nil {
			return nil, errors.Wrap(ErrTimestampOutOfRange, "validating timestamp")
		}

		tsAsVal := TimestampToVal(t.granularity(), ts)

		dur = tsAsVal - epochAsVal
	} else if _, ok := val.([]uint8); ok {
		valAsString := string(val.([]uint8)[:])
		// try to convert as time string
		ts, err := timeFromTimestring(valAsString, t.layout())
		if err != nil {
			// if that doesn't work, maybe it's an int represented as a string
			valAsInt, err := strconv.ParseInt(valAsString, 0, 64)
			if err == nil {
				return valAsInt - epochAsVal, nil
			} else {
				return nil, errors.Wrap(err, "converting TimestampField")
			}
		}
		if err := validateTimestamp(t.granularity(), ts); err != nil {
			return nil, errors.Wrap(ErrTimestampOutOfRange, "validating timestamp")
		}

		tsAsVal := TimestampToVal(t.granularity(), ts)

		dur = tsAsVal - epochAsVal
	} else {
		valAsInt, err := toInt64(val)
		if err != nil {
			if strings.Contains(err.Error(), "out of range") {
				return nil, errors.Wrap(err, ErrTimestampOutOfRange.Error())
			}
			return nil, errors.Wrap(err, "converting value to int64")
		}

		// Conversion ratio to scale incoming Units to Granularity
		granNanos, err := Unit(t.granularity()).ToNanos()
		if err != nil || granNanos == 0 {
			return nil, errors.Wrap(err, "granularity not supported")
		}
		unitNanos, err := Unit(t.Unit).ToNanos()
		if err != nil {
			return nil, errors.Wrap(err, "unit not supported")
		}
		scale := float64(unitNanos) / float64(granNanos)

		dur = int64(float64(valAsInt) * scale)
		if (dur >= 0 && valAsInt < 0) || (dur < 0 && valAsInt > 0) {
			return nil, errors.Wrap(ErrTimestampOutOfRange, "timestamp value out of range at specified granularity")
		}

		if err := validateDuration(dur, epochAsVal, Unit(t.granularity())); err != nil {
			return nil, errors.Wrap(err, "validating duration")
		}
	}

	return dur, nil
}

// validateTimestamp checks if the timestamp is within the range of what FB accepts.
func validateTimestamp(unit Unit, ts time.Time) error {
	// Min and Max timestamps that Featurebase accepts
	var minStamp, maxStamp time.Time
	switch unit {
	case Nanosecond:
		minStamp = MinTimestampNano
		maxStamp = MaxTimestampNano
	default:
		minStamp = MinTimestamp
		maxStamp = MaxTimestamp
	}

	if ts.Before(minStamp) || ts.After(maxStamp) {
		return errors.New(fmt.Sprintf("timestamp value must be within min: %v and max: %v", minStamp, maxStamp))
	}
	return nil
}

// validateDuration checks if the duration will overflow. Users can provide a custom epoch but
// Featurebase will ultimately convert this to some duration relative to the Unix epoch.
// So if the custom epoch + the provided value in the desired units is too far from
// Unix epoch such that it causes an interger overflow, this will return an error.
func validateDuration(dur int64, offset int64, granularity Unit) error {
	var minInt, maxInt int64
	switch granularity {
	case Second:
		minInt = MinTimestamp.Unix()
		maxInt = MaxTimestamp.Unix()
	case Millisecond:
		minInt = MinTimestamp.UnixMilli()
		maxInt = MaxTimestamp.UnixMilli()
	case Microsecond:
		minInt = MinTimestamp.UnixMicro()
		maxInt = MaxTimestamp.UnixMicro()
	case Nanosecond:
		minInt = MinTimestampNano.UnixNano()
		maxInt = MaxTimestampNano.UnixNano()
	}

	if offset > 0 {
		if dur > maxInt-offset {
			return errors.Wrap(ErrTimestampOutOfRange, "value + epoch is too far from Unix epoch")
		}
	} else if dur < minInt-offset {
		return errors.Wrap(ErrTimestampOutOfRange, "value + epoch is too far from Unix epoch")
	}
	return nil
}

// Return default granularity if not set
func (t TimestampField) granularity() Unit {
	if t.Granularity == "" {
		return "s"
	}
	return Unit(t.Granularity)
}

// Return default layout if not set
func (t TimestampField) layout() string {
	if t.Layout == "" {
		return time.RFC3339Nano
	}
	return t.Layout
}

// Return default epoch if not set
func (t TimestampField) epoch() time.Time {
	if t.Epoch.IsZero() {
		return time.Unix(0, 0)
	}
	return t.Epoch
}

type DateIntField struct {
	NameVal     string
	DestNameVal string
	Layout      string
	Epoch       time.Time
	Unit        Unit
	CustomUnit  string
}

func (d DateIntField) Name() string { return d.NameVal }
func (d DateIntField) DestName() string {
	if d.DestNameVal == "" {
		return d.NameVal
	}

	return d.DestNameVal
}

// PilosafyVal for a DateIntField takes a time.Time and int64 which
// represents the number units from the epoch.
func (d DateIntField) PilosafyVal(val interface{}) (interface{}, error) {
	var vt time.Time
	var err error
	switch valt := val.(type) {
	case nil:
		return nil, nil
	case []byte:
		if len(valt) == 0 {
			return nil, nil
		}
		vt, err = parseTimeWithLayout(d.layout(), string(valt))
		if err != nil {
			return nil, errors.Wrap(err, "converting DateIntField []byte")
		}
	case string:
		vt, err = parseTimeWithLayout(d.layout(), valt)
		if err != nil {
			return nil, errors.Wrap(err, "converting DateIntField string")
		}
	case time.Time:
		vt = valt
	case uint, int, uint8, uint16, uint32, uint64, int8, int16, int32, int64:
		return toInt64(valt)
	default:
		return nil, errors.Errorf("didn't know how to handle %v of type %[1]T in DateIntField", valt)
	}

	dur := vt.Sub(d.epoch())

	var unit time.Duration
	if d.Unit.IsCustom() {
		if unit, err = time.ParseDuration(d.CustomUnit); err != nil {
			return nil, errors.Wrapf(err, "parsing custom unit %s", d.CustomUnit)
		}
	} else {
		if unit, err = d.Unit.Duration(); err != nil {
			return nil, errors.Wrapf(err, "parsing unit %s", d.Unit)
		}
	}

	return int64(dur / unit), nil
}

func (d DateIntField) epoch() time.Time {
	if d.Epoch.IsZero() {
		return time.Unix(0, 0)
	}
	return d.Epoch
}

func (d DateIntField) layout() string {
	if d.Layout == "" {
		// this is kind of a ridiculous default for layout
		return "2006-01-02"
	}
	return d.Layout
}

func parseTimeWithLayout(layout string, val string) (time.Time, error) {
	if val == "0000-00-00" { // TODO this is kind of a special hack
		// that we should remove - was added
		// for a particular case of unparsable
		// data that we wanted to ignore.
		return time.Time{}, nil
	}
	tim, err := time.Parse(layout, val)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "parsing time string %s", val)
	}
	return tim, nil
}

func timeFromEpoch(val interface{}, epoch time.Time, unit Unit) (interface{}, error) {
	valAsInt, err := toInt64(val)
	if err != nil {
		return nil, errors.Wrap(err, "converting value to int64")
	}

	dur, err := unit.DurationFromValue(valAsInt)
	if err != nil {
		return nil, errors.Wrap(err, "getting duration from value")
	}

	return epoch.Add(dur), err
}

func timeFromTimestring(val interface{}, layout string) (time.Time, error) {
	if val == nil {
		return time.Time{}, nil
	}
	switch valt := val.(type) {
	case nil:
		return time.Time{}, nil
	case []byte:
		if len(valt) == 0 {
			return time.Time{}, nil
		}
		vt, err := parseTimeWithLayout(layout, string(valt))
		if err != nil {
			return time.Time{}, errors.Wrap(err, "parsing []byte")
		}
		return vt, nil
	case string:
		if valt == "" {
			return time.Time{}, nil
		}
		vt, err := parseTimeWithLayout(layout, valt)
		if err != nil {
			return time.Time{}, errors.Wrapf(err, "parsing time string %s", valt)
		}
		return vt, nil
	case time.Time:
		return valt, nil
	default:
		return time.Time{}, errors.Errorf("didn't know how to interpret %v of %[1]T as time", valt)
	}
}

func toUint64(val interface{}) (uint64, error) {
	switch vt := val.(type) {
	case uint:
		return uint64(vt), nil
	case uint8:
		return uint64(vt), nil
	case uint16:
		return uint64(vt), nil
	case uint32:
		return uint64(vt), nil
	case uint64:
		return vt, nil
	case int:
		return uint64(vt), nil
	case int8:
		return uint64(vt), nil
	case int16:
		return uint64(vt), nil
	case int32:
		return uint64(vt), nil
	case int64:
		return uint64(vt), nil
	case float64:
		return uint64(vt), nil
	case string:
		v, err := strconv.ParseUint(strings.TrimSpace(vt), 10, 64)
		if err != nil {
			return 0, err
		}
		return v, nil
	default:
		return 0, errors.Errorf("couldn't convert %v of %[1]T to uint64", vt)
	}
}

func toBool(val interface{}) (bool, error) {
	switch vt := val.(type) {
	case bool:
		return vt, nil
	case byte:
		if vt == '0' || vt == 'f' || vt == 'F' {
			return false, nil
		}
		return vt != 0, nil
	case string:
		vt = strings.ToLower(vt)
		vt = strings.TrimSpace(vt)
		switch vt {
		case "", "0", "f", "false":
			return false, nil
		case "1", "t", "true":
			return true, nil
		}
		return false, errors.Errorf("couldn't convert %v of %[1]T to bool", vt)

	default:
		if vint, err := toInt64(val); err == nil {
			return vint != 0, nil
		}
		return false, errors.Errorf("couldn't convert %v of %[1]T to bool", vt)
	}
}

func toString(val interface{}) (string, error) {
	switch vt := val.(type) {
	case string:
		return vt, nil
	case []byte:
		return string(vt), nil
	default:
		if vt == nil {
			return "", nil
		}
		return fmt.Sprintf("%v", val), nil
	}
}

func toInt64(val interface{}) (int64, error) {
	switch vt := val.(type) {
	case uint:
		return int64(vt), nil
	case uint8:
		return int64(vt), nil
	case uint16:
		return int64(vt), nil
	case uint32:
		return int64(vt), nil
	case uint64:
		return int64(vt), nil
	case int:
		return int64(vt), nil
	case int8:
		return int64(vt), nil
	case int16:
		return int64(vt), nil
	case int32:
		return int64(vt), nil
	case int64:
		return vt, nil
	case float32:
		return int64(vt), nil
	case float64:
		return int64(vt), nil
	case string: // added this case because of mysql driver sending the ids as strings
		v, err := strconv.ParseInt(strings.TrimSpace(vt), 10, 64)
		if err != nil {
			return 0, err
		}
		return v, nil
	case []uint8:
		return toInt64(string(vt[:]))
	default:
		return 0, errors.Errorf("couldn't convert %v of %[1]T to int64", vt)
	}
}

func toStringArray(val interface{}) ([]string, error) {
	switch vt := val.(type) {
	case []string:
		return vt, nil
	case []uint64:
		ret := make([]string, len(vt))
		for i, v := range vt {
			ret[i] = strconv.FormatUint(v, 10)
		}
		return ret, nil
	case map[uint64]struct{}:
		arr, err := toUint64Array(val)
		if err != nil {
			return nil, err
		}
		return toStringArray(arr)
	case string:
		if vt == "" {
			return nil, nil
		}
		if strings.HasPrefix(vt, "[") && strings.HasSuffix(vt, "]") {
			vt = vt[1 : len(vt)-1]
		}
		vals := strings.Split(vt, ",")
		return vals, nil
	case []interface{}:
		ret := make([]string, len(vt))
		for i, v := range vt {
			switch v.(type) {
			case []uint8: // byte slice
				ret[i] = string(v.([]uint8)[:])
			default:
				vs, ok := v.(string)
				if !ok {
					return nil, errors.Errorf("couldn't convert []interface{} to []string, value %v of type %[1]T at %d", v, i)
				}
				ret[i] = vs
			}
		}
		return ret, nil
	default:
		return nil, errors.Errorf("couldn't convert %v of %[1]T to []string", vt)
	}
}

func toUint64Array(val interface{}) ([]uint64, error) {
	switch vt := val.(type) {
	case []interface{}:
		if len(vt) == 0 {
			// Empty/nil set.
			return nil, nil
		}
		arr := make([]uint64, len(vt))
		for i := range vt {
			vv, err := toUint64(vt[i])
			if err != nil {
				return nil, errors.Wrapf(err, "non uint64 value in []interface{}: %v (%[1]T)", vt[i])
			}
			arr[i] = vv
		}
		return arr, nil
	case map[uint64]struct{}:
		if len(vt) == 0 {
			// Empty/nil set.
			return nil, nil
		}
		arr := make([]uint64, len(vt))
		i := 0
		for v := range vt {
			arr[i] = v
			i++
		}

		// Move the elements into a deterministic order.
		sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })

		return arr, nil
	case []uint64:
		return vt, nil
	case string:
		if vt == "" {
			return nil, nil
		}
		if strings.HasPrefix(vt, "[") && strings.HasSuffix(vt, "]") {
			vt = vt[1 : len(vt)-1]
		}
		parts := strings.Split(vt, ",")
		ret := make([]uint64, len(parts))
		for i := range parts {
			v, err := strconv.ParseUint(strings.TrimSpace(parts[i]), 10, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "parsing uint64 from string: %s", vt)
			}
			ret[i] = v
		}
		return ret, nil
	default:
		return nil, errors.Errorf("couldn't convert %v of %[1]T to []uint64", vt)
	}
}

// Fields is a list of Field, representing a schema.
type Fields []Field

// ContainsBool returns true if at least one field
// in the list is a BoolField.
func (f Fields) ContainsBool() bool {
	for i := range f {
		if _, ok := f[i].(BoolField); ok {
			return true
		}
	}
	return false
}

// SchemaManager is meant to be an interface for managing schema information;
// i.e. for interacting with a single source of truth for schema information,
// like the Serverless Schemar. But... it currently contains methods which are
// not related to schema because the first goal was just to introduce an
// interface in ingest.go for any methods being called on *m.client. We don't
// want a FeatureBase client directly called from ingest, rather, we want to
// call these interface methods and allow for different implementations (such as
// a Serverless implementation which uses the Schemar in Serverless as opposed
// to a FeatureBase node or cluster).
type SchemaManager interface {
	StartTransaction(id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*pilosacore.Transaction, error)
	FinishTransaction(id string) (*pilosacore.Transaction, error)
	Schema() (*pilosaclient.Schema, error)
	SyncIndex(index *pilosaclient.Index) error
	DeleteIndex(index *pilosaclient.Index) error
	Status() (pilosaclient.Status, error)
	SetAuthToken(string)
}

// Ensure type implements interface.
var _ SchemaManager = &nopSchemaManager{}

// NopSchemaManager is an implementation of the SchemaManager interface that
// doesn't do anything.
var NopSchemaManager SchemaManager = &nopSchemaManager{}

type nopSchemaManager struct{}

func (n *nopSchemaManager) StartTransaction(id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*pilosacore.Transaction, error) {
	return nil, nil
}
func (n *nopSchemaManager) FinishTransaction(id string) (*pilosacore.Transaction, error) {
	return nil, nil
}
func (n *nopSchemaManager) Schema() (*pilosaclient.Schema, error) {
	return nil, nil
}
func (n *nopSchemaManager) SyncIndex(index *pilosaclient.Index) error {
	return nil
}
func (n *nopSchemaManager) DeleteIndex(index *pilosaclient.Index) error {
	return nil
}
func (n *nopSchemaManager) Status() (pilosaclient.Status, error) {
	return pilosaclient.Status{}, nil
}
func (n *nopSchemaManager) SetAuthToken(token string) {}
