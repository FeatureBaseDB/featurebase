// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
)

// Featurebase has the following field types as of this writing, we plan to
// support all of them but not all are implemented.
//
// Type		Single	Signed	Timestamp
// set		no	no	no
// time		no	no	yes
// mutex	yes	no	no
// int		yes	yes	no
// decimal	yes	yes	no
// timestamp	yes	yes	no

// KeyTranslator is a thing that can translate strings to IDs, and also
// IDs back to strings. The ID->string conversion is used only to render
// a request back to JSON, which in turn is only used in testing. The
// functions optionally take an existing map which they then augment.
type KeyTranslator interface {
	TranslateKeys(keys ...string) (map[string]uint64, error)
	TranslateIDs(ids ...uint64) (map[uint64]string, error)
}

// Codec is a single-use parser which decodes data into columnar vectors.
type Codec interface {
	AddSetField(name string, keys KeyTranslator) error
	AddTimeQuantumField(name string, keys KeyTranslator) error
	AddMutexField(name string, keys KeyTranslator) error
	AddBoolField(name string) error
	AddIntField(name string, keys KeyTranslator) error
	AddDecimalField(name string, scale int64) error
	AddTimestampField(name string, scale string, epoch int64) error

	// Parse data from a reader into the vectors.
	// This must only be called once on a codec.
	Parse(io.Reader) (*Request, error)
}

type jsonDecFn func(recID uint64, typ jsonparser.ValueType, data []byte) error

// jsonEncFn encodes a value, or range of values, according to a given
// fieldCodec's translation rules. key values, if present, will be used to
// replace whichever values could have been provided as keys. so for instance,
// with an int field which is keyed, values are actually of type int64, but
// if strings are present, those are used. for a time quantum field, the time
// is set from the signed field, and the value is replaced by the key if
// keys are provided.
type jsonEncFn func(dst *jsonBuffer, values []uint64, signed []int64, keys []string) error

// fieldCodec represents something that can encode and decode a
// particular field. Its methods are not reentrant, it uses internal
// buffers.
type fieldCodec struct {
	valueKeys *StringTable
	currentOp *FieldOperation
	decode    jsonDecFn
	encode    jsonEncFn
	// For decimal: Decimal digits of precision. So for instance, with
	// scale 2, scaleUnit is 100, "1" is stored as 100 and "1.2" is stored as
	// 120.
	scaleUnit int64
	// For timestamp: we use timeUnit to determine the scale at which
	// to store a timestamp. For example if the timeUnit is milliseconds
	// we store the number of milliseconds from the given epoch.
	timeUnit  string
	scale     int64
	epoch     int64    // used only by Timestamp fields
	scratch   []uint64 // reusable scratch space for sets of values
	keys      KeyTranslator
	buf       []byte // scratch space for format operations.
	fieldType FieldType
}

// JSONCodec is a Codec which accepts a JSON map of record keys/ids to updated value maps.
type JSONCodec struct {
	fieldTypes map[string]FieldType
	recKeys    *StringTable
	fields     map[string]*fieldCodec
	keys       KeyTranslator
	currentOp  *Operation
}

// jsonBuffer is a bytes.Buffer which has an associated json.Encoder which
// can be used to write to its buffer. Both types are just embedded because
// they have non-overlapping APIs. Please don't look at my horrible face.
//
// This has some internal objects it can use to stash things
// so that it can pass &foo to enc.Encode() without needing to heap-allocate
// something for the interface conversion, and a buffer it can use for
// converting numbers or times.
//
// It also has an internal error buffer. In fact, in many cases, errors
// are so far as I can tell absolutely impossible -- bytes.Buffer specifically
// promises never to yield an error, and nothing seems to hint that
// enc.Encode can error on integers, strings, booleans, or arrays. So we
// have dozens of error checks that we can't cause to happen even with
// malformed inputs... so we eat those errors, don't require them to be
// checked externally, and return them when done if anyone checks.
type jsonBuffer struct {
	*bytes.Buffer
	enc     *json.Encoder
	buf     [48]byte // scratch space for using strconv.AppendInt, etc
	uintbuf []uint64 // dummy buffer so that we don't have to alloc to print []uint64
	strbuf  []string // dummy buffer so that we don't have to alloc to print []string
	str     string   // and again, "so we don't have to alloc a copy"
	err     error
}

// Encode removes the stray newlines added by json.Encoder.
func (j *jsonBuffer) Encode(v interface{}) {
	err := j.enc.Encode(v)
	if err == nil {
		j.Truncate(j.Len() - 1)
	} else {
		j.err = err
	}
}

// EncodeInt uses AppendInt into a static buffer to reduce allocs.
func (j *jsonBuffer) EncodeInt(i int64) {
	rep := strconv.AppendInt(j.buf[:0], i, 10)
	_, _ = j.Write(rep)
}

// EncodeUint uses AppendUint into a static buffer to reduce allocs.
func (j *jsonBuffer) EncodeUint(u uint64) {
	rep := strconv.AppendUint(j.buf[:0], u, 10)
	_, _ = j.Write(rep)
}

// EncodeUints uses a static copy of a []uint64 -- the slice, not its
// contents --  in already-allocated memory so the interface conversion
// doesn't have to do that.
func (j *jsonBuffer) EncodeUints(u []uint64) {
	j.uintbuf = u
	j.Encode(&j.uintbuf)
	j.strbuf = nil
}

// EncodeStrings uses a static copy of a []string -- the slice, not its
// contents --  in already-allocated memory so the interface conversion
// doesn't have to do that.
func (j *jsonBuffer) EncodeStrings(s []string) {
	j.strbuf = s
	j.Encode(&j.strbuf)
	j.strbuf = nil
}

// EncodeQuotedUint uses AppendUint into a static buffer to reduce allocs,
// while also surrounding the value with quotes.
func (j *jsonBuffer) EncodeQuotedUint(u uint64) {
	j.buf[0] = '"'
	rep := strconv.AppendUint(j.buf[1:1], u, 10)
	j.buf[len(rep)+1] = '"'
	_, _ = j.Write(j.buf[:len(rep)+2])
}

// EncodeString appends the JSON encoding of a string. This exists
// because otherwise runtime allocates a heap-allocated copy of the
// string to live inside an interface{} for the duration of a function
// call...
func (j *jsonBuffer) EncodeString(s string) {
	j.str = s
	j.Encode(&j.str)
	j.str = ""
}

// EncodeTime exists because time's MarshalJSON allocates a new
// buffer every time it gets called, resulting in a full 13% of
// all the allocations produced in a test run, plus another 13%
// or so of them which were for the copies of the time objects
// made to stuff them into an interface{}. Eww.
func (j *jsonBuffer) EncodeTime(t time.Time) {
	j.buf[0] = '"'
	rep := t.AppendFormat(j.buf[1:1], time.RFC3339Nano)
	j.buf[len(rep)+1] = '"'
	_, _ = j.Write(j.buf[:len(rep)+2])
}

// EncodeBool writes a literal representation directly to reduce allocs.
func (j *jsonBuffer) EncodeBool(b bool) {
	if b {
		_, _ = j.WriteString("true")
	} else {
		_, _ = j.WriteString("false")
	}
}

func (j *jsonBuffer) Err() error {
	return j.err
}

func newJSONBuffer(data []byte) *jsonBuffer {
	e := &jsonBuffer{Buffer: bytes.NewBuffer(data)}
	e.enc = json.NewEncoder(e.Buffer)
	e.enc.SetEscapeHTML(false) // we are not doing HTML, just JSON
	return e
}

var _ Codec = &JSONCodec{}

func NewJSONCodec(keys KeyTranslator) (*JSONCodec, error) {
	j := &JSONCodec{
		fields:     map[string]*fieldCodec{},
		fieldTypes: map[string]FieldType{},
	}
	if keys != nil {
		j.recKeys = NewStringTable()
		j.keys = keys
	}
	return j, nil
}

func (codec *JSONCodec) AddTimeQuantumField(name string, keys KeyTranslator) error {
	fieldCodec := &fieldCodec{
		fieldType: FieldTypeTimeQuantum,
		keys:      keys,
	}
	fieldCodec.decode = fieldCodec.DecodeTimeQuantumValue
	fieldCodec.encode = fieldCodec.EncodeTimeQuantumValue
	return codec.addField(name, fieldCodec)
}

func (codec *JSONCodec) AddSetField(name string, keys KeyTranslator) error {
	fieldCodec := &fieldCodec{
		fieldType: FieldTypeSet,
		keys:      keys,
	}
	fieldCodec.decode = fieldCodec.DecodeSetValue
	fieldCodec.encode = fieldCodec.EncodeSetValue
	return codec.addField(name, fieldCodec)
}

func (codec *JSONCodec) AddIntField(name string, keys KeyTranslator) error {
	fieldCodec := &fieldCodec{
		fieldType: FieldTypeInt,
		keys:      keys,
	}
	fieldCodec.decode = fieldCodec.DecodeIntValue
	fieldCodec.encode = fieldCodec.EncodeIntValue
	return codec.addField(name, fieldCodec)
}

func (codec *JSONCodec) AddMutexField(name string, keys KeyTranslator) error {
	fieldCodec := &fieldCodec{
		fieldType: FieldTypeMutex,
		keys:      keys,
	}
	fieldCodec.decode = fieldCodec.DecodeMutexValue
	fieldCodec.encode = fieldCodec.EncodeMutexValue
	return codec.addField(name, fieldCodec)
}

func (codec *JSONCodec) AddBoolField(name string) error {
	fieldCodec := &fieldCodec{
		fieldType: FieldTypeBool,
	}
	fieldCodec.decode = fieldCodec.DecodeBoolValue
	fieldCodec.encode = fieldCodec.EncodeBoolValue
	return codec.addField(name, fieldCodec)
}

// TimestampField is used to store seconds since unix epoch. The numeric values
// stored are adjusted based on the given time.Duration; for instance, if the
// time scale is time.Second, then the second after the epoch is stored as 1,
// if it's time.Millisecond, then it's stored as 1000, etcetera. The epoch
// passed to this function should be the offset from the Unix epoch to the
// desired epoch, in the same scale. (So if the scale is milliseconds,
// it should be the Unix timestamp in seconds, times 1000.)
func (codec *JSONCodec) AddTimestampField(name string, timeScale string, epoch int64) error {
	fieldCodec := &fieldCodec{
		fieldType: FieldTypeTimeStamp,
		timeUnit:  timeScale,
		epoch:     epoch,
	}
	fieldCodec.decode = fieldCodec.DecodeTimeValue
	fieldCodec.encode = fieldCodec.EncodeTimeValue
	return codec.addField(name, fieldCodec)
}

// AddDecimalField adds a decimal field, which is stored as integer values
// with a scale offset, but parsed as floating point values. For instance,
// with decimalScale=2, `0.01` would store the value 1.
func (codec *JSONCodec) AddDecimalField(name string, decimalScale int64) error {
	fieldCodec := &fieldCodec{
		fieldType: FieldTypeDecimal,
		scale:     decimalScale,
		scaleUnit: int64(math.Pow(10, float64(decimalScale))),
	}
	fieldCodec.decode = fieldCodec.DecodeDecimalValue
	fieldCodec.encode = fieldCodec.EncodeDecimalValue
	return codec.addField(name, fieldCodec)
}

func (codec *JSONCodec) addField(name string, fieldCodec *fieldCodec) error {
	if _, ok := codec.fields[name]; ok {
		return fmt.Errorf("duplicate field %q", name)
	}
	if fieldCodec.keys != nil {
		fieldCodec.valueKeys = NewStringTable()
	}
	codec.fields[name] = fieldCodec
	codec.fieldTypes[name] = fieldCodec.fieldType
	return nil
}

// decodeSetOrValue decodes a value which might be either an array of values or a
// single value, where values might be string keys or bare numbers, calling cb for
// each value it finds.
func (j *fieldCodec) decodeSetOrValue(dataType jsonparser.ValueType, data []byte, cb func(uint64) error) (err error) {
	switch dataType {
	case jsonparser.Array:
		_, arrayErr := jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, unused error) {
			var id uint64
			switch dataType {
			case jsonparser.String:
				id, err = j.valueKeys.ID(value)
				if err != nil {
					return
				}
				err = cb(id)
			case jsonparser.Number:
				if j.valueKeys != nil {
					err = errors.New("expecting key, got numeric value")
					return
				}
				id, err = strconv.ParseUint(pretendByteIsString(value), 10, 64)
				if err != nil {
					return
				}
				err = cb(id)
			default:
				err = fmt.Errorf("expecting value or array, got %v", dataType)
			}
		})
		if err != nil {
			return err
		}
		if arrayErr != nil {
			return arrayErr
		}
	case jsonparser.String:
		id, err := j.valueKeys.ID(data)
		if err != nil {
			return err
		}
		return cb(id)
	case jsonparser.Number:
		if j.valueKeys != nil {
			return errors.New("expecting key, got numeric value")
		}
		id, err := strconv.ParseUint(pretendByteIsString(data), 10, 64)
		if err != nil {
			return err
		}
		return cb(id)
	default:
		return fmt.Errorf("expecting value or array, got %v", dataType)
	}
	return err
}

// DecodeSetValue decodes a set of unsigned values from the provided data into the
// associated currentOp.
func (j *fieldCodec) DecodeSetValue(recID uint64, dataType jsonparser.ValueType, data []byte) (err error) {
	return j.decodeSetOrValue(dataType, data, func(id uint64) error {
		j.currentOp.AddPair(recID, id)
		return nil
	})
}

func (j *fieldCodec) EncodeSetValue(dst *jsonBuffer, values []uint64, signed []int64, keys []string) error {
	if len(keys) == 1 || len(values) == 1 {
		// simplify: just hand this off to a single-value case
		return j.EncodeMutexValue(dst, values, signed, keys)
	}
	appendKeysJSON(dst, values, keys)
	return nil
}

// DecodeIntValue decodes a single signed value from the provided data
// into the associated currentOp.
func (j *fieldCodec) DecodeIntValue(recID uint64, dataType jsonparser.ValueType, data []byte) error {
	switch dataType {
	case jsonparser.String:
		value, err := j.valueKeys.IntID(data)
		if err != nil {
			return err
		}
		j.currentOp.AddSignedPair(recID, value)
	case jsonparser.Number:
		if j.valueKeys != nil {
			return errors.New("expecting string key, got numeric value")
		}
		value, err := strconv.ParseInt(pretendByteIsString(data), 10, 64)
		if err != nil {
			return err
		}
		j.currentOp.AddSignedPair(recID, value)
	default:
		if j.valueKeys != nil {
			return fmt.Errorf("expecting string key, got %v", dataType)
		} else {
			return fmt.Errorf("expecting integer value, got %v", dataType)
		}
	}
	return nil
}

func (j *fieldCodec) EncodeIntValue(dst *jsonBuffer, values []uint64, signed []int64, keys []string) error {
	if len(keys) == 0 {
		if len(signed) == 0 {
			return errors.New("encodeIntValue: need a value")
		}
		dst.EncodeInt(signed[0])
		return nil
	}
	dst.EncodeString(keys[0])
	return nil
}

// DecodeMutexValue decodes a single unsigned value from the provided data
// into the associated currentOp.
func (j *fieldCodec) DecodeMutexValue(recID uint64, dataType jsonparser.ValueType, data []byte) error {
	switch dataType {
	case jsonparser.String:
		value, err := j.valueKeys.ID(data)
		if err != nil {
			return err
		}
		j.currentOp.AddPair(recID, value)
	case jsonparser.Number:
		if j.valueKeys != nil {
			return errors.New("expecting string key, got numeric value")
		}
		value, err := strconv.ParseUint(pretendByteIsString(data), 10, 64)
		if err != nil {
			return err
		}
		j.currentOp.AddPair(recID, value)
	default:
		if j.valueKeys != nil {
			return fmt.Errorf("expecting string key, got %v", dataType)
		} else {
			return fmt.Errorf("expecting integer value, got %v", dataType)
		}
	}
	return nil
}

func (j *fieldCodec) EncodeMutexValue(dst *jsonBuffer, values []uint64, signed []int64, keys []string) error {
	if len(keys) == 0 {
		if len(values) == 0 {
			return errors.New("encodeMutexValue: need a value")
		}
		dst.EncodeUint(values[0])
		return nil
	}
	dst.EncodeString(keys[0])
	return nil
}

// DecodeBoolValue decodes a single true/false value from the provided data
// into the associated currentOp.
func (j *fieldCodec) DecodeBoolValue(recID uint64, typ jsonparser.ValueType, data []byte) error {
	value := uint64(0)
	switch typ {
	case jsonparser.String:
		if string(data) != "true" && string(data) != "false" {
			return fmt.Errorf("expecting boolean, got %q", data)
		}
		// if it's exactly "true" or "false" let's be forgiving
		fallthrough
	case jsonparser.Boolean:
		if data[0] == 't' {
			value = 1
		}
	case jsonparser.Number:
		v, err := jsonparser.GetInt(data)
		if err != nil {
			return err
		}
		if v == 1 {
			value = 1
		} else if v != 0 {
			return errors.New("boolean should be true/false/0/1")
		}
	default:
		return errors.New("boolean should be true/false/0/1")
	}
	j.currentOp.AddPair(recID, value)
	return nil
}

func (j *fieldCodec) EncodeBoolValue(dst *jsonBuffer, values []uint64, signed []int64, keys []string) error {
	if len(values) == 0 {
		return errors.New("encoding boolean value, but none provided")
	}
	var x bool
	if values[0] != 0 {
		x = true
	}
	dst.EncodeBool(x)
	return nil
}

// DecodeTimeQuantumValue decodes a timestamp, and a set of bits from the
// provided data into the associated currentOp.
func (j *fieldCodec) DecodeTimeQuantumValue(recID uint64, typ jsonparser.ValueType, data []byte) error {
	j.scratch = j.scratch[:0]
	stamp := time.Unix(0, 0).UTC()
	err := jsonparser.ObjectEach(data, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) (err error) {
		switch string(key) {
		case "time":
			switch dataType {
			case jsonparser.String:
				stamp, err = time.Parse(time.RFC3339, pretendByteIsString(value))
			case jsonparser.Number:
				var unix int64
				unix, err = strconv.ParseInt(pretendByteIsString(value), 10, 64)
				stamp = time.Unix(unix, 0)
			default:
				return fmt.Errorf("expecting time, got %q", value)
			}
		case "values":
			err = j.decodeSetOrValue(dataType, value, func(id uint64) error {
				j.scratch = append(j.scratch, id)
				return nil
			})
		}
		return err
	})
	if err != nil {
		return err
	}
	if len(j.scratch) > 0 {
		defer func() {
			// mark these as consumed so if we get called
			// again, and don't see a "values" key, we don't reuse them.
			j.scratch = j.scratch[:0]
		}()
		unix := stamp.UnixNano()
		for _, value := range j.scratch {
			j.currentOp.AddStampedPair(recID, value, unix)
		}
	}
	return nil
}

// Our format does not allow a record to have multiple values set with
// different timestamps at the same time. We use the first timestamp for
// the whole set.
func (j *fieldCodec) EncodeTimeQuantumValue(dst *jsonBuffer, values []uint64, signed []int64, keys []string) error {
	if len(values) == 0 {
		return errors.New("encoding time quantum value: no value provided")
	}
	dst.WriteString(`{"values":`)
	appendKeysJSON(dst, values, keys)
	// a zero timestamp is idiomatic for no-time-provided, and i sort of
	// hate that, but here we are.
	if len(signed) > 0 && signed[0] != 0 {
		_, _ = dst.WriteString(`,"time":`)
		dst.EncodeTime(time.Unix(0, signed[0]).UTC())
	}
	_, _ = dst.WriteString(`}`)
	return nil
}

// DecodeTimeValue will eventually work but right now it doesn't actually.
func (j *fieldCodec) DecodeTimeValue(recID uint64, dataType jsonparser.ValueType, data []byte) (err error) {
	var stamp time.Time
	switch dataType {
	case jsonparser.String:
		stamp, err = time.Parse(time.RFC3339Nano, pretendByteIsString(data))
		if err != nil {
			return fmt.Errorf("parsing timestamp: %w", err)
		}
		j.currentOp.AddSignedPair(recID, TimestampToVal(j.timeUnit, stamp)-j.epoch)
	case jsonparser.Number:
		// We could in theory convert this to a time, then convert it
		// back, by multiplying by scaleUnit, then dividing. Or... not.
		i64, err := strconv.ParseInt(pretendByteIsString(data), 10, 64)
		if err != nil {
			return fmt.Errorf("parsing numeric timestamp: %w", err)
		}
		j.currentOp.AddSignedPair(recID, i64)
	default:
		return fmt.Errorf("expecting time, got %s", dataType)
	}
	return nil
}

func (j *fieldCodec) EncodeTimeValue(dst *jsonBuffer, values []uint64, signed []int64, keys []string) error {
	if len(signed) == 0 {
		return errors.New("encoding time value: no value provided")
	}
	t, err := ValToTimestamp(j.timeUnit, signed[0]+j.epoch)
	if err != nil {
		return errors.Wrap(err, "translating value to timestamp")
	}
	dst.EncodeTime(t)
	return nil
}

// DecodeDecimalValue will eventually work but right now it doesn't actually.
func (j *fieldCodec) DecodeDecimalValue(recID uint64, dataType jsonparser.ValueType, data []byte) error {
	switch dataType {
	case jsonparser.String, jsonparser.Number:
		value, err := jsonparser.GetFloat(data)
		if err != nil {
			return err
		}
		j.currentOp.AddSignedPair(recID, int64(value*float64(j.scaleUnit)))
	default:
		return fmt.Errorf("expecting floating-point value, got %v", dataType)
	}

	return nil
}

func (j *fieldCodec) EncodeDecimalValue(dst *jsonBuffer, values []uint64, signed []int64, keys []string) error {
	j.buf = strconv.AppendInt(j.buf[:0], signed[0], 10)
	scale := int(j.scale)
	if len(j.buf) > scale {
		j.buf = append(j.buf, '.')
		// shove the last scaleUnit values over
		copy(j.buf[len(j.buf)-scale:], j.buf[len(j.buf)-scale-1:])
		j.buf[len(j.buf)-scale-1] = '.'
	}
	_, _ = dst.Write(j.buf)
	return nil
}

// FieldTypes gives a mapping of fields to their basic types used by this
// codec.
func (codec *JSONCodec) FieldTypes() map[string]FieldType {
	return codec.fieldTypes
}

// ParseKeyedRecords parses the records it finds. Note that, when you're doing
// a Write op, this will update ClearRecordIDs automatically as it goes,
// even though record_ids isn't specified in the JSON for that case.
func (codec *JSONCodec) ParseKeyedRecords(data []byte) (err error) {
	seen := make(map[uint64]struct{})
	return jsonparser.ObjectEach(data, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		id, err := codec.recKeys.ID(key)
		if err != nil {
			return err
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("key %q duplicated in input", key)
		}
		seen[id] = struct{}{}
		if codec.currentOp.OpType == OpWrite {
			codec.currentOp.ClearRecordIDs = append(codec.currentOp.ClearRecordIDs, id)
		}
		return jsonparser.ObjectEach(value, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			fieldCodec, ok := codec.fields[string(key)]
			if !ok {
				return errFieldNotFound{string(key)}
			}
			err := fieldCodec.decode(id, dataType, value)
			if err != nil {
				return fmt.Errorf("parsing value for field %q: %v", key, err)
			}
			return nil
		})
	})
}

func (codec *JSONCodec) ParseOperation(data []byte, seq int) (op *Operation, err error) {
	op = &Operation{FieldOps: make(map[string]*FieldOperation, len(codec.fields)), Seq: seq}
	codec.currentOp = op
	err = jsonparser.ObjectEach(data, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		switch string(key) {
		case "action":
			op.OpType, err = ParseOpType(string(value))
			if err != nil {
				return fmt.Errorf("unknown action type %q", value)
			}
		case "records":
			for name, fieldCodec := range codec.fields {
				fieldOp := &FieldOperation{}
				op.FieldOps[name] = fieldOp
				// cache this op so we don't have to do the lookups every time
				fieldCodec.currentOp = fieldOp
			}
			err = codec.ParseKeyedRecords(value)
			if err != nil {
				return fmt.Errorf("parsing records: %v", err)
			}
		case "record_ids":
			var id uint64
			var idErr error
			_, err = jsonparser.ArrayEach(value, func(value []byte, dataType jsonparser.ValueType, offset int, unused error) {
				id, idErr = codec.recKeys.ID(value)
				if idErr != nil {
					return
				}
				op.ClearRecordIDs = append(op.ClearRecordIDs, id)

			})
			// if we got an error converting an ID, error out with it here.
			// we can't stop the ArrayEach early, though?
			if idErr != nil {
				return idErr
			}
			if err != nil {
				return err
			}
		case "fields":
			_, err = jsonparser.ArrayEach(value, func(value []byte, dataType jsonparser.ValueType, offset int, unused error) {
				op.ClearFields = append(op.ClearFields, string(value))
			})
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown operation field %q", key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if op.OpType == OpNone {
		return nil, fmt.Errorf("action not specified")
	}
	return op, err
}

// Parse reads a request, but does not sort the results at all or divide
// them into shards.
func (codec *JSONCodec) Parse(r io.Reader) (req *Request, err error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return codec.ParseBytes(data)
}

// ParseBytes reads a request from a slice of bytes. We need to use a
// byte slice because jsonparser's model is fundamentally built around
// being able to random-access the slice and return slices of it, so
// it can't really admit functional streaming. If we need streaming, the
// streaming needs to be at a higher level.
func (codec *JSONCodec) ParseBytes(data []byte) (req *Request, err error) {
	var ops []*Operation
	var lastErr error
	var seq int
	_, err = jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		switch dataType {
		case jsonparser.Object:
			op, err := codec.ParseOperation(value, seq)
			seq++
			if err != nil {
				lastErr = fmt.Errorf("parsing operation: %v", err)
				return
			}
			ops = append(ops, op)
		default:
			lastErr = fmt.Errorf("expected operation, found %s", dataType)
		}
	})
	if lastErr != nil {
		return nil, lastErr
	}
	if err != nil {
		return nil, err
	}
	// and now, key translation!
	var keyMap []uint64
	if codec.keys != nil {
		keyMap, err = codec.recKeys.MakeIDMap(codec.keys)
		if err != nil {
			return nil, fmt.Errorf("trying to find record key mapping: %w", err)
		}
	}
	req = &Request{}
	valueMaps := map[string]func(*FieldOperation) error{}
	for name, fieldCodec := range codec.fields {
		// make closure survive iteration
		fieldCodec := fieldCodec
		if fieldCodec.keys != nil {
			fieldMap, err := fieldCodec.valueKeys.MakeIDMap(fieldCodec.keys)
			if err != nil {
				return nil, fmt.Errorf("trying to find value mapping for %q: %w", name, err)
			}
			if fieldCodec.fieldType == FieldTypeInt {
				valueMaps[name] = func(fo *FieldOperation) error {
					return translateSigned(fieldMap, fo.Signed)
				}
			} else {
				valueMaps[name] = func(fo *FieldOperation) error {
					return translateUnsigned(fieldMap, fo.Values)
				}
			}
		}
	}
	for _, op := range ops {
		// For Clear and Write, we need to translate/sort our record ID
		// list.
		if op.OpType == OpClear || op.OpType == OpWrite || op.OpType == OpDelete {
			if keyMap != nil {
				if err = translateUnsigned(keyMap, op.ClearRecordIDs); err != nil {
					return nil, fmt.Errorf("mapping record keys for clear op: %w", err)
				}
			}
		}
		// For clear/delete, that's all we need to do; there's no meaningful fieldops under them.
		if op.OpType == OpClear || op.OpType == OpDelete {
			continue
		}
		for field, fieldOp := range op.FieldOps {
			if len(fieldOp.RecordIDs) == 0 {
				delete(op.FieldOps, field)
				continue
			}
			if keyMap != nil {
				if err = translateUnsigned(keyMap, fieldOp.RecordIDs); err != nil {
					return nil, fmt.Errorf("mapping record keys for op on %q: %w", field, err)
				}
			}
			if fieldTranslate, ok := valueMaps[field]; ok {
				if err = fieldTranslate(fieldOp); err != nil {
					return nil, fmt.Errorf("mapping values for op on %q: %w", field, err)
				}
			}
			// Sort by column keys, for now.
			// Write op will also want to clear every field we saw.
			if op.OpType == OpWrite {
				op.ClearFields = append(op.ClearFields, field)
			}
		}
	}
	req.Ops = ops
	return req, nil
}

// AppendBytes appends bytes which we would expect to produce the same
// request, using this codec. It does not try to recreate FieldTypes, which
// would be handled by the codec anyway. It's written as an Append so you
// can reuse a buffer.
func (codec *JSONCodec) AppendBytes(req *Request, data []byte) (out []byte, err error) {
	if req == nil || len(req.Ops) == 0 {
		return append(data, "[]"...), nil
	}
	dst := newJSONBuffer(data)
	// we ignore the FieldTypes part of the Request, which is just there to
	// let the request's ByShard use the correct sorting routines, which is
	// itself sort of awful. The codec will insert it again when parsing the
	// bytes.
	_, _ = dst.WriteString(`,`)
	for _, op := range req.Ops {
		// EncodeJSON needs to have access to this codec's field data
		// and key translators.
		err = op.EncodeJSON(dst, codec)
		if err != nil {
			return nil, err
		}
		_, _ = dst.WriteString(`,`)
	}
	// delete the last comma
	dst.Truncate(dst.Len() - 1)
	_, _ = dst.WriteString(`]`)
	// there's very few ways an error could occur, possibly none, but
	// just in case lots of internal operations stashed an error if one
	// happened, so we'll return anything they came up with.
	return dst.Bytes(), dst.Err()
}

// appendIDsJSON appends the provided IDs to a JSON stream as a bracketed
// list of quoted strings if there's a key translator, or integer values
// if the KeyTranslator is nil, comma-separated. It can error because a
// translator can error.
func (codec *JSONCodec) appendIDsJSON(dst *jsonBuffer, values []uint64, keys KeyTranslator) error {
	return appendIDsJSON(dst, values, keys)
}

func appendKeysJSON(dst *jsonBuffer, values []uint64, keys []string) {
	if len(values) == 0 && len(keys) == 0 {
		_, _ = dst.WriteString("[]")
		return
	}
	if len(keys) != 0 {
		dst.EncodeStrings(keys)
	} else {
		dst.EncodeUints(values)
	}
}

func appendIDsJSON(dst *jsonBuffer, values []uint64, keys KeyTranslator) error {
	if len(values) == 0 {
		_, _ = dst.WriteString("[]")
		return nil
	}

	if keys == nil {
		dst.EncodeUints(values)
		return nil
	}
	_, _ = dst.WriteString(`[`)
	translated, err := keys.TranslateIDs(values...)
	if err != nil {
		return err
	}
	for _, v := range values {
		dst.EncodeString(translated[v])
		_, _ = dst.WriteString(`,`)
	}
	dst.Truncate(dst.Len() - 1)
	_, _ = dst.WriteString(`]`)
	return nil
}

// RequestByShard makes up for the fact that we don't want to stash the
// field type data in the request, but we need it to actually do the by-shard.
func (codec *JSONCodec) RequestByShard(req *Request) (*ShardedRequest, error) {
	return req.ByShard(codec.fieldTypes)
}

// EncodeJSON encodes an operation as JSON using a provided buffer.
// It uses the provided codec where necessary to help with key
// translation.
func (o *Operation) EncodeJSON(dst *jsonBuffer, codec *JSONCodec) (err error) {
	// We're not trying to guarantee that what we produce makes sense or is
	// identical to what produced us, just to write our current state out.
	if o == nil {
		_, _ = dst.WriteString(`{}`)
		return nil
	}
	_, _ = dst.WriteString(`{"action":`)
	dst.EncodeString(o.OpType.String())
	// it is intentional that o.Seq isn't encoded here; it makes no sense
	// to allow an op to specify its seq in JSON.
	if o.OpType != OpWrite {
		// for Write, ClearRecordIDs and ClearFields were computed
		// from the records being written, and aren't actually part of the data.
		if len(o.ClearRecordIDs) != 0 {
			_, _ = dst.WriteString(`,"record_ids":`)
			err = codec.appendIDsJSON(dst, o.ClearRecordIDs, codec.keys)
			if err != nil {
				return err
			}
		}
		if len(o.ClearFields) != 0 {
			_, _ = dst.WriteString(`,"fields":`)
			dst.EncodeStrings(o.ClearFields)
		}
	}
	if len(o.FieldOps) == 0 {
		_, _ = dst.WriteString(`}`)
		return
	}
	// collect all the fields that have a non-empty set of records. we can
	// just ignore the others.
	fieldNames := make([]string, 0, len(o.FieldOps))
	for field, op := range o.FieldOps {
		if op != nil && len(op.RecordIDs) != 0 {
			fieldNames = append(fieldNames, field)
		}
	}
	// nevermind then
	if len(fieldNames) == 0 {
		_, _ = dst.WriteString(`}`)
		return nil
	}
	_, _ = dst.WriteString(`,"records":{`)

	// now we have to invert the logic, creating records from
	// fields with corresponding ops. uh-oh.
	fieldOps := make([]*FieldOperation, len(o.FieldOps))
	fieldCodecs := make([]*fieldCodec, len(o.FieldOps))
	fieldKeys := make([][]string, len(o.FieldOps)) // translated field keys
	indexes := make([]int, len(o.FieldOps))
	sort.Slice(fieldNames, func(i, j int) bool { return fieldNames[i] < fieldNames[j] })
	next := ^uint64(0)
	var idKeys map[uint64]string
	if codec.keys != nil {
		idKeys = make(map[uint64]string)
	}
	for i, field := range fieldNames {
		// populate a parallel slice of field ops so we don't have
		// to do map lookups for every single piece of data
		op := o.FieldOps[field]
		fieldOps[i] = op
		fc := codec.fields[field]
		if fc == nil {
			return fmt.Errorf("unknown field: %q", field)
		}
		fieldCodecs[i] = fc
		if codec.keys != nil {
			// we'll build a list of keys we need for any records
			for _, v := range op.RecordIDs {
				idKeys[v] = ""
			}
		}
		id := op.RecordIDs[0]
		if id < next {
			next = id
		}
		if fc.keys != nil {
			thisFieldKeys := make([]string, len(op.RecordIDs))
			if fc.fieldType == FieldTypeInt {
				u := make([]uint64, len(op.Signed))
				for i := range op.Signed {
					u[i] = uint64(op.Signed[i])
				}
				valueKeys, err := fc.keys.TranslateIDs(u...)
				if err != nil {
					return err
				}
				for j, v := range op.Signed {
					thisFieldKeys[j] = valueKeys[uint64(v)]
				}
			} else {
				valueKeys, err := fc.keys.TranslateIDs(op.Values...)
				if err != nil {
					return err
				}
				for j, v := range op.Values {
					thisFieldKeys[j] = valueKeys[v]
				}
			}
			fieldKeys[i] = thisFieldKeys
		}
	}
	// ... no fieldops actually have any entries. therefore no record will
	// have any fields set, therefore no records exist...
	if next == ^uint64(0) {
		_, _ = dst.WriteString(`}}`)
		return nil
	}
	if codec.keys != nil {
		idList := make([]uint64, 0, len(idKeys))
		for k := range idKeys {
			idList = append(idList, k)
		}
		idKeys, err = codec.keys.TranslateIDs(idList...)
		if err != nil {
			return err
		}
	}

	// next is the ID of a record which exists for at least one field.
	// we'll recompute it every loop.
	for next != ^uint64(0) {
		if codec.keys != nil {
			dst.EncodeString(idKeys[next])
		} else {
			dst.EncodeQuotedUint(next)
		}
		_, _ = dst.WriteString(`:{`)
		current := next
		next = ^uint64(0)
		for i, field := range fieldNames {
			op := fieldOps[i]
			idx := indexes[i]
			if idx >= len(op.RecordIDs) {
				continue
			}
			id := op.RecordIDs[idx]
			if id == current {
				var j int
				// count ahead to first index which is either outside the list
				// or a different id
				for j = idx; j < len(op.RecordIDs) && op.RecordIDs[j] == id; j++ {
				}

				// 	field, idx, j, len(op.Values), len(op.Signed), len(fieldKeys[i]))
				// print this one, and advance this index to next position
				dst.EncodeString(field)
				_, _ = dst.WriteString(`:`)
				var values []uint64
				var signed []int64
				var keys []string
				if len(op.Values) >= j {
					values = op.Values[idx:j]
				}
				if len(op.Signed) >= j {
					signed = op.Signed[idx:j]
				}
				if len(fieldKeys[i]) >= j {
					keys = fieldKeys[i][idx:j]
				}
				err = fieldCodecs[i].encode(dst, values, signed, keys)
				if err != nil {
					return err
				}
				_, _ = dst.WriteString(`,`)
				indexes[i] = j
				// and we'll fall through to the id < next check, so we
				// just set id here.
				if indexes[i] < len(op.RecordIDs) {
					id = op.RecordIDs[indexes[i]]
				} else {
					id = ^uint64(0)
				}
			}
			if id < next {
				next = id
			}
		}
		// we should always have a trailing comma after any entry, and
		// if there were no entries we shouldn't have had a list...
		dst.Truncate(dst.Len() - 1)
		_, _ = dst.WriteString(`},`)
	}
	dst.Truncate(dst.Len() - 1)
	// close both the records list, and the whole object.
	_, _ = dst.WriteString(`}}`)
	// In theory, there's no way for most of these to produce errors,
	// but just in case, we'll check for an error every operation or so.
	return dst.Err()
}

type errFieldNotFound struct {
	field string
}

func (err errFieldNotFound) Error() string {
	return fmt.Sprintf("field not found: %q", err.field)
}

// TimestampToVal takes a time unit and a time.Time and converts it to an integer value
func TimestampToVal(unit string, ts time.Time) int64 {
	switch unit {
	case "s":
		return ts.Unix()
	case "ms":
		return ts.UnixMilli()
	case "us":
		return ts.UnixMicro()
	case "ns":
		return ts.UnixNano()
	}
	return 0

}

// ValToTimestamp takes a timeunit and an integer value and converts it to time.Time
func ValToTimestamp(unit string, val int64) (time.Time, error) {
	switch unit {
	case "s":
		return time.Unix(val, 0).UTC(), nil
	case "ms":
		return time.UnixMilli(val).UTC(), nil
	case "us", "μs":
		return time.UnixMicro(val).UTC(), nil
	case "ns":
		return time.Unix(0, val).UTC(), nil
	default:
		return time.Time{}, errors.Errorf("Unknown time unit: '%v'", unit)
	}
}
