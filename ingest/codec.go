// Copyright 2021 Molecula Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ingest

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
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

type KeyLookupFunc func(...string) (map[string]uint64, error)

// Codec is a single-use parser which decodes data into columnar vectors.
type Codec interface {
	AddSetField(name string, lookup KeyLookupFunc) error
	AddTimeQuantumField(name string, lookup KeyLookupFunc) error
	AddMutexField(name string, lookup KeyLookupFunc) error
	AddBoolField(name string) error
	AddIntField(name string, lookup KeyLookupFunc) error
	AddDecimalField(name string, scale int64) error
	AddTimestampField(name string, scale time.Duration, epoch int64) error

	// Parse data from a reader into the vectors.
	// This must only be called once on a codec.
	Parse(io.Reader) (*Request, error)
}

type jsonDecFn func(recID uint64, typ jsonparser.ValueType, data []byte) error

// applyTranslationFn is a function which applies key lookups to the values
// of an operation, meaning it needs to know whether it's applying them
// to the signed or unsigned values.
type applyTranslationFn func(*FieldOperation, []uint64) error

type jsonFieldCodec struct {
	valueKeys *StringTable
	currentOp *FieldOperation
	decode    jsonDecFn
	translate applyTranslationFn
	// For timestamp: scale-in-nanoseconds; for instance, if scaleUnit is
	// 1,000,000,000, we are storing numbers-of-seconds since the Unix epoch.
	// The actual value recorded in BSI will be offset by the field's
	// epoch, but we don't need to know that.
	// For decimal: Decimal digits of precision. So for instance, with
	// scaleUnit 2, "1" is stored as 100 and "1.2" is stored as 120.
	scaleUnit int64
	epoch     int64    // used only by Timestamp fields
	scratch   []uint64 // reusable scratch space for sets of values
	lookup    KeyLookupFunc
}

// JSONCodec is a Codec which accepts a JSON map of record keys/ids to updated value maps.
type JSONCodec struct {
	recKeys   *StringTable
	fields    map[string]*jsonFieldCodec
	keyLookup KeyLookupFunc
	currentOp *Operation
}

var _ Codec = &JSONCodec{}

func NewJSONCodec(lookup KeyLookupFunc) (*JSONCodec, error) {
	j := &JSONCodec{fields: map[string]*jsonFieldCodec{}}
	if lookup != nil {
		j.recKeys = NewStringTable()
		j.keyLookup = lookup
	}
	return j, nil
}

func (codec *JSONCodec) AddTimeQuantumField(name string, lookup KeyLookupFunc) error {
	fieldCodec := &jsonFieldCodec{}
	fieldCodec.decode = fieldCodec.DecodeTimeQuantumValue
	if lookup != nil {
		fieldCodec.translate = (*FieldOperation).TranslateUnsigned
	}
	return codec.addField(name, fieldCodec, lookup)
}

func (codec *JSONCodec) AddSetField(name string, lookup KeyLookupFunc) error {
	fieldCodec := &jsonFieldCodec{}
	fieldCodec.decode = fieldCodec.DecodeSetValue
	if lookup != nil {
		fieldCodec.translate = (*FieldOperation).TranslateUnsigned
	}
	return codec.addField(name, fieldCodec, lookup)
}

func (codec *JSONCodec) AddIntField(name string, lookup KeyLookupFunc) error {
	fieldCodec := &jsonFieldCodec{}
	fieldCodec.decode = fieldCodec.DecodeIntValue
	if lookup != nil {
		fieldCodec.translate = (*FieldOperation).TranslateSigned
	}
	return codec.addField(name, fieldCodec, lookup)
}

func (codec *JSONCodec) AddMutexField(name string, lookup KeyLookupFunc) error {
	fieldCodec := &jsonFieldCodec{}
	fieldCodec.decode = fieldCodec.DecodeMutexValue
	if lookup != nil {
		fieldCodec.translate = (*FieldOperation).TranslateUnsigned
	}
	return codec.addField(name, fieldCodec, lookup)
}

func (codec *JSONCodec) AddBoolField(name string) error {
	fieldCodec := &jsonFieldCodec{}
	fieldCodec.decode = fieldCodec.DecodeBoolValue
	return codec.addField(name, fieldCodec, nil)
}

// TimestampField is used to store seconds since unix epoch. The numeric values
// stored are adjusted based on the given time.Duration; for instance, if the
// time scale is time.Second, then the second after the epoch is stored as 1,
// if it's time.Millisecond, then it's stored as 1000, etcetera.
func (codec *JSONCodec) AddTimestampField(name string, timeScale time.Duration, epoch int64) error {
	fieldCodec := &jsonFieldCodec{scaleUnit: int64(timeScale), epoch: epoch}
	fieldCodec.decode = fieldCodec.DecodeTimeValue
	return codec.addField(name, fieldCodec, nil)
}

// AddDecimalField adds a decimal field, which is stored as integer values
// with a scale offset, but parsed as floating point values. For instance,
// with decimalScale=2, `0.01` would store the value 1.
func (codec *JSONCodec) AddDecimalField(name string, decimalScale int64) error {
	fieldCodec := &jsonFieldCodec{scaleUnit: int64(math.Pow10(int(decimalScale)))}
	fieldCodec.decode = fieldCodec.DecodeDecimalValue
	return codec.addField(name, fieldCodec, nil)
}

func (codec *JSONCodec) addField(name string, fieldCodec *jsonFieldCodec, lookup KeyLookupFunc) error {
	if _, ok := codec.fields[name]; ok {
		return fmt.Errorf("duplicate field %q", name)
	}
	if lookup != nil {
		fieldCodec.valueKeys = NewStringTable()
		fieldCodec.lookup = lookup
	}
	codec.fields[name] = fieldCodec
	return nil
}

// decodeSetOrValue decodes a value which might be either an array of values or a
// single value, where values might be string keys or bare numbers, calling cb for
// each value it finds.
func (j *jsonFieldCodec) decodeSetOrValue(dataType jsonparser.ValueType, data []byte, cb func(uint64) error) (err error) {
	switch dataType {
	case jsonparser.Array:
		_, arrayErr := jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, unused error) {
			id, idErr := j.valueKeys.ID(value)
			if idErr != nil {
				err = idErr
				return
			}
			// stash an error if we got one
			valueErr := cb(id)
			if valueErr != nil {
				err = valueErr
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
		return fmt.Errorf("expecting array, got %v", dataType)
	}
	return err
}

// DecodeSetValue decodes a set of unsigned values from the provided data into the
// associated currentOp.
func (j *jsonFieldCodec) DecodeSetValue(recID uint64, dataType jsonparser.ValueType, data []byte) (err error) {
	return j.decodeSetOrValue(dataType, data, func(id uint64) error {
		j.currentOp.AddPair(recID, id)
		return nil
	})
}

// DecodeIntValue decodes a single signed value from the provided data
// into the associated currentOp.
func (j *jsonFieldCodec) DecodeIntValue(recID uint64, dataType jsonparser.ValueType, data []byte) error {
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

// DecodeMutexValue decodes a single unsigned value from the provided data
// into the associated currentOp.
func (j *jsonFieldCodec) DecodeMutexValue(recID uint64, dataType jsonparser.ValueType, data []byte) error {
	switch dataType {
	case jsonparser.Number, jsonparser.String:
		id, err := j.valueKeys.ID(data)
		if err != nil {
			return err
		}
		j.currentOp.AddPair(recID, id)
	default:
		return fmt.Errorf("expecting integer value, got %v", dataType)
	}
	return nil
}

// DecodeBoolValue decodes a single true/false value from the provided data
// into the associated currentOp.
func (j *jsonFieldCodec) DecodeBoolValue(recID uint64, typ jsonparser.ValueType, data []byte) error {
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

// DecodeTimeQuantumValue decodes a timestamp, and a set of bits from the
// provided data into the associated currentOp.
func (j *jsonFieldCodec) DecodeTimeQuantumValue(recID uint64, typ jsonparser.ValueType, data []byte) error {
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

// DecodeTimeValue will eventually work but right now it doesn't actually.
func (j *jsonFieldCodec) DecodeTimeValue(recID uint64, dataType jsonparser.ValueType, data []byte) (err error) {
	var stamp time.Time
	switch dataType {
	case jsonparser.String:
		stamp, err = time.Parse(time.RFC3339Nano, pretendByteIsString(data))
		if err != nil {
			return fmt.Errorf("parsing timestamp: %w", err)
		}
		j.currentOp.AddSignedPair(recID, (stamp.UnixNano()/j.scaleUnit)-j.epoch)
	case jsonparser.Number:
		// We could in theory convert this to a time, then convert it
		// back, by multiplying by scaleUnit, then dividing. Or... not.
		i64, err := strconv.ParseInt(pretendByteIsString(data), 10, 64)
		if err != nil {
			return fmt.Errorf("parsing numeric timestamp: %w", err)
		}
		j.currentOp.AddSignedPair(recID, i64-j.epoch)
	}
	return nil
}

// DecodeDecimalValue will eventually work but right now it doesn't actually.
func (j *jsonFieldCodec) DecodeDecimalValue(recID uint64, dataType jsonparser.ValueType, data []byte) error {
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

func (codec *JSONCodec) ParseKeyedRecords(data []byte) (err error) {
	return jsonparser.ObjectEach(data, func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
		id, err := codec.recKeys.ID(key)
		if err != nil {
			return err
		}
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

func (codec *JSONCodec) ParseOperation(data []byte) (op *Operation, err error) {
	op = &Operation{FieldOps: make(map[string]*FieldOperation, len(codec.fields))}
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
	if op.OpType == OpNone {
		return nil, fmt.Errorf("action not specified")
	}
	return op, err
}

func (codec *JSONCodec) Parse(r io.Reader) (req *Request, err error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return codec.ParseBytes(data)
}

func (codec *JSONCodec) ParseBytes(data []byte) (req *Request, err error) {
	var ops []*Operation
	var lastErr error
	_, err = jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		switch dataType {
		case jsonparser.Object:
			op, err := codec.ParseOperation(value)
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
	if codec.keyLookup != nil {
		keyMap, err = MapForStringTable(codec.recKeys, codec.keyLookup)
		if err != nil {
			return nil, fmt.Errorf("trying to find record key mapping: %w", err)
		}
	}
	valueMaps := map[string]func(*FieldOperation) error{}
	for name, fieldCodec := range codec.fields {
		// make closure survive iteration
		fieldCodec := fieldCodec
		if fieldCodec.lookup != nil {
			fieldMap, err := MapForStringTable(fieldCodec.valueKeys, fieldCodec.lookup)
			if err != nil {
				return nil, fmt.Errorf("trying to find value mapping for %q: %w", name, err)
			}
			valueMaps[name] = func(fo *FieldOperation) error {
				return fieldCodec.translate(fo, fieldMap)
			}
		}
	}
	for _, op := range ops {
		// For Clear and Write, we need to translate/sort our record ID
		// list.
		if op.OpType == OpClear || op.OpType == OpWrite || op.OpType == OpDelete {
			if keyMap != nil {
				if err = translateUnsignedSlice(op.ClearRecordIDs, keyMap); err != nil {
					return nil, fmt.Errorf("mapping record keys for clear op: %w", err)
				}
			}
			op.Sort()
		}
		// For clear/delete, that's all we need to do; there's no meaningful fieldops under them.
		if op.OpType == OpClear || op.OpType == OpDelete {
			continue
		}
		for field, fieldOp := range op.FieldOps {
			if keyMap != nil {
				if err = fieldOp.TranslateKeys(keyMap); err != nil {
					return nil, fmt.Errorf("mapping record keys for op on %q: %w", field, err)
				}
			}
			if fieldTranslate, ok := valueMaps[field]; ok {
				if err = fieldTranslate(fieldOp); err != nil {
					return nil, fmt.Errorf("mapping values for op on %q: %w", field, err)
				}
			}
			// Sort by column keys, for now.
			fieldOp.Sort()
			// Write op will also want to clear every field we saw.
			if op.OpType == OpWrite {
				op.ClearFields = append(op.ClearFields, field)
			}
		}
	}
	return &Request{Ops: ops}, nil
}

type errFieldNotFound struct {
	field string
}

func (err errFieldNotFound) Error() string {
	return fmt.Sprintf("field not found: %q", err.field)
}
