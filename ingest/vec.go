// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ingest

import (
	"fmt"
	"reflect"
	"strconv"
	"unsafe"
)

// StringTable is a mapping of strings to temporary IDs.
// All mapped-to IDs fall in the range [0, len). The zero value,
// a nil map, instead just parses the numbers.
//
// We keep the array of names in creation order because we want reproducibility;
// the first key we see is always key 0. Otherwise, the keys are created in
// arbitrary orders.
type StringTable struct {
	names  []string
	values map[string]uint64
}

// NewStringTable just creates a string table with a non-nil map.
func NewStringTable() *StringTable {
	return &StringTable{values: map[string]uint64{}}
}

// unsafe is
func pretendByteIsString(data []byte) (result string) {
	dH := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sH := (*reflect.StringHeader)(unsafe.Pointer(&result))
	sH.Data = dH.Data
	sH.Len = dH.Len
	return result
}

// ID returns an ID associated to the string, adding it to the table if it is not already present,
// or parsing an integer if there's no table.
func (tbl *StringTable) ID(in []byte) (uint64, error) {
	str := pretendByteIsString(in)
	if tbl != nil {
		id, ok := tbl.values[str]
		if !ok {
			id = uint64(len(tbl.values))
			tbl.values[str] = id
			tbl.names = append(tbl.names, str)
		}
		return id, nil
	}
	return strconv.ParseUint(str, 10, 64)
}

// SignedID returns an ID associated to the string, adding it to the table if it is not already present,
// or parsing an integer if there's no table. It yields signed values only.
func (tbl *StringTable) IntID(in []byte) (int64, error) {
	str := pretendByteIsString(in)
	if tbl != nil {
		id, ok := tbl.values[str]
		if !ok {
			id = uint64(len(tbl.values))
			tbl.values[str] = id
			tbl.names = append(tbl.names, str)
		}
		return int64(id), nil
	}
	return strconv.ParseInt(str, 10, 64)
}

// MapForStringTable, given a string table mapping strings to consecutive
// integers and a translation function from strings to "real" keys, yields
// a translation/lookup slice. If it cannot translate all the keys, it
// returns an error.
func (tbl *StringTable) MakeIDMap(keys KeyTranslator) ([]uint64, error) {
	lookedUp, err := keys.TranslateKeys(tbl.names...)
	if err != nil {
		return nil, err
	}
	if len(lookedUp) != len(tbl.names) {
		return nil, fmt.Errorf("missing keys: expected %d keys, got %d", len(tbl.values), len(lookedUp))
	}
	out := make([]uint64, len(tbl.names))
	for i, v := range tbl.names {
		out[i] = lookedUp[v]
	}
	return out, nil
}

// translateSigned replaces values from 0 to len(mapping)-1 with the
// elements of mapping. It yields an error if any values aren't
// mapped.
func translateSigned(mapping []uint64, values []int64) error {
	oops := 0
	for i, v := range values {
		if v >= int64(len(mapping)) {
			oops++
		} else {
			values[i] = int64(mapping[v])
		}
	}
	if oops > 0 {
		return fmt.Errorf("encountered %d out-of-range signed values when applying translation mapping", oops)
	}
	return nil
}

// translateUnsigned replaces values from 0 to len(mapping)-1 with the
// elements of mapping. It yields an error if any values aren't
// mapped.
func translateUnsigned(mapping []uint64, values []uint64) error {
	oops := 0
	for i, v := range values {
		if v >= uint64(len(mapping)) {
			oops++
		} else {
			values[i] = mapping[v]
		}
	}
	if oops > 0 {
		return fmt.Errorf("encountered %d out-of-range signed values when applying translation mapping", oops)
	}
	return nil
}
