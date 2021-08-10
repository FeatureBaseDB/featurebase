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
	"reflect"
	"strconv"
	"unsafe"

	"github.com/pkg/errors"
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
//
// The lookup function corresponds to the FindKeys/CreateKeys methods of
// featurebase translators, by an AMAZING coincidence.
func MapForStringTable(tbl *StringTable, lookup func(...string) (map[string]uint64, error)) ([]uint64, error) {
	lookedUp, err := lookup(tbl.names...)
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

// TimeFormatForUnit returns the time transfer format (between the update encoder and the update applier) with appropriate resolution for a quantum unit.
func TimeFormatForUnit(unit rune) string {
	switch unit {
	case 'Y':
		return "2006"
	case 'M':
		return "200601"
	case 'D':
		return "20060102"
	case 'H':
		return "2006010203"
	default:
		panic(errors.Errorf("invalid quantum unit: %q", unit))
	}
}

// TODO: bool

// TODO: timestamp (just sugar on top of IntVector)
