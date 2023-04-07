// Copyright 2023 Molecula Corp. All rights reserved.

package tstore

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

const (
	KEY_SIZE_INT64 = 8
)

var ErrNeedsExclusive = errors.New("ErrNeedsExclusive")

type BTreeTupleHeader struct {
	writeTID      TID
	schemaVersion int16
	versionsPtr   int64
	flags         int8
}

func NewBTreeTupleHeaderFromBytes(b []byte) *BTreeTupleHeader {
	rdr := bytes.NewReader(b)

	var writeTID int64
	binary.Read(rdr, binary.BigEndian, &writeTID)

	var schemaVersion int16
	binary.Read(rdr, binary.BigEndian, &schemaVersion)

	var versionsPtr int64
	binary.Read(rdr, binary.BigEndian, &versionsPtr)

	var flags int8
	binary.Read(rdr, binary.BigEndian, &flags)

	return &BTreeTupleHeader{
		writeTID:      TID(writeTID),
		schemaVersion: schemaVersion,
		versionsPtr:   versionsPtr,
		flags:         flags,
	}
}

type BTreeTuple struct {
	TupleSchema types.Schema
	Tuple       types.Row

	tupleSchemaIndex map[string]int
}

func (t *BTreeTuple) keyValue() Sortable {
	kv, ok := t.Tuple[0].(int64)
	if !ok {
		return nil
	}
	return Int(kv)
}

func (t *BTreeTuple) column(name string) (int, *types.PlannerColumn) {
	if t.tupleSchemaIndex == nil {
		t.tupleSchemaIndex = make(map[string]int)
		for i, s := range t.TupleSchema {
			t.tupleSchemaIndex[s.ColumnName] = i
		}
	}
	idx, ok := t.tupleSchemaIndex[name]
	if !ok {
		return -1, nil
	}
	ps := t.TupleSchema[idx]
	return idx, ps
}

func NewBTreeTupleFromBytes(b []byte, schema types.Schema) *BTreeTuple {
	t := &BTreeTuple{
		TupleSchema: schema,
		Tuple:       make(types.Row, len(schema)),
	}

	rdr := bytes.NewReader(b)
	var writeTID int64
	binary.Read(rdr, binary.BigEndian, &writeTID)

	var schemaVersion int16
	binary.Read(rdr, binary.BigEndian, &schemaVersion)

	var versionsPtr int64
	binary.Read(rdr, binary.BigEndian, &versionsPtr)

	var flags int8
	binary.Read(rdr, binary.BigEndian, &flags)

	dataRdr := bytes.NewReader(b)

	for i, s := range schema {
		// read the offset
		var fieldOffset uint32
		binary.Read(rdr, binary.BigEndian, &fieldOffset)
		if fieldOffset == 0xFFFFFFFF {
			t.Tuple[i] = nil
			continue
		}

		switch s.Type.(type) {
		case *parser.DataTypeVarchar:
			dataRdr.Seek(int64(fieldOffset), io.SeekStart)
			var l int32
			binary.Read(dataRdr, binary.BigEndian, &l)
			bvalue := make([]byte, l)
			binary.Read(dataRdr, binary.BigEndian, &bvalue)
			t.Tuple[i] = string(bvalue)

		case *parser.DataTypeVector:
			dataRdr.Seek(int64(fieldOffset), io.SeekStart)
			var l int32
			binary.Read(dataRdr, binary.BigEndian, &l)
			bvalue := make([]float64, l)

			for j := 0; j < int(l); j++ {
				var fvalue float64
				binary.Read(dataRdr, binary.BigEndian, &fvalue)
				bvalue[j] = fvalue
			}
			t.Tuple[i] = bvalue

		case *parser.DataTypeVarbinary:
			dataRdr.Seek(int64(fieldOffset), io.SeekStart)
			var l int32
			binary.Read(dataRdr, binary.BigEndian, &l)
			bvalue := make([]byte, l)
			binary.Read(dataRdr, binary.BigEndian, &bvalue)
			t.Tuple[i] = bvalue
		default:
			panic("unexpected type")
		}
	}
	return t
}

// tuple format
// 		writeTID (int64)
// 		schemaVersion (int16) (will we ever have > 64K of schema versions??)
// 		versionsPtr (int64) ptr to old versions page for this tuple
//		flags (int8) flags that include a deletion marker
// 		fieldOffsets (one for each field, int32, 0xFFFFFFFF is null)
//   		offsets point to:
// 				fieldData (one for each field)
//   				[valueLen (int32)] optional only used for variable length types (right now varchar)
//					valueBytes

func (b *BTreeTuple) Bytes(tid TID, schema types.Schema, schemaVersion int, versionsPtr int64, forDeletion bool) ([]byte, error) {
	var valueBuf bytes.Buffer

	hdrBytes := make([]byte, 8+2+8+1)
	offset := 0

	// write TID
	binary.BigEndian.PutUint64(hdrBytes[offset:], uint64(tid))
	offset += 8

	// schemaVersion
	binary.BigEndian.PutUint16(hdrBytes[offset:], uint16(schemaVersion))
	offset += 2

	// versionsPtr
	binary.BigEndian.PutUint64(hdrBytes[offset:], uint64(versionsPtr))
	offset += 8

	// flags
	hdrBytes[offset] = 0
	offset += 1

	// write header
	valueBuf.Write(hdrBytes)

	// hang on to a null value
	nullValue := make([]byte, 4)
	binary.BigEndian.PutUint32(nullValue, uint32(0xFFFFFFFF))

	// initialize offset to be header + field array
	offset += len(schema) * 4

	// now write offsets and field data
	var fieldData bytes.Buffer
	for _, sc := range schema {
		cidx, tsc := b.column(sc.ColumnName)
		// if the schema column is not in the schema being used for insert we write a null
		if tsc == nil {
			valueBuf.Write(nullValue)
			continue
		}

		rd := b.Tuple[cidx]

		// if we get an explict null write it
		if rd == nil {
			valueBuf.Write(nullValue)
			continue
		}

		switch tsc.Type.(type) {
		case *parser.DataTypeVarchar:
			// write field offset
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, uint32(offset))
			valueBuf.Write(b)

			// write field data
			data := rd.(string)
			b = make([]byte, 4)
			l := len(data)
			// update the offset to be the end of the data we're about to write
			offset += 4 + l
			binary.BigEndian.PutUint32(b, uint32(l))
			fieldData.Write(b)
			fieldData.WriteString(data)

		case *parser.DataTypeVector:
			// write field offset
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, uint32(offset))
			valueBuf.Write(b)

			// write field data
			data := rd.([]float64)
			b = make([]byte, 4)
			l := len(data)
			// update the offset to be the end of the data we're about to write
			offset += 4 + l*8
			binary.BigEndian.PutUint32(b, uint32(l))
			fieldData.Write(b)
			for _, f := range data {
				var fbuf [8]byte
				binary.BigEndian.PutUint64(fbuf[:], math.Float64bits(f))
				fieldData.Write(fbuf[:])
			}

		case *parser.DataTypeVarbinary:
			// write field offset
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, uint32(offset))
			valueBuf.Write(b)

			// write field data
			data := rd.([]byte)
			b = make([]byte, 4)
			l := len(data)
			// update the offset to be the end of the data we're about to write
			offset += 4 + l
			binary.BigEndian.PutUint32(b, uint32(l))
			binary.BigEndian.PutUint32(b, uint32(len(data)))
			fieldData.Write(b)
			fieldData.Write(data)

		default:
			panic("unexpected field type")
		}
	}

	// write the field data
	valueBuf.Write(fieldData.Bytes())

	// and we're done
	return valueBuf.Bytes(), nil
}

type Equatable interface {
	Equals(b Equatable) bool
}

type Sortable interface {
	Equatable
	Less(b Sortable) bool
	Length() int
	Bytes() []byte
}

type String string

func (s String) Equals(other Equatable) bool {
	if o, ok := other.(String); ok {
		return s == o
	} else {
		return false
	}
}

func (s String) Less(other Sortable) bool {
	if o, ok := other.(String); ok {
		return s < o
	} else {
		return false
	}
}

func (i String) Length() int {
	return len(i)
}

func (i String) Bytes() []byte {
	return []byte(i)
}

type ByteSlice []byte

// func (i ByteSlice) Hash() int {
// 	return int(i)
// }

// func (i ByteSlice) Less(other Sortable) bool {
// 	if o, ok := other.(Int); ok {
// 		return i < o
// 	} else {
// 		return false
// 	}
// }

// func (i ByteSlice) Equals(other Equatable) bool {
// 	if o, ok := other.(Int); ok {
// 		return i == o
// 	} else {
// 		return false
// 	}
// }

func (i ByteSlice) Length() int {
	return len(i)
}

func (i ByteSlice) Bytes() []byte {
	return i
}

func (i ByteSlice) AsInt() int {
	return int(binary.BigEndian.Uint32(i))
}

type Int int

func (i Int) Hash() int {
	return int(i)
}

func (i Int) Less(other Sortable) bool {
	if o, ok := other.(Int); ok {
		return i < o
	} else {
		return false
	}
}

func (i Int) Equals(other Equatable) bool {
	if o, ok := other.(Int); ok {
		return i == o
	} else {
		return false
	}
}

func (i Int) Length() int {
	return 4
}

func (i Int) Bytes() []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(i))
	return b
}
