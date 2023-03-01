// Copyright 2023 Molecula Corp. All rights reserved.

package tstore

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

const (
	KEY_SIZE_INT64 = 8
)

var ErrNeedsExclusive = errors.New("ErrNeedsExclusive")

type BTreeTuple struct {
	TupleSchema types.Schema
	Tuple       types.Row
}

func (t *BTreeTuple) keyValue() Sortable {
	kv, ok := t.Tuple[0].(int64)
	if !ok {
		return nil
	}
	return Int(kv)
}

func NewBTreeTupleFromBytes(b []byte, schema types.Schema) *BTreeTuple {
	t := &BTreeTuple{
		TupleSchema: schema,
		Tuple:       make(types.Row, len(schema)),
	}

	rdr := bytes.NewReader(b)
	for i, s := range schema {
		switch s.Type.(type) {
		case *parser.DataTypeVarchar:
			var l int32
			binary.Read(rdr, binary.BigEndian, &l)
			bvalue := make([]byte, l)
			binary.Read(rdr, binary.BigEndian, &bvalue)
			t.Tuple[i] = string(bvalue)
		default:
			panic("unexpected type")
		}
	}
	return t
}

func (b *BTreeTuple) Bytes() ([]byte, error) {
	var valueBuf bytes.Buffer
	for cidx, c := range b.TupleSchema {
		// skip the key column
		if cidx == 0 {
			continue
		}
		rd := b.Tuple[cidx]

		switch ty := c.Type.(type) {
		case *parser.DataTypeVarchar:
			if rd == nil {
				b := []byte{0, 0, 0, 0}
				valueBuf.Write(b)
			} else {
				data, ok := rd.(string)
				if !ok {
					return []byte{}, errors.Errorf("unexpected type conversion '%T'", rd)
				}
				b := make([]byte, 4)
				binary.BigEndian.PutUint32(b, uint32(len(data)))
				valueBuf.Write(b)
				valueBuf.WriteString(data)
			}
		default:
			return []byte{}, errors.Errorf("unexpected type '%T'", ty)
		}
	}
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
