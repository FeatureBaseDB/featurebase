// Copyright 2017 Pilosa Corp.
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

package pilosa

import (
	"bytes"
	"sort"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/v2/internal"
)

// Attribute data type enum.
const (
	attrTypeString = 1
	attrTypeInt    = 2
	attrTypeBool   = 3
	attrTypeFloat  = 4
)

// AttrStore represents an interface for handling row/column attributes.
type AttrStore interface {
	Path() string
	Open() error
	Close() error
	Attrs(id uint64) (m map[string]interface{}, err error)
	SetAttrs(id uint64, m map[string]interface{}) error
	SetBulkAttrs(m map[uint64]map[string]interface{}) error
	Blocks() ([]AttrBlock, error)
	BlockData(i uint64) (map[uint64]map[string]interface{}, error)
}

// nopStore represents an AttrStore that doesn't do anything.
var nopStore AttrStore = nopAttrStore{}

// newNopAttrStore returns an attr store which does nothing. It returns a global
// object to avoid unnecessary allocations.
func newNopAttrStore(string) AttrStore { return nopStore }

// nopAttrStore represents a no-op implementation of the AttrStore interface.
type nopAttrStore struct{}

// Path is a no-op implementation of AttrStore Path method.
func (s nopAttrStore) Path() string { return "" }

// Open is a no-op implementation of AttrStore Open method.
func (s nopAttrStore) Open() error { return nil }

// Close is a no-op implementation of AttrStore Close method.
func (s nopAttrStore) Close() error { return nil }

// Attrs is a no-op implementation of AttrStore Attrs method.
func (s nopAttrStore) Attrs(id uint64) (m map[string]interface{}, err error) { return nil, nil }

// SetAttrs is a no-op implementation of AttrStore SetAttrs method.
func (s nopAttrStore) SetAttrs(id uint64, m map[string]interface{}) error { return nil }

// SetBulkAttrs is a no-op implementation of AttrStore SetBulkAttrs method.
func (s nopAttrStore) SetBulkAttrs(m map[uint64]map[string]interface{}) error { return nil }

// Blocks is a no-op implementation of AttrStore Blocks method.
func (s nopAttrStore) Blocks() ([]AttrBlock, error) { return nil, nil }

// BlockData is a no-op implementation of AttrStore BlockData method.
func (s nopAttrStore) BlockData(i uint64) (map[uint64]map[string]interface{}, error) { return nil, nil }

// AttrBlock represents a checksummed block of the attribute store.
type AttrBlock struct {
	ID       uint64 `json:"id"`
	Checksum []byte `json:"checksum"`
}

// attrBlocks represents a list of blocks.
type attrBlocks []AttrBlock

// Diff returns a list of block ids that are different or are new in other.
// Block lists must be in sorted order.
func (a attrBlocks) Diff(other []AttrBlock) []uint64 {
	var ids []uint64
	for {
		// Read next block from each list.
		var blk0, blk1 *AttrBlock
		if len(a) > 0 {
			blk0 = &a[0]
		}
		if len(other) > 0 {
			blk1 = &other[0]
		}

		// Exit if "a" contains no more blocks.
		if blk0 == nil {
			return ids
		}

		// Add block ID if it's different or if it's only in "a".
		if blk1 == nil || blk0.ID < blk1.ID {
			ids = append(ids, blk0.ID)
			a = a[1:]
		} else if blk1.ID < blk0.ID {
			other = other[1:]
		} else {
			if !bytes.Equal(blk0.Checksum, blk1.Checksum) {
				ids = append(ids, blk0.ID)
			}
			a, other = a[1:], other[1:]
		}
	}
}

func encodeAttrs(m map[string]interface{}) []*internal.Attr {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	a := make([]*internal.Attr, len(keys))
	for i := range keys {
		a[i] = encodeAttr(keys[i], m[keys[i]])
	}
	return a
}

func decodeAttrs(pb []*internal.Attr) map[string]interface{} {
	m := make(map[string]interface{}, len(pb))
	for i := range pb {
		key, value := decodeAttr(pb[i])
		m[key] = value
	}
	return m
}

// encodeAttr converts a key/value pair into an Attr internal representation.
func encodeAttr(key string, value interface{}) *internal.Attr {
	pb := &internal.Attr{Key: key}
	switch value := value.(type) {
	case string:
		pb.Type = attrTypeString
		pb.StringValue = value
	case float64:
		pb.Type = attrTypeFloat
		pb.FloatValue = value
	case uint64:
		pb.Type = attrTypeInt
		pb.IntValue = int64(value)
	case int64:
		pb.Type = attrTypeInt
		pb.IntValue = value
	case bool:
		pb.Type = attrTypeBool
		pb.BoolValue = value
	}
	return pb
}

// decodeAttr converts from an Attr internal representation to a key/value pair.
func decodeAttr(attr *internal.Attr) (key string, value interface{}) {
	switch attr.Type {
	case attrTypeString:
		return attr.Key, attr.StringValue
	case attrTypeInt:
		return attr.Key, attr.IntValue
	case attrTypeBool:
		return attr.Key, attr.BoolValue
	case attrTypeFloat:
		return attr.Key, attr.FloatValue
	default:
		return attr.Key, nil
	}
}

// cloneAttrs returns a shallow clone of m.
func cloneAttrs(m map[string]interface{}) map[string]interface{} {
	other := make(map[string]interface{}, len(m))
	for k, v := range m {
		other[k] = v
	}
	return other
}

// EncodeAttrs encodes an attribute map into a byte slice.
func EncodeAttrs(attr map[string]interface{}) ([]byte, error) {
	return proto.Marshal(&internal.AttrMap{Attrs: encodeAttrs(attr)})
}

// DecodeAttrs decodes a byte slice into an  attribute map.
func DecodeAttrs(v []byte) (map[string]interface{}, error) {
	var pb internal.AttrMap
	if err := proto.Unmarshal(v, &pb); err != nil {
		return nil, err
	}
	return decodeAttrs(pb.GetAttrs()), nil
}
