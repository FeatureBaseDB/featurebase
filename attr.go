package pilosa

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa/internal"
)

// AttrStore represents a storage layer for attributes.
type AttrStore struct {
	mu   sync.Mutex
	path string
	db   *bolt.DB

	// in-memory cache
	attrs map[uint64]map[string]interface{}
}

// NewAttrStore returns a new instance of AttrStore.
func NewAttrStore(path string) *AttrStore {
	return &AttrStore{
		path:  path,
		attrs: make(map[uint64]map[string]interface{}),
	}
}

// Path returns path to the store's data file.
func (s *AttrStore) Path() string { return s.path }

// Open opens and initializes the store.
func (s *AttrStore) Open() error {
	// Open storage.
	db, err := bolt.Open(s.path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	s.db = db

	// Initialize database.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("attrs")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Close closes the store.
func (s *AttrStore) Close() error {
	if s.db != nil {
		s.db.Close()
	}
	return nil
}

// Attrs returns a set of attributes by ID.
func (s *AttrStore) Attrs(id uint64) (m map[string]interface{}, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check cache for map.
	if m = s.attrs[id]; m != nil {
		return m, nil
	}

	// Find attributes from storage.
	if err = s.db.View(func(tx *bolt.Tx) error {
		m, err = txAttrs(tx, id)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Add to cache.
	s.attrs[id] = m

	return
}

// SetAttrs sets attribute values for a given ID.
func (s *AttrStore) SetAttrs(id uint64, m map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var attr map[string]interface{}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		tmp, err := txAttrs(tx, id)
		if err != nil {
			return err
		}
		attr = tmp

		// Create a new map if it is empty so we don't update emptyMap.
		if len(attr) == 0 {
			attr = make(map[string]interface{}, len(m))
		}

		// Merge attributes with original values.
		// Nil values should delete keys.
		for k, v := range m {
			if v == nil {
				delete(attr, k)
				continue
			}

			switch v := v.(type) {
			case int:
				attr[k] = uint64(v)
			case uint:
				attr[k] = uint64(v)
			case int64:
				attr[k] = uint64(v)
			case string, uint64, bool:
				attr[k] = v
			default:
				return fmt.Errorf("invalid attr type: %T", v)
			}
		}

		// Marshal and save new values.
		buf, err := proto.Marshal(&internal.AttrMap{Attrs: encodeAttrs(attr)})
		if err != nil {
			return err
		}
		if err := tx.Bucket([]byte("attrs")).Put(u64tob(id), buf); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// Swap attributes map in cache.
	s.attrs[id] = attr

	return nil
}

// txAttrs returns a map of attributes for a bitmap.
func txAttrs(tx *bolt.Tx, id uint64) (map[string]interface{}, error) {
	v := tx.Bucket([]byte("attrs")).Get(u64tob(id))
	if v == nil {
		return emptyMap, nil
	}

	var pb internal.AttrMap
	if err := proto.Unmarshal(v, &pb); err != nil {
		return nil, err
	}
	return decodeAttrs(pb.GetAttrs()), nil
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
	pb := &internal.Attr{Key: proto.String(key)}
	switch value := value.(type) {
	case string:
		pb.StringValue = proto.String(value)
	case float64:
		pb.UintValue = proto.Uint64(uint64(value))
	case uint64:
		pb.UintValue = proto.Uint64(value)
	case int64:
		pb.UintValue = proto.Uint64(uint64(value))
	case bool:
		pb.BoolValue = proto.Bool(value)
	}
	return pb
}

// decodeAttr converts from an Attr internal representation to a key/value pair.
func decodeAttr(attr *internal.Attr) (key string, value interface{}) {
	if attr.StringValue != nil {
		return attr.GetKey(), attr.GetStringValue()
	} else if attr.UintValue != nil {
		return attr.GetKey(), attr.GetUintValue()
	} else if attr.BoolValue != nil {
		return attr.GetKey(), attr.GetBoolValue()
	}
	return attr.GetKey(), nil
}

// u64tob encodes v to big endian encoding.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 decodes b from big endian encoding.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// emptyMap is a reusable map that contains no keys.
var emptyMap = make(map[string]interface{})
