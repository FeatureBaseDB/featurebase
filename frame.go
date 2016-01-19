package pilosa

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

// Frame represents a container for fragments.
type Frame struct {
	mu   sync.Mutex
	path string
	db   string
	name string

	// Fragments
	fragments map[uint64]*Fragment

	// Attribute storage and cache
	store  *bolt.DB
	battrs map[uint64]map[string]interface{}
}

// NewFrame returns a new instance of frame.
func NewFrame(path, db, name string) *Frame {
	return &Frame{
		path: path,
		db:   db,
		name: name,

		fragments: make(map[uint64]*Fragment),

		battrs: make(map[uint64]map[string]interface{}),
	}
}

// Open opens and initializes the frame.
func (f *Frame) Open() error {
	// Ensure the frame's path exists.
	if err := os.MkdirAll(f.path, 0777); err != nil {
		return err
	}

	// Open attribute store.
	store, err := bolt.Open(filepath.Join(f.path, "data"), 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	f.store = store

	// Initialize database.
	if err := f.store.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("battrs")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		_ = f.Close()
		return err
	}

	return nil
}

// Close closes the frame and its fragments.
func (f *Frame) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Close the attribute store.
	if f.store != nil {
		_ = f.store.Close()
	}

	// Close all fragments.
	for _, frag := range f.fragments {
		_ = frag.Close()
	}
	f.fragments = make(map[uint64]*Fragment)

	return nil
}

// Path returns the path the frame was initialized with.
func (f *Frame) Path() string { return f.path }

// FragmentPath returns the path to a fragment in the frame.
func (f *Frame) FragmentPath(slice uint64) string {
	return filepath.Join(f.path, strconv.FormatUint(slice, 10))
}

// Fragment returns a fragment in the frame by slice.
func (f *Frame) Fragment(slice uint64) *Fragment {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fragment(slice)
}

func (f *Frame) fragment(slice uint64) *Fragment { return f.fragments[slice] }

// CreateFragmentIfNotExists returns a fragment in the frame by slice.
func (f *Frame) CreateFragmentIfNotExists(slice uint64) (*Fragment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createFragmentIfNotExists(slice)
}

func (f *Frame) createFragmentIfNotExists(slice uint64) (*Fragment, error) {
	// Find fragment in cache first.
	if frag := f.fragments[slice]; frag != nil {
		return frag, nil
	}

	// Initialize and open fragment.
	frag := NewFragment(f.FragmentPath(slice), f.db, f.name, slice)
	if err := frag.Open(); err != nil {
		return nil, err
	}
	f.fragments[slice] = frag

	return frag, nil
}

// BitmapAttrs returns the value of the attribute for a bitmap.
func (f *Frame) BitmapAttrs(id uint64) (m map[string]interface{}, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Check cache for map.
	if m = f.battrs[id]; m != nil {
		return m, nil
	}

	// Find attributes from storage.
	if err = f.store.View(func(tx *bolt.Tx) error {
		m, err = f.bitmapAttrs(tx, id)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Add to cache.
	f.battrs[id] = m

	return
}

// SetBitmapAttr sets attribute values for a bitmap.
func (f *Frame) SetBitmapAttrs(id uint64, m map[string]interface{}) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var battr map[string]interface{}
	if err := f.store.Update(func(tx *bolt.Tx) error {
		attr, err := f.bitmapAttrs(tx, id)
		if err != nil {
			return err
		}
		battr = attr

		// Create a new map if it is empty so we don't update emptyMap.
		if len(battr) == 0 {
			battr = make(map[string]interface{}, len(m))
		}

		// Merge attributes with original values.
		// Nil values should delete keys.
		for k, v := range m {
			if v == nil {
				delete(battr, k)
			} else {
				battr[k] = v
			}
		}

		// Marshal and save new values.
		buf, err := json.Marshal(battr)
		if err != nil {
			return err
		}
		if err := tx.Bucket([]byte("battrs")).Put(u64tob(id), buf); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// Swap attributes map in cache.
	f.battrs[id] = battr

	return nil
}

// bitmapAttrs returns a map of attributes for a bitmap.
func (f *Frame) bitmapAttrs(tx *bolt.Tx, id uint64) (map[string]interface{}, error) {
	if v := tx.Bucket([]byte("battrs")).Get(u64tob(id)); v != nil {
		m := make(map[string]interface{})
		if err := json.Unmarshal(v, &m); err != nil {
			return nil, err
		}
		return m, nil
	}
	return emptyMap, nil
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
