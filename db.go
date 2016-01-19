package pilosa

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

// DB represents a container for frames.
type DB struct {
	mu   sync.Mutex
	path string
	name string

	// Frames by name.
	frames map[string]*Frame

	// Profile attribute storage and cache
	store *bolt.DB
	attrs map[uint64]map[string]interface{}
}

// NewDB returns a new instance of DB.
func NewDB(path, name string) *DB {
	return &DB{
		path:   path,
		name:   name,
		frames: make(map[string]*Frame),
		attrs:  make(map[uint64]map[string]interface{}),
	}
}

// Name returns name of the database.
func (db *DB) Name() string { return db.name }

// Path returns the path the database was initialized with.
func (db *DB) Path() string { return db.path }

// Open opens and initializes the database.
func (db *DB) Open() error {
	// Ensure the path exists.
	if err := os.MkdirAll(db.path, 0777); err != nil {
		return err
	}

	if err := db.openFrames(); err != nil {
		return err
	}

	if err := db.openStore(); err != nil {
		return err
	}

	return nil
}

// openFrames opens and initializes the frames inside the database.
func (db *DB) openFrames() error {
	f, err := os.Open(db.path)
	if err != nil {
		return err
	}
	defer f.Close()

	fis, err := f.Readdir(0)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}

		fr := NewFrame(db.FramePath(filepath.Base(fi.Name())), db.name, filepath.Base(fi.Name()))
		if err := fr.Open(); err != nil {
			return fmt.Errorf("open frame: name=%s, err=%s", fr.Name(), err)
		}
		db.frames[fr.Name()] = fr
	}
	return nil
}

// openStore opens and initializes the attribute store.
func (db *DB) openStore() error {
	// Open attribute store.
	store, err := bolt.Open(filepath.Join(db.path, "data"), 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	db.store = store

	// Initialize database.
	if err := db.store.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("attrs")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return err
	}

	return nil
}

// Close closes the database and its frames.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Close the attribute store.
	if db.store != nil {
		db.store.Close()
	}

	// Close all frames.
	for _, f := range db.frames {
		f.Close()
	}
	db.frames = make(map[string]*Frame)

	return nil
}

// SliceN returns the max slice in the database.
func (db *DB) SliceN() uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()

	var max uint64
	for _, f := range db.frames {
		if slice := f.SliceN(); slice > max {
			max = slice
		}
	}
	return max
}

// FramePath returns the path to a frame in the database.
func (db *DB) FramePath(name string) string { return filepath.Join(db.path, name) }

// Frame returns a frame in the database by name.
func (db *DB) Frame(name string) *Frame {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.frame(name)
}

func (db *DB) frame(name string) *Frame { return db.frames[name] }

// CreateFrameIfNotExists returns a frame in the database by name.
func (db *DB) CreateFrameIfNotExists(name string) (*Frame, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.createFrameIfNotExists(name)
}

func (db *DB) createFrameIfNotExists(name string) (*Frame, error) {
	// Find frame in cache first.
	if f := db.frames[name]; f != nil {
		return f, nil
	}

	// Initialize and open frame.
	f := NewFrame(db.FramePath(name), db.name, name)
	if err := f.Open(); err != nil {
		return nil, err
	}
	db.frames[name] = f

	return f, nil
}

// ProfileAttrs returns the value of the attribute for a profile.
func (db *DB) ProfileAttrs(id uint64) (m map[string]interface{}, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check cache for map.
	if m = db.attrs[id]; m != nil {
		return m, nil
	}

	// Find attributes from storage.
	if err = db.store.View(func(tx *bolt.Tx) error {
		m, err = txProfileAttrs(tx, id)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Add to cache.
	db.attrs[id] = m

	return
}

// SetProfileAttrs sets attribute values for a profile.
func (db *DB) SetProfileAttrs(id uint64, m map[string]interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var attr map[string]interface{}
	if err := db.store.Update(func(tx *bolt.Tx) error {
		tmp, err := txProfileAttrs(tx, id)
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
			} else {
				attr[k] = v
			}
		}

		// Marshal and save new values.
		buf, err := json.Marshal(attr)
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
	db.attrs[id] = attr

	return nil
}

// txProfileAttrs returns a map of attributes for a profile.
func txProfileAttrs(tx *bolt.Tx, id uint64) (map[string]interface{}, error) {
	if v := tx.Bucket([]byte("attrs")).Get(u64tob(id)); v != nil {
		m := make(map[string]interface{})
		if err := json.Unmarshal(v, &m); err != nil {
			return nil, err
		}
		return m, nil
	}
	return emptyMap, nil
}
