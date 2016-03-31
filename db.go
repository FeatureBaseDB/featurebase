package pilosa

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// DB represents a container for frames.
type DB struct {
	mu   sync.Mutex
	path string
	name string

	// Frames by name.
	frames map[string]*Frame

	// Profile attribute storage and cache
	profileAttrStore *AttrStore
}

// NewDB returns a new instance of DB.
func NewDB(path, name string) *DB {
	return &DB{
		path:   path,
		name:   name,
		frames: make(map[string]*Frame),

		profileAttrStore: NewAttrStore(filepath.Join(path, "data")),
	}
}

// Name returns name of the database.
func (db *DB) Name() string { return db.name }

// Path returns the path the database was initialized with.
func (db *DB) Path() string { return db.path }

// ProfileAttrStore returns the storage for profile attributes.
func (db *DB) ProfileAttrStore() *AttrStore { return db.profileAttrStore }

// Open opens and initializes the database.
func (db *DB) Open() error {
	// Ensure the path exists.
	if err := os.MkdirAll(db.path, 0777); err != nil {
		return err
	}

	if err := db.openFrames(); err != nil {
		return err
	}

	if err := db.profileAttrStore.Open(); err != nil {
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

// Close closes the database and its frames.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Close the attribute store.
	if db.profileAttrStore != nil {
		db.profileAttrStore.Close()
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

// Frames returns a list of all frames in the database.
func (db *DB) Frames() []*Frame {
	db.mu.Lock()
	defer db.mu.Unlock()

	a := make([]*Frame, 0, len(db.frames))
	for _, f := range db.frames {
		a = append(a, f)
	}
	sort.Sort(frameSlice(a))

	return a
}

// CreateFrameIfNotExists returns a frame in the database by name.
func (db *DB) CreateFrameIfNotExists(name string) (*Frame, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.createFrameIfNotExists(name)
}

func (db *DB) createFrameIfNotExists(name string) (*Frame, error) {
	if name == "" {
		return nil, errors.New("frame name required")
	}

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

type dbSlice []*DB

func (p dbSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p dbSlice) Len() int           { return len(p) }
func (p dbSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }
