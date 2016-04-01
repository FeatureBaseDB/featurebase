package pilosa

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Index represents a container for fragments.
type Index struct {
	mu        sync.Mutex
	path      string
	remoteMax uint64

	// Databases by name.
	dbs map[string]*DB
}

// NewIndex returns a new instance of Index.
func NewIndex(path string) *Index {
	return &Index{
		path:      path,
		dbs:       make(map[string]*DB),
		remoteMax: 0,
	}
}

// Open initializes the root data directory for the index.
func (i *Index) Open() error {
	if err := os.MkdirAll(i.path, 0777); err != nil {
		return err
	}

	// Open path to read all database directories.
	f, err := os.Open(i.path)
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

		db := NewDB(i.DBPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if err := db.Open(); err != nil {
			return fmt.Errorf("open db: name=%s, err=%s", db.Name(), err)
		}
		i.dbs[db.Name()] = db
	}
	return nil
}

// Close closes all open fragments.
func (i *Index) Close() error {
	for _, db := range i.dbs {
		db.Close()
	}
	return nil
}

// Path returns the path the index was initialized with.
func (i *Index) Path() string { return i.path }

// SliceN returns the highest slice across all frames.
func (i *Index) SliceN() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()

	sliceN := i.remoteMax
	for _, db := range i.dbs {
		if n := db.SliceN(); n > sliceN {
			sliceN = n
		}
	}
	return sliceN
}

// DBPath returns the path where a given database is stored.
func (i *Index) DBPath(name string) string { return filepath.Join(i.path, name) }

// DB returns the database by name.
func (i *Index) DB(name string) *DB {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.db(name)
}

func (i *Index) db(name string) *DB { return i.dbs[name] }

// DBs returns a list of all databases in the index.
func (i *Index) DBs() []*DB {
	i.mu.Lock()
	defer i.mu.Unlock()

	a := make([]*DB, 0, len(i.dbs))
	for _, db := range i.dbs {
		a = append(a, db)
	}
	sort.Sort(dbSlice(a))

	return a
}

// CreateDBIfNotExists returns a database by name.
// The database is created if it does not already exist.
func (i *Index) CreateDBIfNotExists(name string) (*DB, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.createDBIfNotExists(name)
}

func (i *Index) createDBIfNotExists(name string) (*DB, error) {
	if name == "" {
		return nil, errors.New("database name required")
	}

	// Return database if it exists.
	if db := i.db(name); db != nil {
		return db, nil
	}

	// Otherwise create a new database.
	db := NewDB(i.DBPath(name), name)
	if err := db.Open(); err != nil {
		return nil, err
	}
	i.dbs[db.Name()] = db

	return db, nil
}

// Frame returns the frame for a database and name.
func (i *Index) Frame(db, name string) *Frame {
	d := i.DB(db)
	if d == nil {
		return nil
	}
	return d.Frame(name)
}

// CreateFrameIfNotExists returns the frame for a database & name.
// The frame is created if it doesn't already exist.
func (i *Index) CreateFrameIfNotExists(db, name string) (*Frame, error) {
	d, err := i.CreateDBIfNotExists(db)
	if err != nil {
		return nil, err
	}
	return d.CreateFrameIfNotExists(name)
}

// Fragment returns the fragment for a database, frame & slice.
func (i *Index) Fragment(db, frame string, slice uint64) *Fragment {
	f := i.Frame(db, frame)
	if f == nil {
		return nil
	}
	return f.fragment(slice)
}

// CreateFragmentIfNotExists returns the fragment for a database, frame & slice.
// The fragment is created if it doesn't already exist.
func (i *Index) CreateFragmentIfNotExists(db, frame string, slice uint64) (*Fragment, error) {
	f, err := i.CreateFrameIfNotExists(db, frame)
	if err != nil {
		return nil, err
	}
	return f.CreateFragmentIfNotExists(slice)
}
func (i *Index) SetMax(newmax uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.remoteMax = newmax
}
