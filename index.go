package pilosa

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// Index represents a container for fragments.
type Index struct {
	mu     sync.Mutex
	path   string
	sliceN uint64

	frames map[frameKey]*Frame
}

// NewIndex returns a new instance of Index.
func NewIndex(path string) *Index {
	return &Index{
		path:   path,
		frames: make(map[frameKey]*Frame),
	}
}

// Open initializes the root data directory for the index.
func (i *Index) Open() error {
	if err := os.MkdirAll(i.path, 0777); err != nil {
		return err
	}

	// Open all databases.
	if err := i.openDatabases(); err != nil {
		return err
	}

	return nil
}

// openDatabases recursively opens all directories within the data directory.
func (i *Index) openDatabases() error {
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
		} else if err := i.openDatabase(filepath.Base(fi.Name())); err != nil {
			return err
		}
	}
	return nil
}

// openDatabase recursively opens all frames within a database directory.
func (i *Index) openDatabase(db string) error {
	f, err := os.Open(filepath.Join(i.path, db))
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
		} else if err := i.openFrame(db, filepath.Base(fi.Name())); err != nil {
			return err
		}
	}
	return nil
}

// openFrame recursively opens all fragments within a frame directory.
func (i *Index) openFrame(db, frame string) error {
	f, err := os.Open(filepath.Join(i.path, db, frame))
	if err != nil {
		return err
	}
	defer f.Close()

	fis, err := f.Readdir(0)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		slice, err := strconv.ParseUint(filepath.Base(fi.Name()), 10, 64)
		if err != nil || fi.IsDir() {
			continue
		}
		if _, err := i.CreateFragmentIfNotExists(db, frame, slice); err != nil {
			return fmt.Errorf("open fragment: db=%s, frame=%d, slice=%d, err=%s", db, frame, slice, err)
		}
	}
	return nil
}

// Close closes all open fragments.
func (i *Index) Close() error {
	for key, f := range i.frames {
		if err := f.Close(); err != nil {
			log.Println("error closing frame(%s/%s): %s", key.db, key.frame, err)
		}
	}
	return nil
}

// Path returns the path the index was initialized with.
func (i *Index) Path() string { return i.path }

// SliceN returs the total number of slices managed by the index.
func (i *Index) SliceN() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.sliceN
}

// FramePath returns the path where a given frame is stored.
func (i *Index) FramePath(db, frame string) string { return filepath.Join(i.path, db, frame) }

// Fragment returns the fragment for a database, frame & slice.
func (i *Index) Fragment(db, frame string, slice uint64) *Fragment {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.frames[frameKey{db, frame}].fragment(slice)
}

// CreateFragmentIfNotExists returns the fragment for a database, frame & slice.
// The fragment is created if it doesn't already exist.
func (i *Index) CreateFragmentIfNotExists(db, frame string, slice uint64) (*Fragment, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Track the highest slice.
	if slice > i.sliceN {
		i.sliceN = slice
	}

	// Create frame, if not exists.
	key := frameKey{db, frame}
	if i.frames[key] == nil {
		f := NewFrame(i.FramePath(db, frame), db, frame)
		if err := f.Open(); err != nil {
			return nil, err
		}
		i.frames[key] = f
	}

	// Create fragment, if not exists.
	return i.frames[key].createFragmentIfNotExists(slice)
}

// frameKey is the map key for frame look ups.
type frameKey struct {
	db    string
	frame string
}
