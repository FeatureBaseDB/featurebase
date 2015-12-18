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
	mu        sync.Mutex
	path      string
	sliceN    uint64
	fragments map[fragmentKey]*Fragment
}

// NewIndex returns a new instance of Index.
func NewIndex(path string) *Index {
	return &Index{
		path:      path,
		fragments: make(map[fragmentKey]*Fragment),
	}
}

// Open initializes the root data directory for the index.
func (i *Index) Open() error {
	if err := os.MkdirAll(i.path, 0777); err != nil {
		return err
	}
	return nil
}

// Close closes all open fragments.
func (i *Index) Close() error {
	for key, f := range i.fragments {
		if err := f.Close(); err != nil {
			log.Println("error closing fragment(%v): %s", key, err)
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

// FragmentPath returns the path where a given fragment is stored.
func (i *Index) FragmentPath(db, frame string, slice uint64) string {
	return filepath.Join(i.path, db, frame, strconv.FormatUint(slice, 10))
}

// Fragment returns the fragment for a database, frame & slice.
// The fragment is created if it doesn't already exist.
func (i *Index) Fragment(db, frame string, slice uint64) (*Fragment, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Track the highest slice.
	if slice > i.sliceN {
		i.sliceN = slice
	}

	// Create fragment, if not exists.
	key := fragmentKey{db, frame, slice}
	if i.fragments[key] == nil {
		path := i.FragmentPath(db, frame, slice)

		// Create parent directory, if necessary.
		if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
			return nil, fmt.Errorf("parent fragment dir: %s", err)
		}

		// Initialize and open fragment.
		f := NewFragment(path, db, frame, slice)
		if err := f.Open(); err != nil {
			return nil, err
		}
		i.fragments[key] = f
	}

	return i.fragments[key], nil
}

// fragmentKey is the map key for fragment look ups.
type fragmentKey struct {
	db    string
	frame string
	slice uint64
}
