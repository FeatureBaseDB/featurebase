package pilosa

import (
	"sync"
)

// Index represents a container for fragments.
type Index struct {
	mu        sync.Mutex
	sliceN    uint64
	fragments map[fragmentKey]*Fragment
}

// NewIndex returns a new instance of Index.
func NewIndex() *Index {
	return &Index{
		fragments: make(map[fragmentKey]*Fragment),
	}
}

// SliceN returs the total number of slices managed by the index.
func (i *Index) SliceN() uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.sliceN
}

// Fragment returns the fragment for a database, frame & slice.
// The fragment is created if it doesn't already exist.
func (i *Index) Fragment(db, frame string, slice uint64) *Fragment {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Track the highest slice.
	if slice > i.sliceN {
		i.sliceN = slice
	}

	// Create fragment, if not exists.
	key := fragmentKey{db, frame, slice}
	if i.fragments[key] == nil {
		i.fragments[key] = NewFragment(db, frame, slice)
	}

	return i.fragments[key]
}

// fragmentKey is the map key for fragment look ups.
type fragmentKey struct {
	db    string
	frame string
	slice uint64
}
