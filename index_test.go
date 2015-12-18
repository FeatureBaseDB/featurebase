package pilosa_test

import (
	"io/ioutil"
	"os"

	"github.com/umbel/pilosa"
)

// Index is a test wrapper for pilosa.Index.
type Index struct {
	*pilosa.Index
}

// NewIndex returns a new instance of Index with a temporary path.
func NewIndex() *Index {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}
	return &Index{Index: pilosa.NewIndex(path)}
}

// MustOpenIndex creates and opens an index at a temporary path. Panic on error.
func MustOpenIndex() *Index {
	i := NewIndex()
	if err := i.Open(); err != nil {
		panic(err)
	}
	return i
}

// Close closes the index and removes all underlying data.
func (i *Index) Close() error {
	defer os.RemoveAll(i.Path())
	return i.Index.Close()
}

// MustFragment returns a given fragment. Panic on error.
func (i *Index) MustFragment(db, frame string, slice uint64) *Fragment {
	f, err := i.Index.Fragment(db, frame, slice)
	if err != nil {
		panic(err)
	}
	return &Fragment{Fragment: f}
}
