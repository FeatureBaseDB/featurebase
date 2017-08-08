package test

import (
	"io/ioutil"
	"os"

	"github.com/pilosa/pilosa"
)

// SliceWidth is a helper reference to use when testing.
const SliceWidth = pilosa.SliceWidth

// Fragment is a test wrapper for pilosa.Fragment.
type Fragment struct {
	*pilosa.Fragment
	RowAttrStore *AttrStore
}

// NewFragment returns a new instance of Fragment with a temporary path.
func NewFragment(index, frame, view string, slice uint64, cacheType string) *Fragment {
	file, err := ioutil.TempFile("", "pilosa-fragment-")
	if err != nil {
		panic(err)
	}
	file.Close()

	f := &Fragment{
		Fragment:     pilosa.NewFragment(file.Name(), index, frame, view, slice),
		RowAttrStore: MustOpenAttrStore(),
	}
	f.Fragment.CacheType = cacheType
	f.Fragment.RowAttrStore = f.RowAttrStore.AttrStore
	return f
}

// MustOpenFragment creates and opens an fragment at a temporary path. Panic on error.
func MustOpenFragment(index, frame, view string, slice uint64, cacheType string) *Fragment {
	if cacheType == "" {
		cacheType = pilosa.DefaultCacheType
	}
	f := NewFragment(index, frame, view, slice, cacheType)

	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the fragment and removes all underlying data.
func (f *Fragment) Close() error {
	defer os.Remove(f.Path())
	defer os.Remove(f.CachePath())
	defer f.RowAttrStore.Close()
	return f.Fragment.Close()
}

// Reopen closes the fragment and reopens it as a new instance.
func (f *Fragment) Reopen() error {
	cacheType := f.Fragment.CacheType
	path := f.Path()
	if err := f.Fragment.Close(); err != nil {
		return err
	}

	f.Fragment = pilosa.NewFragment(path, f.Index(), f.Frame(), f.View(), f.Slice())
	f.Fragment.CacheType = cacheType
	f.Fragment.RowAttrStore = f.RowAttrStore.AttrStore
	if err := f.Open(); err != nil {
		return err
	}
	return nil
}

// MustSetBits sets bits on a row. Panic on error.
// This function does not accept a timestamp or quantum.
func (f *Fragment) MustSetBits(rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		if _, err := f.SetBit(rowID, columnID); err != nil {
			panic(err)
		}
	}
}

// MustClearBits clears bits on a row. Panic on error.
func (f *Fragment) MustClearBits(rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		if _, err := f.ClearBit(rowID, columnID); err != nil {
			panic(err)
		}
	}
}

// RowAttrStore provides simple storage for attributes.
type RowAttrStore struct {
	attrs map[uint64]map[string]interface{}
}

// NewRowAttrStore returns a new instance of RowAttrStore.
func NewRowAttrStore() *RowAttrStore {
	return &RowAttrStore{
		attrs: make(map[uint64]map[string]interface{}),
	}
}

// RowAttrs returns the attributes set to a row id.
func (s *RowAttrStore) RowAttrs(id uint64) (map[string]interface{}, error) {
	return s.attrs[id], nil
}

// SetRowAttrs assigns a set of attributes to a row id.
func (s *RowAttrStore) SetRowAttrs(id uint64, m map[string]interface{}) {
	s.attrs[id] = m
}

// GenerateImportFill generates a set of bits pairs that evenly fill a fragment chunk.
func GenerateImportFill(rowN int, pct float64) (rowIDs, columnIDs []uint64) {
	ipct := int(pct * 100)
	for i := 0; i < SliceWidth*rowN; i++ {
		if i%100 >= ipct {
			continue
		}

		rowIDs = append(rowIDs, uint64(i%SliceWidth))
		columnIDs = append(columnIDs, uint64(i/SliceWidth))
	}
	return
}
