package pilosa_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa"
)

// Ensure database can set and retrieve profile attributes.
func TestAttrStore_Attrs(t *testing.T) {
	s := MustOpenAttrStore()
	defer s.Close()

	// Set attributes.
	if err := s.SetAttrs(1, map[string]interface{}{"A": uint64(100)}); err != nil {
		t.Fatal(err)
	} else if err := s.SetAttrs(2, map[string]interface{}{"A": uint64(200)}); err != nil {
		t.Fatal(err)
	} else if err := s.SetAttrs(1, map[string]interface{}{"B": "VALUE"}); err != nil {
		t.Fatal(err)
	}

	// Retrieve attributes for profile #1.
	if m, err := s.Attrs(1); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": uint64(100), "B": "VALUE"}) {
		t.Fatalf("unexpected attrs(1): %#v", m)
	}

	// Retrieve attributes for profile #2.
	if m, err := s.Attrs(2); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": uint64(200)}) {
		t.Fatalf("unexpected attrs(2): %#v", m)
	}
}

// Ensure database returns a non-nil empty map if unset.
func TestAttrStore_Attrs_Empty(t *testing.T) {
	s := MustOpenAttrStore()
	defer s.Close()

	if m, err := s.Attrs(100); err != nil {
		t.Fatal(err)
	} else if m == nil || len(m) > 0 {
		t.Fatalf("unexpected attrs: %#v", m)
	}
}

// Ensure database can unset attributes if explicitly set to nil.
func TestAttrStore_Attrs_Unset(t *testing.T) {
	s := MustOpenAttrStore()
	defer s.Close()

	// Set attributes.
	if err := s.SetAttrs(1, map[string]interface{}{"A": "X", "B": "Y"}); err != nil {
		t.Fatal(err)
	} else if err := s.SetAttrs(1, map[string]interface{}{"B": nil}); err != nil {
		t.Fatal(err)
	}

	// Verify attributes.
	if m, err := s.Attrs(1); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": "X"}) {
		t.Fatalf("unexpected attrs: %#v", m)
	}
}

// Ensure attribute block checksums can be returned.
func TestAttrStore_Blocks(t *testing.T) {
	s := MustOpenAttrStore()
	defer s.Close()

	// Set attributes.
	if err := s.SetAttrs(1, map[string]interface{}{"A": uint64(100)}); err != nil {
		t.Fatal(err)
	} else if err := s.SetAttrs(2, map[string]interface{}{"A": uint64(200)}); err != nil {
		t.Fatal(err)
	} else if err := s.SetAttrs(100, map[string]interface{}{"B": "VALUE"}); err != nil {
		t.Fatal(err)
	} else if err := s.SetAttrs(350, map[string]interface{}{"C": "FOO"}); err != nil {
		t.Fatal(err)
	}

	// Retrieve blocks.
	blks0, err := s.Blocks()
	if err != nil {
		t.Fatal(err)
	} else if len(blks0) != 3 || blks0[0].ID != 0 || blks0[1].ID != 1 || blks0[2].ID != 3 {
		t.Fatalf("unexpected blocks: %#v", blks0)
	}

	// Change second block.
	if err := s.SetAttrs(100, map[string]interface{}{"X": 12}); err != nil {
		t.Fatal(err)
	}

	// Ensure second block changed.
	blks1, err := s.Blocks()
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(blks0[0], blks1[0]) {
		t.Fatalf("block 0 mismatch: %#v != %#v", blks0[0], blks1[0])
	} else if reflect.DeepEqual(blks0[1], blks1[1]) {
		t.Fatalf("block 1 match: %#v ", blks0[0])
	} else if !reflect.DeepEqual(blks0[2], blks1[2]) {
		t.Fatalf("block 2 mismatch: %#v != %#v", blks0[2], blks1[2])
	}
}

// AttrStore represents a test wrapper for pilosa.AttrStore.
type AttrStore struct {
	*pilosa.AttrStore
}

// NewAttrStore returns a new instance of AttrStore.
func NewAttrStore() *AttrStore {
	f, err := ioutil.TempFile("", "pilosa-attr-")
	if err != nil {
		panic(err)
	}
	f.Close()
	os.Remove(f.Name())

	return &AttrStore{AttrStore: pilosa.NewAttrStore(f.Name())}
}

// MustOpenAttrStore returns a new, opened attribute store at a temporary path. Panic on error.
func MustOpenAttrStore() *AttrStore {
	s := NewAttrStore()
	if err := s.Open(); err != nil {
		panic(err)
	}
	return s
}

// Close closes the database and removes the underlying data.
func (s *AttrStore) Close() error {
	defer os.RemoveAll(s.Path())
	return s.AttrStore.Close()
}
