package pilosa_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/umbel/pilosa"
)

// Ensure database can set and retrieve profile attributes.
func TestAttrStore_Attrs(t *testing.T) {
	s := MustOpenAttrStore()
	defer s.Close()

	// Set attributes.
	if err := s.SetAttrs(1, map[string]interface{}{"A": int64(100)}); err != nil {
		t.Fatal(err)
	} else if err := s.SetAttrs(2, map[string]interface{}{"A": int64(200)}); err != nil {
		t.Fatal(err)
	} else if err := s.SetAttrs(1, map[string]interface{}{"B": "VALUE"}); err != nil {
		t.Fatal(err)
	}

	// Retrieve attributes for profile #1.
	if m, err := s.Attrs(1); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": int64(100), "B": "VALUE"}) {
		t.Fatalf("unexpected attrs(1): %#v", m)
	}

	// Retrieve attributes for profile #2.
	if m, err := s.Attrs(2); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": int64(200)}) {
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
