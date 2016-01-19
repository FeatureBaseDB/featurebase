package pilosa_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/umbel/pilosa"
)

// Ensure database can open and retrieve a frame.
func TestDB_CreateFrameIfNotExists(t *testing.T) {
	db := MustOpenDB()
	defer db.Close()

	// Create frame.
	f, err := db.CreateFrameIfNotExists("f")
	if err != nil {
		t.Fatal(err)
	} else if f == nil {
		t.Fatal("expected frame")
	}

	// Retrieve existing frame.
	other, err := db.CreateFrameIfNotExists("f")
	if err != nil {
		t.Fatal(err)
	} else if f != other {
		t.Fatal("frame mismatch")
	}

	if f != db.Frame("f") {
		t.Fatal("frame mismatch")
	}
}

// Ensure database can set and retrieve profile attributes.
func TestDB_ProfileAttrs(t *testing.T) {
	db := MustOpenDB()
	defer db.Close()

	// Set attributes.
	if err := db.SetProfileAttrs(1, map[string]interface{}{"A": float64(100)}); err != nil {
		t.Fatal(err)
	} else if err := db.SetProfileAttrs(2, map[string]interface{}{"A": float64(200)}); err != nil {
		t.Fatal(err)
	} else if err := db.SetProfileAttrs(1, map[string]interface{}{"B": "VALUE"}); err != nil {
		t.Fatal(err)
	}

	// Retrieve attributes for profile #1.
	if m, err := db.ProfileAttrs(1); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": float64(100), "B": "VALUE"}) {
		t.Fatalf("unexpected attrs(1): %#v", m)
	}

	// Retrieve attributes for profile #2.
	if m, err := db.ProfileAttrs(2); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": float64(200)}) {
		t.Fatalf("unexpected attrs(2): %#v", m)
	}
}

// Ensure database returns a non-nil empty map if unset.
func TestDB_ProfileAttrs_Empty(t *testing.T) {
	db := MustOpenDB()
	defer db.Close()

	if m, err := db.ProfileAttrs(100); err != nil {
		t.Fatal(err)
	} else if m == nil || len(m) > 0 {
		t.Fatalf("unexpected attrs: %#v", m)
	}
}

// Ensure database can unset attributes if explicitly set to nil.
func TestDB_ProfileAttrs_Unset(t *testing.T) {
	db := MustOpenDB()
	defer db.Close()

	// Set attributes.
	if err := db.SetProfileAttrs(1, map[string]interface{}{"A": "X", "B": "Y"}); err != nil {
		t.Fatal(err)
	} else if err := db.SetProfileAttrs(1, map[string]interface{}{"B": nil}); err != nil {
		t.Fatal(err)
	}

	// Verify attributes.
	if m, err := db.ProfileAttrs(1); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"A": "X"}) {
		t.Fatalf("unexpected attrs: %#v", m)
	}
}

// DB represents a test wrapper for pilosa.DB.
type DB struct {
	*pilosa.DB
}

// NewDB returns a new instance of DB d.
func NewDB() *DB {
	path, err := ioutil.TempDir("", "pilosa-db-")
	if err != nil {
		panic(err)
	}

	return &DB{DB: pilosa.NewDB(path, "d")}
}

// MustOpenDB returns a new, opened database at a temporary path. Panic on error.
func MustOpenDB() *DB {
	db := NewDB()
	if err := db.Open(); err != nil {
		panic(err)
	}
	return db
}

// Close closes the database and removes the underlying data.
func (db *DB) Close() error {
	defer os.RemoveAll(db.Path())
	return db.DB.Close()
}
