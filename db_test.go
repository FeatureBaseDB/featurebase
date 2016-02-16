package pilosa_test

import (
	"io/ioutil"
	"os"
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
