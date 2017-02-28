package pilosa_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
)

// Ensure database can open and retrieve a frame.
func TestDB_CreateFrameIfNotExists(t *testing.T) {
	db := MustOpenDB()
	defer db.Close()

	// Create frame.
	f, err := db.CreateFrameIfNotExists("f", pilosa.FrameOptions{})
	if err != nil {
		t.Fatal(err)
	} else if f == nil {
		t.Fatal("expected frame")
	}

	// Retrieve existing frame.
	other, err := db.CreateFrameIfNotExists("f", pilosa.FrameOptions{})
	if err != nil {
		t.Fatal(err)
	} else if f != other {
		t.Fatal("frame mismatch")
	}

	if f != db.Frame("f") {
		t.Fatal("frame mismatch")
	}
}

// Ensure database can delete a frame.
func TestDB_DeleteFrame(t *testing.T) {
	db := MustOpenDB()
	defer db.Close()

	// Create frame.
	if _, err := db.CreateFrameIfNotExists("f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Delete frame & verify it's gone.
	if err := db.DeleteFrame("f"); err != nil {
		t.Fatal(err)
	} else if db.Frame("f") != nil {
		t.Fatal("expected nil frame")
	}

	// Delete again to make sure it doesn't error.
	if err := db.DeleteFrame("f"); err != nil {
		t.Fatal(err)
	}
}

// Ensure database can set the default time quantum.
func TestDB_SetTimeQuantum(t *testing.T) {
	db := MustOpenDB()
	defer db.Close()

	// Set & retrieve time quantum.
	if err := db.SetTimeQuantum(pilosa.TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	} else if q := db.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum: %s", q)
	}

	// Reload database and verify that it is persisted.
	if err := db.Reopen(); err != nil {
		t.Fatal(err)
	} else if q := db.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum (reopen): %s", q)
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

// Reopen closes the database and reopens it.
func (db *DB) Reopen() error {
	if err := db.DB.Close(); err != nil {
		return err
	}

	path, name := db.Path(), db.Name()
	db.DB = pilosa.NewDB(path, name)

	if err := db.Open(); err != nil {
		return err
	}
	return nil
}

// MustSetBit sets a bit on the database. Panic on error.
func (db *DB) MustSetBit(name string, bitmapID, profileID uint64, t *time.Time) (changed bool) {
	changed, err := db.SetBit(name, bitmapID, profileID, t)
	if err != nil {
		panic(err)
	}
	return changed
}
