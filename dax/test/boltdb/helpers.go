package boltdb

import (
	"os"
	"testing"

	"github.com/molecula/featurebase/v3/dax/boltdb"
	"github.com/stretchr/testify/assert"
)

func MustGetDB(tb testing.TB) *boltdb.DB {
	tb.Helper()

	f, err := os.CreateTemp("", "dax-boltdb")
	assert.NoError(tb, err)

	dsn := "file:" + f.Name()

	db := boltdb.NewDB(dsn)
	return db
}

// MustOpenDB returns a new, open DB. Fatal on error.
func MustOpenDB(tb testing.TB) *boltdb.DB {
	db := MustGetDB(tb)

	if err := db.Open(); err != nil {
		tb.Fatal(err)
	}
	return db
}

// MustCloseDB closes the DB. Fatal on error.
func MustCloseDB(tb testing.TB, db *boltdb.DB) {
	tb.Helper()
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
}

func CleanupDB(tb testing.TB, path string) {
	tb.Helper()

	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil {
		tb.Fatal(err)
	}
}
