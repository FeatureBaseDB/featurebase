package boltdb_test

import (
	"testing"

	"github.com/molecula/featurebase/v3/dax/test/boltdb"
)

// Ensure the test database can open & close.
func TestDB(t *testing.T) {
	db := boltdb.MustOpenDB(t)
	defer boltdb.MustCloseDB(t, db)

	t.Cleanup(func() {
		boltdb.CleanupDB(t, db.Path())
	})
}
