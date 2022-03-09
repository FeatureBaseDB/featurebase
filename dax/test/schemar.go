package test

import (
	"os"
	"testing"

	"github.com/molecula/featurebase/v3/dax/boltdb"
	"github.com/molecula/featurebase/v3/dax/mds/schemar"
	schemarbolt "github.com/molecula/featurebase/v3/dax/mds/schemar/boltdb"
	testbolt "github.com/molecula/featurebase/v3/dax/test/boltdb"
	"github.com/molecula/featurebase/v3/logger"
)

func NewSchemar(t *testing.T) (schemar schemar.Schemar, cleanup func()) {
	td, err := os.MkdirTemp("", "schemartest_*")
	if err != nil {
		t.Fatalf(": %v", err)
	}
	db, err := boltdb.NewSvcBolt(td, "schemar", schemarbolt.SchemarBuckets...)
	if err != nil {
		t.Fatalf("opening boltdb: %v", err)
	}

	s := schemarbolt.NewSchemar(db, logger.StderrLogger)
	return s, func() {
		testbolt.MustCloseDB(t, db)
		testbolt.CleanupDB(t, db.Path())
	}
}
