package pilosa

import (
	"testing"

	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
)

func TestRoaring_HasData(t *testing.T) {
	holder := newHolderWithTempPath(t, "roaring")

	idx, err := holder.CreateIndex("i", IndexOptions{})
	PanicOn(err)
	defer idx.Close()

	db, err := globalRoaringReg.OpenDBWrapper(idx.path, false, nil)
	PanicOn(err)
	db.SetHolder(idx.holder)

	// HasData should start out false.
	hasAnything, err := db.HasData()
	PanicOn(err)

	if hasAnything {
		t.Fatalf("HasData reported existing data on an empty database")
	}

	// check that HasData sees a committed record.

	field, shard := "f", uint64(123)

	tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard})
	defer tx.Rollback()

	f, err := idx.CreateField(field)
	PanicOn(err)
	_, err = f.SetBit(tx, 1, 1, nil)
	PanicOn(err)
	PanicOn(tx.Commit())

	hasAnything, err = db.HasData()
	if err != nil {
		t.Fatal(err)
	}
	if !hasAnything {
		t.Fatalf("HasData() reported no data on a database that has 'x' written to it")
	}
}
