// Copyright 2020 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
