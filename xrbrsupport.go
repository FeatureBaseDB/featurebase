// Copyright 2017 Pilosa Corp.
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
	"fmt"

	"github.com/pilosa/pilosa/v2/rbf"
	"github.com/pilosa/pilosa/v2/roaring"
)

type Converter interface {
	Convert(index, field, view string, shard uint64, rb *roaring.Bitmap) error
	Shutdown()
}

type RBFConverter struct {
	Dbs  map[string]*rbf.DB
	Base string
}

func (rbc *RBFConverter) GetOrCreateDB(index string, shard uint64) (*rbf.DB, error) {
	key := fmt.Sprintf("%s/%d", index, shard)
	db, found := rbc.Dbs[key]
	if found {
		return db, nil
	}
	path := rbc.Base + "/" + key
	db = rbf.NewDB(path)
	err := db.Open()
	if err != nil {
		return nil, err
	}
	rbc.Dbs[key] = db
	return db, nil
}
func (rbc *RBFConverter) Shutdown() {
	for key, db := range rbc.Dbs {
		fmt.Println("Shutdown", key)
		db.Close()

	}

}
func (rbc *RBFConverter) Convert(index, field, view string, shard uint64, rb *roaring.Bitmap) error {
	fmt.Println("CONVERT", index, field, view, shard)
	db, err := rbc.GetOrCreateDB(index, shard)
	if err != nil {
		return err
	}
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	name := fmt.Sprintf("%s/%s", field, view)
	err = tx.CreateBitmap(name)
	if err != nil {
		return err
	}
	_, err = tx.AddRoaring(name, rb)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (h *Holder) ConvertToRBF(c Converter) {
	/*
		for idxname, idx := range h.indexes {
			for fieldName, field := range idx.fields {
				for _, view := range field.views() {
					for shard, fragment := range view.fragments {
						panic("NEED bitmap from storage")
						junk := roaring.NewBitmap()
						err := c.Convert(idxname, fieldName, view.name, shard, junk)
						if err != nil {
							fmt.Println("ERR", err, fragment.shard) //just added shard for compile
						}
					}
				}

			}

		}
		c.Shutdown()
	*/
}
