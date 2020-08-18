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
	"fmt"
	"sync"
)

// types to support a database file per shard

type DBnode struct {
	Holder map[string]*DBholder
}
type DBholder struct {
	Index map[string]*DBindex
}
type DBindex struct {
	Field map[string]*DBfield
}
type DBfield struct {
	View map[string]*DBview
}
type DBview struct {
	Shard map[uint64]*DBshard
}

type DBWrapper interface {
	DeleteDBPath(path string) error
	Close() error
}

type DBRegistry interface {
	OpenDBWrapper(path string) (DBWrapper, error)
}

type DBshard struct {
	Path string
	ID   DBID
	Open bool

	// With RWMutex, the
	// writer who calls Lock() automatically gets priority over
	// any reader who arrives later, even if the lock is held
	// by a reader to start with.
	RWMut sync.RWMutex

	W            DBWrapper
	ParentDBview *DBview
}

type DBID struct {
	Node   string
	Holder string
	Index  string
	Field  string
	View   string
	Shard  uint64
}

func (id *DBID) Path() (s string) {
	s = id.Node + sep + id.Holder + sep + id.Index +
		sep + id.Field + sep + id.View + sep + fmt.Sprintf("%04v", id.Shard)
	return
}

type DBPerShard struct {
	Mu sync.Mutex

	Dir  string
	Node map[string]*DBnode

	// just flat, not buried within the Node heirarchy.
	// Easily see how many we have.
	Flatmap map[*DBshard]struct{}
}

func NewDBPerShard(dir string) (d *DBPerShard) {
	d = &DBPerShard{
		Dir:     dir,
		Node:    make(map[string]*DBnode),
		Flatmap: make(map[*DBshard]struct{}),
	}
	return
}

func (per *DBPerShard) GetDBshard(registry DBRegistry, id DBID) (dbs *DBshard, err error) {
	per.Mu.Lock()
	defer per.Mu.Unlock()

	dbn, ok := per.Node[id.Node]
	if !ok {
		dbn = &DBnode{
			Holder: make(map[string]*DBholder),
		}
		per.Node[id.Node] = dbn
	}
	dbh, ok := dbn.Holder[id.Holder]
	if !ok {
		dbh = &DBholder{
			Index: make(map[string]*DBindex),
		}
		dbn.Holder[id.Holder] = dbh
	}
	dbi, ok := dbh.Index[id.Index]
	if !ok {
		dbi = &DBindex{
			Field: make(map[string]*DBfield),
		}
		dbh.Index[id.Index] = dbi
	}
	dbf, ok := dbi.Field[id.Field]
	if !ok {
		dbf = &DBfield{
			View: make(map[string]*DBview),
		}
		dbi.Field[id.Field] = dbf
	}
	dbv, ok := dbf.View[id.View]
	if !ok {
		dbv = &DBview{
			Shard: make(map[uint64]*DBshard),
		}
		dbf.View[id.View] = dbv
	}
	dbs, ok = dbv.Shard[id.Shard]
	if !ok {
		dbs = &DBshard{
			ParentDBview: dbv,
			ID:           id,
			Path:         per.Dir + sep + id.Path(),
		}
		dbv.Shard[id.Shard] = dbs
	}
	if !dbs.Open {
		dbs.W, err = registry.OpenDBWrapper(dbs.Path)
		if dbs.W != nil {
			per.Flatmap[dbs] = struct{}{}
		}
	}
	return
}

func (per *DBPerShard) Del(dbs *DBshard) (err error) {
	per.Mu.Lock()
	defer per.Mu.Unlock()

	err = dbs.W.Close()
	if err != nil {
		return
	}
	panicOn(dbs.W.DeleteDBPath(dbs.Path))
	delete(per.Flatmap, dbs)

	// delete from the heirarchy
	delete(dbs.ParentDBview.Shard, dbs.ID.Shard)
	return
}

func (per *DBPerShard) Close() (err error) {
	per.Mu.Lock()
	defer per.Mu.Unlock()

	for _, dbn := range per.Node {
		for _, dbh := range dbn.Holder {
			for _, dbi := range dbh.Index {
				for _, dbf := range dbi.Field {
					for _, dbv := range dbf.View {
						for _, dbs := range dbv.Shard {
							err = dbs.W.Close()
							panicOn(err)
						}
					}
				}
			}
		}
	}
	return
}
