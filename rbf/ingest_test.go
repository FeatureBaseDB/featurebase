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

package rbf

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"

	//"time"

	"github.com/molecula/featurebase/v2/rbf/cfg"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/testhook"

	txkey "github.com/molecula/featurebase/v2/short_txkey"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
)

func rbfName(index, field, view string, shard uint64) string {
	return string(txkey.Prefix(index, field, view, shard))
}

var _ = rbfName // keep linter happy

/*
// rbtree uses 15% memory and needs half the ingest time
// for our 10K view ingest.
//
// previous master with slice copying instead of rbtree:

=== RUN   TestIngest_lots_of_views
ingest_test.go:141 2020-11-13T03:14:09.778839Z m0.TotalAlloc = 728408
ingest_test.go:144 2020-11-13T03:14:37.492104Z m1.TotalAlloc = 41,816,617,216
--- PASS: TestIngest_lots_of_views (27.71s)

// lots_views with rbtree

=== RUN   TestIngest_lots_of_views
ingest_test.go:141 2020-11-13T03:11:01.540076Z m0.TotalAlloc = 726072
ingest_test.go:144 2020-11-13T03:11:15.003591Z m1.TotalAlloc = 35,510,273,184
--- PASS: TestIngest_lots_of_views (13.46s)
*/
func TestIngest_lots_of_views(t *testing.T) {

	// realistic
	//nCt := 10000

	// fast CI
	nCt := 10

	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	//vv("m0.TotalAlloc = %v", m0.TotalAlloc)
	defer func() {
		runtime.ReadMemStats(&m1)
		//vv("m1.TotalAlloc = %v", m1.TotalAlloc)
	}()

	path, err := testhook.TempDir(t, "rbf_ingest_lots_of_views")
	PanicOn(err)
	defer os.Remove(path)

	cfg := cfg.NewDefaultConfig()
	db := NewDB(path, cfg)
	PanicOn(db.Open())

	// setup profiling
	if false {
		profile, err := os.Create("./rbf_ingest_put_ct.cpu")
		PanicOn(err)
		_ = pprof.StartCPUProfile(profile)
		defer func() {
			pprof.StopCPUProfile()
			profile.Close()
		}()
	}

	// put containers
	tx, err := db.Begin(true)
	PanicOn(err)

	index := "i"
	field := "f"
	var view string // set below in the loop.

	// put a raw-bitmap container to many views.
	bits := []uint16{}
	//for i := 0; i < 1<<16; i++ {
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			bits = append(bits, uint16(i))
		}
	}
	ct := roaring.NewContainerArray(bits)

	ckey := uint64(0)
	shard := ckey / ShardWidth

	for i := 0; i < nCt; i++ {
		view = fmt.Sprintf("view_%v", i)
		name := rbfName(index, field, view, shard)
		err = tx.PutContainer(name, ckey, ct)
		PanicOn(err)
		ct2, err := tx.Container(name, ckey)
		PanicOn(err)
		if err := ct2.BitwiseCompare(ct); err != nil {
			PanicOn("ct2 != ct")
		}

		// write .dot of it...
		if false { //ckey == nCt-1 {
			c, err := tx.cursor(name)
			if err == ErrBitmapNotFound {
				PanicOn("not found")
			} else if err != nil {
				PanicOn(err)
			}
			defer c.Close()
			c.Dump("one.bitmap.dot.dump")
		}
	}

	PanicOn(tx.Commit())

	sz, err := DiskUse(path, "")
	PanicOn(err)
	_ = sz
	//vv("sz in bytes= %v", sz)
	db.Close()
}

func DiskUse(root string, requiredSuffix string) (tot int, err error) {
	if !DirExists(root) {
		return -1, fmt.Errorf("listFilesUnderDir error: root directory '%v' not found", root)
	}

	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info == nil {
			PanicOn(fmt.Sprintf("info was nil for path = '%v'", path))
		}
		if info.IsDir() {
			// skip the size of directories themselves, only summing files.
		} else {
			sz := info.Size()
			if requiredSuffix == "" || strings.HasSuffix(path, requiredSuffix) {
				tot += int(sz)
			}
		}
		return nil
	})
	return
}
