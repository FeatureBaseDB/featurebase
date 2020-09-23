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

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
	"github.com/pilosa/pilosa/v2/hash"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/test"
)

func Test_Repair(t *testing.T) {

	// a) setup 1 primary + 3 replicas of disagree-ing cluster dirs.

	nNodes := 4
	nReplicas := 3

	name := t.Name()
	var nodeid []string
	for i := 0; i < nNodes; i++ {
		// work around a bug in the test.MustRunCluster that corrupts
		// the .topology file if we only join name with one "_" underscore.
		nodeid = append(nodeid, name+"__"+strconv.Itoa(i))
	}

	c := test.MustRunCluster(t, nNodes,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID(nodeid[0]),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
				pilosa.OptServerReplicaN(nReplicas),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID(nodeid[1]),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
				pilosa.OptServerReplicaN(nReplicas),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID(nodeid[2]),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
				pilosa.OptServerReplicaN(nReplicas),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID(nodeid[3]),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
				pilosa.OptServerReplicaN(nReplicas),
			)},
	)
	// note: do not defer c.Close() here. We manually close below.

	var nodes []*test.Command
	var dirs []string
	for i := 0; i < nNodes; i++ {
		nd := c.GetNode(i)
		nodes = append(nodes, nd)
		dirs = append(dirs, nd.Server.Holder().Path())
	}

	ctx := context.Background()

	index := "rick"
	fieldName := "f"

	idx, err := nodes[0].API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: true, TrackExistence: true})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	if idx.CreatedAt() == 0 {
		t.Fatal("index createdAt is empty")
	}

	field, err := nodes[0].API.CreateField(ctx, index, fieldName, pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	if field.CreatedAt() == 0 {
		t.Fatal("field createdAt is empty")
	}

	rowID := uint64(1)
	timestamp := int64(0)

	// Generate some keyed records.
	rowIDs := []uint64{}
	timestamps := []int64{}
	N := 10
	for i := 1; i <= N; i++ {
		rowIDs = append(rowIDs, rowID)
		timestamps = append(timestamps, timestamp)
	}

	// Keys are sharded so ordering is not guaranteed.
	colKeys := []string{"col10", "col8", "col9", "col6", "col7", "col4", "col5", "col2", "col3", "col1"}

	colKeys = colKeys[:N]

	// Import data with keys to the coordinator (node0) and verify that it gets
	// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
	req := &pilosa.ImportRequest{
		Index:          index,
		IndexCreatedAt: idx.CreatedAt(),
		Field:          fieldName,
		FieldCreatedAt: field.CreatedAt(),

		// even though this says Shard: 0, that won't matter. The column keys
		// get hashed and that decides the actual shard.
		Shard:      0,
		RowIDs:     rowIDs,
		ColumnKeys: colKeys,
		Timestamps: timestamps,
	}

	qcx := nodes[0].API.Txf().NewQcx()

	if err := nodes[0].API.Import(ctx, qcx, req); err != nil {
		t.Fatal(err)
	}
	panicOn(qcx.Finish())

	pql := fmt.Sprintf("Row(%s=%d)", fieldName, rowID)

	// Query node0.
	if res, err := nodes[0].API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
		t.Fatal(err)
	} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
		t.Fatalf("expected colKeys='%#v'; observed column keys: %#v", colKeys, keys)
	}

	// Query node1.
	if err := test.RetryUntil(5*time.Second, func() error {
		if res, err := nodes[1].API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			return err
		} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
			return fmt.Errorf("unexpected column keys: %#v", keys)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// end of setup.

	// partitionID in use: 6, 31, 57, 133, 185, 235
	targetPartition := 31  // which partitionID we mess with.
	targetNode := nodes[0] // this is the first replica.
	// 0 first replica
	// 1 second replica
	// 2  -- not a replica
	// 3 primary

	cfg := &FsckConfig{
		Fix:    false,
		FixCol: false,
		Quiet:  true,
		//Verbose:  true,
		ReplicaN: nReplicas,
		Dirs:     dirs,
	}
	panicOn(cfg.ValidateConfig())

	// for this test, mess up a replica that is not the primary.

	h := targetNode.API.Holder()
	idx = h.Index(index)
	store := idx.TranslateStore(targetPartition)
	fwd, rev := getFwdRev(store, targetPartition)
	//vv("targetPartition=%v, store.PartitionID=%v, before corruption, fwd='%#v', rev='%#v'", targetPartition, store.PartitionID, fwd, rev)

	// # fsck_test.go:288 2020-10-01T13:39:57.718995-05:00 partition 31, key 'col5' -> db00001
	presz := len(rev)
	delete(rev, fwd["col5"])
	postsz := len(rev)

	if postsz == presz {
		panic("did not delete any key!")
	}

	bolt := store.(*boltdb.TranslateStore)
	//vv("pre corruption, bolt = '%v'", fileChecksum(bolt.Path))
	//bolt.DumpBolt("pre-corruption")

	if err := bolt.SetFwdRevMaps(nil, fwd, rev); err != nil {
		t.Fatal(err)
	}
	//vv("post corruption, bolt = '%v'", fileChecksum(bolt.Path))
	//bolt.DumpBolt("post-corruption")

	//fwd3, rev3 := getFwdRev(store, targetPartition)
	//vv("after corruption, fwd='%#v', rev='%#v'", fwd3, rev3)

	for _, nd := range nodes {
		nd.Command.Close()
	}
	//panicOn(bolt.Open())
	//bolt.DumpBolt("post-corruption, after Close. bolt:")
	//bolt.Close()

	//chksums := getChecksums(dirs, cfg, targetPartition)
	//vv("post corruption, pre repair chksums = '%#v'", chksums)

	// first we check that the corruption can be detected
	// by our test with the checksums.

	chk, err := check(dirs, cfg, targetPartition)
	_ = chk
	//vv("pre-fix, chk='%v'; err='%v'", chk, err)

	if err == nil {
		panic("expected to see checksums not match! but no corruption detected.")
	}

	// b) running in reporting mode only should report that a fix is needed.
	fixNeeded, err := cfg.Run()
	panicOn(err)
	if !fixNeeded {
		panic("fix should be needed now, before repair")
	}

	// c) run the fix.
	cfg.Fix = true
	cfg.FixCol = true
	fixNeeded, err = cfg.Run()
	panicOn(err)
	if !fixNeeded {
		panic("fix should be marked needed if repair was made")
	}

	// d) check that the replicas all look like the primary.

	//chksums = getChecksums(dirs, cfg, targetPartition)
	//vv("after repair chksums = '%#v'", chksums)

	chk, err = check(dirs, cfg, targetPartition)
	_ = chk
	//vv("chk = '%v' after repair; err='%v'", chk, err)
	panicOn(err)

	// e) run again, should see no fix needed.
	fixNeeded, err = cfg.Run()
	panicOn(err)
	if fixNeeded {
		panic("should see no fix needed after the prior repair")
	}

}

func getFwdRev(store pilosa.TranslateStore, partitionID int) (fwd map[string]uint64, rev map[uint64]string) {
	fwd = make(map[string]uint64)
	rev = make(map[uint64]string)
	_ = store.KeyWalker(func(key string, col uint64) {
		//vv("partition %v, key '%v' -> %x", partitionID, key, col)
		fwd[key] = col
	})
	_ = store.IDWalker(func(key string, col uint64) {
		//vv("partition %v, id %x -> '%v'", partitionID, col, key)
		rev[col] = key
	})
	return
}

func check(dirs []string, cfg *FsckConfig, targetPartition int) (chksum string, err error) {

	firstChecksum := ""
	firstDir := ""
	quiet := cfg.Quiet
	defer func() {
		cfg.Quiet = quiet
	}()
	cfg.Quiet = true
	for i := range dirs {
		dir := dirs[i]
		_, _, ats, err := cfg.readOneDir(dir)
		panicOn(err)

		for _, s := range ats.Sums {
			if s.PartitionID != targetPartition {
				continue
			}
			if s.IsPrimary || s.IsReplica {
				chksum := s.Checksum
				if firstChecksum == "" {
					firstChecksum = chksum
					firstDir = dir
				} else {
					if chksum != firstChecksum {
						return chksum, fmt.Errorf("bolt chksum on node %v '%v' disagrees with '%v' on '%v'", dir, chksum, firstChecksum, firstDir)
					}
				}
			}
		}
	}
	return firstChecksum, nil
}

var _ = getChecksums

func getChecksums(dirs []string, cfg *FsckConfig, targetPartition int) (chksum []string) {

	for i := range dirs {
		dir := dirs[i]
		_, _, ats, err := cfg.readOneDir(dir)
		panicOn(err)

		for _, s := range ats.Sums {
			if s.PartitionID != targetPartition {
				continue
			}
			chksum = append(chksum, s.Checksum)
		}
	}
	return
}

/* on shardwidth 20
# fsck_test.go:211 2020-09-30T17:19:05.823278-05:00 partition 6, key 'col2' -> dc00001
# fsck_test.go:214 2020-09-30T17:19:05.823309-05:00 partition 6, id dc00001 -> 'col2'
# fsck_test.go:211 2020-09-30T17:19:05.823430-05:00 partition 31, key 'col5' -> db00001
# fsck_test.go:214 2020-09-30T17:19:05.823447-05:00 partition 31, id db00001 -> 'col5'
# fsck_test.go:211 2020-09-30T17:19:05.823970-05:00 partition 57, key 'col10' -> 5d00001
# fsck_test.go:214 2020-09-30T17:19:05.823998-05:00 partition 57, id 5d00001 -> 'col10'
# fsck_test.go:211 2020-09-30T17:19:05.827007-05:00 partition 133, key 'col7' -> d900001
# fsck_test.go:214 2020-09-30T17:19:05.827071-05:00 partition 133, id d900001 -> 'col7'
# fsck_test.go:211 2020-09-30T17:19:05.827549-05:00 partition 185, key 'col3' -> dd00001
# fsck_test.go:214 2020-09-30T17:19:05.827573-05:00 partition 185, id dd00001 -> 'col3'
# fsck_test.go:211 2020-09-30T17:19:05.827792-05:00 partition 235, key 'col9' -> d700001
# fsck_test.go:214 2020-09-30T17:19:05.827809-05:00 partition 235, id d700001 -> 'col9'
*/

var _ = fileChecksum

func fileChecksum(path string) string {
	by, err := ioutil.ReadFile(path)
	panicOn(err)
	return hash.Blake3sum16(by)
}
