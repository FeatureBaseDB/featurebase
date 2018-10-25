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

package http_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	gohttp "net/http"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/http"
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/server"
	"github.com/pilosa/pilosa/test"
)

// Test distributed TopN Row count across 3 nodes.
func TestClient_MultiNode(t *testing.T) {
	c := test.MustRunCluster(t, 3,
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node0"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node1"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node2"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
	)
	defer c.Close()

	hldr := []test.Holder{}
	for _, command := range c {
		hldr = append(hldr, test.Holder{Holder: command.Server.Holder()})
	}

	// Create a dispersed set of bitmaps across 3 nodes such that each individual node and shard width increment would reveal a different TopN.
	shardNums := []uint64{1, 2, 6}

	// This was generated with: `owns := s[i].Handler.Handler.API.Cluster.OwnsShards("i", 20, s[i].HostURI())`
	owns := [][]uint64{
		{1, 3, 4, 8, 10, 13, 17, 19},
		{2, 5, 7, 11, 12, 14, 18},
		{0, 6, 9, 15, 16, 20},
	}

	for i, num := range shardNums {
		ownsNum := false
		for _, ownNum := range owns[i] {
			if ownNum == num {
				ownsNum = true
				break
			}
		}
		if !ownsNum {
			t.Fatalf("Trying to use shard %d on host %s, but it doesn't own that shard. It owns %v", num, c[i].URL(), owns)
		}
	}

	baseBit0 := pilosa.ShardWidth * shardNums[0]
	baseBit1 := pilosa.ShardWidth * shardNums[1]
	baseBit2 := pilosa.ShardWidth * shardNums[2]

	maxShard := uint64(0)
	for _, x := range shardNums {
		if x > maxShard {
			maxShard = x
		}
	}
	_, err := c[0].API.CreateIndex(context.Background(), "i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = c[0].API.CreateField(context.Background(), "i", "f", pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}

	hldr[0].MustSetBits("i", "f", 100, baseBit0+10)
	hldr[0].MustSetBits("i", "f", 4, baseBit0+10, baseBit0+11, baseBit0+12)
	hldr[0].MustSetBits("i", "f", 4, baseBit0+10, baseBit0+11, baseBit0+12, baseBit0+13, baseBit0+14, baseBit0+15)
	hldr[0].MustSetBits("i", "f", 2, baseBit0+1, baseBit0+2, baseBit0+3, baseBit0+4)
	hldr[0].MustSetBits("i", "f", 3, baseBit0+1, baseBit0+2, baseBit0+3, baseBit0+4, baseBit0+5)
	hldr[0].MustSetBits("i", "f", 22, baseBit0+1, baseBit0+2)

	hldr[1].MustSetBits("i", "f", 99, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4)
	hldr[1].MustSetBits("i", "f", 100, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4, baseBit1+5, baseBit1+6, baseBit1+7, baseBit1+8, baseBit1+9, baseBit1+10)
	hldr[1].MustSetBits("i", "f", 98, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4, baseBit1+5, baseBit1+6)
	hldr[1].MustSetBits("i", "f", 1, baseBit1+4)
	hldr[1].MustSetBits("i", "f", 22, baseBit1+1, baseBit1+2, baseBit1+3, baseBit1+4, baseBit1+5)

	hldr[2].MustSetBits("i", "f", 24, baseBit2+10, baseBit2+11, baseBit2+12, baseBit2+13, baseBit2+14)
	hldr[2].MustSetBits("i", "f", 20, baseBit2+10, baseBit2+11, baseBit2+12, baseBit2+13)
	hldr[2].MustSetBits("i", "f", 21, baseBit2+10)
	hldr[2].MustSetBits("i", "f", 100, baseBit2+10)
	hldr[2].MustSetBits("i", "f", 99, baseBit2+10, baseBit2+11, baseBit2+12)
	hldr[2].MustSetBits("i", "f", 98, baseBit2+10, baseBit2+11)
	hldr[2].MustSetBits("i", "f", 22, baseBit2+10, baseBit2+11, baseBit2+12)

	// Rebuild the RankCache.
	// We have to do this to avoid the 10-second cache invalidation delay
	// built into cache.Invalidate()
	c[0].RecalculateCaches()
	c[1].RecalculateCaches()
	c[2].RecalculateCaches()

	// Connect to each node to compare results.
	client := make([]*Client, 3)
	client[0] = MustNewClient(c[0].URL(), http.GetHTTPClient(nil))
	client[1] = MustNewClient(c[1].URL(), http.GetHTTPClient(nil))
	client[2] = MustNewClient(c[2].URL(), http.GetHTTPClient(nil))

	topN := 4
	queryRequest := &pilosa.QueryRequest{
		Query:  fmt.Sprintf(`TopN(f, n=%d)`, topN),
		Remote: false,
	}

	result, err := client[0].Query(context.Background(), "i", queryRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Test must return exactly N results.
	if len(result.Results[0].([]pilosa.Pair)) != topN {
		t.Fatalf("unexpected number of TopN results: %s", spew.Sdump(result))
	}
	p := []pilosa.Pair{
		{ID: 100, Count: 12},
		{ID: 22, Count: 10},
		{ID: 98, Count: 8},
		{ID: 99, Count: 7}}

	// Valdidate the Top 4 result counts.
	if !reflect.DeepEqual(result.Results[0].([]pilosa.Pair), p) {
		t.Fatalf("Invalid TopN result set: %s", spew.Sdump(result))
	}

	result1, err := client[1].Query(context.Background(), "i", queryRequest)
	if err != nil {
		t.Fatal(err)
	}
	result2, err := client[2].Query(context.Background(), "i", queryRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Compare TopN results across all nodes in the cluster.
	if !reflect.DeepEqual(result, result1) {
		t.Fatalf("TopN result should be the same on node0 and node1: %s", spew.Sdump(result1))
	}

	if !reflect.DeepEqual(result, result2) {
		t.Fatalf("TopN result should be the same on node0 and node2: %s", spew.Sdump(result2))
	}
}

// Ensure client can export data.
func TestClient_Export(t *testing.T) {
	cmd := test.MustRunCluster(t, 1)[0]
	host := cmd.URL()

	cmd.MustCreateIndex(t, "keyed", pilosa.IndexOptions{Keys: true})
	cmd.MustCreateIndex(t, "unkeyed", pilosa.IndexOptions{Keys: false})

	cmd.MustCreateField(t, "keyed", "keyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())
	cmd.MustCreateField(t, "keyed", "unkeyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000))
	cmd.MustCreateField(t, "unkeyed", "keyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())
	cmd.MustCreateField(t, "unkeyed", "unkeyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000))

	c := MustNewClient(host, http.GetHTTPClient(nil))

	data := []pilosa.Bit{
		{RowID: 1, ColumnID: 100, RowKey: "row1", ColumnKey: "col100"},
		{RowID: 1, ColumnID: 101, RowKey: "row1", ColumnKey: "col101"},
		{RowID: 1, ColumnID: 102, RowKey: "row1", ColumnKey: "col102"},
		{RowID: 1, ColumnID: 103, RowKey: "row1", ColumnKey: "col103"},
		{RowID: 2, ColumnID: 200, RowKey: "row2", ColumnKey: "col200"},
		{RowID: 2, ColumnID: 201, RowKey: "row2", ColumnKey: "col201"},
		{RowID: 2, ColumnID: 202, RowKey: "row2", ColumnKey: "col202"},
		{RowID: 2, ColumnID: 203, RowKey: "row2", ColumnKey: "col203"},
	}

	t.Run("Export unkeyed,unkeyedf", func(t *testing.T) {
		// Populate data.
		for _, bit := range data {
			_, err := c.Query(context.Background(), "unkeyed", &pilosa.QueryRequest{
				Query:  fmt.Sprintf(`Set(%d, unkeyedf=%d)`, bit.ColumnID, bit.RowID),
				Remote: false,
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		buf := bytes.NewBuffer(nil)
		bw := bufio.NewWriter(buf)

		// Send export request.
		if err := c.ExportCSV(context.Background(), "unkeyed", "unkeyedf", 0, bw); err != nil {
			t.Fatal(err)
		}

		got := buf.String()

		// Expected output.
		exp := ""
		for _, bit := range data {
			exp += fmt.Sprintf("%d,%d\n", bit.RowID, bit.ColumnID)
		}

		// Verify data.
		if got != exp {
			t.Fatalf("unexpected export data: %s", got)
		}
	})

	t.Run("Export unkeyed,keyedf", func(t *testing.T) {
		// Populate data.
		for _, bit := range data {
			_, err := c.Query(context.Background(), "unkeyed", &pilosa.QueryRequest{
				Query:  fmt.Sprintf(`Set(%d, keyedf=%s)`, bit.ColumnID, bit.RowKey),
				Remote: false,
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		buf := bytes.NewBuffer(nil)
		bw := bufio.NewWriter(buf)

		// Send export request.
		if err := c.ExportCSV(context.Background(), "unkeyed", "keyedf", 0, bw); err != nil {
			t.Fatal(err)
		}

		got := buf.String()

		// Expected output.
		exp := ""
		for _, bit := range data {
			exp += fmt.Sprintf("%s,%d\n", bit.RowKey, bit.ColumnID)
		}

		// Verify data.
		if got != exp {
			t.Fatalf("unexpected export data: %s", got)
		}
	})

	t.Run("Export keyed,unkeyedf", func(t *testing.T) {
		// Populate data.
		for _, bit := range data {
			_, err := c.Query(context.Background(), "keyed", &pilosa.QueryRequest{
				Query:  fmt.Sprintf(`Set("%s", unkeyedf=%d)`, bit.ColumnKey, bit.RowID),
				Remote: false,
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		buf := bytes.NewBuffer(nil)
		bw := bufio.NewWriter(buf)

		// Send export request.
		if err := c.ExportCSV(context.Background(), "keyed", "unkeyedf", 0, bw); err != nil {
			t.Fatal(err)
		}

		got := buf.String()

		// Expected output.
		exp := ""
		for _, bit := range data {
			exp += fmt.Sprintf("%d,%s\n", bit.RowID, bit.ColumnKey)
		}

		// Verify data.
		if got != exp {
			t.Fatalf("unexpected export data: %s", got)
		}
	})

	t.Run("Export keyed,keyedf", func(t *testing.T) {
		// Populate data.
		for _, bit := range data {
			_, err := c.Query(context.Background(), "keyed", &pilosa.QueryRequest{
				Query:  fmt.Sprintf(`Set("%s", keyedf=%s)`, bit.ColumnKey, bit.RowKey),
				Remote: false,
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		buf := bytes.NewBuffer(nil)
		bw := bufio.NewWriter(buf)

		// Send export request.
		if err := c.ExportCSV(context.Background(), "keyed", "keyedf", 0, bw); err != nil {
			t.Fatal(err)
		}

		got := buf.String()

		// Expected output.
		exp := ""
		for _, bit := range data {
			exp += fmt.Sprintf("%s,%s\n", bit.RowKey, bit.ColumnKey)
		}

		// Verify data.
		if got != exp {
			t.Fatalf("unexpected export data: %s", got)
		}
	})
}

// Ensure client can bulk import data.
func TestClient_Import(t *testing.T) {
	cmd := test.MustRunCluster(t, 1)[0]
	host := cmd.URL()
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	// Load bitmap into cache to ensure cache gets updated.
	hldr.SetBit("i", "f", 1, 0) // set a bit so the view gets created.
	hldr.Row("i", "f", 0)

	// Send import request.
	c := MustNewClient(host, http.GetHTTPClient(nil))
	if err := c.Import(context.Background(), "i", "f", 0, []pilosa.Bit{
		{RowID: 0, ColumnID: 1},
		{RowID: 0, ColumnID: 5},
		{RowID: 200, ColumnID: 6},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := hldr.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 5}) {
		t.Fatalf("unexpected columns: %+v", a)
	}
	if a := hldr.Row("i", "f", 200).Columns(); !reflect.DeepEqual(a, []uint64{6}) {
		t.Fatalf("unexpected columns: %+v", a)
	}

	// Clear some data.
	if err := c.Import(context.Background(), "i", "f", 0, []pilosa.Bit{
		{RowID: 0, ColumnID: 5},
		{RowID: 200, ColumnID: 6},
	}, pilosa.OptImportOptionsClear(true)); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if a := hldr.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1}) {
		t.Fatalf("unexpected columns: %+v", a)
	}
	if a := hldr.Row("i", "f", 200).Columns(); !reflect.DeepEqual(a, []uint64{}) {
		t.Fatalf("unexpected columns: %+v", a)
	}
}

// Ensure client can bulk import data.
func TestClient_ImportRoaring(t *testing.T) {
	cluster := test.MustNewCluster(t, 2)
	for _, c := range cluster {
		c.Config.Cluster.ReplicaN = 2
	}
	err := cluster.Start()
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	defer cluster.Close()

	_, err = cluster[0].API.CreateIndex(context.Background(), "i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = cluster[0].API.CreateField(context.Background(), "i", "f", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 100))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	_, err = cluster[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: "Set(0, f=1)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	// Send import request.
	host := cluster[0].URL()
	c := MustNewClient(host, http.GetHTTPClient(nil))
	roaringData, _ := hex.DecodeString("3B3001000100000900010000000100010009000100") // [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537]
	if err := c.ImportRoaring(context.Background(), &cluster[0].API.Node().URI, "i", "f", 0, false, roaringData); err != nil {
		t.Fatal(err)
	}

	hldr := test.Holder{Holder: cluster[0].Server.Holder()}
	// Verify data on node 0.
	if a := hldr.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537}) {
		t.Fatalf("unexpected columns: %+v", a)
	}
	if a := hldr.Row("i", "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected columns: %+v", a)
	}

	hldr2 := test.Holder{Holder: cluster[1].Server.Holder()}
	// Verify data on node 1.
	if a := hldr2.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537}) {
		t.Fatalf("unexpected columns: %+v", a)
	}
	if a := hldr2.Row("i", "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected columns: %+v", a)
	}

	// Ensure that sending a roaring import with the clear flag works as expected.
	roaringDataClear, _ := hex.DecodeString("3A30000001000000010001001000000003000400") // [65539, 65540]
	if err := c.ImportRoaring(context.Background(), &cluster[0].API.Node().URI, "i", "f", 0, false, roaringDataClear, pilosa.OptImportOptionsClear(true)); err != nil {
		t.Fatal(err)
	}

	// Verify data on node 0.
	if a := hldr.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr.Row("i", "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}

	// Verify data on node 1.
	if a := hldr2.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr2.Row("i", "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}

	// Ensure that sending a roaring import with the clear flag works as expected.
	roaringDataClear, _ = hex.DecodeString("3A300000020000000000010001000100180000001C0000000400060001000300") // [4, 6, 65537, 65539]
	if err := c.ImportRoaring(context.Background(), &cluster[0].API.Node().URI, "i", "f", 0, false, roaringDataClear, pilosa.OptImportOptionsClear(true)); err != nil {
		t.Fatal(err)
	}

	// Verify data on node 0.
	if a := hldr.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 5, 7, 8, 9, 10}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr.Row("i", "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}

	// Verify data on node 1.
	if a := hldr2.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 5, 7, 8, 9, 10}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr2.Row("i", "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}

	// Ensure that sending a roaring import with the clear flag works as expected.
	roaringDataClear, _ = hex.DecodeString("3B3001000100000900010000000100010009000100") // [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537]
	if err := c.ImportRoaring(context.Background(), &cluster[0].API.Node().URI, "i", "f", 0, false, roaringDataClear, pilosa.OptImportOptionsClear(true)); err != nil {
		t.Fatal(err)
	}

	// Verify data on node 0.
	if a := hldr.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr.Row("i", "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}

	// Verify data on node 1.
	if a := hldr2.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr2.Row("i", "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
}

// Ensure client can bulk import data.
func TestClient_ImportKeys(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		cmd := test.MustRunCluster(t, 1)[0]
		host := cmd.URL()

		cmd.MustCreateIndex(t, "keyed", pilosa.IndexOptions{Keys: true})
		cmd.MustCreateIndex(t, "unkeyed", pilosa.IndexOptions{Keys: false})

		cmd.MustCreateField(t, "keyed", "keyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())
		cmd.MustCreateField(t, "keyed", "unkeyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000))
		cmd.MustCreateField(t, "unkeyed", "keyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())

		// Send import request.
		c := MustNewClient(host, http.GetHTTPClient(nil))

		t.Run("Import keyed,keyed", func(t *testing.T) {
			if err := c.Import(context.Background(), "keyed", "keyedf", 0, []pilosa.Bit{
				{RowKey: "green", ColumnKey: "eve"},
				{RowKey: "green", ColumnKey: "alice"},
				{RowKey: "green", ColumnKey: "bob"},
				{RowKey: "blue", ColumnKey: "eve"},
				{RowKey: "blue", ColumnKey: "alice"},
				{RowKey: "purple", ColumnKey: "eve"},
			}); err != nil {
				t.Fatal(err)
			}
			cmd.MustRecalculateCaches(t)
			resp := cmd.MustQuery(t, &pilosa.QueryRequest{
				Index: "keyed",
				Query: "TopN(keyedf)",
			})
			if pairs, ok := resp.Results[0].([]pilosa.Pair); !ok {
				t.Fatalf("unexpected response type %T", resp.Results[0])
			} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
				{Key: "green", Count: 3},
				{Key: "blue", Count: 2},
				{Key: "purple", Count: 1},
			}) {
				t.Fatalf("unexpected topn result: %v", pairs)
			}
		})

		t.Run("Import keyed,unkeyedf", func(t *testing.T) {
			if err := c.Import(context.Background(), "keyed", "unkeyedf", 0, []pilosa.Bit{
				{RowID: 1, ColumnKey: "eve"},
				{RowID: 1, ColumnKey: "alice"},
				{RowID: 1, ColumnKey: "bob"},
				{RowID: 2, ColumnKey: "eve"},
				{RowID: 2, ColumnKey: "alice"},
				{RowID: 3, ColumnKey: "eve"},
			}); err != nil {
				t.Fatal(err)
			}
			cmd.MustRecalculateCaches(t)
			resp := cmd.MustQuery(t, &pilosa.QueryRequest{
				Index: "keyed",
				Query: "TopN(unkeyedf)",
			})
			if pairs, ok := resp.Results[0].([]pilosa.Pair); !ok {
				t.Fatalf("unexpected response type %T", resp.Results[0])
			} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
				{ID: 1, Count: 3},
				{ID: 2, Count: 2},
				{ID: 3, Count: 1},
			}) {
				t.Fatalf("unexpected topn result: %v", pairs)
			}
		})

		t.Run("Import unkeyed,keyed", func(t *testing.T) {
			if err := c.Import(context.Background(), "unkeyed", "keyedf", 0, []pilosa.Bit{
				{RowKey: "green", ColumnID: 1},
				{RowKey: "green", ColumnID: 2},
				{RowKey: "green", ColumnID: 3},
				{RowKey: "blue", ColumnID: 1},
				{RowKey: "blue", ColumnID: 2},
				{RowKey: "purple", ColumnID: 1},
			}); err != nil {
				t.Fatal(err)
			}
			cmd.MustRecalculateCaches(t)
			resp := cmd.MustQuery(t, &pilosa.QueryRequest{
				Index: "unkeyed",
				Query: "TopN(keyedf)",
			})
			if pairs, ok := resp.Results[0].([]pilosa.Pair); !ok {
				t.Fatalf("unexpected response type %T", resp.Results[0])
			} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
				{Key: "green", Count: 3},
				{Key: "blue", Count: 2},
				{Key: "purple", Count: 1},
			}) {
				t.Fatalf("unexpected topn result: %v", pairs)
			}
		})
	})

	t.Run("MultiNode", func(t *testing.T) {
		cluster := test.MustRunCluster(t, 2)
		cmd0 := cluster[0]
		cmd1 := cluster[1]
		host0 := cmd0.URL()
		host1 := cmd1.URL()

		cmd0.MustCreateIndex(t, "keyed", pilosa.IndexOptions{Keys: true})
		cmd0.MustCreateField(t, "keyed", "keyedf0", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())
		cmd0.MustCreateField(t, "keyed", "keyedf1", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())

		// Send import request.
		c0 := MustNewClient(host0, http.GetHTTPClient(nil))
		c1 := MustNewClient(host1, http.GetHTTPClient(nil))

		// Import to node0.
		t.Run("Import node0", func(t *testing.T) {
			if err := c0.ImportK(context.Background(), "keyed", "keyedf0", []pilosa.Bit{
				{RowKey: "green", ColumnKey: "eve"},
				{RowKey: "green", ColumnKey: "alice"},
				{RowKey: "green", ColumnKey: "bob"},
				{RowKey: "blue", ColumnKey: "eve"},
				{RowKey: "blue", ColumnKey: "alice"},
				{RowKey: "purple", ColumnKey: "eve"},
			}); err != nil {
				t.Fatal(err)
			}
			cmd0.MustRecalculateCaches(t)
			resp := cmd0.MustQuery(t, &pilosa.QueryRequest{
				Index: "keyed",
				Query: "TopN(keyedf0)",
			})
			if pairs, ok := resp.Results[0].([]pilosa.Pair); !ok {
				t.Fatalf("unexpected response type %T", resp.Results[0])
			} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
				{Key: "green", Count: 3},
				{Key: "blue", Count: 2},
				{Key: "purple", Count: 1},
			}) {
				t.Fatalf("unexpected topn result: %v", pairs)
			}
		})

		// Import to node1 (ensure import is routed to coordinator for translation).
		t.Run("Import node1", func(t *testing.T) {
			if err := c1.ImportK(context.Background(), "keyed", "keyedf1", []pilosa.Bit{
				{RowKey: "green", ColumnKey: "eve"},
				{RowKey: "green", ColumnKey: "alice"},
				{RowKey: "green", ColumnKey: "bob"},
				{RowKey: "blue", ColumnKey: "eve"},
				{RowKey: "blue", ColumnKey: "alice"},
				{RowKey: "purple", ColumnKey: "eve"},
			}); err != nil {
				t.Fatal(err)
			}

			// Wait for translation replication.
			time.Sleep(500 * time.Millisecond)

			cmd1.MustRecalculateCaches(t)
			resp := cmd1.MustQuery(t, &pilosa.QueryRequest{
				Index: "keyed",
				Query: "TopN(keyedf1)",
			})
			if pairs, ok := resp.Results[0].([]pilosa.Pair); !ok {
				t.Fatalf("unexpected response type %T", resp.Results[0])
			} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
				{Key: "green", Count: 3},
				{Key: "blue", Count: 2},
				{Key: "purple", Count: 1},
			}) {
				t.Fatalf("unexpected topn result: %v", pairs)
			}
		})
	})

	t.Run("IntegerFieldSingleNode", func(t *testing.T) {
		cmd := test.MustRunCluster(t, 1)[0]
		host := cmd.URL()
		holder := cmd.Server.Holder()
		hldr := test.Holder{Holder: holder}

		fldName := "f"

		// Load bitmap into cache to ensure cache gets updated.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{Keys: true})
		field, err := index.CreateFieldIfNotExists(fldName, pilosa.OptFieldTypeInt(-100, 100))
		if err != nil {
			t.Fatal(err)
		}

		// Send import request.
		c := MustNewClient(host, http.GetHTTPClient(nil))
		if err := c.ImportValue(context.Background(), "i", "f", 0, []pilosa.FieldValue{
			{ColumnKey: "col1", Value: -10},
			{ColumnKey: "col2", Value: 20},
			{ColumnKey: "col3", Value: 40},
		}); err != nil {
			t.Fatal(err)
		}

		// Verify Sum.
		sum, cnt, err := field.Sum(nil, fldName)
		if err != nil {
			t.Fatal(err)
		}
		if sum != 50 || cnt != 3 {
			t.Fatalf("unexpected values: got sum=%v, count=%v; expected sum=50, cnt=3", sum, cnt)
		}

		// Verify Range.
		queryRequest := &pilosa.QueryRequest{
			Query:  fmt.Sprintf(`Range(%s>10)`, fldName),
			Remote: false,
		}

		result, err := c.Query(context.Background(), "i", queryRequest)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result.Results[0].(*pilosa.Row).Keys, []string{"col2", "col3"}) {
			t.Fatalf("unexpected column keys: %s", spew.Sdump(result))
		}

		// Clear data.
		if err := c.ImportValue(context.Background(), "i", "f", 0, []pilosa.FieldValue{
			{ColumnKey: "col2", Value: 20},
		}, pilosa.OptImportOptionsClear(true)); err != nil {
			t.Fatal(err)
		}

		// Verify Sum.
		sum, cnt, err = field.Sum(nil, fldName)
		if err != nil {
			t.Fatal(err)
		}
		if sum != 30 || cnt != 2 {
			t.Fatalf("unexpected values: got sum=%v, count=%v; expected sum=30, cnt=2", sum, cnt)
		}

		// Verify Range.
		queryRequest = &pilosa.QueryRequest{
			Query:  fmt.Sprintf(`Range(%s>10)`, fldName),
			Remote: false,
		}

		result, err = c.Query(context.Background(), "i", queryRequest)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result.Results[0].(*pilosa.Row).Keys, []string{"col3"}) {
			t.Fatalf("unexpected column keys: %s", spew.Sdump(result))
		}
	})
}

// Ensure client can bulk import value data.
func TestClient_ImportValue(t *testing.T) {
	cmd := test.MustRunCluster(t, 1)[0]
	host := cmd.URL()
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	fldName := "f"

	// Load bitmap into cache to ensure cache gets updated.
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	field, err := index.CreateFieldIfNotExists(fldName, pilosa.OptFieldTypeInt(-100, 100))
	if err != nil {
		t.Fatal(err)
	}

	// Send import request.
	c := MustNewClient(host, http.GetHTTPClient(nil))
	if err := c.ImportValue(context.Background(), "i", "f", 0, []pilosa.FieldValue{
		{ColumnID: 1, Value: -10},
		{ColumnID: 2, Value: 20},
		{ColumnID: 3, Value: 40},
	}); err != nil {
		t.Fatal(err)
	}

	// Verify Sum.
	sum, cnt, err := field.Sum(nil, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if sum != 50 || cnt != 3 {
		t.Fatalf("unexpected values: got sum=%v, count=%v; expected sum=50, cnt=3", sum, cnt)
	}

	// Verify Min.
	min, cnt, err := field.Min(nil, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if min != -10 || cnt != 1 {
		t.Fatalf("unexpected values: got min=%v, count=%v; expected min=-10, cnt=1", min, cnt)
	}

	// Verify Min with Filter.
	filter, err := field.Range(fldName, pql.GT, 40)
	if err != nil {
		t.Fatal(err)
	}
	min, cnt, err = field.Min(filter, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if min != -100 || cnt != 0 {
		t.Fatalf("unexpected values: got min=%v, count=%v; expected min=-100, cnt=0", min, cnt)
	}

	// Verify Max.
	max, cnt, err := field.Max(nil, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if max != 40 || cnt != 1 {
		t.Fatalf("unexpected values: got max=%v, count=%v; expected max=40, cnt=1", max, cnt)
	}

	// Send import request.
	if err := c.ImportValue(context.Background(), "i", "f", 0, []pilosa.FieldValue{
		{ColumnID: 1, Value: -10},
		{ColumnID: 3, Value: 40},
	}, pilosa.OptImportOptionsClear(true)); err != nil {
		t.Fatal(err)
	}

	// Verify Sum.
	sum, cnt, err = field.Sum(nil, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if sum != 20 || cnt != 1 {
		t.Fatalf("unexpected values: got sum=%v, count=%v; expected sum=20, cnt=1", sum, cnt)
	}

	// Verify Min with Filter.
	filter, err = field.Range(fldName, pql.GT, 40)
	if err != nil {
		t.Fatal(err)
	}
	min, cnt, err = field.Min(filter, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if min != -100 || cnt != 0 {
		t.Fatalf("unexpected values: got min=%v, count=%v; expected min=-100, cnt=0", min, cnt)
	}

	// Verify Max.
	max, cnt, err = field.Max(nil, fldName)
	if err != nil {
		t.Fatal(err)
	}
	if max != 20 || cnt != 1 {
		t.Fatalf("unexpected values: got max=%v, count=%v; expected max=20, cnt=1", max, cnt)
	}
}

// Ensure client can bulk import data while tracking existence.
func TestClient_ImportExistence(t *testing.T) {
	cmd := test.MustRunCluster(t, 1)[0]
	host := cmd.URL()
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	t.Run("Set", func(t *testing.T) {
		idxName := "iset"
		fldName := "fset"

		index := hldr.MustCreateIndexIfNotExists(idxName, pilosa.IndexOptions{TrackExistence: true})
		_, err := index.CreateFieldIfNotExists(fldName)
		if err != nil {
			t.Fatal(err)
		}

		// Send import request.
		c := MustNewClient(host, http.GetHTTPClient(nil))
		if err := c.Import(context.Background(), idxName, fldName, 0, []pilosa.Bit{
			{RowID: 0, ColumnID: 1},
			{RowID: 0, ColumnID: 5},
			{RowID: 200, ColumnID: 6},
		}); err != nil {
			t.Fatal(err)
		}

		// Verify data.
		if a := hldr.Row(idxName, fldName, 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 5}) {
			t.Fatalf("unexpected columns: %+v", a)
		}
		if a := hldr.Row(idxName, fldName, 200).Columns(); !reflect.DeepEqual(a, []uint64{6}) {
			t.Fatalf("unexpected columns: %+v", a)
		}

		// Verify existence.
		if a := hldr.ReadRow(idxName, "exists", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 5, 6}) {
			t.Fatalf("unexpected existence columns: %+v", a)
		}
	})

	t.Run("Int", func(t *testing.T) {
		idxName := "iint"
		fldName := "fint"

		index := hldr.MustCreateIndexIfNotExists(idxName, pilosa.IndexOptions{TrackExistence: true})
		field, err := index.CreateFieldIfNotExists(fldName, pilosa.OptFieldTypeInt(-100, 100))
		if err != nil {
			t.Fatal(err)
		}

		// Send import request.
		c := MustNewClient(host, http.GetHTTPClient(nil))
		if err := c.ImportValue(context.Background(), idxName, fldName, 0, []pilosa.FieldValue{
			{ColumnID: 1, Value: -10},
			{ColumnID: 2, Value: 20},
			{ColumnID: 3, Value: 40},
		}); err != nil {
			t.Fatal(err)
		}

		// Verify Sum.
		sum, cnt, err := field.Sum(nil, fldName)
		if err != nil {
			t.Fatal(err)
		}
		if sum != 50 || cnt != 3 {
			t.Fatalf("unexpected values: got sum=%v, count=%v; expected sum=50, cnt=3", sum, cnt)
		}

		// Verify existence.
		if a := hldr.ReadRow(idxName, "exists", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3}) {
			t.Fatalf("unexpected existence columns: %+v", a)
		}
	})
}

// Ensure client can retrieve a list of all checksums for blocks in a fragment.
func TestClient_FragmentBlocks(t *testing.T) {
	cmd := test.MustRunCluster(t, 1)[0]
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", pilosa.HashBlockSize*3, 100)

	// Set a bit on a different shard.
	hldr.SetBit("i", "f", 0, 1)
	c := MustNewClient(cmd.URL(), http.GetHTTPClient(nil))
	blocks, err := c.FragmentBlocks(context.Background(), nil, "i", "f", "standard", 0)
	if err != nil {
		t.Fatal(err)
	} else if len(blocks) != 2 {
		t.Fatalf("unexpected blocks: %s", spew.Sdump(blocks))
	} else if blocks[0].ID != 0 {
		t.Fatalf("unexpected block id(0): %d", blocks[0].ID)
	} else if blocks[1].ID != 3 {
		t.Fatalf("unexpected block id(1): %d", blocks[1].ID)
	}

	// Verify data matches local blocks.
	if a, err := cmd.API.FragmentBlocks(context.Background(), "i", "f", "standard", 0); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(a, blocks) {
		t.Fatalf("blocks mismatch:\n\nexp=%s\n\ngot=%s\n\n", spew.Sdump(a), spew.Sdump(blocks))
	}
}

// Client represents a test wrapper for pilosa.Client.
type Client struct {
	*http.InternalClient
}

// MustNewClient returns a new instance of Client. Panic on error.
func MustNewClient(host string, h *gohttp.Client) *Client {
	c, err := http.NewInternalClient(host, h)
	if err != nil {
		panic(err)
	}
	return &Client{InternalClient: c}
}
