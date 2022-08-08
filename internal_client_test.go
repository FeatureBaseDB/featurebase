// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	gohttp "net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/authn"
	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/encoding/proto"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/test"
	"github.com/molecula/featurebase/v3/vprint"
	"github.com/pkg/errors"
	"github.com/ricochet2200/go-disk-usage/du"
)

// Test distributed TopN Row count across 3 nodes.
func TestClient_MultiNode(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	_, err := c.GetNode(0).API.CreateIndex(context.Background(), c.Idx(), pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = c.GetNode(0).API.CreateField(context.Background(), c.Idx(), "f", pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}

	// Connect to each node to compare results.
	client := make([]*Client, 3)
	client[0] = MustNewClient(c.GetNode(0).URL(), pilosa.GetHTTPClient(nil))
	client[1] = MustNewClient(c.GetNode(1).URL(), pilosa.GetHTTPClient(nil))
	client[2] = MustNewClient(c.GetNode(2).URL(), pilosa.GetHTTPClient(nil))

	b0 := uint64(ShardWidth * 0)
	b1 := uint64(ShardWidth * 1)
	b2 := uint64(ShardWidth * 2)

	// helper to let us avoid repeating the b0+, etc, over and over
	collate := func(a, b, c []uint64) []uint64 {
		d := make([]uint64, len(a)+len(b)+len(c))
		n := 0
		for _, v := range a {
			d[n] = b0 + v
			n++
		}
		for _, v := range b {
			d[n] = b1 + v
			n++
		}
		for _, v := range c {
			d[n] = b2 + v
			n++
		}
		return d
	}
	// data set. part of the goal of this is to have different top counts using a single node than
	// using all three nodes
	rows := map[uint64][]uint64{
		100: collate([]uint64{10}, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, []uint64{10}),
		4:   collate([]uint64{10, 11, 12, 13, 14, 15}, nil, nil),
		2:   collate([]uint64{1, 2, 3, 4}, nil, nil),
		3:   collate([]uint64{1, 2, 3, 4, 5}, nil, nil),
		99:  collate(nil, []uint64{1, 2, 3, 4}, []uint64{10, 11, 12}),
		98:  collate(nil, []uint64{1, 2, 3, 4, 5, 6}, []uint64{10, 11}),
		22:  collate([]uint64{1, 2}, []uint64{1, 2, 3, 4, 5}, []uint64{10, 11, 12}),
		1:   collate(nil, []uint64{4}, nil),
		21:  collate(nil, nil, []uint64{10}),
	}

	bits := 0
	for _, v := range rows {
		bits += len(v)
	}
	rowIDs := make([]uint64, bits)
	colIDs := make([]uint64, bits)
	n := 0
	for k, cols := range rows {
		for _, v := range cols {
			rowIDs[n] = k
			colIDs[n] = v
			n++
		}
	}

	req := &pilosa.ImportRequest{
		Index:     c.Idx(),
		Field:     "f",
		RowIDs:    rowIDs,
		ColumnIDs: colIDs,
		Shard:     ^uint64(0),
	}
	err = client[0].Import(context.Background(), nil, req, &pilosa.ImportOptions{})
	if err != nil {
		t.Fatalf("importing data: %v", err)
	}
	// Rebuild the RankCache.
	// We have to do this to avoid the 10-second cache invalidation delay
	// built into cache.Invalidate()
	err = c.GetNode(0).RecalculateCaches(t)
	if err != nil {
		t.Fatalf("recalculating cache: %v", err)
	}
	err = c.GetNode(1).RecalculateCaches(t)
	if err != nil {
		t.Fatalf("recalculating cache: %v", err)
	}
	err = c.GetNode(2).RecalculateCaches(t)
	if err != nil {
		t.Fatalf("recalculating cache: %v", err)
	}

	topN := 4
	queryRequest := &pilosa.QueryRequest{
		Query:  fmt.Sprintf(`TopN(f, n=%d)`, topN),
		Remote: false,
	}

	result, err := client[0].Query(context.Background(), c.Idx(), queryRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Test must return exactly N results.
	pairsField := result.Results[0].(*pilosa.PairsField)
	if len(pairsField.Pairs) != topN {
		t.Fatalf("unexpected number of TopN results: %s", spew.Sdump(result))
	}
	p := []pilosa.Pair{
		{ID: 100, Count: 12},
		{ID: 22, Count: 10},
		{ID: 98, Count: 8},
		{ID: 99, Count: 7}}

	// Valdidate the Top 4 result counts.
	if !reflect.DeepEqual(pairsField.Pairs, p) {
		t.Fatalf("Invalid TopN result set: %s", spew.Sdump(result))
	}

	result1, err := client[1].Query(context.Background(), c.Idx(), queryRequest)
	if err != nil {
		t.Fatal(err)
	}
	result2, err := client[2].Query(context.Background(), c.Idx(), queryRequest)
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
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	keyed := cluster.Idx("k")
	unkeyed := cluster.Idx("u")

	host := cmd.URL()

	cmd.MustCreateIndex(t, keyed, pilosa.IndexOptions{Keys: true})
	cmd.MustCreateIndex(t, unkeyed, pilosa.IndexOptions{Keys: false})

	cmd.MustCreateField(t, keyed, "keyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())
	cmd.MustCreateField(t, keyed, "unkeyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000))
	cmd.MustCreateField(t, unkeyed, "keyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())
	cmd.MustCreateField(t, unkeyed, "unkeyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000))

	c := MustNewClient(host, pilosa.GetHTTPClient(nil))
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
			_, err := c.Query(context.Background(), unkeyed, &pilosa.QueryRequest{
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
		if err := c.ExportCSV(context.Background(), unkeyed, "unkeyedf", 0, bw); err != nil {
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
			_, err := c.Query(context.Background(), unkeyed, &pilosa.QueryRequest{
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
		if err := c.ExportCSV(context.Background(), unkeyed, "keyedf", 0, bw); err != nil {
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
			_, err := c.Query(context.Background(), keyed, &pilosa.QueryRequest{
				Query:  fmt.Sprintf(`Set("%s", unkeyedf=%d)`, bit.ColumnKey, bit.RowID),
				Remote: false,
			})
			if err != nil {
				t.Fatal(err)
			}
		}

		buf := bytes.NewBuffer(nil)
		bw := bufio.NewWriter(buf)

		// Send export request for every partition.
		for i := 0; i < disco.DefaultPartitionN; i++ {
			if err := c.ExportCSV(context.Background(), keyed, "unkeyedf", uint64(i), bw); err != nil {
				t.Fatal(err)
			}
		}

		got := buf.String()
		gotSlice := strings.Split(got, "\n")

		// Expected output is not sorted because of key sharding.
		expSlice := []string{
			"1,col103",
			"1,col102",
			"1,col101",
			"1,col100",
			"2,col200",
			"2,col201",
			"2,col202",
			"2,col203",
			"",
		}
		if !sameStringSlice(gotSlice, expSlice) {
			t.Fatalf("unexpected results: %q", gotSlice)
		}
	})

	t.Run("Export keyed,keyedf", func(t *testing.T) {
		// Populate data.
		for _, bit := range data {
			_, err := c.Query(context.Background(), keyed, &pilosa.QueryRequest{
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
		for i := 0; i < disco.DefaultPartitionN; i++ {
			if err := c.ExportCSV(context.Background(), keyed, "keyedf", uint64(i), bw); err != nil {
				t.Fatal(err)
			}
		}

		got := buf.String()
		gotSlice := strings.Split(got, "\n")

		// Expected output is not sorted because of key sharding.
		expSlice := []string{
			"row1,col103",
			"row1,col102",
			"row1,col101",
			"row1,col100",
			"row2,col200",
			"row2,col201",
			"row2,col202",
			"row2,col203",
			"",
		}
		if !sameStringSlice(gotSlice, expSlice) {
			t.Fatalf("unexpected results: %q", gotSlice)
		}
	})
}

// Ensure client can bulk import data.
func TestClient_Import(t *testing.T) {
	// Need a cluster to verify hitting multiple nodes
	cluster := test.MustRunCluster(t, 3)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	host := cmd.URL()
	api := cmd.API
	keyed := cluster.Idx("k")
	unkeyed := cluster.Idx("u")

	cmd.MustCreateIndex(t, keyed, pilosa.IndexOptions{Keys: true})
	cmd.MustCreateIndex(t, unkeyed, pilosa.IndexOptions{Keys: false})

	cmd.MustCreateField(t, keyed, "keyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeNone, 0), pilosa.OptFieldKeys())
	cmd.MustCreateField(t, keyed, "unkeyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeNone, 0))
	cmd.MustCreateField(t, unkeyed, "keyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeNone, 0), pilosa.OptFieldKeys())
	cmd.MustCreateField(t, unkeyed, "unkeyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeNone, 0))

	indexes := map[bool]string{true: keyed, false: unkeyed}
	fields := map[bool]string{true: "keyedf", false: "unkeyedf"}

	recKeys := []string{"rec-a", "rec-b", "rec-c"}
	valueKeys := []string{"val-a", "val-b", "val-c"}
	recIDs := []uint64{0, 3, 7}
	valueIDs := []uint64{0, 3, 7}

	c := MustNewClient(host, pilosa.GetHTTPClient(nil))
	// set API to point at the local node
	c.SetInternalAPI(cmd.API)

	checkResults := func(results pilosa.QueryResponse, keyed bool, maxN int) {
		for i, r := range results.Results {
			row, ok := r.(*pilosa.Row)
			if !ok {
				t.Fatalf("expected row, got %T", r)
			}
			if i >= maxN {
				// we cleared these, so they should be empty
				if keyed {
					vals := row.Keys
					if len(vals) != 0 {
						t.Fatalf("expected empty row, got %d result(s) back, first result %q",
							len(vals), vals[0])
					}
				} else {
					vals := row.Columns()
					if len(vals) != 0 {
						t.Fatalf("expected empty row, got %d result(s) back, first result %d",
							len(vals), vals[0])
					}
				}
				return
			}
			if keyed {
				vals := row.Keys
				if len(vals) != 1 {
					t.Fatalf("expected one result, didn't get it")
				}
				if vals[0] != recKeys[i] {
					t.Fatalf("expected %q, got %q", recKeys[i], vals[0])
				}
			} else {
				vals := row.Columns()
				if len(vals) != 1 {
					t.Fatalf("expected one result, didn't get it")
				}
				if vals[0] != recIDs[i] {
					t.Fatalf("expected %d, got %d", recIDs[i], vals[0])
				}
			}
		}
	}

	for useKeys, indexName := range indexes {
		for _, fieldName := range fields {
			req := pilosa.ImportRequest{
				Index: indexName,
				Field: fieldName,
			}
			if indexName == keyed {
				req.ColumnKeys = recKeys
				req.Shard = ^uint64(0)
			} else {
				req.ColumnIDs = recIDs
				req.Shard = 0
			}
			if fieldName == "keyedf" {
				req.RowKeys = valueKeys
			} else {
				req.RowIDs = valueIDs
			}
			// clone request because some imports will modify their input parameters
			if err := c.Import(context.Background(), nil, req.Clone(), &pilosa.ImportOptions{}); err != nil {
				t.Fatalf("%s/%s: %v",
					indexName, fieldName, err)
			}

			// Now do a query to see whether it worked...
			var pql string
			if fieldName == "keyedf" {
				pql = `Row(keyedf="val-a") Row(keyedf="val-b") Row(keyedf="val-c")`
			} else {
				pql = `Row(unkeyedf=0) Row(unkeyedf=3) Row(unkeyedf=7)`
			}
			results, err := api.Query(context.Background(), &pilosa.QueryRequest{Index: indexName, Query: pql})
			if err != nil {
				t.Fatal(err)
			}
			checkResults(results, useKeys, 3)

			// Now clear a bit...
			req = pilosa.ImportRequest{
				Index: indexName,
				Field: fieldName,
			}
			if indexName == keyed {
				req.ColumnKeys = recKeys[2:]
				req.Shard = ^uint64(0)
			} else {
				req.ColumnIDs = recIDs[2:]
				req.Shard = 0
			}
			if fieldName == "keyedf" {
				req.RowKeys = valueKeys[2:]
			} else {
				req.RowIDs = valueIDs[2:]
			}
			// do a clear. also, do the clear with a Qcx.
			func() {
				// inner function so the deferred abort isn't delayed a lot
				qcx := api.Txf().NewQcx()
				defer qcx.Abort()
				if err := c.Import(context.Background(), qcx, req.Clone(), &pilosa.ImportOptions{Clear: true}); err != nil {
					t.Fatalf("%s/%s: %v",
						indexName, fieldName, err)
				}
				if err := qcx.Finish(); err != nil {
					t.Fatalf("committing write: %v", err)
				}
			}()
			// Now do a query to see whether it worked...
			if fieldName == "keyedf" {
				pql = `Row(keyedf="val-a") Row(keyedf="val-b") Row(keyedf="val-c")`
			} else {
				pql = `Row(unkeyedf=0) Row(unkeyedf=3) Row(unkeyedf=7)`
			}
			results, err = api.Query(context.Background(), &pilosa.QueryRequest{Index: indexName, Query: pql})
			if err != nil {
				t.Fatal(err)
			}
			checkResults(results, useKeys, 2)
		}
	}
}

// Ensure client can bulk import data.
func TestClient_ImportRoaring(t *testing.T) {
	cluster := test.MustUnsharedCluster(t, 3)
	// Unshared because we want to set ReplicaN = 3 so we can verify data present on all nodes
	for _, c := range cluster.Nodes {
		c.Config.Cluster.ReplicaN = 3
	}
	err := cluster.Start()
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	defer cluster.Close()

	_, err = cluster.GetNode(0).API.CreateIndex(context.Background(), cluster.Idx(), pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = cluster.GetNode(0).API.CreateField(context.Background(), cluster.Idx(), "f", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 100))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	_, err = cluster.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: cluster.Idx(), Query: "Set(0, f=1)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	// Send import request.
	host := cluster.GetNode(0).URL()
	c := MustNewClient(host, pilosa.GetHTTPClient(nil))
	// [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537]
	roaringReq := makeImportRoaringRequest(false, "3B3001000100000900010000000100010009000100")
	if err := c.ImportRoaring(context.Background(), &cluster.GetNode(0).API.Node().URI, cluster.Idx(), "f", 0, false, roaringReq); err != nil {
		t.Fatal(err)
	}

	hldr := test.Holder{Holder: cluster.GetNode(0).Server.Holder()}
	// Verify data on node 0.
	if a := hldr.Row(cluster.Idx(), "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537}) {
		t.Fatalf("unexpected columns: %+v", a)
	}
	if a := hldr.Row(cluster.Idx(), "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected columns: %+v", a)
	}

	hldr2 := test.Holder{Holder: cluster.GetNode(1).Server.Holder()}
	// Verify data on node 1.
	if a := hldr2.Row(cluster.Idx(), "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537}) {
		t.Fatalf("unexpected columns: %+v", a)
	}
	if a := hldr2.Row(cluster.Idx(), "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected columns: %+v", a)
	}

	// Ensure that sending a roaring import with the clear flag works as expected.
	// [65539, 65540]
	roaringReq = makeImportRoaringRequest(true, "3A30000001000000010001001000000003000400")
	if err := c.ImportRoaring(context.Background(), &cluster.GetNode(0).API.Node().URI, cluster.Idx(), "f", 0, false, roaringReq); err != nil {
		t.Fatal(err)
	}

	// Verify data on node 0.
	if a := hldr.Row(cluster.Idx(), "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr.Row(cluster.Idx(), "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}

	// Verify data on node 1.
	if a := hldr2.Row(cluster.Idx(), "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr2.Row(cluster.Idx(), "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}

	// Ensure that sending a roaring import with the clear flag works as expected.
	// [4, 6, 65537, 65539]
	roaringReq = makeImportRoaringRequest(true, "3A300000020000000000010001000100180000001C0000000400060001000300")
	if err := c.ImportRoaring(context.Background(), &cluster.GetNode(0).API.Node().URI, cluster.Idx(), "f", 0, false, roaringReq); err != nil {
		t.Fatal(err)
	}

	// Verify data on node 0.
	if a := hldr.Row(cluster.Idx(), "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 5, 7, 8, 9, 10}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr.Row(cluster.Idx(), "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}

	// Verify data on node 1.
	if a := hldr2.Row(cluster.Idx(), "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3, 5, 7, 8, 9, 10}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr2.Row(cluster.Idx(), "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}

	// Ensure that sending a roaring import with the clear flag works as expected.
	// [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537]
	roaringReq = makeImportRoaringRequest(true, "3B3001000100000900010000000100010009000100")
	if err := c.ImportRoaring(context.Background(), &cluster.GetNode(0).API.Node().URI, cluster.Idx(), "f", 0, false, roaringReq); err != nil {
		t.Fatal(err)
	}

	// Verify data on node 0.
	if a := hldr.Row(cluster.Idx(), "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr.Row(cluster.Idx(), "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}

	// Verify data on node 1.
	if a := hldr2.Row(cluster.Idx(), "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
	if a := hldr2.Row(cluster.Idx(), "f", 1).Columns(); !reflect.DeepEqual(a, []uint64{0}) {
		t.Fatalf("unexpected clear columns: %+v", a)
	}
}

// Ensure client can bulk import data with multiple views and not deadlock.
func TestClient_ImportRoaring_MultiView(t *testing.T) {
	cluster := test.MustUnsharedCluster(t, 2)
	for _, c := range cluster.Nodes {
		c.Config.Cluster.ReplicaN = 2
	}
	err := cluster.Start()
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	defer cluster.Close()

	api := cluster.GetNode(0).API

	_, err = api.CreateIndex(context.Background(), cluster.Idx(), pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = api.CreateField(context.Background(), cluster.Idx(), "f", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 100))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	_, err = api.Query(context.Background(), &pilosa.QueryRequest{Index: cluster.Idx(), Query: "Set(0, f=1)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	// Send import request.
	host := cluster.GetNode(0).URL()
	c := MustNewClient(host, pilosa.GetHTTPClient(nil))
	req := &pilosa.ImportRoaringRequest{Views: map[string][]byte{}}
	req.Views["a"], _ = hex.DecodeString("3B3001000100000900010000000100010009000100")
	req.Views["b"], _ = hex.DecodeString("3B3001000100000900010000000100010009000100")
	if err := c.ImportRoaring(context.Background(), &cluster.GetNode(0).API.Node().URI, cluster.Idx(), "f", 0, false, req); err != nil {
		t.Fatal(err)
	}
}

// Ensure client can bulk import data.
func TestClient_ImportKeys(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster.GetNode(0)
		host := cmd.URL()
		keyed := cluster.Idx("k")
		unkeyed := cluster.Idx("u")

		cmd.MustCreateIndex(t, keyed, pilosa.IndexOptions{Keys: true})
		cmd.MustCreateIndex(t, unkeyed, pilosa.IndexOptions{Keys: false})

		cmd.MustCreateField(t, keyed, "keyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())
		cmd.MustCreateField(t, keyed, "unkeyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000))
		cmd.MustCreateField(t, unkeyed, "keyedf", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())

		// Send import request.
		c := MustNewClient(host, pilosa.GetHTTPClient(nil))
		baseReq := &pilosa.ImportRequest{
			Index:      keyed,
			Field:      "keyedf",
			ColumnKeys: []string{"eve", "alice", "bob", "eve", "alice", "eve"},
			ColumnIDs:  []uint64{1, 2, 3, 1, 2, 1},
			RowKeys:    []string{"green", "green", "green", "blue", "blue", "purple"},
			RowIDs:     []uint64{1, 1, 1, 2, 2, 3},
		}

		t.Run("Import keyed,keyed", func(t *testing.T) {
			req := baseReq.Clone()
			req.Index = keyed
			req.Field = "keyedf"
			req.ColumnIDs, req.RowIDs = nil, nil
			if err := c.Import(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
				t.Fatal(err)
			}
			resp := cmd.QueryAPI(t, &pilosa.QueryRequest{
				Index: keyed,
				Query: "TopN(keyedf)",
			})
			if pairs, ok := resp.Results[0].(*pilosa.PairsField); !ok {
				t.Fatalf("unexpected response type %T", resp.Results[0])
			} else if !reflect.DeepEqual(pairs.Pairs, []pilosa.Pair{
				{Key: "green", Count: 3},
				{Key: "blue", Count: 2},
				{Key: "purple", Count: 1},
			}) {
				t.Fatalf("unexpected topn result: %v", pairs.Pairs)
			}
		})

		t.Run("Import keyed,unkeyedf", func(t *testing.T) {
			req := baseReq.Clone()
			req.Index = keyed
			req.Field = "unkeyedf"
			req.ColumnIDs, req.RowKeys = nil, nil
			if err := c.Import(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
				t.Fatal(err)
			}
			resp := cmd.QueryAPI(t, &pilosa.QueryRequest{
				Index: keyed,
				Query: "TopN(unkeyedf)",
			})
			if pairs, ok := resp.Results[0].(*pilosa.PairsField); !ok {
				t.Fatalf("unexpected response type %T", resp.Results[0])
			} else if !reflect.DeepEqual(pairs.Pairs, []pilosa.Pair{
				{ID: 1, Count: 3},
				{ID: 2, Count: 2},
				{ID: 3, Count: 1},
			}) {
				t.Fatalf("unexpected topn result: %v", pairs.Pairs)
			}
		})

		t.Run("Import unkeyed,keyed", func(t *testing.T) {
			req := baseReq.Clone()
			req.Index = unkeyed
			req.Field = "keyedf"
			req.ColumnKeys, req.RowIDs = nil, nil
			if err := c.Import(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
				t.Fatal(err)
			}
			resp := cmd.QueryAPI(t, &pilosa.QueryRequest{
				Index: unkeyed,
				Query: "TopN(keyedf)",
			})
			if pairs, ok := resp.Results[0].(*pilosa.PairsField); !ok {
				t.Fatalf("unexpected response type %T", resp.Results[0])
			} else if !reflect.DeepEqual(pairs.Pairs, []pilosa.Pair{
				{Key: "green", Count: 3},
				{Key: "blue", Count: 2},
				{Key: "purple", Count: 1},
			}) {
				t.Fatalf("unexpected topn result: %v", pairs.Pairs)
			}
		})
	})

	t.Run("MultiNode", func(t *testing.T) {
		cluster := test.MustRunCluster(t, 3)
		defer cluster.Close()
		cmd0 := cluster.GetNode(0)
		cmd1 := cluster.GetNode(1)
		host0 := cmd0.URL()
		host1 := cmd1.URL()
		keyed := cluster.Idx("k")

		cmd0.MustCreateIndex(t, keyed, pilosa.IndexOptions{Keys: true})
		cmd0.MustCreateField(t, keyed, "keyedf0", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())
		cmd0.MustCreateField(t, keyed, "keyedf1", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000), pilosa.OptFieldKeys())

		// Send import request.
		c0 := MustNewClient(host0, pilosa.GetHTTPClient(nil))
		c1 := MustNewClient(host1, pilosa.GetHTTPClient(nil))

		// Import to node0.
		t.Run("Import node0", func(t *testing.T) {
			req := &pilosa.ImportRequest{
				Index:      keyed,
				Field:      "keyedf0",
				ColumnKeys: []string{"eve", "alice", "bob", "eve", "alice", "eve"},
				RowKeys:    []string{"green", "green", "green", "blue", "blue", "purple"},
			}
			if err := c0.Import(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
				t.Fatal(err)
			}
			resp := cmd0.QueryAPI(t, &pilosa.QueryRequest{
				Index: keyed,
				Query: "TopN(keyedf0)",
			})
			if pairs, ok := resp.Results[0].(*pilosa.PairsField); !ok {
				t.Fatalf("unexpected response type %T", resp.Results[0])
			} else if !reflect.DeepEqual(pairs.Pairs, []pilosa.Pair{
				{Key: "green", Count: 3},
				{Key: "blue", Count: 2},
				{Key: "purple", Count: 1},
			}) {
				t.Fatalf("unexpected topn result: %v", pairs.Pairs)
			}
		})

		// Import to node1 (ensure import is routed to primary for translation).
		t.Run("Import node1", func(t *testing.T) {
			req := &pilosa.ImportRequest{
				Index:      keyed,
				Field:      "keyedf1",
				ColumnKeys: []string{"eve", "alice", "bob", "eve", "alice", "eve"},
				RowKeys:    []string{"green", "green", "green", "blue", "blue", "purple"},
			}
			if err := c1.Import(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
				t.Fatal(err)
			}

			// Wait for translation replication.
			time.Sleep(500 * time.Millisecond)

			resp := cmd1.QueryAPI(t, &pilosa.QueryRequest{
				Index: keyed,
				Query: "TopN(keyedf1)",
			})
			if pairs, ok := resp.Results[0].(*pilosa.PairsField); !ok {
				t.Fatalf("unexpected response type %T", resp.Results[0])
			} else if !reflect.DeepEqual(pairs.Pairs, []pilosa.Pair{
				{Key: "green", Count: 3},
				{Key: "blue", Count: 2},
				{Key: "purple", Count: 1},
			}) {
				t.Fatalf("unexpected topn result: %#v", pairs.Pairs)
			}
		})
	})

	t.Run("IntegerFieldSingleNode", func(t *testing.T) {
		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster.GetNode(0)
		host := cmd.URL()
		holder := cmd.Server.Holder()
		hldr := test.Holder{Holder: holder}

		fldName := "f"

		// Load bitmap into cache to ensure cache gets updated.
		index := hldr.MustCreateIndexIfNotExists(cluster.Idx(), pilosa.IndexOptions{Keys: true})
		_, err := index.CreateFieldIfNotExists(fldName, pilosa.OptFieldTypeInt(-100, 100))
		if err != nil {
			t.Fatal(err)
		}

		// Send import request.
		c := MustNewClient(host, pilosa.GetHTTPClient(nil))
		req := &pilosa.ImportValueRequest{
			Index:      cluster.Idx(),
			Field:      "f",
			ColumnKeys: []string{"col1", "col2", "col3"},
			Values:     []int64{-10, 20, 40},
		}
		if err := c.ImportValue(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
			t.Fatal(err)
		}

		// Verify range.
		queryRequest := &pilosa.QueryRequest{
			Query:  fmt.Sprintf(`Row(%s>10)`, fldName),
			Remote: false,
		}

		result, err := c.Query(context.Background(), cluster.Idx(), queryRequest)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result.Results[0].(*pilosa.Row).Keys, []string{"col2", "col3"}) {
			t.Fatalf("unexpected column keys: %s", spew.Sdump(result))
		}

		// Clear data.
		req = &pilosa.ImportValueRequest{
			Index:      cluster.Idx(),
			Field:      "f",
			ColumnKeys: []string{"col2"},
			Values:     []int64{20},
		}
		if err := c.ImportValue(context.Background(), nil, req, &pilosa.ImportOptions{Clear: true}); err != nil {
			t.Fatal(err)
		}

		// Verify Range.
		queryRequest = &pilosa.QueryRequest{
			Query:  fmt.Sprintf(`Row(%s>10)`, fldName),
			Remote: false,
		}

		result, err = c.Query(context.Background(), cluster.Idx(), queryRequest)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(result.Results[0].(*pilosa.Row).Keys, []string{"col3"}) {
			t.Fatalf("unexpected column keys: %s", spew.Sdump(result))
		}
	})
}

func TestClient_ImportIDs(t *testing.T) {
	// Ensure that running a query between two imports does
	// not affect the result set. It turns out, this is caused
	// by the fragment.rowCache failing to be cleared after an
	// importValue. This ensures that the rowCache is cleared
	// after an import.
	t.Run("ImportRangeImport", func(t *testing.T) {
		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster.GetNode(0)
		host := cmd.URL()
		holder := cmd.Server.Holder()
		hldr := test.Holder{Holder: holder}

		idxName := cluster.Idx()
		fldName := "f"

		// Load bitmap into cache to ensure cache gets updated.
		index := hldr.MustCreateIndexIfNotExists(idxName, pilosa.IndexOptions{Keys: false})
		_, err := index.CreateFieldIfNotExists(fldName, pilosa.OptFieldTypeInt(-10000, 10000))
		if err != nil {
			t.Fatal(err)
		}

		// Send import request.
		c := MustNewClient(host, pilosa.GetHTTPClient(nil))
		req := &pilosa.ImportValueRequest{
			Index:     idxName,
			Field:     fldName,
			ColumnIDs: []uint64{2},
			Values:    []int64{1},
		}
		if err := c.ImportValue(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
			t.Fatal(err)
		}

		// Verify range.
		queryRequest := &pilosa.QueryRequest{
			Query:  fmt.Sprintf(`Row(%s>0)`, fldName),
			Remote: false,
		}

		if result, err := c.Query(context.Background(), idxName, queryRequest); err != nil {
			t.Fatal(err)
		} else {
			res := result.Results[0].(*pilosa.Row).Columns()
			if !reflect.DeepEqual(res, []uint64{2}) {
				t.Fatalf("unexpected column ids: %v", res)
			}
		}

		// Send import request.
		req = &pilosa.ImportValueRequest{
			Index:     idxName,
			Field:     fldName,
			ColumnIDs: []uint64{1000},
			Values:    []int64{1},
		}
		if err := c.ImportValue(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
			t.Fatal(err)
		}

		// Verify range.
		if result, err := c.Query(context.Background(), idxName, queryRequest); err != nil {
			t.Fatal(err)
		} else {
			res := result.Results[0].(*pilosa.Row).Columns()
			if !reflect.DeepEqual(res, []uint64{2, 1000}) {
				t.Fatalf("unexpected column ids: %v", res)
			}
		}
	})
}

// Ensure client can bulk import value data.
func TestClient_ImportValue(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	host := cmd.URL()
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	fldName := "f"

	// Load bitmap into cache to ensure cache gets updated.
	index := hldr.MustCreateIndexIfNotExists(cluster.Idx(), pilosa.IndexOptions{})
	_, err := index.CreateFieldIfNotExists(fldName, pilosa.OptFieldTypeInt(-100, 100))
	if err != nil {
		t.Fatal(err)
	}

	// Send import request.
	c := MustNewClient(host, pilosa.GetHTTPClient(nil))
	req := &pilosa.ImportValueRequest{
		Index:     cluster.Idx(),
		Field:     "f",
		ColumnIDs: []uint64{1, 2, 3},
		Values:    []int64{-10, 20, 40},
	}
	if err := c.ImportValue(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
		t.Fatal(err)
	}

	// Verify Sum.
	if resp, err := c.Query(context.Background(), cluster.Idx(), &pilosa.QueryRequest{Query: `Sum(field=f)`}); err != nil {
		t.Fatal(err)
	} else if vc, ok := resp.Results[0].(pilosa.ValCount); !ok {
		t.Fatalf("expected ValCount; got %T", resp.Results[0])
	} else if vc.Val != 50 || vc.Count != 3 {
		t.Fatalf("unexpected values: got sum=%v, count=%v; expected sum=50, cnt=3", vc.Val, vc.Count)
	}

	// Verify Max.
	if resp, err := c.Query(context.Background(), cluster.Idx(), &pilosa.QueryRequest{Query: `Max(field=f)`}); err != nil {
		t.Fatal(err)
	} else if vc, ok := resp.Results[0].(pilosa.ValCount); !ok {
		t.Fatalf("expected ValCount; got %T", resp.Results[0])
	} else if vc.Val != 40 || vc.Count != 1 {
		t.Fatalf("unexpected values: got max=%v, count=%v; expected max=40, cnt=1", vc.Val, vc.Count)
	}

	// Calculate Data Usage before Import
	preDUsage, err := c.GetDiskUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	preIUsage, err := c.GetIndexUsage(context.Background(), cluster.Idx())
	if err != nil {
		t.Fatal(err)
	}

	// Send import request.
	req = &pilosa.ImportValueRequest{
		Index:     cluster.Idx(),
		Field:     "f",
		ColumnIDs: []uint64{1, 3},
		Values:    []int64{-10, 40},
	}
	if err := c.ImportValue(context.Background(), nil, req, &pilosa.ImportOptions{Clear: true}); err != nil {
		t.Fatal(err)
	}

	// Check Equivalent growth in data directory and index
	postDUsage, err := c.GetDiskUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	postIUsage, err := c.GetIndexUsage(context.Background(), cluster.Idx())
	if err != nil {
		t.Fatal(err)
	}

	freeSpace := du.NewDiskUsage("/dev/disk1s5").Free()
	vprint.VV("freespace: %+v", freeSpace)

	if (postDUsage.Usage - preDUsage.Usage) != (postIUsage.Usage - preIUsage.Usage) {
		t.Errorf("expected size of data directory to grow the same amount as size of index: Before Import: disk usage: %v, index usage: %v, After Import: disk usage: %v, index usage: %v", preDUsage.Usage, preIUsage.Usage, postDUsage.Usage, preIUsage.Usage)
		return
	}
	if postDUsage.Usage <= postIUsage.Usage {
		t.Errorf("expected disk usage to be greater than index usage")
		return
	}

	// Verify Sum.
	if resp, err := c.Query(context.Background(), cluster.Idx(), &pilosa.QueryRequest{Query: `Sum(field=f)`}); err != nil {
		t.Fatal(err)
	} else if vc, ok := resp.Results[0].(pilosa.ValCount); !ok {
		t.Fatalf("expected ValCount; got %T", resp.Results[0])
	} else if vc.Val != 20 || vc.Count != 1 {
		t.Fatalf("unexpected values: got sum=%v, count=%v; expected sum=20, cnt=1", vc.Val, vc.Count)
	}

	// Verify Max.
	if resp, err := c.Query(context.Background(), cluster.Idx(), &pilosa.QueryRequest{Query: `Max(field=f)`}); err != nil {
		t.Fatal(err)
	} else if vc, ok := resp.Results[0].(pilosa.ValCount); !ok {
		t.Fatalf("expected ValCount; got %T", resp.Results[0])
	} else if vc.Val != 20 || vc.Count != 1 {
		t.Fatalf("unexpected values: got max=%v, count=%v; expected max=20, cnt=1", vc.Val, vc.Count)
	}
}

// Ensure client can bulk import data while tracking existence.
func TestClient_ImportExistence(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	host := cmd.URL()
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	t.Run("Set", func(t *testing.T) {
		idxName := cluster.Idx("s")
		fldName := "fset"

		index := hldr.MustCreateIndexIfNotExists(idxName, pilosa.IndexOptions{TrackExistence: true})
		_, err := index.CreateFieldIfNotExists(fldName)
		if err != nil {
			t.Fatal(err)
		}

		// Send import request.
		c := MustNewClient(host, pilosa.GetHTTPClient(nil))
		req := &pilosa.ImportRequest{
			Index:     cluster.Idx("s"),
			Field:     "fset",
			ColumnIDs: []uint64{1, 5, 6},
			RowIDs:    []uint64{0, 0, 200},
		}
		if err := c.Import(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
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
		if a := hldr.ReadRow(idxName, "_exists", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 5, 6}) {
			t.Fatalf("unexpected existence columns: %+v", a)
		}
	})

	t.Run("Int", func(t *testing.T) {
		idxName := cluster.Idx("i")
		fldName := "fint"

		index := hldr.MustCreateIndexIfNotExists(idxName, pilosa.IndexOptions{TrackExistence: true})
		_, err := index.CreateFieldIfNotExists(fldName, pilosa.OptFieldTypeInt(-100, 100))
		if err != nil {
			t.Fatal(err)
		}

		// Send import request.
		c := MustNewClient(host, pilosa.GetHTTPClient(nil))
		req := &pilosa.ImportValueRequest{
			Index:     cluster.Idx("i"),
			Field:     "fint",
			ColumnIDs: []uint64{1, 2, 3},
			Values:    []int64{-10, 20, 40},
		}
		if err := c.ImportValue(context.Background(), nil, req, &pilosa.ImportOptions{}); err != nil {
			t.Fatal(err)
		}

		// Verify Sum.
		if resp, err := c.Query(context.Background(), idxName, &pilosa.QueryRequest{Query: fmt.Sprintf(`Sum(field=%s)`, fldName)}); err != nil {
			t.Fatal(err)
		} else if vc, ok := resp.Results[0].(pilosa.ValCount); !ok {
			t.Fatalf("expected ValCount; got %T", resp.Results[0])
		} else if vc.Val != 50 || vc.Count != 3 {
			t.Fatalf("unexpected values: got sum=%v, count=%v; expected sum=50, cnt=3", vc.Val, vc.Count)
		}

		// Verify existence.
		if a := hldr.ReadRow(idxName, "_exists", 0).Columns(); !reflect.DeepEqual(a, []uint64{1, 2, 3}) {
			t.Fatalf("unexpected existence columns: %+v", a)
		}
	})
}

// Ensure client can retrieve a list of all checksums for blocks in a fragment.
func TestClient_FragmentBlocks(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	hldr.SetBit(cluster.Idx(), "f", 0, 1)
	hldr.SetBit(cluster.Idx(), "f", pilosa.HashBlockSize*3, 100)

	// Set a bit on a different shard.
	hldr.SetBit(cluster.Idx(), "f", 0, 1)
	c := MustNewClient(cmd.URL(), pilosa.GetHTTPClient(nil))
	blocks, err := c.FragmentBlocks(context.Background(), nil, cluster.Idx(), "f", "standard", 0)
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
	if a, err := cmd.API.FragmentBlocks(context.Background(), cluster.Idx(), "f", "standard", 0); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(a, blocks) {
		t.Fatalf("blocks mismatch:\n\nexp=%s\n\ngot=%s\n\n", spew.Sdump(a), spew.Sdump(blocks))
	}
}

// Try to request translation data which won't exist.
func TestClient_IndexTranslateDataReader(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	hldr.SetBit("i", "f", 0, 1)
	c := MustNewClient(cmd.URL(), pilosa.GetHTTPClient(nil))
	reader, err := c.IndexTranslateDataReader(context.Background(), "i", 0)
	if reader != nil {
		reader.Close()
	}
	if err == nil {
		t.Fatalf("expected to get an error reading translation data that shouldn't exist")
	}
}

func TestClient_CreateTimeField(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	c := MustNewClient(cmd.URL(), pilosa.GetHTTPClient(nil))

	index := cluster.Idx()
	err := c.CreateIndex(context.Background(), index, pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}

	field := "field"
	err = c.CreateFieldWithOptions(context.Background(), index, field, pilosa.FieldOptions{Type: pilosa.FieldTypeTime, TimeQuantum: "YMDH"})
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	fld, err := cmd.API.Field(context.Background(), index, field)
	if err != nil {
		t.Fatalf("getting field: %v", err)
	}
	if fld.TTL() != 0 {
		t.Fatalf("expected TTL to be 0, got: %+v", fld.Options().TTL.String())
	}

	fieldTTL := "field_ttl"
	err = c.CreateFieldWithOptions(context.Background(), index, fieldTTL, pilosa.FieldOptions{Type: pilosa.FieldTypeTime, TimeQuantum: "YMDH", TTL: time.Hour})
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	fldTTL, err := cmd.API.Field(context.Background(), index, fieldTTL)
	if err != nil {
		t.Fatalf("getting field: %v", err)
	}
	if fldTTL.TTL() != time.Hour {
		t.Fatalf("expected TTL 1 hour, got: %+v", fldTTL.TTL().String())
	}
}

func TestClient_CreateDecimalField(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)

	c := MustNewClient(cmd.URL(), pilosa.GetHTTPClient(nil))

	index := cluster.Idx()
	err := c.CreateIndex(context.Background(), index, pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	field := "dfield"
	err = c.CreateFieldWithOptions(context.Background(), index, field, pilosa.FieldOptions{Type: pilosa.FieldTypeDecimal, Scale: 1, Min: pql.NewDecimal(-1000, 0), Max: pql.NewDecimal(1000, 0)})
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}

	fld, err := cmd.API.Field(context.Background(), index, field)
	if err != nil {
		t.Fatalf("getting field: %v", err)
	}
	if fld.Options().Scale != 1 {
		t.Fatalf("expected Scale 1, got: %+v", fld.Options())
	}

	err = c.ImportValue(context.Background(), nil, &pilosa.ImportValueRequest{Index: index, Field: field, ColumnIDs: []uint64{1, 2, 3}, Shard: 0, FloatValues: []float64{1.1, 2.2, 3.3}}, &pilosa.ImportOptions{})
	if err != nil {
		t.Fatalf("importing float values: %v", err)
	}

	// Integer predicate.
	resp, err := c.Query(context.Background(), index, &pilosa.QueryRequest{Index: index, Query: "Row(dfield>2)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Columns(), []uint64{2, 3}) {
		t.Fatalf("unexpected results: %v", resp.Results[0].(*pilosa.Row).Columns())
	}

	// Float predicate.
	resp, err = c.Query(context.Background(), index, &pilosa.QueryRequest{Index: index, Query: "Row(dfield>2.1)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Columns(), []uint64{2, 3}) {
		t.Fatalf("unexpected results: %v", resp.Results[0].(*pilosa.Row).Columns())
	}

	// Integer predicates.
	resp, err = c.Query(context.Background(), index, &pilosa.QueryRequest{Index: index, Query: "Row(1<dfield<3)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Columns(), []uint64{1, 2}) {
		t.Fatalf("unexpected results: %v", resp.Results[0].(*pilosa.Row).Columns())
	}

	// Float predicates.
	resp, err = c.Query(context.Background(), index, &pilosa.QueryRequest{Index: index, Query: "Row(1.1<dfield<3.3)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Columns(), []uint64{2}) {
		t.Fatalf("unexpected results: %v", resp.Results[0].(*pilosa.Row).Columns())
	}

	resp, err = c.Query(context.Background(), index, &pilosa.QueryRequest{Index: index, Query: "Row(1.1<=dfield<3.3)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Columns(), []uint64{1, 2}) {
		t.Fatalf("unexpected results: %v", resp.Results[0].(*pilosa.Row).Columns())
	}

	resp, err = c.Query(context.Background(), index, &pilosa.QueryRequest{Index: index, Query: "Row(1.1<dfield<=3.3)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Columns(), []uint64{2, 3}) {
		t.Fatalf("unexpected results: %v", resp.Results[0].(*pilosa.Row).Columns())
	}

	resp, err = c.Query(context.Background(), index, &pilosa.QueryRequest{Index: index, Query: "Row(dfield<3.3)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Columns(), []uint64{1, 2}) {
		t.Fatalf("unexpected results: %v", resp.Results[0].(*pilosa.Row).Columns())
	}

	resp, err = c.Query(context.Background(), index, &pilosa.QueryRequest{Index: index, Query: "Row(dfield>2.2)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Columns(), []uint64{3}) {
		t.Fatalf("unexpected results: %v", resp.Results[0].(*pilosa.Row).Columns())
	}

	resp, err = c.Query(context.Background(), index, &pilosa.QueryRequest{Index: index, Query: "Row(dfield>=2.2)"})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Columns(), []uint64{2, 3}) {
		t.Fatalf("unexpected results: %v", resp.Results[0].(*pilosa.Row).Columns())
	}
}

func TestClientTransactions(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	coord := c.GetPrimary()
	other := c.GetNonPrimary()

	client0 := MustNewClient(coord.URL(), pilosa.GetHTTPClient(nil))
	client1 := MustNewClient(other.URL(), pilosa.GetHTTPClient(nil))

	// can create, list, get, and finish a transaction
	var expDeadline time.Time
	if trns, err := client0.StartTransaction(context.Background(), "blah", time.Minute, false); err != nil {
		t.Fatalf("error starting transaction: %v", err)
	} else {
		expDeadline = time.Now().Add(time.Minute)
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blah", Timeout: time.Minute, Active: true, Deadline: expDeadline},
			trns)
	}

	if trnsMap, err := client0.Transactions(context.Background()); err != nil {
		t.Errorf("listing transactions: %v", err)
	} else {
		if len(trnsMap) != 1 {
			t.Errorf("unexpected trnsMap: %+v", trnsMap)
		}
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blah", Timeout: time.Minute, Active: true, Deadline: expDeadline},
			trnsMap["blah"])
	}

	if trns, err := client0.GetTransaction(context.Background(), "blah"); err != nil {
		t.Fatalf("error getting transaction: %v", err)
	} else {
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blah", Timeout: time.Minute, Active: true, Deadline: expDeadline},
			trns)
	}

	if trns, err := client0.FinishTransaction(context.Background(), "blah"); err != nil {
		t.Fatalf("error finishing transaction: %v", err)
	} else {
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blah", Timeout: time.Minute, Active: true, Deadline: expDeadline},
			trns)
	}

	// can create exclusive transaction
	if trns, err := client0.StartTransaction(context.Background(), "blahe", time.Minute, true); err != nil {
		t.Fatalf("error starting transaction: %v", err)
	} else {
		expDeadline = time.Now().Add(time.Minute)
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blahe", Timeout: time.Minute, Active: true, Exclusive: true, Deadline: expDeadline},
			trns)
	}

	// cannot start new transaction - correct error and exclusive transaction are returned
	if trns, err := client0.StartTransaction(context.Background(), "blah", time.Minute, false); errors.Cause(err) != pilosa.ErrTransactionExclusive {
		t.Fatalf("shouldn't be able to start transaction while an exclusive is running, but got: %+v, %v", trns, err)
	} else {
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blahe", Timeout: time.Minute, Active: true, Exclusive: true, Deadline: expDeadline},
			trns)
	}

	// finish exclusive transaction
	if trns, err := client0.FinishTransaction(context.Background(), "blahe"); err != nil {
		t.Fatalf("error finishing transaction: %v", err)
	} else {
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blahe", Timeout: time.Minute, Active: true, Exclusive: true, Deadline: expDeadline},
			trns)
	}

	// start new transaction
	if trns, err := client0.StartTransaction(context.Background(), "blah", time.Minute, false); err != nil {
		t.Fatalf("error starting transaction: %v", err)
	} else {
		expDeadline = time.Now().Add(time.Minute)
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blah", Timeout: time.Minute, Active: true, Deadline: expDeadline},
			trns)
	}

	// try to start same transaction
	if trns, err := client0.StartTransaction(context.Background(), "blah", time.Minute, false); err == nil ||
		!strings.Contains(err.Error(), pilosa.ErrTransactionExists.Error()) {
		t.Fatalf("expected ErrTransactionExists, but got: %v", err)
	} else {
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blah", Timeout: time.Minute, Active: true, Deadline: expDeadline},
			trns)
	}

	// start an exclusive transaction which can't go active
	if trns, err := client0.StartTransaction(context.Background(), "blahe", time.Minute, true); err != nil {
		t.Fatalf("error starting transaction: %v", err)
	} else {
		expDeadline = time.Now().Add(time.Minute)
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blahe", Timeout: time.Minute, Active: false, Exclusive: true, Deadline: expDeadline},
			trns)
	}

	// finish exclusive transaction that never went active
	if trns, err := client0.FinishTransaction(context.Background(), "blahe"); err != nil {
		t.Fatalf("error finishing transaction: %v", err)
	} else {
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blahe", Timeout: time.Minute, Active: false, Exclusive: true, Deadline: expDeadline},
			trns)
	}

	// finish non-existent transaction
	if trns, err := client0.FinishTransaction(context.Background(), "zzz"); err == nil ||
		!strings.Contains(err.Error(), pilosa.ErrTransactionNotFound.Error()) {
		t.Fatalf("unexpected error finishing nonexistent transaction: %v", err)
	} else {
		test.CompareTransactions(t,
			nil,
			trns)
	}

	// get non-existent transaction
	if trns, err := client0.GetTransaction(context.Background(), "xxx"); err == nil ||
		!strings.Contains(err.Error(), pilosa.ErrTransactionNotFound.Error()) {
		t.Fatalf("unexpected error getting nonexistent transaction: %v", err)
	} else {
		test.CompareTransactions(t,
			nil,
			trns)
	}

	// non-primary
	if trns, err := client1.StartTransaction(context.Background(), "blah", time.Minute, false); err != nil {
		t.Fatalf("unexpected error starting on non-primary: %v", err)
	} else {
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: "blah", Timeout: time.Minute, Active: true, Exclusive: false, Deadline: expDeadline},
			trns)
	}

	// start transaction with blank id
	if trns, err := client0.StartTransaction(context.Background(), "", time.Minute, false); err != nil {
		t.Fatalf("error starting transaction: %v", err)
	} else {
		expDeadline = time.Now().Add(time.Minute)
		if len(trns.ID) != 36 {
			t.Errorf("expected generated UUID, but got '%s'", trns.ID)
		}
		test.CompareTransactions(t,
			&pilosa.Transaction{ID: trns.ID, Timeout: time.Minute, Active: true, Deadline: expDeadline},
			trns)
	}

}

// Client represents a test wrapper for pilosa.Client.
type Client struct {
	*pilosa.InternalClient
}

// MustNewClient returns a new instance of Client. Panic on error.
func MustNewClient(host string, h *gohttp.Client) *Client {
	c, err := pilosa.NewInternalClient(host, h, pilosa.WithSerializer(proto.Serializer{}))
	if err != nil {
		panic(err)
	}
	return &Client{InternalClient: c}
}

func makeImportRoaringRequest(clear bool, viewData string) *pilosa.ImportRoaringRequest {
	roaringData, _ := hex.DecodeString(viewData)
	return &pilosa.ImportRoaringRequest{
		Clear: clear,
		Views: map[string][]byte{
			"": roaringData,
		},
	}
}

// verify that serverInfo has Backend
func TestClient_ServerInfoHasBackend(t *testing.T) {
	//srcs := []string{"roaring", "rbf", "lmdb"}
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	si := cmd.API.Info()
	if si.StorageBackend == "" {
		panic("should have gotten a StorageBackend back")
	}
	pilosa.MustBackendToTxtype(si.StorageBackend) // panics if invalid
}
func TestClient_ImportRoaringExists(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()

	node := cluster.GetNode(0)
	_, err := node.API.CreateIndex(context.Background(), cluster.Idx(), pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = node.API.CreateField(context.Background(), cluster.Idx(), "f", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 100))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	// Send import request.
	host := node.URL()
	c := MustNewClient(host, pilosa.GetHTTPClient(nil))
	// [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537]
	roaringReq := makeImportRoaringRequest(false, "3B3001000100000900010000000100010009000100")

	if err := c.ImportRoaring(context.Background(), &cluster.GetNode(0).API.Node().URI, cluster.Idx(), "f", 0, false, roaringReq); err != nil {
		t.Fatal(err)
	}
	qr, err := node.API.Query(context.Background(), &pilosa.QueryRequest{Index: cluster.Idx(), Query: "All()"})
	if err != nil {
		t.Fatalf(" %v ", err)
	}
	got := qr.Results[0].(*pilosa.Row).Columns()
	if !reflect.DeepEqual(got, []uint64{}) {
		t.Fatalf(" Row unexpected columns: got %+v  expected: %+v", got, []uint64{})
	}
	roaringReq.UpdateExistence = true
	if err := c.ImportRoaring(context.Background(), &cluster.GetNode(0).API.Node().URI, cluster.Idx(), "f", 0, false, roaringReq); err != nil {
		t.Fatal(err)
	}

	expected := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 65537}
	qr, err = node.API.Query(context.Background(), &pilosa.QueryRequest{Index: cluster.Idx(), Query: "All()"})
	if err != nil {
		t.Fatalf("Query error: %+v", err)
	}
	got = qr.Results[0].(*pilosa.Row).Columns()
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("All unexpected columns: got %+v  expected: %+v", got, expected)
	}

}

func TestAddAuthToken(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		req, err := gohttp.NewRequest("GET", "dontmatternone", strings.NewReader("this doesn't matter"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		pilosa.AddAuthToken(context.Background(), &req.Header)
		if req.Header.Get("Authorization") != "" {
			t.Fatalf("Authorization header set when it should be empty")
		}
	})
	t.Run("userinfo", func(t *testing.T) {
		req, err := gohttp.NewRequest("GET", "dontmatternone", strings.NewReader("this doesn't matter"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		uinfo := &authn.UserInfo{Token: "ayo"}
		pilosa.AddAuthToken(context.WithValue(context.Background(), "userinfo", uinfo), &req.Header)
		if got := req.Header.Get("Authorization"); got != "Bearer "+uinfo.Token {
			t.Fatalf("got '%v', expected 'Bearer %v'", got, uinfo.Token)
		}
	})
	t.Run("token", func(t *testing.T) {
		req, err := gohttp.NewRequest("GET", "dontmatternone", strings.NewReader("this doesn't matter"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tok := "Bearer thisisatoken"
		pilosa.AddAuthToken(
			context.WithValue(context.Background(),
				authn.ContextValueAccessToken,
				tok,
			),
			&req.Header,
		)
		if got := req.Header.Get("Authorization"); got != tok {
			t.Fatalf("got '%v', expected '%v'", got, tok)
		}
	})
	t.Run("originalIP", func(t *testing.T) {
		req, err := gohttp.NewRequest("GET", "dontmatternone", strings.NewReader("this doesn't matter"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ogIP := "10.0.0.1"
		pilosa.AddAuthToken(context.WithValue(context.Background(), pilosa.OriginalIPHeader, ogIP), &req.Header)
		if got := req.Header.Get(pilosa.OriginalIPHeader); got != ogIP {
			t.Fatalf("got '%v', expected '%v'", got, ogIP)
		}
	})
}
