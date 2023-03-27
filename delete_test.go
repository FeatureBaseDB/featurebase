// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/stretchr/testify/require"
)

func TestExecutor_DeleteRecords(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	indexName := c.Idx()
	defer c.Close()
	setup := func(t *testing.T, r *require.Assertions, c *test.Cluster) {
		t.Helper()
		c.CreateField(t, indexName, pilosa.IndexOptions{TrackExistence: true}, "setfield")
		c.ImportBits(t, indexName, "setfield", [][2]uint64{
			{0, 0},
			{0, 1},
			{0, ShardWidth + 2},
			{10, 2},
			{10, ShardWidth},
			{10, 2 * ShardWidth},
			{10, ShardWidth + 1},
			{20, ShardWidth},
		})
		c.CreateField(t, indexName, pilosa.IndexOptions{TrackExistence: true}, "bsi", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		c.ImportIntID(t, indexName, "bsi", []test.IntID{
			{ID: 0, Val: 4},
			{ID: 2, Val: 8},
		})
		c.CreateField(t, indexName, pilosa.IndexOptions{TrackExistence: true}, "timefield", pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH"), "0"))
		c.ImportBitsWithTimestamp(t, indexName, "timefield", [][2]uint64{
			{0, 0},
			{0, 1},
			{0, 1},
			{0, 1},
			{0, 1},
		}, []int64{
			time.Date(2020, time.January, 2, 15, 45, 0, 0, time.UTC).Unix(),
			time.Date(2019, time.January, 2, 16, 45, 0, 0, time.UTC).Unix(),
			time.Date(2019, time.January, 2, 16, 45, 0, 0, time.UTC).Unix(),
			time.Date(2019, time.January, 2, 17, 45, 0, 0, time.UTC).Unix(),
			time.Date(2019, time.January, 2, 17, 45, 0, 0, time.UTC).Unix(),
		})

	}
	setupBig := func(t *testing.T, r *require.Assertions, c *test.Cluster, Rows uint64) {
		t.Helper()
		fieldName := "setfield"
		c.CreateField(t, indexName, pilosa.IndexOptions{TrackExistence: true}, fieldName)
		// we don't need to populate the whole thing, just enough to get a sample of it
		width := uint64(ShardWidth / 4)
		rows := make([][2]uint64, width*Rows)
		n := 0
		// populate rows with decreasing density
		for columnID := uint64(0); columnID < width; columnID++ {
			for rowID := uint64(0); rowID < Rows; rowID++ {
				if (columnID % (rowID + 1)) == 0 {
					rows[n] = [2]uint64{rowID, columnID}
					n++
				}
			}
		}
		c.ImportBits(t, indexName, "setfield", rows[:n])
	}

	setupKeys := func(t *testing.T, r *require.Assertions, c *test.Cluster) {
		t.Helper()
		c.CreateField(t, indexName, pilosa.IndexOptions{Keys: true, TrackExistence: true}, "timefield", pilosa.OptFieldKeys(), pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH"), "0"))
		c.ImportTimeQuantumKey(t, indexName, "timefield", []test.TimeQuantumKey{
			{RowKey: "fish", ColKey: "one", Ts: time.Date(2019, time.January, 2, 17, 45, 0, 0, time.UTC).Unix()},
			{RowKey: "fish", ColKey: "one", Ts: time.Date(2020, time.January, 2, 17, 45, 0, 0, time.UTC).Unix()},
			{RowKey: "fish", ColKey: "two", Ts: time.Date(2019, time.January, 3, 17, 45, 0, 0, time.UTC).Unix()},
		})
		c.CreateField(t, indexName, pilosa.IndexOptions{Keys: true, TrackExistence: true}, "keystuff")
		c.ImportIDKey(t, indexName, "keystuff", []test.KeyID{
			{ID: 1, Key: "A"},
			{ID: 2, Key: "B"},
			{ID: 3, Key: "C"},
			{ID: 4, Key: "D"},
		})

	}
	setupOverlap := func(t *testing.T, r *require.Assertions, c *test.Cluster) {
		t.Helper()
		c.CreateField(t, indexName, pilosa.IndexOptions{TrackExistence: true}, "setfield")
		c.ImportBits(t, indexName, "setfield", [][2]uint64{
			{0, 0},
			{0, 1},
			{1, 1},
			{2, 1},
			{3, 1},
			{0, ShardWidth},
			{2, ShardWidth},
			{4, ShardWidth},
			{6, ShardWidth},
		})
	}
	tearDown := func(t *testing.T, require *require.Assertions, c *test.Cluster) {
		t.Helper()
		api := c.GetPrimary().API
		err := api.DeleteIndex(context.Background(), indexName)
		require.NoErrorf(err, "DeleteIndex %v", indexName)
	}
	require := require.New(t)
	t.Run("DeleteRecords", func(t *testing.T) {

		t.Run("Delete", func(t *testing.T) {
			setup(t, require, c)
			defer tearDown(t, require, c)
			resp := c.Query(t, indexName, `Extract(All())`)
			m := resp.Results[0].(pilosa.ExtractedTable)
			before := convert(m.Columns)
			require.Equal([]uint64{0, 1, 2, ShardWidth, ShardWidth + 1, ShardWidth + 2, 2 * ShardWidth}, before, "these records are expected")
			resp = c.Query(t, indexName, fmt.Sprintf(`Delete(ConstRow(columns=[1,2,3,%v]))`, ShardWidth+1))
			require.NotNil(resp, "Response should not be nil")
			require.NotEmpty(resp.Results)
			require.Equal(true, resp.Results[0], "Change should have happened")

			resp = c.Query(t, indexName, `Extract(All())`)

			//Note none of the removed records should remain
			m = resp.Results[0].(pilosa.ExtractedTable)
			after := convert(m.Columns)
			require.Equal([]uint64{0, ShardWidth, ShardWidth + 2, 2 * ShardWidth}, after, "these records should be remaining")
		})
		t.Run("DeleteKey", func(t *testing.T) {
			setupKeys(t, require, c)
			defer tearDown(t, require, c)
			resp := c.Query(t, indexName, `Extract(All())`)
			m := resp.Results[0].(pilosa.ExtractedTable)
			before := convertKey(m.Columns)
			sort.Strings(before)
			expected := []string{"A", "B", "C", "D", "one", "two"}
			require.Equal(expected, before, "these keyed records before")
			resp = c.Query(t, indexName, `Delete(ConstRow(columns=["A","one"]))`)
			require.NotEmpty(resp.Results)
			require.Equal(true, resp.Results[0], "Change should have happened")

			resp = c.Query(t, indexName, `Extract(All())`)
			m = resp.Results[0].(pilosa.ExtractedTable)
			after := convertKey(m.Columns)
			sort.Strings(after)
			require.Equal([]string{"B", "C", "D", "two"}, after, "these keyed records after delete")
			//validate that column keys got deleted
			node := c.GetNode(0)
			keys := []string{"A", "one"}
			res, err := node.API.FindIndexKeys(context.Background(), indexName, keys...)
			require.Nil(err)
			require.Empty(res)
		})
		t.Run("Delete Row", func(t *testing.T) {
			setup(t, require, c)
			defer tearDown(t, require, c)
			resp := c.Query(t, indexName, `Delete(Row(setfield=20))`)
			require.NotNil(resp, "Response should not be nil")
			require.NotEmpty(resp.Results)
			require.Equal(true, resp.Results[0], "Change should have happened")

			resp = c.Query(t, indexName, `Extract(All())`)

			//Note none of the removed records should remain
			m := resp.Results[0].(pilosa.ExtractedTable)
			after := convert(m.Columns)
			require.Equal([]uint64{0, 1, 2, ShardWidth + 1, ShardWidth + 2, 2 * ShardWidth}, after, "these records are expected")
		})
		t.Run("Delete Not Row", func(t *testing.T) {
			setup(t, require, c)
			defer tearDown(t, require, c)
			resp := c.Query(t, indexName, `Delete(Not(Row(setfield=20)))`)
			require.NotNil(resp, "Response should not be nil")
			require.NotEmpty(resp.Results)
			require.Equal(true, resp.Results[0], "Change should have happened")

			resp = c.Query(t, indexName, `Extract(All())`)

			//Note none of the removed records should remain
			m := resp.Results[0].(pilosa.ExtractedTable)
			after := convert(m.Columns)
			require.Equal([]uint64{ShardWidth}, after, "these records are expected")
		})
		t.Run("Delete All", func(t *testing.T) {
			setup(t, require, c)
			defer tearDown(t, require, c)
			resp := c.Query(t, indexName, `Count(All())`)
			//Note none of the removed records should remain
			before := resp.Results[0].(uint64)
			require.Equal(uint64(7), before, "these records are expected")

			resp = c.Query(t, indexName, `Delete(All())`)
			require.NotNil(resp, "Response should not be nil")
			require.NotEmpty(resp.Results)
			require.Equal(true, resp.Results[0], "Change should have happened")

			resp = c.Query(t, indexName, `Count(All())`)
			//Note none of the removed records should remain
			after := resp.Results[0].(uint64)
			require.Equal(uint64(0), after, "these records are expected")
		})
		t.Run("DeleteOverlap", func(t *testing.T) {
			setupOverlap(t, require, c)
			defer tearDown(t, require, c)
			resp := c.Query(t, indexName, `Extract(All())`)
			m := resp.Results[0].(pilosa.ExtractedTable)
			before := convert(m.Columns)
			require.Equal([]uint64{0, 1, ShardWidth}, before, "these records are expected")
			resp = c.Query(t, indexName, fmt.Sprintf(`Delete(ConstRow(columns=[%v]))`, ShardWidth))
			require.NotNil(resp, "Response should not be nil")
			require.NotEmpty(resp.Results)
			require.Equal(true, resp.Results[0], "Change should have happened")

			resp = c.Query(t, indexName, `Extract(All())`)

			//Note none of the removed records should remain
			m = resp.Results[0].(pilosa.ExtractedTable)
			after := convert(m.Columns)
			require.Equal([]uint64{0, 1}, after, "these records should be remaining")
		})

		// FB-1281: Delete() calls with an invalid bitmap filter would cause a panic.
		// This test validates that the error is correctly propagated to the caller.
		t.Run("DeleteWithBitmapError", func(t *testing.T) {
			setup(t, require, c)
			defer tearDown(t, require, c)
			_, err := c.GetPrimary().API.Query(context.Background(), &pilosa.QueryRequest{Index: indexName, Query: `Delete(Row(setfield > 1))`})
			// we don't allow `>` operators on set fields
			if err == nil || !strings.Contains(err.Error(), "row call: only support") {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	})
	t.Run("DeleteRecordsBigWithRestart", func(t *testing.T) {
		// restarting doesn't work correctly for a shared cluster
		c := test.MustRunUnsharedCluster(t, 1)
		defer c.Close()
		setupBig(t, require, c, 16)
		defer tearDown(t, require, c)
		node := c.GetNode(0)
		resp := c.Query(t, indexName, `Delete(Row(setfield=12))`)
		require.NotNil(resp, "Response should not be nil")
		require.NotEmpty(resp.Results)
		require.Equal(true, resp.Results[0], "Change should have happened")
		resp = c.Query(t, indexName, `Count(Row(setfield=12))`)
		require.NotNil(resp, "Response should not be nil")
		require.NotEmpty(resp.Results)
		require.Equal(uint64(0), resp.Results[0], "Should have removed")
		err := node.Reopen()
		require.NoError(err, "restart cluster DeleteRecordsBig")
		err = c.AwaitState(disco.ClusterStateNormal, 10*time.Second)
		require.NoError(err, "backToNormal")
	})
}

func convert(before []pilosa.ExtractedTableColumn) []uint64 {
	result := make([]uint64, 0)
	for _, i := range before {
		result = append(result, i.Column.ID)
	}
	return result
}
func convertKey(before []pilosa.ExtractedTableColumn) []string {
	result := make([]string, 0)
	for _, i := range before {
		result = append(result, i.Column.Key)
	}
	return result
}
