package pilosa_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/test"
	"github.com/stretchr/testify/require"
)

func TestExecutor_DeleteRecords(t *testing.T) {
	indexName := "i"
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
		c.CreateField(t, indexName, pilosa.IndexOptions{TrackExistence: true}, "timefield", pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))
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
	setupKeys := func(t *testing.T, r *require.Assertions, c *test.Cluster) {
		t.Helper()
		c.CreateField(t, indexName, pilosa.IndexOptions{Keys: true, TrackExistence: true}, "timefield", pilosa.OptFieldKeys(), pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))
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
		c := test.MustNewCluster(t, 1)
		for _, n := range c.Nodes {
			n.Config.Cluster.ReplicaN = 1
		}
		err := c.Start()
		require.NoError(err, "Start cluster DeleteRecords")
		defer c.Close()

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
			require.Equal([]string{"one", "A", "B", "C", "D", "two"}, before, "these keyed records before")
			resp = c.Query(t, indexName, `Delete(ConstRow(columns=["A","one"]))`)
			require.NotEmpty(resp.Results)
			require.Equal(true, resp.Results[0], "Change should have happened")

			resp = c.Query(t, indexName, `Extract(All())`)
			m = resp.Results[0].(pilosa.ExtractedTable)
			after := convertKey(m.Columns)
			require.Equal([]string{"B", "C", "D", "two"}, after, "these keyed records after delete")
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
