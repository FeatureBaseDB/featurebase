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

package pilosa_test

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/server"
	"github.com/pilosa/pilosa/test"
)

func TestAPI_Import(t *testing.T) {
	c := test.MustRunCluster(t, 2,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node1"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
			)},
	)
	defer c.Close()

	m0 := c[0]
	m1 := c[1]

	t.Run("RowIDColumnKey", func(t *testing.T) {
		ctx := context.Background()
		index := "rick"
		field := "f"

		_, err := m0.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: true, TrackExistence: true})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = m0.API.CreateField(ctx, index, field, pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		rowID := uint64(1)
		timestamp := int64(0)

		// Generate some keyed records.
		rowIDs := []uint64{}
		colKeys := []string{}
		timestamps := []int64{}
		for i := 1; i <= 10; i++ {
			rowIDs = append(rowIDs, rowID)
			timestamps = append(timestamps, timestamp)
			colKeys = append(colKeys, fmt.Sprintf("col%d", i))
		}

		// Import data with keys to the coordinator (node0) and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
		req := &pilosa.ImportRequest{
			Index:      index,
			Field:      field,
			Shard:      0,
			RowIDs:     rowIDs,
			ColumnKeys: colKeys,
			Timestamps: timestamps,
		}
		if err := m0.API.Import(ctx, req); err != nil {
			t.Fatal(err)
		}

		pql := fmt.Sprintf("Row(%s=%d)", field, rowID)

		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
			t.Fatalf("unexpected column keys: %+v", keys)
		}

		// Query node1.
		if res, err := m1.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
			t.Fatalf("unexpected column keys: %+v", keys)
		}

	})

	// Relies on the previous test creating an index with TrackExistence and
	// adding some data.
	t.Run("SchemaHasNoExists", func(t *testing.T) {
		schema := m1.API.Schema(context.Background())
		for _, f := range schema[0].Fields {
			if f.Name == "_exists" {
				t.Fatalf("found _exists field in schema")
			}
			if strings.HasPrefix(f.Name, "_") {
				t.Fatalf("found internal field '%s' in schema output", f.Name)
			}
		}

	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		ctx := context.Background()
		index := "rkci"
		field := "f"

		_, err := m0.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: false})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = m0.API.CreateField(ctx, index, field, pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100), pilosa.OptFieldKeys())
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		rowKey := "rowkey"

		// Generate some keyed records.
		rowKeys := []string{rowKey, rowKey, rowKey}
		colIDs := []uint64{1, 2, pilosa.ShardWidth + 1}
		timestamps := []int64{0, 0, 0}

		// Import data with keys to the coordinator (node0) and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
		req := &pilosa.ImportRequest{
			Index:      index,
			Field:      field,
			Shard:      0,
			RowKeys:    rowKeys,
			ColumnIDs:  colIDs,
			Timestamps: timestamps,
		}
		if err := m0.API.Import(ctx, req); err != nil {
			t.Fatal(err)
		}

		pql := fmt.Sprintf("Row(%s=%s)", field, rowKey)

		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, colIDs) {
			t.Fatalf("unexpected column ids: %+v", columns)
		}

		// Query node1.
		if res, err := m1.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, colIDs) {
			t.Fatalf("unexpected column ids: %+v", columns)
		}
	})
}

func TestAPI_ImportValue(t *testing.T) {
	c := test.MustRunCluster(t, 2,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node1"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
			)},
	)
	defer c.Close()

	m0 := c[0]
	m1 := c[1]

	t.Run("ValColumnKey", func(t *testing.T) {
		ctx := context.Background()
		index := "valck"
		field := "f"

		_, err := m0.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: true})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = m0.API.CreateField(ctx, index, field, pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		// Generate some keyed records.
		values := []int64{}
		colKeys := []string{}
		for i := 1; i <= 10; i++ {
			values = append(values, int64(i))
			colKeys = append(colKeys, fmt.Sprintf("col%d", i))
		}

		// Import data with keys to the coordinator (node0) and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
		req := &pilosa.ImportValueRequest{
			Index:      index,
			Field:      field,
			ColumnKeys: colKeys,
			Values:     values,
		}
		if err := m0.API.ImportValue(ctx, req); err != nil {
			t.Fatal(err)
		}

		pql := fmt.Sprintf("Row(%s>0)", field)

		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
			t.Fatalf("unexpected column keys: %+v", keys)
		}

		// Query node1.
		if res, err := m1.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
			t.Fatalf("unexpected column keys: %+v", keys)
		}
	})
}

func TestAPI_ShardDistributionByIndex(t *testing.T) {
	c := test.MustRunCluster(t, 5)
	defer c.Close()

	m0 := c[0]

	ctx := context.Background()
	index := "idx"
	field := "f"

	_, err := m0.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: false, TrackExistence: false})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = m0.API.CreateField(ctx, index, field, pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}

	// Generate a shard range by setting a bit.
	rowIDs := []uint64{1}
	colIDs := []uint64{3145728}
	timestamps := []int64{0}

	// Import data with keys to the coordinator (node0) and verify that it gets
	// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
	req := &pilosa.ImportRequest{
		Index:      index,
		Field:      field,
		Shard:      0,
		RowIDs:     rowIDs,
		ColumnIDs:  colIDs,
		Timestamps: timestamps,
	}
	if err := m0.API.Import(ctx, req); err != nil {
		t.Fatal(err)
	}

	// TODO: instead of sleeping here, we need to know when the shard-creation
	// message has propagated.
	time.Sleep(500 * time.Millisecond)

	tests := []struct {
		name            string
		idx             string
		provideMaxShard bool
		maxShard        uint64
		expected        [][]uint64
	}{
		{
			name:            "ExistingIndexProvideMaxShard",
			idx:             index,
			provideMaxShard: true,
			maxShard:        10,
			expected: [][]uint64{
				{0, 1, 2, 4, 5},
				{6},
				{8},
				{7, 9},
				{3, 10},
			},
		},
		{
			name:            "ExistingIndexNotProvideMaxShard",
			idx:             index,
			provideMaxShard: false,
			maxShard:        0,
			expected: [][]uint64{
				{0, 1, 2},
				{},
				{},
				{},
				{3},
			},
		},
		{
			name:            "NonExistingIndexProvideMaxShard",
			idx:             "foobar",
			provideMaxShard: true,
			maxShard:        10,
			expected: [][]uint64{
				{0, 1, 2, 3, 6},
				{5},
				{8},
				{4, 9},
				{7, 10},
			},
		},
		{
			name:            "NonExistingIndexNotProvideMaxShard",
			idx:             "foobar",
			provideMaxShard: false,
			maxShard:        0,
			expected: [][]uint64{
				{0},
				{},
				{},
				{},
				{},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, shards := m0.API.ShardDistributionByIndex(ctx, test.idx, test.provideMaxShard, test.maxShard)

			if !reflect.DeepEqual(shards, test.expected) {
				t.Fatalf("unexpected shard distribution for index: %v", shards)
			}
		})
	}
}

// offsetModHasher represents a simple, mod-based hashing offset by 1.
type offsetModHasher struct{}

func (*offsetModHasher) Hash(key uint64, n int) int {
	return int(key+1) % n
}
