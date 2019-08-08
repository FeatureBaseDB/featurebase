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

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/server"
	"github.com/pilosa/pilosa/test"
)

const MOD = "mod"

func TestAPI_Import(t *testing.T) {
	partitionN := 2
	c := test.MustRunCluster(t, 2,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerShardDistributors(map[string]pilosa.ShardDistributor{MOD: newOffsetModDistributor(partitionN)}),
				pilosa.OptServerDefaultShardDistributor(MOD),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node1"),
				pilosa.OptServerShardDistributors(map[string]pilosa.ShardDistributor{MOD: newOffsetModDistributor(partitionN)}),
				pilosa.OptServerDefaultShardDistributor(MOD),
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
	partitionN := 2
	c := test.MustRunCluster(t, 2,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerShardDistributors(map[string]pilosa.ShardDistributor{MOD: newOffsetModDistributor(partitionN)}),
				pilosa.OptServerDefaultShardDistributor(MOD),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node1"),
				pilosa.OptServerShardDistributors(map[string]pilosa.ShardDistributor{MOD: newOffsetModDistributor(partitionN)}),
				pilosa.OptServerDefaultShardDistributor(MOD),
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

// offsetModDistributor represents a simple, mod-based shard distributor offset by 1.
type offsetModDistributor struct {
	partitionN int
}

// NewModDistributor returns a new instance of ModDistributor.
func newOffsetModDistributor(partitionN int) *offsetModDistributor {
	return &offsetModDistributor{partitionN: partitionN}
}

// NodeOwners satisfies the ShardDistributor interface.
func (d *offsetModDistributor) NodeOwners(nodeIDs []string, replicaN int, index string, shard uint64) []string {
	idx := int((shard + 1) % uint64(d.partitionN))
	owners := make([]string, 0, replicaN)
	for i := 0; i < replicaN; i++ {
		owners = append(owners, nodeIDs[(idx+i)%len(nodeIDs)])
	}
	return owners
}

// AddNode is a nop and only exists to satisfy the `ShardDistributor` interface.
func (d *offsetModDistributor) AddNode(nodeID string) error { return nil }

// RemoveNode is a nop and only exists to satisfy the `ShardDistributor` interface.
func (d *offsetModDistributor) RemoveNode(nodeID string) error { return nil }
