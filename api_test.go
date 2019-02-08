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
	"reflect"
	"strings"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/server"
	"github.com/pilosa/pilosa/test"
)

func TestAPI_Import(t *testing.T) {
	field := "f"

	/////////////////////////////////////////////////////////////
	// Generate some test records.
	rowID := uint64(1)
	rowKey := "rowkey"

	rowIDs := []uint64{}
	rowKeys := []string{}
	colIDs := []uint64{}
	colKeys := []string{}
	timestamps := []int64{}
	for i := 1; i <= 10; i++ {
		rowIDs = append(rowIDs, rowID)
		rowKeys = append(rowKeys, rowKey)
		colIDs = append(colIDs, uint64((i-1)*pilosa.ShardWidth+i))
		colKeys = append(colKeys, fmt.Sprintf("col%d", i))
		timestamps = append(timestamps, int64(0))
	}

	// rici
	// because rici has no keys, we direct it to a specific shard.
	shard3 := []uint64{}
	for i := 1; i <= 10; i++ {
		shard3 = append(shard3, uint64(3*pilosa.ShardWidth+i))
	}
	rici := &pilosa.ImportRequest{
		Index:      "rici",
		Field:      field,
		Shard:      3,
		RowIDs:     rowIDs,
		ColumnIDs:  shard3,
		Timestamps: timestamps,
	}
	// rick
	rick := &pilosa.ImportRequest{
		Index:      "rick",
		Field:      field,
		Shard:      0,
		RowIDs:     rowIDs,
		ColumnKeys: colKeys,
		Timestamps: timestamps,
	}
	// rkci
	rkci := &pilosa.ImportRequest{
		Index:      "rkci",
		Field:      field,
		Shard:      0,
		RowKeys:    rowKeys,
		ColumnIDs:  colIDs,
		Timestamps: timestamps,
	}
	// rkck
	rkck := &pilosa.ImportRequest{
		Index:      "rkck",
		Field:      field,
		Shard:      0,
		RowKeys:    rowKeys,
		ColumnKeys: colKeys,
		Timestamps: timestamps,
	}
	/////////////////////////////////////////////////////////////

	tests := []struct {
		req        *pilosa.ImportRequest
		hasRowKeys bool
		hasColKeys bool
		pql        string
		expColIDs  []uint64
		expColKeys []string
	}{
		{rici, false, false, fmt.Sprintf("Row(%s=%d)", field, rowID), shard3, []string{}},
		{rick, false, true, fmt.Sprintf("Row(%s=%d)", field, rowID), []uint64{}, colKeys},
		{rkci, true, false, fmt.Sprintf("Row(%s=%s)", field, rowKey), colIDs, []string{}},
		{rkck, true, true, fmt.Sprintf("Row(%s=%s)", field, rowKey), []uint64{}, colKeys},
	}

	// Test with different replication values.
	for repl := 1; repl <= 3; repl++ {
		t.Run(fmt.Sprintf("replication-%d", repl), func(t *testing.T) {
			c := test.MustRunCluster(t, 3,
				[]server.CommandOption{
					server.OptCommandServerOptions(
						pilosa.OptServerNodeID("node0"),
						pilosa.OptServerReplicaN(repl),
					)},
				[]server.CommandOption{
					server.OptCommandServerOptions(
						pilosa.OptServerNodeID("node1"),
						pilosa.OptServerReplicaN(repl),
					)},
				[]server.CommandOption{
					server.OptCommandServerOptions(
						pilosa.OptServerNodeID("node2"),
						pilosa.OptServerReplicaN(repl),
					)},
			)
			defer c.Close()

			for _, test := range tests {
				for n := 0; n < 3; n++ {
					// Deep copy req to allow for translation modifications.
					req := &pilosa.ImportRequest{
						Index:      test.req.Index,
						Field:      test.req.Field,
						Shard:      test.req.Shard,
						RowIDs:     test.req.RowIDs,
						RowKeys:    test.req.RowKeys,
						ColumnIDs:  test.req.ColumnIDs,
						ColumnKeys: test.req.ColumnKeys,
						Timestamps: test.req.Timestamps,
					}

					testName := fmt.Sprintf("%s-node-%d", test.req.Index, n)
					t.Run(testName, func(t *testing.T) {
						ctx := context.Background()
						index := fmt.Sprintf("%s%d", test.req.Index, n)
						// Update the req to contain the dynamically created index name.
						req.Index = index

						_, err := c[n].API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: test.hasColKeys, TrackExistence: true})
						if err != nil {
							t.Fatalf("creating index: %v", err)
						}
						fieldOpts := []pilosa.FieldOption{pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100)}
						if test.hasRowKeys {
							fieldOpts = append(fieldOpts, pilosa.OptFieldKeys())
						}
						_, err = c[n].API.CreateField(ctx, index, field, fieldOpts...)
						if err != nil {
							t.Fatalf("creating field: %v", err)
						}

						// Import into a specific node
						if err := c[n].API.Import(ctx, req); err != nil {
							t.Fatal(err)
						}

						// Query each node for expected result.
						for ci := range c {
							if res, err := c[ci].API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: test.pql}); err != nil {
								t.Fatal(err)
							} else {
								if test.hasColKeys {
									if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, test.expColKeys) {
										t.Fatalf("querying node %d: unexpected column keys: %+v", ci, keys)
									}
								} else {
									if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, test.expColIDs) {
										t.Fatalf("querying node %d: unexpected column ids: %+v", ci, columns)
									}
								}
							}
						}
					})
				}
			}
		})
	}
}

func TestAPI_Schema(t *testing.T) {
	t.Run("SchemaHasNoExists", func(t *testing.T) {
		c := test.MustRunCluster(t, 2,
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerNodeID("node0"),
				)},
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerNodeID("node1"),
				)},
		)
		defer c.Close()

		ctx := context.Background()
		index := "i"
		field := "f"

		_, err := c[0].API.CreateIndex(ctx, index, pilosa.IndexOptions{TrackExistence: true})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = c[0].API.CreateField(ctx, index, field, pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		schema := c[1].API.Schema(context.Background())
		for _, f := range schema[0].Fields {
			if f.Name == "_exists" {
				t.Fatalf("found _exists field in schema")
			}
			if strings.HasPrefix(f.Name, "_") {
				t.Fatalf("found internal field '%s' in schema output", f.Name)
			}
		}
	})
}

func TestAPI_ImportValue(t *testing.T) {
	field := "f"

	/////////////////////////////////////////////////////////////
	// Generate some test records.

	colIDs := []uint64{}
	colKeys := []string{}
	values := []int64{}
	for i := 1; i <= 10; i++ {
		colIDs = append(colIDs, uint64((i-1)*pilosa.ShardWidth+i))
		colKeys = append(colKeys, fmt.Sprintf("col%d", i))
		values = append(values, int64(i))
	}

	// vci
	// because vci has no keys, we direct it to a specific shard.
	shard3 := []uint64{}
	for i := 1; i <= 10; i++ {
		shard3 = append(shard3, uint64(3*pilosa.ShardWidth+i))
	}
	vci := &pilosa.ImportValueRequest{
		Index:     "rici",
		Field:     field,
		Shard:     3,
		ColumnIDs: shard3,
		Values:    values,
	}
	// vck
	vck := &pilosa.ImportValueRequest{
		Index:      "rick",
		Field:      field,
		Shard:      0,
		ColumnKeys: colKeys,
		Values:     values,
	}
	/////////////////////////////////////////////////////////////

	tests := []struct {
		req        *pilosa.ImportValueRequest
		hasColKeys bool
		pql        string
		expColIDs  []uint64
		expColKeys []string
	}{
		{vci, false, fmt.Sprintf("Row(%s>0)", field), shard3, []string{}},
		{vck, true, fmt.Sprintf("Row(%s>0)", field), []uint64{}, colKeys},
	}

	// Test with different replication values.
	for repl := 1; repl <= 3; repl++ {
		t.Run(fmt.Sprintf("replication-%d", repl), func(t *testing.T) {
			c := test.MustRunCluster(t, 3,
				[]server.CommandOption{
					server.OptCommandServerOptions(
						pilosa.OptServerNodeID("node0"),
						pilosa.OptServerReplicaN(repl),
					)},
				[]server.CommandOption{
					server.OptCommandServerOptions(
						pilosa.OptServerNodeID("node1"),
						pilosa.OptServerReplicaN(repl),
					)},
				[]server.CommandOption{
					server.OptCommandServerOptions(
						pilosa.OptServerNodeID("node2"),
						pilosa.OptServerReplicaN(repl),
					)},
			)
			defer c.Close()

			for _, test := range tests {
				for n := 0; n < 3; n++ {
					// Deep copy req to allow for translation modifications.
					req := &pilosa.ImportValueRequest{
						Index:      test.req.Index,
						Field:      test.req.Field,
						Shard:      test.req.Shard,
						ColumnIDs:  test.req.ColumnIDs,
						ColumnKeys: test.req.ColumnKeys,
						Values:     test.req.Values,
					}

					testName := fmt.Sprintf("%s-node-%d", test.req.Index, n)
					t.Run(testName, func(t *testing.T) {
						ctx := context.Background()
						index := fmt.Sprintf("%s%d", test.req.Index, n)
						// Update the req to contain the dynamically created index name.
						req.Index = index

						_, err := c[n].API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: test.hasColKeys})
						if err != nil {
							t.Fatalf("creating index: %v", err)
						}
						_, err = c[n].API.CreateField(ctx, index, field, pilosa.OptFieldTypeInt(0, 100))
						if err != nil {
							t.Fatalf("creating field: %v", err)
						}

						// Import into a specific node
						if err := c[n].API.ImportValue(ctx, req); err != nil {
							t.Fatal(err)
						}

						// Query each node for expected result.
						for ci := range c {
							if res, err := c[ci].API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: test.pql}); err != nil {
								t.Fatal(err)
							} else {
								if test.hasColKeys {
									if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, test.expColKeys) {
										t.Fatalf("querying node %d: unexpected column keys: %+v", ci, keys)
									}
								} else {
									if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, test.expColIDs) {
										t.Fatalf("querying node %d: unexpected column ids: %+v", ci, columns)
									}
								}
							}
						}
					})
				}
			}
		})
	}
}
