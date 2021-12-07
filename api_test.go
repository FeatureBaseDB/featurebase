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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/boltdb"
	"github.com/molecula/featurebase/v2/http"
	"github.com/molecula/featurebase/v2/server"
	"github.com/molecula/featurebase/v2/shardwidth"
	"github.com/molecula/featurebase/v2/test"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
)

func TestAPI_Import(t *testing.T) {
	c := test.MustRunCluster(t, 3,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node1"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node2"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
	)
	defer c.Close()

	m0 := c.GetNode(0)
	m1 := c.GetNode(1)

	indexNames := map[bool]string{false: "i", true: "ki"}
	fieldNames := map[bool]string{false: "f", true: "kf"}

	ctx := context.Background()
	for ik, indexName := range indexNames {
		_, err := m0.API.CreateIndex(ctx, indexName, pilosa.IndexOptions{Keys: ik, TrackExistence: true})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		for fk, fieldName := range fieldNames {
			if fk {
				_, err = m0.API.CreateField(ctx, indexName, fieldName, pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100), pilosa.OptFieldKeys())
			} else {
				_, err = m0.API.CreateField(ctx, indexName, fieldName, pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100))

			}
			if err != nil {
				t.Fatalf("creating field: %v", err)
			}
		}
	}

	N := 10

	// Keys are sharded so ordering is not guaranteed.
	colKeys := make([]string, N)
	rowKeys := make([]string, N)
	rowIDs := make([]uint64, N)
	colIDs := make([]uint64, N)
	for i := range colKeys {
		colKeys[i] = fmt.Sprintf("col%d", i)
		rowKeys[i] = fmt.Sprintf("row%d", i)
		colIDs[i] = (uint64(i) + 1) * 3
		rowIDs[i] = 1
	}
	sort.Strings(colKeys)
	sort.Strings(rowKeys)

	t.Run("RowIDColumnKey", func(t *testing.T) {
		// Import data with keys to the primary and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
		req := &pilosa.ImportRequest{
			Index:      indexNames[true],
			Field:      fieldNames[false],
			Shard:      0, // inaccurate, but keys override it
			RowIDs:     rowIDs,
			ColumnKeys: colKeys,
		}

		qcx := m0.API.Txf().NewQcx()

		if err := m0.API.Import(ctx, qcx, req); err != nil {
			t.Fatal(err)
		}
		PanicOn(qcx.Finish())

		pql := fmt.Sprintf("Row(%s=%d)", fieldNames[false], rowIDs[0])

		// Query node0.
		var keys []string
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: indexNames[true], Query: pql}); err != nil {
			t.Fatal(err)
		} else {
			keys = res.Results[0].(*pilosa.Row).Keys
		}
		sort.Strings(keys)
		if !reflect.DeepEqual(keys, colKeys) {
			t.Fatalf("expected colKeys='%#v'; observed column keys: %#v", colKeys, keys)
		}

		// Query node1.
		if err := test.RetryUntil(5*time.Second, func() error {
			if res, err := m1.API.Query(ctx, &pilosa.QueryRequest{Index: indexNames[true], Query: pql}); err != nil {
				return err
			} else {
				keys = res.Results[0].(*pilosa.Row).Keys

			}
			sort.Strings(keys)
			if !reflect.DeepEqual(keys, colKeys) {
				return fmt.Errorf("unexpected column keys: %#v", keys)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ExpectedErrors", func(t *testing.T) {
		ctx := context.Background()
		for ik, indexName := range indexNames {
			for fk, fieldName := range fieldNames {
				req := pilosa.ImportRequest{
					Index: indexName,
					Field: fieldName,
					Shard: 0,
				}
				for rik := range indexNames {
					if rik {
						req.ColumnKeys = colKeys
						req.ColumnIDs = nil
					} else {
						req.ColumnKeys = nil
						req.ColumnIDs = colIDs
					}
					for rfk := range fieldNames {
						if rfk {
							req.RowKeys = rowKeys
							req.RowIDs = nil
						} else {
							req.RowKeys = nil
							req.RowIDs = rowIDs
						}
						err := func() error {
							qcx := m0.API.Txf().NewQcx()
							defer qcx.Abort()
							err := m0.API.Import(ctx, qcx, req.Clone())
							e2 := qcx.Finish()
							if e2 != nil {
								t.Fatalf("unexpected error committing: %v", e2)
							}
							return err
						}()
						if err != nil {
							if rfk == fk && rik == ik {
								t.Errorf("unexpected error: schema keys %t/%t, req keys %t/%t: %v",
									ik, fk, rik, rfk, err)
							}
						} else {
							if rfk != fk || rik != ik {
								t.Errorf("unexpected no error: schema keys %t/%t, req keys %t/%t, req %#v",
									ik, fk, rik, rfk, req)
							}
						}
					}
				}
			}
		}
	})

	// Relies on the previous test creating an index with TrackExistence and
	// adding some data.
	t.Run("SchemaHasNoExists", func(t *testing.T) {
		schema, err := m1.API.Schema(context.Background(), false)
		if err != nil {
			t.Fatal(err)
		}

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
	c := test.MustRunCluster(t, 3,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node1"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node2"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
	)
	defer c.Close()

	coord := c.GetPrimary()
	m0 := c.GetNode(0)
	m1 := c.GetNode(1)
	m2 := c.GetNode(2)

	t.Run("ValColumnKey", func(t *testing.T) {
		ctx := context.Background()
		index := "valck"
		field := "f"

		_, err := coord.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: true})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = coord.API.CreateField(ctx, index, field, pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		// Generate some keyed records.
		values := []int64{}
		for i := 1; i <= 10; i++ {
			values = append(values, int64(i))
		}

		// Column keys are sharded so their order is not guaranteed.
		colKeys := []string{"col10", "col8", "col9", "col6", "col7", "col4", "col5", "col2", "col3", "col1"}

		// Import data with keys to the primary and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
		req := &pilosa.ImportValueRequest{
			Index:      index,
			Field:      field,
			ColumnKeys: colKeys,
			Values:     values,
			Shard:      0, // inaccurate but keys override it
		}

		qcx := coord.API.Txf().NewQcx()
		if err := coord.API.ImportValue(ctx, qcx, req); err != nil {
			t.Fatal(err)
		}
		PanicOn(qcx.Finish())

		pql := fmt.Sprintf("Row(%s>0)", field)

		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
			t.Fatalf("unexpected column keys: %+v", keys)
		}

		// Query node1.
		if err := test.RetryUntil(5*time.Second, func() error {
			if res, err := m1.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
				return err
			} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
				return fmt.Errorf("unexpected column keys: %+v", keys)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("ValIntEmpty", func(t *testing.T) {
		ctx := context.Background()
		index := "valintempty"
		field := "fld"
		createIndexForTest(index, coord, t)
		createFieldForTest(index, field, coord, t)

		// Column keys are sharded so their order is not guaranteed.
		colKeys := []string{"col2", "col1", "col3"}
		values := []int64{1, 2, 3, 4}

		// Import without data, verify that it succeeds
		req := &pilosa.ImportValueRequest{
			Index: index,
			Field: field,
		}
		qcx1 := coord.API.Txf().NewQcx()
		defer qcx1.Abort()

		// Import with empty request, should succeed
		if err := coord.API.ImportValue(ctx, qcx1, req); err != nil {
			t.Fatal(err)
		}
		PanicOn(qcx1.Finish())

		// Import without data but with columnkeys, verify that it errors
		req.ColumnKeys = colKeys
		qcx2 := coord.API.Txf().NewQcx()
		defer qcx2.Abort()
		if err := coord.API.ImportValue(ctx, qcx2, req); err == nil {
			t.Fatal("expected error but succeeded")
		}
		PanicOn(qcx2.Finish())

		// Import with mismatch column and value lengths
		req.Values = values
		qcx3 := coord.API.Txf().NewQcx()
		defer qcx3.Abort()
		if err := coord.API.ImportValue(ctx, qcx3, req); err == nil {
			t.Fatal("expected error but succeeded")
		}
		PanicOn(qcx3.Finish())

		// Import with data but no columns
		req.ColumnKeys = make([]string, 0)
		qcx4 := coord.API.Txf().NewQcx()
		defer qcx4.Abort()
		if err := coord.API.ImportValue(ctx, qcx4, req); err == nil {
			t.Fatal("expected error but succeeded")
		}
		PanicOn(qcx4.Finish())

	})

	t.Run("ValDecimalField", func(t *testing.T) {
		ctx := context.Background()
		index := "valdec"
		field := "fdec"
		_, err := m2.API.CreateIndex(ctx, index, pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = m2.API.CreateField(ctx, index, field, pilosa.OptFieldTypeDecimal(1))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}
		// Generate some records.
		values := []float64{}
		colIDs := []uint64{}
		for i := 0; i < 10; i++ {
			values = append(values, float64(i)+0.1)
			colIDs = append(colIDs, uint64(i))
		}
		// Import data with keys to node1 and verify that it gets translated and
		// forwarded to the owner of shard 0 (node0; because of offsetModHasher)
		req := &pilosa.ImportValueRequest{
			Index:       index,
			Field:       field,
			ColumnIDs:   colIDs,
			FloatValues: values,
		}
		qcx := m0.API.Txf().NewQcx()
		if err := m0.API.ImportValue(ctx, qcx, req); err != nil {
			t.Fatal(err)
		}
		PanicOn(qcx.Finish())
		query := fmt.Sprintf("Row(%s>6)", field)
		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: query}); err != nil {
			t.Fatal(err)
		} else if ids := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(ids, colIDs[6:]) {
			t.Fatalf("unexpected column keys: observerd %+v;  expected '%+v'", ids, colIDs[6:])
		}
	})

	t.Run("ValDecimalFieldNegativeScale", func(t *testing.T) {
		ctx := context.Background()
		index := "valdecneg"
		field := "fdecneg"

		_, err := m0.API.CreateIndex(ctx, index, pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = m0.API.CreateField(ctx, index, field, pilosa.OptFieldTypeDecimal(-1))
		if err == nil {
			t.Fatal("expected error creating field")
		}
	})

	t.Run("ValTimestampField", func(t *testing.T) {
		ctx := context.Background()
		index := "valts"
		field := "fts"

		_, err := m1.API.CreateIndex(ctx, index, pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = m1.API.CreateField(ctx, index, field, pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		// Generate some records.
		values := []time.Time{}
		colIDs := []uint64{}
		for i := 0; i < 10; i++ {
			values = append(values, pilosa.MinTimestamp.Add(time.Duration(i)*time.Second))
			colIDs = append(colIDs, uint64(i))
		}

		// Import data with keys to node1 and verify that it gets translated and
		// forwarded to the owner of shard 0 (node0; because of offsetModHasher)
		req := &pilosa.ImportValueRequest{
			Index:           index,
			Field:           field,
			ColumnIDs:       colIDs,
			TimestampValues: values,
		}

		qcx := m2.API.Txf().NewQcx()
		if err := m2.API.ImportValue(ctx, qcx, req); err != nil {
			t.Fatal(err)
		}
		PanicOn(qcx.Finish())

		query := fmt.Sprintf("Row(%s>='1833-11-24T17:31:50Z')", field) // 6s after MinTimestamp

		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: query}); err != nil {
			t.Fatal(err)
		} else if ids := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(ids, colIDs[6:]) {
			t.Fatalf("unexpected column keys: observerd %+v;  expected '%+v'", ids, colIDs[6:])
		}
	})

	t.Run("ValStringField", func(t *testing.T) {
		ctx := context.Background()
		index := "valstr"
		field := "fstr"

		fgnIndex := "fgnvalstr"

		_, err := coord.API.CreateIndex(ctx, index, pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}

		_, err = coord.API.CreateIndex(ctx, fgnIndex, pilosa.IndexOptions{Keys: true})
		if err != nil {
			t.Fatalf("creating foreign index: %v", err)
		}
		_, err = coord.API.CreateField(ctx, index, field,
			pilosa.OptFieldTypeInt(0, math.MaxInt64),
			pilosa.OptFieldForeignIndex(fgnIndex),
		)
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		// Generate some keyed records.
		values := []string{}
		colIDs := []uint64{}
		for i := 0; i < 10; i++ {
			value := fmt.Sprintf("strval-%d", (i)*100+10)
			values = append(values, value)
			colIDs = append(colIDs, uint64(i))
		}

		// Import data with keys to the node0 and verify that it gets translated
		// and forwarded to the owner of shard 0 (node1; because of
		// offsetModHasher)
		req := &pilosa.ImportValueRequest{
			Index:        index,
			Field:        field,
			ColumnIDs:    colIDs,
			StringValues: values,
		}
		qcx := m0.API.Txf().NewQcx()
		if err := m0.API.ImportValue(ctx, qcx, req); err != nil {
			t.Fatal(err)
		}
		PanicOn(qcx.Finish())

		pql := fmt.Sprintf(`Row(%s=="strval-110")`, field)

		// Query node1.
		if res, err := m1.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if ids := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(ids, []uint64{1}) {
			t.Fatalf("unexpected columns: observerd %+v;  expected '%+v'", ids, []uint64{1})
		}
	})
}

func TestAPI_Ingest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := test.MustRunCluster(t, 1,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
	)
	defer c.Close()

	coord := c.GetPrimary()
	// m0 := c.GetNode(0)
	// m1 := c.GetNode(1)
	// m2 := c.GetNode(2)

	index := "ingest"
	setField := "set"
	timeField := "tq"

	_, err := coord.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: false})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = coord.API.CreateField(ctx, index, setField, pilosa.OptFieldTypeSet("none", 0))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	_, err = coord.API.CreateField(ctx, index, timeField, pilosa.OptFieldTypeTime("YMD"))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	sampleJson := []byte(`
[
  {
    "action": "set",
    "records": {
      "2": {
	"set": [2],
	"tq": { "time": "2006-01-02T15:04:05.999999999Z", "values": [6] }
      },
      "5": { "set": [3] },
      "8": { "set": [3] },
      "1": {
	"set": [2],
	"tq": { "time": "2006-01-02T15:04:05.999999999Z", "values": [3, 4] }
      },
      "4": { "set": [3, 7] }
    }
  },
  {
    "action": "clear",
    "record_ids": [ 5, 6, 7 ],
    "fields": [ "tq", "set" ]
  },
  {
    "action": "write",
    "records": {
      "8": { "tq": { "time": "2006-01-02T15:04:05.999999999Z", "values": [3, 4] } },
      "9": { "set": [7, 3] }
    }
  },
  {
    "action": "delete",
    "record_ids": [ 9 ]
  }
]
`)
	// just for set row 3:
	// first operation should set it for 4, 5, and 8.
	// clear operation should clear it for 5, 6, and 7, leaving it still set for 4 and 8.
	// the write operation should clear set for record 8, even though record 8 doesn't
	// contain that field in that op, because set is present in record 9, which also
	// gets row 3 set. but then we delete 9.
	// so after all that we expect Row(set=3) to be 4...
	sampleBuf := bytes.NewBuffer(sampleJson)
	qcx := coord.API.Txf().NewQcx()
	defer func() {
		if err := qcx.Finish(); err != nil {
			t.Fatalf("finishing qcx: %v", err)
		}
	}()
	err = coord.API.IngestOperations(ctx, qcx, index, sampleBuf)
	if err != nil {
		t.Fatalf("importing data: %v", err)
	}
	query := "Row(set=3)"
	res, err := coord.API.Query(context.Background(), &pilosa.QueryRequest{Index: index, Query: query})
	if err != nil {
		t.Errorf("query: %v", err)
	}
	r := res.Results[0].(*pilosa.Row).Columns()
	if len(r) != 1 || r[0] != 4 {
		t.Fatalf("expected row with 4 set, got %d", r)
	}
}

// ingestBenchmarkHelper makes it easier to exclude this from benchmark computations
// and profiles.
func ingestBenchmarkHelper() []byte {
	buf := &bytes.Buffer{}
	buf.WriteString(`[{"action": "write", "records": {`)
	comma := ""
	now := time.Now().Add(-3840000 * time.Second)
	for i := 0; i < 1000000; i++ {
		then := now.Add(time.Duration(rand.Int63n(1234567)) * time.Second)
		fmt.Fprintf(buf, `%s"%d": { "set": [%d, %d], "int": %d, "tq": { "time": "%s", "values": %d } }`, comma, i, i%2, (i%4)+2, rand.Int63n(163840),
			then.Format(time.RFC3339), rand.Int63n(25))
		comma = ", "
	}
	buf.WriteString(`}}]`)
	data := buf.Bytes()
	return data
}

func BenchmarkIngest(b *testing.B) {
	b.StopTimer()
	data := ingestBenchmarkHelper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := test.MustRunCluster(b, 1,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
	)
	defer c.Close()

	coord := c.GetPrimary()
	m0 := c.GetNode(0)
	// m1 := c.GetNode(1)
	// m2 := c.GetNode(2)

	index := "ingest"
	setField := "set"
	intField := "int"
	tqField := "tq"
	_, err := coord.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: false})
	if err != nil {
		b.Fatalf("creating index: %v", err)
	}
	_, err = coord.API.CreateField(ctx, index, setField, pilosa.OptFieldTypeSet("none", 0))
	if err != nil {
		b.Fatalf("creating field: %v", err)
	}
	_, err = coord.API.CreateField(ctx, index, intField, pilosa.OptFieldTypeInt(0, 163840))
	if err != nil {
		b.Fatalf("creating field: %v", err)
	}
	_, err = coord.API.CreateField(ctx, index, tqField, pilosa.OptFieldTypeTime("YMDH"))
	if err != nil {
		b.Fatalf("creating field: %v", err)
	}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		qcx := m0.API.Txf().NewQcx()
		defer qcx.Abort()
		err = coord.API.IngestOperations(ctx, qcx, index, bytes.NewBuffer(data))
		if err != nil {
			b.Fatalf("ingest: %v", err)
		}
		err = qcx.Finish()
		if err != nil {
			b.Fatalf("finish: %v", err)
		}
	}
}

// offsetModHasher represents a simple, mod-based hashing offset by 1.
type offsetModHasher struct{}

func (*offsetModHasher) Hash(key uint64, n int) int {
	return int(key+1) % n
}

func (*offsetModHasher) Name() string { return "mod" }

func TestAPI_ClearFlagForImportAndImportValues(t *testing.T) {
	c := test.MustRunCluster(t, 1,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerClusterHasher(&offsetModHasher{}),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			)},
	)
	defer c.Close()

	// plan:
	//  1. set a bit
	//  2. clear with Import() using the ImportRequest.Clear flag
	//  3. verifiy the clear is done.
	//  repeat for ImportValueRequest and ImportValues()

	m0 := c.GetNode(0)
	m0api := m0.API

	ctx := context.Background()
	index := "i"
	fieldAcct0 := "acct0"

	opts := pilosa.OptFieldTypeInt(-1000, 1000)

	_, err := m0api.CreateIndex(ctx, index, pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = m0api.CreateField(ctx, index, fieldAcct0, opts)
	if err != nil {
		t.Fatalf("creating fieldAcct0: %v", err)
	}

	iraField := "ira" // set field.
	iraRowID := uint64(3)
	_, err = m0api.CreateField(ctx, index, iraField)
	if err != nil {
		t.Fatalf("creating fieldIRA: %v", err)
	}

	acctOwnerID := uint64(78) // ColumnID
	shard := acctOwnerID / ShardWidth
	acct0bal := int64(500)

	ivr0 := &pilosa.ImportValueRequest{
		Index:     index,
		Field:     fieldAcct0,
		Shard:     shard,
		ColumnIDs: []uint64{acctOwnerID},
		Values:    []int64{acct0bal},
	}
	ir0 := &pilosa.ImportRequest{
		Index:     index,
		Field:     iraField,
		Shard:     shard,
		ColumnIDs: []uint64{acctOwnerID},
		RowIDs:    []uint64{iraRowID},
	}

	qcx := m0api.Txf().NewQcx()
	if err := m0api.Import(ctx, qcx, ir0.Clone()); err != nil {
		t.Fatal(err)
	}
	if err := m0api.ImportValue(ctx, qcx, ivr0.Clone()); err != nil {
		t.Fatal(err)
	}
	PanicOn(qcx.Finish())

	bitIsSet := func() bool {
		query := fmt.Sprintf("Row(%v=%v)", iraField, iraRowID)
		res, err := m0api.Query(context.Background(), &pilosa.QueryRequest{Index: index, Query: query})
		PanicOn(err)
		cols := res.Results[0].(*pilosa.Row).Columns()
		for i := range cols {
			if cols[i] == acctOwnerID {
				return true
			}
		}
		return false
	}

	if !bitIsSet() {
		PanicOn("IRA bit should have been set")
	}

	queryAcct := func(m0api *pilosa.API, acctOwnerID uint64, fieldAcct0, index string) (acctBal int64) {
		query := fmt.Sprintf("FieldValue(field=%v, column=%v)", fieldAcct0, acctOwnerID)
		res, err := m0api.Query(context.Background(), &pilosa.QueryRequest{Index: index, Query: query})
		PanicOn(err)

		if len(res.Results) == 0 {
			return 0
		}
		valCount := res.Results[0].(pilosa.ValCount)
		return valCount.Val
	}

	bal := queryAcct(m0api, acctOwnerID, fieldAcct0, index)

	if bal != acct0bal {
		PanicOn(fmt.Sprintf("expected %v, observed %v starting acct0 balance", acct0bal, bal))
	}

	// clear the bit
	qcx = m0api.Txf().NewQcx()
	ir0.Clear = true
	if err := m0api.Import(ctx, qcx, ir0); err != nil {
		t.Fatal(err)
	}
	PanicOn(qcx.Finish())

	if bitIsSet() {
		PanicOn("IRA bit should have been cleared")
	}

	// clear the BSI
	qcx = m0api.Txf().NewQcx()
	ivr0.Clear = true
	if err := m0api.ImportValue(ctx, qcx, ivr0); err != nil {
		t.Fatal(err)
	}
	PanicOn(qcx.Finish())

	bal = queryAcct(m0api, acctOwnerID, fieldAcct0, index)
	if bal != 0 {
		PanicOn(fmt.Sprintf("expected %v, observed %v starting acct0 balance", acct0bal, 0))
	}
}

func TestAPI_IDAlloc(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	primary := c.GetPrimary().API

	t.Run("Normal", func(t *testing.T) {
		key := pilosa.IDAllocKey{
			Index: "normal",
			Key:   "key",
		}
		var session [32]byte
		_, err := rand.Read(session[:])
		if err != nil {
			t.Fatalf("obtaining random bytes: %v", err)
		}

		const toReserve = 2

		ids, err := primary.ReserveIDs(key, session, ^uint64(0), toReserve)
		if err != nil {
			t.Fatalf("reserving IDs: %v", err)
		}

		var numIds uint64
		for _, idr := range ids {
			numIds += (idr.Last - idr.First) + 1
		}
		if numIds != toReserve {
			t.Errorf("expected %d ids but got %d: %v", toReserve, numIds, ids)
		}

		err = primary.CommitIDs(key, session, numIds)
		if err != nil {
			t.Fatalf("committing IDs: %v", err)
		}

		err = primary.ResetIDAlloc(key.Index)
		if err != nil {
			t.Fatalf("resetting ID alloc: %v", err)
		}
	})
	t.Run("Offset", func(t *testing.T) {
		key := pilosa.IDAllocKey{
			Index: "offset",
			Key:   "key",
		}
		var session [32]byte
		_, err := rand.Read(session[:])
		if err != nil {
			t.Fatalf("obtaining random bytes: %v", err)
		}

		ids, err := primary.ReserveIDs(key, session, 0, 2)
		if err != nil {
			t.Fatalf("reserving IDs: %v", err)
		}

		{
			var numIds uint64
			for _, idr := range ids {
				numIds += (idr.Last - idr.First) + 1
			}
			if numIds != 2 {
				t.Errorf("expected %d ids but got %d: %v", 2, numIds, ids)
			}
		}

		_, err = rand.Read(session[:])
		if err != nil {
			t.Fatalf("obtaining random bytes: %v", err)
		}
		ids2, err := primary.ReserveIDs(key, session, 1, 2)
		if err != nil {
			t.Fatalf("reserving IDs with partially increased offset: %v", err)
		}

		var numIds uint64
		for _, idr := range ids2 {
			numIds += (idr.Last - idr.First) + 1
		}
		if numIds != 2 {
			t.Errorf("expected %d ids but got %d: %v", 2, numIds, ids2)
		}

		if prevEnd, newStart := ids[len(ids)-1].Last, ids2[0].First; prevEnd != newStart {
			t.Errorf("expected reuse of last ID (%d), but started with %d", prevEnd, newStart)
		}

		err = primary.CommitIDs(key, session, numIds)
		if err != nil {
			t.Errorf("committing IDs: %v", err)
		}

		_, err = rand.Read(session[:])
		if err != nil {
			t.Fatalf("obtaining random bytes: %v", err)
		}
		ids3, err := primary.ReserveIDs(key, session, 0, 2)
		var esync pilosa.ErrIDOffsetDesync
		if errors.As(err, &esync) {
			if esync.Requested != 0 {
				t.Errorf("incorrect requested offset in error: provided %d but got %d", 0, esync.Requested)
			}
			if esync.Base != 3 {
				t.Errorf("incorrect base offset: expected %d but got %d", 3, esync.Base)
			}
		} else if err == nil {
			t.Errorf("successfully re-reserved at a committed offset: %v", ids3)
		} else {
			t.Fatalf("unexpected error when reserving committed IDs: %v", err)
		}

		err = primary.ResetIDAlloc(key.Index)
		if err != nil {
			t.Fatalf("resetting ID alloc: %v", err)
		}
	})
}

func TestAPI_SchemaDetailsOff(t *testing.T) {
	cluster := test.MustRunCluster(t, 2)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
	err := cmd.API.SetAPIOptions(pilosa.OptAPISchemaDetailsOn(false))
	if err != nil {
		t.Fatalf("could not toggle schema details to off: %v", err)
	}
	schema, err := cmd.API.SchemaDetails(context.Background())
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}

	for _, i := range schema {
		for _, f := range i.Fields {
			if f.Cardinality != nil {
				t.Fatalf("expected nil cardinality, got: %v", *f.Cardinality)
			}
		}
	}

}

type mutexCheckIndex struct {
	index     *pilosa.Index
	indexName string
	createdAt int64
	fields    map[bool]mutexCheckField
}

type mutexCheckField struct {
	fieldName string
	field     *pilosa.Field
	createdAt int64
}

func TestAPI_MutexCheck(t *testing.T) {
	c := test.MustNewCluster(t, 3)
	for _, c := range c.Nodes {
		c.Config.Cluster.ReplicaN = 2
	}
	if err := c.Start(); err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	defer c.Close()

	m0 := c.GetNode(0)
	nodesByID := make(map[string]*test.Command, 3)
	qcxsByID := make(map[string]*pilosa.Qcx, 3)
	for i := 0; i < 3; i++ {
		node := c.GetNode(i)
		id := node.API.NodeID()
		nodesByID[id] = node
	}

	indexes := make(map[bool]mutexCheckIndex)

	ctx := context.Background()
	for _, keyedIndex := range []bool{false, true} {
		indexName := fmt.Sprintf("i%t", keyedIndex)
		index, err := m0.API.CreateIndex(ctx, indexName, pilosa.IndexOptions{Keys: keyedIndex, TrackExistence: true})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		if index.CreatedAt() == 0 {
			t.Fatal("index createdAt is empty")
		}
		indexData := mutexCheckIndex{indexName: indexName, index: index, fields: make(map[bool]mutexCheckField), createdAt: index.CreatedAt()}
		for _, keyedField := range []bool{false, true} {
			fieldName := fmt.Sprintf("f%t", keyedField)
			var field *pilosa.Field
			if keyedField {
				field, err = m0.API.CreateField(ctx, indexName, fieldName, pilosa.OptFieldTypeMutex(pilosa.CacheTypeNone, 0), pilosa.OptFieldKeys())
			} else {
				field, err = m0.API.CreateField(ctx, indexName, fieldName, pilosa.OptFieldTypeMutex(pilosa.CacheTypeNone, 0))
			}
			if err != nil {
				t.Fatalf("creating field: %v", err)
			}
			if field.CreatedAt() == 0 {
				t.Fatal("field createdAt is empty")
			}
			indexData.fields[keyedField] = mutexCheckField{fieldName: fieldName, field: field, createdAt: field.CreatedAt()}
		}
		indexes[keyedIndex] = indexData
	}

	rowIDs := []uint64{0, 1, 2, 3}
	colIDs := []uint64{0, 1, 2, 3}
	rowKeysBase := []string{"v0", "v1", "v2", "v3"}
	colKeysBase := []string{"c0", "c1", "c2", "c3"}

	const nShards = 10

	// now, try the same thing for each combination of keyed/unkeyed. we
	// share code between keyed/unkeyed fields, but for indexes, the logic
	// is fundamentally different because we can't know shards in advance.
	indexData := indexes[false]
	for keyedField, fieldData := range indexData.fields {
		t.Run(fmt.Sprintf("%s-%s", indexData.indexName, fieldData.fieldName), func(t *testing.T) {
			for id, node := range nodesByID {
				qcxsByID[id] = node.API.Txf().NewQcx()
			}
			for shard := uint64(0); shard < nShards; shard++ {
				// restore row/col ID values which can get altered by imports
				for i := range rowIDs {
					rowIDs[i] = uint64(i)
					colIDs[i] = (shard << shardwidth.Exponent) + uint64(i) + (shard % 4)
				}
				req := &pilosa.ImportRequest{
					Index:          indexData.indexName,
					IndexCreatedAt: indexData.createdAt,
					Field:          fieldData.fieldName,
					FieldCreatedAt: fieldData.createdAt,
					Shard:          shard,
					ColumnIDs:      colIDs,
				}
				if keyedField {
					req.RowKeys = rowKeysBase
				} else {
					req.RowIDs = rowIDs
				}
				nodesForShard, err := m0.API.ShardNodes(ctx, indexData.indexName, shard)
				if err != nil {
					t.Fatalf("obtaining shard list: %v", err)
				}
				if len(nodesForShard) < 1 {
					t.Fatalf("no nodes for shard %d", shard)
				}
				node := nodesByID[nodesForShard[0].ID]
				if err := node.API.Import(ctx, qcxsByID[nodesForShard[0].ID], req); err != nil {
					t.Fatalf("importing data: %v", err)
				}
			}
			// and then we break the mutex and close the Qcxs
			for id, node := range nodesByID {
				field, err := node.API.Field(ctx, indexData.indexName, fieldData.fieldName)
				if err != nil {
					t.Fatalf("requesting field %s from node %s: %v", fieldData.fieldName, id, err)
				}
				pilosa.CorruptAMutex(t, field, qcxsByID[id])
				err = qcxsByID[id].Finish()
				if err != nil {
					t.Fatalf("closing out transaction on node %s: %v", id, err)
				}
			}
			qcx := m0.API.Txf().NewQcx()
			defer qcx.Abort()

			// first two shards of each group of 4 should have a collision in
			// position 1
			expected := map[uint64]bool{
				(0 << shardwidth.Exponent) + 1: true,
				(1 << shardwidth.Exponent) + 1: true,
				(4 << shardwidth.Exponent) + 1: true,
				(5 << shardwidth.Exponent) + 1: true,
				(8 << shardwidth.Exponent) + 1: true,
				(9 << shardwidth.Exponent) + 1: true,
			}

			results, err := m0.API.MutexCheck(ctx, qcx, indexData.indexName, fieldData.fieldName, true, 0)
			if err != nil {
				t.Fatalf("checking mutexes: %v", err)
			}

			if keyedField {
				mapped, ok := results.(map[uint64][]string)
				if !ok {
					t.Fatalf("expected map[uint64][]string, got %T", results)
				}
				seen := 0
				for k, v := range mapped {
					seen++
					if !expected[k] {
						t.Fatalf("expected all collisions to be 1 shards (s %% 4 in [0,1]), got %d", k)
					}
					if len(v) != 2 {
						t.Fatalf("expected exactly two collisions")
					}
				}
				if seen != len(expected) {
					t.Fatalf("expected exactly %d records to have collisions", len(expected))
				}
			} else {
				mapped, ok := results.(map[uint64][]uint64)
				if !ok {
					t.Fatalf("expected map[uint64][]uint64, got %T", results)
				}
				seen := 0
				for k, v := range mapped {
					seen++
					if !expected[k] {
						t.Fatalf("expected all collisions to be 1 shards (s %% 4 in [0,1]), got %d", k)
					}
					if len(v) != 2 {
						t.Fatalf("expected exactly two collisions")
					}
				}
				if seen != len(expected) {
					t.Fatalf("expected exactly %d records to have collisions, got %d", len(expected), seen)
				}
			}

			// and let's try with no details and a limit of 3...
			results, err = m0.API.MutexCheck(ctx, qcx, indexData.indexName, fieldData.fieldName, false, 3)
			if err != nil {
				t.Fatalf("checking mutexes: %v", err)
			}
			mapped, ok := results.([]uint64)
			if !ok {
				t.Fatalf("expected []uint64, got %T", results)
			}
			seen := 0
			for _, k := range mapped {
				seen++
				if !expected[k] {
					t.Fatalf("expected all collisions to be position 1 in shards (s %% 4 in [0,1]), got %d", k)
				}
			}
			if seen != 3 {
				t.Fatalf("expected results limited to 3, got %d", seen)
			}
		})
	}
	indexData = indexes[true]
	for keyedField, fieldData := range indexData.fields {
		t.Run(fmt.Sprintf("%s-%s", indexData.indexName, fieldData.fieldName), func(t *testing.T) {
			for id, node := range nodesByID {
				qcxsByID[id] = node.API.Txf().NewQcx()
			}
			req := &pilosa.ImportRequest{
				Index:          indexData.indexName,
				IndexCreatedAt: indexData.createdAt,
				Field:          fieldData.fieldName,
				FieldCreatedAt: fieldData.createdAt,
				Shard:          0, // ignored when using keys
			}
			rowKeys := make([]string, 0, len(rowKeysBase)*nShards)
			colKeys := make([]string, 0, len(rowKeysBase)*nShards)
			rowIDs = rowIDs[:0]
			for shard := uint64(0); shard < nShards; shard++ {
				for i := range rowKeysBase {
					colKeys = append(colKeys, fmt.Sprintf("s%d-%s", shard, colKeysBase[i]))
					if keyedField {
						rowKeys = append(rowKeys, rowKeysBase[i])
					} else {
						rowIDs = append(rowIDs, uint64(i))
					}
				}
			}
			req.ColumnKeys = colKeys
			if keyedField {
				req.RowKeys = rowKeys
			} else {
				req.RowIDs = rowIDs
			}
			var id string
			var node *test.Command
			for id, node = range nodesByID {
				break
			}
			if err := node.API.Import(ctx, qcxsByID[id], req); err != nil {
				t.Fatalf("importing data: %v", err)
			}
			expected, err := node.API.FindIndexKeys(ctx, indexData.indexName, colKeys...)
			if err != nil {
				t.Fatalf("looking up index keys: %v", err)
			}
			for key, id := range expected {
				// CorruptAMutex should only corrupt things in position 1 of their
				// shards...
				if id%(1<<shardwidth.Exponent) != 1 {
					delete(expected, key)
				}
			}
			if keyedField {
				fieldValues, err := node.API.FindFieldKeys(ctx, indexData.indexName, fieldData.fieldName, rowKeys...)
				if err != nil {
					t.Fatalf("looking up field keys: %v", err)
				}
				// Figure out which key got the value 3, delete any records
				// which would have had that key, because they won't be
				// conflicts.
				for key, value := range fieldValues {
					if value == 3 {
						for offset, baseKey := range rowKeysBase {
							if baseKey == key {
								for i := offset; i < len(rowKeys); i += len(rowKeysBase) {
									delete(expected, colKeys[i])
								}
							}
						}
					}
				}
			} else {
				// we set rowKeys to 0-1-2-... for rowKeysBase items, which
				// tells us which keys we expect to be 3 already.
				for i := 3; i < len(rowIDs); i += len(rowKeysBase) {
					delete(expected, colKeys[i])
				}
			}
			// and then we break the mutex and close the Qcxs
			for id, node := range nodesByID {
				field, err := node.API.Field(ctx, indexData.indexName, fieldData.fieldName)
				if err != nil {
					t.Fatalf("requesting field %s from node %s: %v", fieldData.fieldName, id, err)
				}
				pilosa.CorruptAMutex(t, field, qcxsByID[id])
				err = qcxsByID[id].Finish()
				if err != nil {
					t.Fatalf("closing out transaction on node %s: %v", id, err)
				}
			}
			qcx := m0.API.Txf().NewQcx()
			defer qcx.Abort()

			results, err := m0.API.MutexCheck(ctx, qcx, indexData.indexName, fieldData.fieldName, true, 0)
			if err != nil {
				t.Fatalf("checking mutexes: %v", err)
			}
			if keyedField {
				mapped, ok := results.(map[string][]string)
				if !ok {
					t.Fatalf("expected map[string][]string, got %T", results)
				}
				seen := 0
				for k, v := range mapped {
					seen++
					if _, ok := expected[k]; !ok {
						t.Fatalf("unexpected collision on key %q", k)
					}
					if len(v) != 2 {
						t.Fatalf("expected exactly two collisions")
					}
				}
				if seen != len(expected) {
					t.Fatalf("expected exactly %d records to have collisions, got %d", len(expected), seen)
				}
			} else {
				mapped, ok := results.(map[string][]uint64)
				if !ok {
					t.Fatalf("expected map[string][]uint64, got %T", results)
				}
				seen := 0
				for k, v := range mapped {
					seen++
					if _, ok := expected[k]; !ok {
						t.Fatalf("unexpected collision on key %q", k)
					}
					if len(v) != 2 {
						t.Fatalf("expected exactly two collisions")
					}
				}
				if seen != len(expected) {
					t.Fatalf("expected exactly %d records to have collisions, got %d", len(expected), seen)
				}
			}

			results, err = m0.API.MutexCheck(ctx, qcx, indexData.indexName, fieldData.fieldName, false, 3)
			if err != nil {
				t.Fatalf("checking mutexes: %v", err)
			}
			mapped, ok := results.([]string)
			if !ok {
				t.Fatalf("expected []string, got %T", results)
			}
			seen := 0
			for _, k := range mapped {
				seen++
				if _, ok := expected[k]; !ok {
					t.Fatalf("unexpected collision on key %q", k)
				}
			}
			if seen != 3 {
				t.Fatalf("expected results limited to 3, got %d", len(expected))
			}
		})
	}
}

func createIndexForTest(index string, coord *test.Command, t *testing.T) {
	ctx := context.Background()
	_, err := coord.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: true})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
}

func createFieldForTest(index string, field string, coord *test.Command, t *testing.T) {
	ctx := context.Background()
	_, err := coord.API.CreateField(ctx, index, field, pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
}

func TestVariousApiTranslateCalls(t *testing.T) {
	for i := 1; i < 8; i += 3 {
		m := test.MustRunCluster(t, i)
		defer m.Close()
		node := m.GetNode(0)
		api := node.API
		// this should never actually get used because we're testing for errors here
		r := strings.NewReader("")
		// test index
		idx, err := api.Holder().CreateIndex("index", pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("%v: could not create test index", err)
		}
		_, err = idx.CreateFieldIfNotExistsWithOptions("field", &pilosa.FieldOptions{Keys: false})
		t.Run("translateIndexDbOnNilIndex",
			func(t *testing.T) {
				err := api.TranslateIndexDB(context.Background(), "nonExistentIndex", 0, r)
				expected := fmt.Errorf("index %q not found", "nonExistentIndex")
				if !reflect.DeepEqual(err, expected) {
					t.Fatalf("expected '%#v', got '%#v'", expected, err)
				}
			})

		t.Run("translateIndexDbOnNilTranslateStore",
			func(t *testing.T) {
				err := api.TranslateIndexDB(context.Background(), "index", 0, r)
				expected := fmt.Errorf("index %q has no translate store", "index")
				if !reflect.DeepEqual(err, expected) {
					t.Fatalf("expected '%#v', got '%#v'", expected, err)
				}
			})

		t.Run("translateFieldDbOnNilIndex",
			func(t *testing.T) {
				err := api.TranslateFieldDB(context.Background(), "nonExistentIndex", "field", r)
				expected := fmt.Errorf("index %q not found", "nonExistentIndex")
				if !reflect.DeepEqual(err, expected) {
					t.Fatalf("expected '%#v', got '%#v'", expected, err)
				}
			})

		t.Run("translateFieldDbOnNilField",
			func(t *testing.T) {
				err := api.TranslateFieldDB(context.Background(), "index", "nonExistentField", r)
				expected := fmt.Errorf("field %q/%q not found", "index", "nonExistentField")
				if !reflect.DeepEqual(err, expected) {
					t.Fatalf("expected '%#v', got '%#v'", expected, err)
				}
			})

		t.Run("translateFieldDbNilField_keys",
			func(t *testing.T) {
				err := api.TranslateFieldDB(context.Background(), "index", "_keys", r)
				if err != nil {
					t.Fatalf("expected 'nil', got '%#v'", err)
				}
			})

		t.Run("translateFieldDbOnNilTranslateStore",
			func(t *testing.T) {
				err := api.TranslateFieldDB(context.Background(), "index", "field", r)
				expected := fmt.Errorf("field %q/%q has no translate store", "index", "field")
				if !reflect.DeepEqual(err, expected) {
					t.Fatalf("expected '%#v', got '%#v'", expected, err)
				}
			})
	}
}
