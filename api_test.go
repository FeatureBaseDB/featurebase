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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/test"
)

// attrFun defines a mapping from columnID -> attr value
func attrFun(id uint64) string {
	//return fmt.Sprintf("%x", md5.Sum([]byte(strconv.FormatInt(int64(id), 10))))
	return strconv.FormatInt(int64(id), 10)
}

func TestAPI_ImportColumnAttrs(t *testing.T) {
	/*
	   columns seconds
	   100      1.150
	   1000     1.568
	   10000    5.156
	   100000  38.179
	*/
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
	t.Run("ImportColumnAttrs", func(t *testing.T) {
		ctx := context.Background()
		index := "i"
		field := "f"
		attrKey := "k"

		_, err := m0.API.CreateIndex(ctx, index, pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = m0.API.CreateField(ctx, index, field)
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		// Generate some attrs for two shards
		numAttrs := 100
		columnIDs0 := make([]uint64, 0, numAttrs)
		attrVals0 := make([]string, 0, numAttrs)
		columnIDs1 := make([]uint64, 0, numAttrs)
		attrVals1 := make([]string, 0, numAttrs)
		for n := 0; n < 1000000; n += 1000000 / numAttrs {
			columnIDs0 = append(columnIDs0, uint64(n))
			val0 := attrFun(uint64(n))
			attrVals0 = append(attrVals0, val0)
			setPql0 := fmt.Sprintf("Set(%d, %s=0) ", n, field)
			m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: setPql0})

			columnIDs1 = append(columnIDs1, uint64(n+ShardWidth))
			val1 := attrFun(uint64(n + ShardWidth))
			attrVals1 = append(attrVals1, val1)
			setPql1 := fmt.Sprintf("Set(%d, %s=0) ", n+ShardWidth, field)
			m1.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: setPql1})
		}

		// send shard0 to node1
		req := &pilosa.ImportColumnAttrsRequest{
			AttrKey:   attrKey,
			ColumnIDs: columnIDs0,
			AttrVals:  attrVals0,
			Shard:     0,
			Index:     index,
		}

		if err := m1.API.ImportColumnAttrs(ctx, req); err != nil {
			t.Fatal(err)
		}

		// send shard1 to node0
		req = &pilosa.ImportColumnAttrsRequest{
			AttrKey:   attrKey,
			ColumnIDs: columnIDs1,
			AttrVals:  attrVals1,
			Shard:     1,
			Index:     index,
		}

		if err := m0.API.ImportColumnAttrs(ctx, req); err != nil {
			t.Fatal(err)
		}

		// Query node0.
		pql := fmt.Sprintf("Options(Row(%s=0), columnAttrs=true)", field)
		res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.ColumnAttrSets) != 100 {
			t.Fatal("incorrect number of column attrs set")
		}

		for _, v := range res.ColumnAttrSets {
			attrVal := attrFun(v.ID)
			if attrVal != v.Attrs[attrKey] {
				t.Fatal(err)
			}
		}
		// Query node1.
		pql = fmt.Sprintf("Options(Row(%s=0), columnAttrs=true)", field)
		res, err = m1.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.ColumnAttrSets) != 100 {
			t.Fatal("incorrect number of column attrs set")
		}

		for _, v := range res.ColumnAttrSets {
			attrVal := attrFun(v.ID)
			if attrVal != v.Attrs[attrKey] {
				t.Fatal(err)
			}
		}

	})
}

func TestAPI_Import(t *testing.T) {
	c := test.MustRunCluster(t, 2,
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
			t.Fatalf("unexpected column keys: %#v", keys)
		}

		// Query node1.
		if err := test.RetryUntil(5*time.Second, func() error {
			if res, err := m1.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
				return err
			} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
				return fmt.Errorf("unexpected column keys: %#v", keys)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
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
		if err := test.RetryUntil(5*time.Second, func() error {
			if res, err := m1.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
				return err
			} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, colIDs) {
				return fmt.Errorf("unexpected column ids: %+v", columns)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAPI_ImportValue(t *testing.T) {
	c := test.MustRunCluster(t, 2,
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

	t.Run("ValDecimalField", func(t *testing.T) {
		ctx := context.Background()
		index := "valdec"
		field := "fdec"

		_, err := m1.API.CreateIndex(ctx, index, pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		fld, err := m1.API.CreateField(ctx, index, field, pilosa.OptFieldTypeDecimal(1))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		// Generate some keyed records.
		values := []float64{}
		colIDs := []uint64{}
		for i := 0; i < 10; i++ {
			values = append(values, float64(i)+0.1)
			colIDs = append(colIDs, uint64(i))
		}

		// Import data with keys to the coordinator (node0) and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
		req := &pilosa.ImportValueRequest{
			Index:       index,
			Field:       field,
			ColumnIDs:   colIDs,
			FloatValues: values,
		}
		if err := m1.API.ImportValue(ctx, req); err != nil {
			t.Fatal(err)
		}

		pql := fmt.Sprintf("Row(%s>6)", field)

		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if ids := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(ids, colIDs[6:]) {
			t.Fatalf("unexpected column keys: %+v", ids)
		}

		sum, count, err := fld.FloatSum(nil, field)
		if err != nil {
			t.Fatalf("getting floatsum: %v", err)
		} else if sum != 0.1+1.1+2.1+3.1+4.1+5.1+6.1+7.1+8.1+9.1 {
			t.Fatalf("unexpected sum: %f", sum)
		} else if count != 10 {
			t.Fatalf("unexpected count: %d", count)
		}

		min, count, err := fld.FloatMin(nil, field)
		if err != nil {
			t.Fatalf("getting floatmin: %v", err)
		} else if min != 0.1 {
			t.Fatalf("unexpected min: %f", min)
		} else if count != 1 {
			t.Fatalf("unexpected count: %d", count)
		}

		max, count, err := fld.FloatMax(nil, field)
		if err != nil {
			t.Fatalf("getting floatmax: %v", err)
		} else if max != 9.1 {
			t.Fatalf("unexpected max: %f", max)
		} else if count != 1 {
			t.Fatalf("unexpected count: %d", count)
		}

		val, exists, err := fld.FloatValue(1)
		if err != nil {
			t.Fatalf("unepxected err getting floatvalue")
		} else if !exists {
			t.Fatalf("column 1 should exist")
		} else if val != 1.1 {
			t.Fatalf("unexpected floatvalue %f", val)
		}

		changed, err := fld.SetFloatValue(11, 11.1)
		if err != nil {
			t.Fatalf("setting float value: %v", err)
		} else if !changed {
			t.Fatalf("expected change")
		}

		val, exists, err = fld.FloatValue(11)
		if err != nil {
			t.Fatalf("getting float val: %v", err)
		} else if !exists {
			t.Fatalf("should exist")
		} else if val != 11.1 {
			t.Fatalf("unexpected val: %f", 11.1)
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
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		// Generate some keyed records.
		values := []float64{}
		colIDs := []uint64{}
		for i := 0; i < 10; i++ {
			values = append(values, float64(i)*100+10)
			colIDs = append(colIDs, uint64(i))
		}

		// Import data with keys to the coordinator (node0) and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
		req := &pilosa.ImportValueRequest{
			Index:       index,
			Field:       field,
			ColumnIDs:   colIDs,
			FloatValues: values,
		}
		if err := m1.API.ImportValue(ctx, req); err != nil {
			t.Fatal(err)
		}

		pql := fmt.Sprintf("Row(%s>600)", field)

		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if ids := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(ids, colIDs[6:]) {
			t.Fatalf("unexpected column keys: %+v", ids)
		}

	})
}

// offsetModHasher represents a simple, mod-based hashing offset by 1.
type offsetModHasher struct{}

func (*offsetModHasher) Hash(key uint64, n int) int {
	return int(key+1) % n
}
