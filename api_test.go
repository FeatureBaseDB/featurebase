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
	"github.com/pilosa/pilosa/v2/shardwidth"
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

	m0 := c.GetNode(0)
	m1 := c.GetNode(1)
	t.Run("ImportColumnAttrs", func(t *testing.T) {
		ctx := context.Background()
		indexName := "i"
		fieldName := "f"
		attrKey := "k"

		index, err := m0.API.CreateIndex(ctx, indexName, pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = m0.API.CreateField(ctx, indexName, fieldName)
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
			setPql0 := fmt.Sprintf("Set(%d, %s=0) ", n, fieldName)
			if _, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: setPql0}); err != nil {
				t.Fatal(err)
			}

			columnIDs1 = append(columnIDs1, uint64(n+ShardWidth))
			val1 := attrFun(uint64(n + ShardWidth))
			attrVals1 = append(attrVals1, val1)
			setPql1 := fmt.Sprintf("Set(%d, %s=0) ", n+ShardWidth, fieldName)
			if _, err := m1.API.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: setPql1}); err != nil {
				t.Fatal(err)
			}
		}

		// send shard0 to node1
		req := &pilosa.ImportColumnAttrsRequest{
			AttrKey:        attrKey,
			ColumnIDs:      columnIDs0,
			AttrVals:       attrVals0,
			Shard:          0,
			Index:          indexName,
			IndexCreatedAt: index.CreatedAt(),
		}

		if err := m1.API.ImportColumnAttrs(ctx, req); err != nil {
			t.Fatal(err)
		}

		// send shard1 to node0
		req = &pilosa.ImportColumnAttrsRequest{
			AttrKey:        attrKey,
			ColumnIDs:      columnIDs1,
			AttrVals:       attrVals1,
			Shard:          1,
			Index:          indexName,
			IndexCreatedAt: index.CreatedAt(),
		}

		if err := m0.API.ImportColumnAttrs(ctx, req); err != nil {
			t.Fatal(err)
		}

		// Query node0.
		pql := fmt.Sprintf("Options(Row(%s=0), columnAttrs=true)", fieldName)
		res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql})
		if err != nil {
			t.Fatal(err)
		}
		m := len(res.ColumnAttrSets)
		if m != 100 {
			t.Fatalf("incorrect number of column attrs set; m = %v", m)
		}

		for _, v := range res.ColumnAttrSets {
			attrVal := attrFun(v.ID)
			if attrVal != v.Attrs[attrKey] {
				t.Fatal(err)
			}
		}
		// Query node1.
		pql = fmt.Sprintf("Options(Row(%s=0), columnAttrs=true)", fieldName)
		res, err = m1.API.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql})
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

	m0 := c.GetNode(0)
	m1 := c.GetNode(1)

	t.Run("RowIDColumnKey", func(t *testing.T) {
		ctx := context.Background()
		indexName := "rick"
		fieldName := "f"

		index, err := m0.API.CreateIndex(ctx, indexName, pilosa.IndexOptions{Keys: true, TrackExistence: true})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		if index.CreatedAt() == 0 {
			t.Fatal("index createdAt is empty")
		}

		field, err := m0.API.CreateField(ctx, indexName, fieldName, pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, 100))
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
			Index:          indexName,
			IndexCreatedAt: index.CreatedAt(),
			Field:          fieldName,
			FieldCreatedAt: field.CreatedAt(),
			Shard:          0, // import is all on shard 0, why are we making lots of other shards? b/c this is not a restriction.
			RowIDs:         rowIDs,
			ColumnKeys:     colKeys,
			Timestamps:     timestamps,
		}

		qcx := m0.API.Txf().NewQcx()

		if err := m0.API.Import(ctx, qcx, req); err != nil {
			t.Fatal(err)
		}
		panicOn(qcx.Finish())

		pql := fmt.Sprintf("Row(%s=%d)", fieldName, rowID)

		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql}); err != nil {
			t.Fatal(err)
		} else if keys := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, colKeys) {
			t.Fatalf("expected colKeys='%#v'; observed column keys: %#v", colKeys, keys)
		}

		// Query node1.
		if err := test.RetryUntil(5*time.Second, func() error {
			if res, err := m1.API.Query(ctx, &pilosa.QueryRequest{Index: indexName, Query: pql}); err != nil {
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

	m0 := c.GetNode(0)
	m1 := c.GetNode(1)

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
		for i := 1; i <= 10; i++ {
			values = append(values, int64(i))
		}

		// Column keys are sharded so their order is not guaranteed.
		colKeys := []string{"col10", "col8", "col9", "col6", "col7", "col4", "col5", "col2", "col3", "col1"}

		// Import data with keys to the coordinator (node0) and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
		req := &pilosa.ImportValueRequest{
			Index:      index,
			Field:      field,
			ColumnKeys: colKeys,
			Values:     values,
		}

		qcx := m0.API.Txf().NewQcx()
		if err := m0.API.ImportValue(ctx, qcx, req); err != nil {
			t.Fatal(err)
		}
		panicOn(qcx.Finish())

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
		_, err = m1.API.CreateField(ctx, index, field, pilosa.OptFieldTypeDecimal(1))
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

		qcx := m1.API.Txf().NewQcx()
		if err := m1.API.ImportValue(ctx, qcx, req); err != nil {
			t.Fatal(err)
		}
		panicOn(qcx.Finish())

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

	t.Run("ValStringField", func(t *testing.T) {
		ctx := context.Background()
		index := "valstr"
		field := "fstr"

		fgnIndex := "fgnvalstr"

		_, err := m0.API.CreateIndex(ctx, index, pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}

		_, err = m0.API.CreateIndex(ctx, fgnIndex, pilosa.IndexOptions{Keys: true})
		if err != nil {
			t.Fatalf("creating foreign index: %v", err)
		}
		_, err = m0.API.CreateField(ctx, index, field,
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

		// Import data with keys to the coordinator (node0) and verify that it gets
		// translated and forwarded to the owner of shard 0 (node1; because of offsetModHasher)
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
		panicOn(qcx.Finish())

		pql := fmt.Sprintf(`Row(%s=="strval-110")`, field)

		// Query node0.
		if res, err := m0.API.Query(ctx, &pilosa.QueryRequest{Index: index, Query: pql}); err != nil {
			t.Fatal(err)
		} else if ids := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(ids, []uint64{1}) {
			t.Fatalf("unexpected columns: observerd %+v;  expected '%+v'", ids, []uint64{1})
		}
	})
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
	if err := m0api.Import(ctx, qcx, ir0); err != nil {
		t.Fatal(err)
	}
	if err := m0api.ImportValue(ctx, qcx, ivr0); err != nil {
		t.Fatal(err)
	}
	panicOn(qcx.Finish())

	bitIsSet := func() bool {
		query := fmt.Sprintf("Row(%v=%v)", iraField, iraRowID)
		res, err := m0api.Query(context.Background(), &pilosa.QueryRequest{Index: index, Query: query})
		panicOn(err)
		cols := res.Results[0].(*pilosa.Row).Columns()
		for i := range cols {
			if cols[i] == acctOwnerID {
				return true
			}
		}
		return false
	}

	if !bitIsSet() {
		panic("IRA bit should have been set")
	}

	queryAcct := func(m0api *pilosa.API, acctOwnerID uint64, fieldAcct0, index string) (acctBal int64) {
		query := fmt.Sprintf("FieldValue(field=%v, column=%v)", fieldAcct0, acctOwnerID)
		res, err := m0api.Query(context.Background(), &pilosa.QueryRequest{Index: index, Query: query})
		panicOn(err)

		if len(res.Results) == 0 {
			return 0
		}
		valCount := res.Results[0].(pilosa.ValCount)
		return valCount.Val
	}

	bal := queryAcct(m0api, acctOwnerID, fieldAcct0, index)

	if bal != acct0bal {
		panic(fmt.Sprintf("expected %v, observed %v starting acct0 balance", acct0bal, bal))
	}

	// clear the bit
	qcx = m0api.Txf().NewQcx()
	ir0.Clear = true
	if err := m0api.Import(ctx, qcx, ir0); err != nil {
		t.Fatal(err)
	}
	panicOn(qcx.Finish())

	if bitIsSet() {
		panic("IRA bit should have been cleared")
	}

	// clear the BSI
	qcx = m0api.Txf().NewQcx()
	ivr0.Clear = true
	if err := m0api.ImportValue(ctx, qcx, ivr0); err != nil {
		t.Fatal(err)
	}
	panicOn(qcx.Finish())

	bal = queryAcct(m0api, acctOwnerID, fieldAcct0, index)
	if bal != 0 {
		panic(fmt.Sprintf("expected %v, observed %v starting acct0 balance", acct0bal, 0))
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
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	m0 := c.GetNode(0)
	nodesByID := make(map[string]*test.Command, 3)
	qcxsByID := make(map[string]*pilosa.Qcx, 3)
	for i := 0; i < 3; i++ {
		node := c.GetNode(i)
		id := node.API.Node().ID
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

/*
func TestAPI_MutexCheck(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	m0 := c.GetNode(0)
	nodesByID := make(map[string]*test.Command, 3)
	qcxsByID := make(map[string]*pilosa.Qcx, 3)
	for i := 0; i < 3; i++ {
		node := c.GetNode(i)
		id := node.API.Node().ID
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
		fmt.Println("running:", indexData.indexName, fieldData.fieldName)
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

			results, err := m0.API.MutexCheck(ctx, qcx, indexData.indexName, fieldData.fieldName, true, 0)

			if err != nil {
				t.Fatalf("checking mutexes: %v", err)
			}
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

			else {
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
					t.Fatalf("expected exactly %d records to have collisions", len(expected))
				}
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

			results, err := m0.API.MutexCheck(ctx, qcx, indexData.indexName, fieldData.fieldName)
			if err != nil {
				t.Fatalf("checking mutexes: %v", err)
			}
			if keyedField {
				// this just sorta comes out this way with our hashing; these are
				// the things which were in position 1 of their shards, and did
				// not have a value which happens to map to 3.
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
		})
	}
}
*/
