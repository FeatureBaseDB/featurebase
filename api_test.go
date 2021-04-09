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
	"crypto/rand"
	"errors"
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
	. "github.com/pilosa/pilosa/v2/vprint" // nolint:staticcheck
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
	c := test.MustRunCluster(t, 3,
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
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node2"),
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

		if err := m0.API.ImportColumnAttrs(ctx, req); err != nil {
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

		if err := m1.API.ImportColumnAttrs(ctx, req); err != nil {
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

		// Import data with keys to the primary and verify that it gets
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
		PanicOn(qcx.Finish())

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
		t.Skip("TODO(benbjohnson): timestamp")

		ctx := context.Background()
		index := "valts"
		field := "fts"

		_, err := m1.API.CreateIndex(ctx, index, pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = m1.API.CreateField(ctx, index, field, pilosa.OptFieldTypeTimestamp(pilosa.MinTimestamp, pilosa.MaxTimestamp, pilosa.TimeUnitSeconds))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		// Generate some records.
		t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
		values := []time.Time{}
		colIDs := []uint64{}
		for i := 0; i < 10; i++ {
			values = append(values, t0.AddDate(0, 1, 0))
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

		qcx := m1.API.Txf().NewQcx()
		if err := m1.API.ImportValue(ctx, qcx, req); err != nil {
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
