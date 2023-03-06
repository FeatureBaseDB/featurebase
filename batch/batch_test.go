// Copyright 2021 Molecula Corp. All rights reserved.

package batch

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/stretchr/testify/assert"

	"github.com/pkg/errors"
)

func TestAgainstCluster(t *testing.T) {
	cli, err := client.NewClient("featurebase:10101")
	assert.NoError(t, err)

	sapi := client.NewSchemaAPI(cli)
	qapi := client.NewQueryAPI(cli)
	importer := client.NewImporter(cli, sapi)

	t.Run("string-slice-combos", func(t *testing.T) { testStringSliceCombos(t, importer, sapi, qapi) })
	t.Run("import-batch-ints", func(t *testing.T) { testImportBatchInts(t, importer, sapi, qapi) })
	t.Run("import-batch-bools", func(t *testing.T) { testImportBatchBools(t, importer, sapi, qapi) })
	t.Run("import-batch-sorting", func(t *testing.T) { testImportBatchSorting(t, importer, sapi, qapi) })
	t.Run("test-trim-null", func(t *testing.T) { testTrimNull(t, importer, sapi, qapi) })
	t.Run("test-string-slice-empty-and-nil", func(t *testing.T) { testStringSliceEmptyAndNil(t, importer, sapi, qapi) })
	t.Run("test-string-slice", func(t *testing.T) { testStringSlice(t, importer, sapi, qapi) })
	t.Run("test-single-clear-batch-regression", func(t *testing.T) { testSingleClearBatchRegression(t, importer, sapi, qapi) })
	t.Run("test-batches", func(t *testing.T) { testBatches(t, importer, sapi, qapi) })
	t.Run("batches-strings-ids", func(t *testing.T) { testBatchesStringIDs(t, importer, sapi, qapi) })
	t.Run("test-batch-staleness", func(t *testing.T) { testBatchStaleness(t, importer, sapi, qapi) })
	t.Run("test-import-batch-multiple-ints", func(t *testing.T) { testImportBatchMultipleInts(t, importer, sapi, qapi) })
	t.Run("test-import-batch-multiple-timestamps", func(t *testing.T) { testImportBatchMultipleTimestamps(t, importer, sapi, qapi) })
	t.Run("test-import-batch-sets-clears", func(t *testing.T) { testImportBatchSetsAndClears(t, importer, sapi, qapi) })
	t.Run("test-topn-cache-regression", func(t *testing.T) { testTopNCacheRegression(t, importer, sapi, qapi) })
	t.Run("test-multiple-int-same-batch", func(t *testing.T) { testMultipleIntSameBatch(t, importer, sapi, qapi) })
	t.Run("test-mutex-clearing-regression", func(t *testing.T) { mutexClearRegression(t, importer, sapi, qapi) })
	t.Run("test-mutex-nil-clear-id", func(t *testing.T) { mutexNilClearID(t, importer, sapi, qapi) })
	t.Run("test-mutex-nil-clear-key", func(t *testing.T) { mutexNilClearKey(t, importer, sapi, qapi) })
}

func testStringSliceCombos(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-string-slice-combos",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "a1",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeSet,
					Keys:      true,
					CacheType: featurebase.CacheTypeRanked,
					CacheSize: 100,
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 5, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("creating new batch: %v", err)
	}

	records := []Row{
		{ID: uint64(0), Values: []interface{}{[]string{"a", "b", "c"}}},
		{ID: uint64(1), Values: []interface{}{[]string{"z"}}},
		{ID: uint64(2), Values: []interface{}{[]string{}}},
		{ID: uint64(3), Values: []interface{}{[]string{"q", "r", "s", "t", "c"}}},
		{ID: uint64(4), Values: []interface{}{nil}},
		{ID: uint64(5), Values: []interface{}{[]string{"a", "b", "c"}}},
		{ID: uint64(6), Values: []interface{}{[]string{"a", "b", "c"}}},
		{ID: uint64(7), Values: []interface{}{[]string{"z"}}},
		{ID: uint64(8), Values: []interface{}{[]string{}}},
		{ID: uint64(9), Values: []interface{}{[]string{"q", "r", "s", "t"}}},
		{ID: uint64(10), Values: []interface{}{nil}},
		{ID: uint64(11), Values: []interface{}{[]string{"a", "b", "c"}}},
		{ID: uint64(12), Values: []interface{}{[]string{}}},
		{ID: uint64(13), Values: []interface{}{[]string{}}},
	}
	assert.NoError(t, ingestRecords(records, b))

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "TopN(a1, n=10)",
	})
	if resp.Err != nil {
		t.Fatalf("unexpected error from TopN query: %v", resp.Err)
	}
	if len(resp.Results) < 1 {
		t.Fatalf("expected non-empty result set, got empty results")
	}
	pairsField, ok := resp.Results[0].(*featurebase.PairsField)
	assert.True(t, ok, "wrong return type: %T", resp.Results[0])

	rez := sortablePairs(pairsField.Pairs)
	sort.Sort(rez)
	exp := sortablePairs{
		{Key: "a", Count: 4},
		{Key: "b", Count: 4},
		{Key: "c", Count: 5},
		{Key: "q", Count: 2},
		{Key: "r", Count: 2},
		{Key: "s", Count: 2},
		{Key: "t", Count: 2},
		{Key: "z", Count: 2},
	}
	sort.Sort(exp)
	errorIfNotEqual(t, exp, rez)

	tests := []struct {
		pql string
		exp interface{}
	}{
		{
			pql: "Row(a1='a')",
			exp: []uint64{0, 5, 6, 11},
		},
		{
			pql: "Row(a1='b')",
			exp: []uint64{0, 5, 6, 11},
		},
		{
			pql: "Row(a1='c')",
			exp: []uint64{0, 3, 5, 6, 11},
		},
		{
			pql: "Row(a1='z')",
			exp: []uint64{1, 7},
		},
		{
			pql: "Row(a1='q')",
			exp: []uint64{3, 9},
		},
		{
			pql: "Row(a1='r')",
			exp: []uint64{3, 9},
		},
		{
			pql: "Row(a1='s')",
			exp: []uint64{3, 9},
		},
		{
			pql: "Row(a1='t')",
			exp: []uint64{3, 9},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			tresp := tq(t, ctx, qapi, &featurebase.QueryRequest{
				Index: idx.Name,
				Query: test.pql,
			})
			row, tok := tresp.Results[0].(*featurebase.Row)
			assert.True(t, tok, "wrong return type: %T", tresp.Results[0])
			assert.Equal(t, test.exp, row.Columns())
		})
	}

	resp = tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Count(All())",
	})
	count, ok := resp.Results[0].(uint64)
	assert.True(t, ok, "wrong return type: %T", resp.Results[0])
	assert.Equal(t, uint64(14), count)
}

func errorIfNotEqual(t *testing.T, exp, got interface{}) {
	t.Helper()
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unequal exp/got:\n%v\n%v", exp, got)
	}
}

// sortablePairs is a sortable slice of featurebase.Pair.
type sortablePairs []featurebase.Pair

func (s sortablePairs) Len() int { return len(s) }
func (s sortablePairs) Less(i, j int) bool {
	if s[i].Count != s[j].Count {
		return s[i].Count > s[j].Count
	}
	if s[i].ID != s[j].ID {
		return s[i].ID < s[j].ID
	}
	if s[i].Key != s[j].Key {
		return s[i].Key < s[j].Key
	}
	return true
}
func (s sortablePairs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func tq(t *testing.T, ctx context.Context, qapi featurebase.QueryAPI, query *featurebase.QueryRequest) featurebase.QueryResponse {
	resp, err := qapi.Query(ctx, query)
	assert.NoError(t, err)
	return resp
}

func ingestRecords(records []Row, batch *Batch) error {
	for _, rec := range records {
		err := batch.Add(rec)
		if err == ErrBatchNowFull {
			err = batch.Import()
			if err != nil {
				return errors.Wrap(err, "importing batch")
			}
		} else if err != nil {
			return errors.Wrap(err, "while adding record")
		}
	}
	if batch.Len() > 0 {
		err := batch.Import()
		if err != nil {
			return errors.Wrap(err, "importing batch")
		}
	}
	return nil
}

func testImportBatchInts(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-import-batch-ints",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "anint",
				Options: featurebase.FieldOptions{
					Type: featurebase.FieldTypeInt,
					Max:  pql.NewDecimal(1000, 0),
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 3, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	r := Row{Values: make([]interface{}, 1)}

	for i := uint64(0); i < 3; i++ {
		r.ID = i
		r.Values[0] = int64(i)
		err := b.Add(r)
		if err != nil && err != ErrBatchNowFull {
			t.Fatalf("adding to batch: %v", err)
		}
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	r.ID = uint64(0)
	r.Values[0] = nil
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding after import: %v", err)
	}
	r.ID = uint64(1)
	r.Values[0] = int64(7)
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding second after import: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("second import: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Row(anint=0) Row(anint=7) Row(anint=2)",
	})
	assert.Equal(t, 3, len(resp.Results))

	for i, result := range resp.Results {
		row, ok := result.(*featurebase.Row)
		assert.True(t, ok, "wrong return type: %T", result)
		assert.Equal(t, []uint64{uint64(i)}, row.Columns())
	}
}

func testImportBatchSorting(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-import-batch-sorting",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "anint",
				Options: featurebase.FieldOptions{
					Type: featurebase.FieldTypeInt,
					Max:  pql.NewDecimal(10_000_000, 0),
				},
			},
			{
				Name: "amutex",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeMutex,
					CacheType: featurebase.CacheTypeNone,
					CacheSize: 0,
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 100, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	r := Row{Values: make([]interface{}, 2)}

	rnd := rand.New(rand.NewSource(7))

	// generate 100 records randomly spread/ordered across multiple
	// shards to test sorting on int/mutex fields
	for i := 0; i < 100; i++ {
		id := rnd.Intn(10_000_000)
		r.ID = uint64(id)
		r.Values[0] = int64(id)
		r.Values[1] = uint64(id)
		err := b.Add(r)
		if err != nil && err != ErrBatchNowFull {
			t.Fatalf("adding to batch: %v", err)
		}
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("second import: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Count(All())",
	})
	count, ok := resp.Results[0].(uint64)
	assert.True(t, ok, "wrong return type: %T", resp.Results[0])
	assert.Equal(t, uint64(100), count)
}

func testTrimNull(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	fieldName := "empty"
	idx := &featurebase.IndexInfo{
		Name: "test-trim-null",
		Fields: []*featurebase.FieldInfo{
			{
				Name: fieldName,
				Options: featurebase.FieldOptions{
					Type: featurebase.FieldTypeInt,
					Max:  pql.NewDecimal(1000, 0),
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 3, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}
	b.nullIndices = make(map[string][]uint64, 1)
	b.nullIndices[fieldName] = []uint64{0, 1, 2}
	r := Row{Values: make([]interface{}, 1)}
	for i := 0; i < 3; i++ {
		r.ID = uint64(i)
		r.Values[0] = int64(i)
		err := b.Add(r)
		if err != nil && err != ErrBatchNowFull {
			t.Fatalf("adding to batch: %v", err)
		}
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Row(empty=0) Row(empty=1) Row(empty=2)",
	})
	assert.Equal(t, 3, len(resp.Results))

	for _, result := range resp.Results {
		row, ok := result.(*featurebase.Row)
		assert.True(t, ok, "wrong return type: %T", result)
		assert.Equal(t, []uint64{}, row.Columns())
	}

	b, err = NewBatch(importer, 4, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}
	r = Row{Values: make([]interface{}, 1)}
	for i := 10; i < 40; i += 10 {
		r.ID = uint64(i)
		r.Values[0] = int64(i)
		err := b.Add(r)
		if err != nil && err != ErrBatchNowFull {
			t.Fatalf("adding to batch: %v", err)
		}
	}

	r.ID = uint64(40)
	r.Values[0] = nil
	err = b.Add(r)
	if err != nil && err != ErrBatchNowFull {
		t.Fatalf("adding to batch: %v", err)
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	tests := []struct {
		pql string
		exp interface{}
	}{
		{
			pql: "Row(empty=10)",
			exp: []uint64{10},
		},
		{
			pql: "Row(empty=40)",
			exp: []uint64{},
		},
		{
			pql: "Row(empty=20)",
			exp: []uint64{20},
		},
		{
			pql: "Row(empty=30)",
			exp: []uint64{30},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
				Index: idx.Name,
				Query: test.pql,
			})
			row, ok := resp.Results[0].(*featurebase.Row)
			assert.True(t, ok, "wrong return type: %T", resp.Results[0])
			assert.Equal(t, test.exp, row.Columns())
		})
	}
}

func testStringSliceEmptyAndNil(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-string-slice-nil",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "strslice",
				Options: featurebase.FieldOptions{
					Type:           featurebase.FieldTypeSet,
					Keys:           true,
					CacheType:      featurebase.CacheTypeRanked,
					CacheSize:      100,
					TrackExistence: true,
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	// first create a batch and test adding a single value with empty
	// string - this failed with a translation error at one point, and
	// how we catch it and treat it like a nil.
	b, err := NewBatch(importer, 2, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("creating new batch: %v", err)
	}
	r := Row{Values: make([]interface{}, len(idx.Fields))}
	r.ID = uint64(1)
	r.Values[0] = ""
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding: %v", err)
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	// now create a batch and add a mixture of string slice values
	b, err = NewBatch(importer, 6, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("creating new batch: %v", err)
	}
	r = Row{Values: make([]interface{}, len(idx.Fields))}
	r.ID = uint64(0)
	r.Values[0] = []string{"a"}
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding to batch: %v", err)
	}

	r.ID = uint64(1)
	r.Values[0] = nil
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding batch with nil stringslice to r: %v", err)
	}

	r.ID = uint64(2)
	r.Values[0] = []string{"a", "b", "z"}
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding batch with  idslice to r: %v", err)
	}

	r.ID = uint64(3)
	r.Values[0] = []string{"b", "c"}
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding batch with stringslice to r: %v", err)
	}

	r.ID = uint64(4)
	r.Values[0] = []string{}
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding batch with stringslice to r: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	tests := []struct {
		pql string
		exp interface{}
	}{
		{
			pql: "Row(strslice='a')",
			exp: []uint64{0, 2},
		},
		{
			pql: "Row(strslice='b')",
			exp: []uint64{2, 3},
		},
		{
			pql: "Row(strslice='c')",
			exp: []uint64{3},
		},
		{
			pql: "Row(strslice='z')",
			exp: []uint64{2},
		},
		{
			pql: "Row(strslice==null)",
			exp: []uint64{1},
		},
		{
			pql: "Row(strslice!=null)",
			exp: []uint64{0, 2, 3, 4},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
				Index: idx.Name,
				Query: test.pql,
			})
			row, ok := resp.Results[0].(*featurebase.Row)
			assert.True(t, ok, "wrong return type: %T", resp.Results[0])
			assert.Equal(t, test.exp, row.Columns())
		})
	}
}

func testStringSlice(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-string-slice",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "strslice",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeSet,
					Keys:      true,
					CacheType: featurebase.CacheTypeRanked,
					CacheSize: 100,
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 3, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("creating new batch: %v", err)
	}

	rowmap := map[string]uint64{
		"c": 9,
		"d": 10,
		"f": 13,
	}
	b.rowTranslations["strslice"] = make(map[string]agedTranslation)
	for k, id := range rowmap {
		b.rowTranslations["strslice"][k] = agedTranslation{
			id: id,
		}
	}

	r := Row{Values: make([]interface{}, len(idx.Fields))}
	r.ID = uint64(0)
	r.Values[0] = []string{"a"}
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding to batch: %v", err)
	}
	if got := b.toTranslateSets["strslice"]["a"]; !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("expected []int{0}, got: %v", got)
	}

	r.ID = uint64(1)
	r.Values[0] = []string{"a", "b", "c"}
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding to batch: %v", err)
	}
	if got := b.toTranslateSets["strslice"]["a"]; !reflect.DeepEqual(got, []int{0, 1}) {
		t.Fatalf("expected []int{0,1}, got: %v", got)
	}
	if got := b.toTranslateSets["strslice"]["b"]; !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("expected []int{1}, got: %v", got)
	}
	if got, ok := b.toTranslateSets["strslice"]["c"]; ok {
		t.Fatalf("should be nothing at c, got: %v", got)
	}
	if got := b.rowIDSets["strslice"][1]; !reflect.DeepEqual(got, []uint64{9}) {
		t.Fatalf("expected c to map to rowID 9 but got %v", got)
	}

	r.ID = uint64(2)
	r.Values[0] = []string{"d", "e", "f"}
	err = b.Add(r)
	if err != ErrBatchNowFull {
		t.Fatalf("adding to batch: %v", err)
	}
	if got, ok := b.toTranslateSets["strslice"]["d"]; ok {
		t.Fatalf("should be nothing at d, got: %v", got)
	}
	if got, ok := b.toTranslateSets["strslice"]["f"]; ok {
		t.Fatalf("should be nothing at f, got: %v", got)
	}
	if got := b.toTranslateSets["strslice"]["e"]; !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("expected []int{2}, got: %v", got)
	}
	if got := b.rowIDSets["strslice"][2]; !reflect.DeepEqual(got, []uint64{10, 13}) {
		t.Fatalf("expected c to map to rowID 9 but got %v", got)
	}

	err = b.doTranslation()
	if err != nil {
		t.Fatalf("translating: %v", err)
	}

	if got0 := b.rowIDSets["strslice"][0]; len(got0) != 1 {
		t.Errorf("after translation, rec 0, wrong len: %v", got0)
	} else if got1 := b.rowIDSets["strslice"][1]; len(got1) != 3 || got1[0] != 9 || (got1[1] != got0[0] && got1[2] != got0[0]) {
		t.Errorf("after translation, rec 1: %v, rec 0: %v", got1, got0)
	} else if got2 := b.rowIDSets["strslice"][2]; len(got2) != 3 || got2[0] != 10 || got2[1] != 13 || got2[2] == got1[2] || got2[2] == got0[0] {
		t.Errorf("after translation, rec 2: %v", got2)
	}

	frags, clearFrags, err := b.makeFragments(make(fragments), make(fragments))
	if err != nil {
		t.Errorf("making fragments: %v", err)
	}

	err = b.doImport(frags, clearFrags)
	if err != nil {
		t.Fatalf("doing import: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Row(strslice='a')",
	})
	row, ok := resp.Results[0].(*featurebase.Row)
	assert.True(t, ok, "wrong return type: %T", resp.Results[0])
	assert.Equal(t, []uint64{0, 1}, row.Columns())
}

func testSingleClearBatchRegression(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-single-clear-batch-regression",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "zero",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeSet,
					Keys:      true,
					CacheType: featurebase.CacheTypeRanked,
					CacheSize: 100,
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	// Set a bit.
	_ = tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Set(1, zero='row1')",
	})

	b, err := NewBatch(importer, 1, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("getting new batch: %v", err)
	}
	r := Row{ID: uint64(1), Values: make([]interface{}, len(idx.Fields)), Clears: make(map[int]interface{})}
	r.Values[0] = nil
	r.Clears[0] = "row1"
	err = b.Add(r)
	if err != ErrBatchNowFull {
		t.Fatalf("wrong error from batch add: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("error importing: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Row(zero='row1')",
	})
	row, ok := resp.Results[0].(*featurebase.Row)
	assert.True(t, ok, "wrong return type: %T", resp.Results[0])
	assert.Equal(t, []uint64{}, row.Columns())
}

func testBatches(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-batches",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "zero",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeSet,
					Keys:      true,
					CacheType: featurebase.CacheTypeRanked,
					CacheSize: 100,
				},
			},
			{
				Name: "one",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeSet,
					Keys:      true,
					CacheType: featurebase.CacheTypeRanked,
					CacheSize: 100,
				},
			},
			{
				Name: "two",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeSet,
					Keys:      true,
					CacheType: featurebase.CacheTypeRanked,
					CacheSize: 100,
				},
			},
			{
				Name: "three",
				Options: featurebase.FieldOptions{
					Type: featurebase.FieldTypeInt,
					Min:  pql.NewDecimal(-1_000_000, 0),
					Max:  pql.NewDecimal(1_000_000, 0),
				},
			},
			{
				Name: "four",
				Options: featurebase.FieldOptions{
					Type:        featurebase.FieldTypeTime,
					TimeQuantum: "YMD",
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 10, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("getting new batch: %v", err)
	}
	r := Row{Values: make([]interface{}, len(idx.Fields)), Clears: make(map[int]interface{})}
	r.Time.Set(time.Date(2019, time.January, 2, 15, 45, 0, 0, time.UTC))

	for i := 0; i < 9; i++ {
		r.ID = uint64(i)
		if i%2 == 0 {
			r.Values[0] = "a"
			r.Values[1] = "b"
			r.Values[2] = "c"
			r.Values[3] = int64(99)
			r.Values[4] = uint64(1)
			r.Time.SetMonth("01")
		} else {
			r.Values[0] = "x"
			r.Values[1] = "y"
			r.Values[2] = "z"
			r.Values[3] = int64(-10)
			r.Values[4] = uint64(1)
			r.Time.SetMonth("02")
		}
		if i == 8 {
			r.Values[0] = nil
			r.Clears[1] = uint64(97)
			r.Clears[2] = "c"
			r.Values[3] = nil
			r.Values[4] = nil
		}
		err := b.Add(r)
		if err != nil {
			t.Fatalf("unexpected err adding record: %v", err)
		}
	}

	if len(b.toTranslate[0]) != 2 {
		t.Fatalf("wrong number of keys in toTranslate[0]")
	}
	for k, ints := range b.toTranslate[0] {
		if k == "a" {
			if !reflect.DeepEqual(ints, []int{0, 2, 4, 6}) {
				t.Fatalf("wrong ints for key a in field zero: %v", ints)
			}
		} else if k == "x" {
			if !reflect.DeepEqual(ints, []int{1, 3, 5, 7}) {
				t.Fatalf("wrong ints for key x in field zero: %v", ints)
			}

		} else {
			t.Fatalf("unexpected key %s", k)
		}
	}
	if !reflect.DeepEqual(b.toTranslateClear, map[int]map[string][]int{2: {"c": {8}}}) {
		t.Errorf("unexpected toTranslateClear: %+v", b.toTranslateClear)
	}
	if !reflect.DeepEqual(b.clearRowIDs, map[int]map[int]uint64{1: {8: 97}, 2: {}}) {
		t.Errorf("unexpected clearRowIDs: %+v", b.clearRowIDs)
	}

	if !reflect.DeepEqual(b.values["three"], []int64{99, -10, 99, -10, 99, -10, 99, -10, 0}) {
		t.Fatalf("unexpected values: %v", b.values["three"])
	}
	if !reflect.DeepEqual(b.nullIndices["three"], []uint64{8}) {
		t.Fatalf("unexpected nullIndices: %v", b.nullIndices["three"])
	}

	if len(b.toTranslate[1]) != 2 {
		t.Fatalf("wrong number of keys in toTranslate[1]")
	}
	for k, ints := range b.toTranslate[1] {
		if k == "b" {
			if !reflect.DeepEqual(ints, []int{0, 2, 4, 6, 8}) {
				t.Fatalf("wrong ints for key b in field one: %v", ints)
			}
		} else if k == "y" {
			if !reflect.DeepEqual(ints, []int{1, 3, 5, 7}) {
				t.Fatalf("wrong ints for key y in field one: %v", ints)
			}

		} else {
			t.Fatalf("unexpected key %s", k)
		}
	}

	if len(b.toTranslate[2]) != 2 {
		t.Fatalf("wrong number of keys in toTranslate[2]")
	}
	for k, ints := range b.toTranslate[2] {
		if k == "c" {
			if !reflect.DeepEqual(ints, []int{0, 2, 4, 6, 8}) {
				t.Fatalf("wrong ints for key c in field two: %v", ints)
			}
		} else if k == "z" {
			if !reflect.DeepEqual(ints, []int{1, 3, 5, 7}) {
				t.Fatalf("wrong ints for key z in field two: %v", ints)
			}

		} else {
			t.Fatalf("unexpected key %s", k)
		}
	}

	err = b.Add(r)
	if err != ErrBatchNowFull {
		t.Fatalf("should have gotten full batch error, but got %v", err)
	}

	err = b.Add(r)
	if err != ErrBatchAlreadyFull {
		t.Fatalf("should have gotten already full batch error, but got %v", err)
	}

	if !reflect.DeepEqual(b.values["three"], []int64{99, -10, 99, -10, 99, -10, 99, -10, 0, 0}) {
		t.Fatalf("unexpected values: %v", b.values["three"])
	}

	err = b.doTranslation()
	if err != nil {
		t.Fatalf("doing translation: %v", err)
	}

	for fidx, rowIDs := range b.rowIDs {
		// we don't know which key will get translated first, but we do know the pattern
		if fidx == 0 {
			if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, nilSentinel, nilSentinel}) &&
				!reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, nilSentinel, nilSentinel}) {
				t.Fatalf("unexpected row ids for field %d: %v", fidx, rowIDs)
			}

		} else if fidx == 4 {
			if !reflect.DeepEqual(rowIDs, []uint64{1, 1, 1, 1, 1, 1, 1, 1, nilSentinel, nilSentinel}) {
				t.Fatalf("unexpected rowids for time field")
			}
		} else if fidx == 3 {
			if len(rowIDs) != 0 {
				t.Fatalf("expected no rowIDs for int field, but got: %v", rowIDs)
			}
		} else {
			if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, nilSentinel}) && !reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, 2, nilSentinel}) {
				t.Fatalf("unexpected row ids for field %d: %v", fidx, rowIDs)
			}
		}
	}

	if !reflect.DeepEqual(b.clearRowIDs[1], map[int]uint64{8: 97}) {
		t.Errorf("unexpected clearRowIDs after translation: %+v", b.clearRowIDs[1])
	}
	if !reflect.DeepEqual(b.clearRowIDs[2], map[int]uint64{8: 2}) && !reflect.DeepEqual(b.clearRowIDs[2], map[int]uint64{8: 1}) {
		t.Errorf("unexpected clearRowIDs: after translation%+v", b.clearRowIDs[2])
	}

	frags, clearFrags, err := b.makeFragments(make(fragments), make(fragments))
	if err != nil {
		t.Errorf("making fragments: %v", err)
	}

	err = b.doImport(frags, clearFrags)
	if err != nil {
		t.Fatalf("doing import: %v", err)
	}

	b.reset()

	for i := 9; i < 19; i++ {
		r.ID = uint64(i)
		if i%2 == 0 {
			r.Values[0] = "a"
			r.Values[1] = "b"
			r.Values[2] = "c"
			r.Values[3] = int64(99)
			r.Values[4] = uint64(1)
		} else {
			r.Values[0] = "x"
			r.Values[1] = "y"
			r.Values[2] = "z"
			r.Values[3] = int64(-10)
			r.Values[4] = uint64(2)
		}
		err := b.Add(r)
		if i != 18 && err != nil {
			t.Fatalf("unexpected err adding record: %v", err)
		}
		if i == 18 && err != ErrBatchNowFull {
			t.Fatalf("unexpected err: %v", err)
		}
	}

	// should do nothing
	err = b.doTranslation()
	if err != nil {
		t.Fatalf("doing translation: %v", err)
	}

	frags, clearFrags, err = b.makeFragments(make(fragments), make(fragments))
	if err != nil {
		t.Errorf("making fragments: %v", err)
	}

	err = b.doImport(frags, clearFrags)
	if err != nil {
		t.Fatalf("doing import: %v", err)
	}

	for fidx, rowIDs := range b.rowIDs {
		if fidx == 3 {
			if len(rowIDs) != 0 {
				t.Fatalf("expected no rowIDs for int field, but got: %v", rowIDs)
			}
			continue
		}
		// we don't know which key will get translated first, but we do know the pattern
		if !reflect.DeepEqual(rowIDs, []uint64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}) && !reflect.DeepEqual(rowIDs, []uint64{2, 1, 2, 1, 2, 1, 2, 1, 2, 1}) {
			t.Fatalf("unexpected row ids for field %d: %v", fidx, rowIDs)
		}
	}

	b.reset()

	for i := 19; i < 29; i++ {
		r.ID = uint64(i)
		if i%2 == 0 {
			r.Values[0] = "d"
			r.Values[1] = "e"
			r.Values[2] = "f"
			r.Values[3] = int64(100)
			r.Values[4] = uint64(3)
		} else {
			r.Values[0] = "u"
			r.Values[1] = "v"
			r.Values[2] = "w"
			r.Values[3] = int64(0)
			r.Values[4] = uint64(4)
		}
		err := b.Add(r)
		if i != 28 && err != nil {
			t.Fatalf("unexpected err adding record: %v", err)
		}
		if i == 28 && err != ErrBatchNowFull {
			t.Fatalf("unexpected err: %v", err)
		}
	}

	err = b.doTranslation()
	if err != nil {
		t.Fatalf("doing translation: %v", err)
	}

	frags, clearFrags, err = b.makeFragments(make(fragments), make(fragments))
	if err != nil {
		t.Errorf("making fragments: %v", err)
	}

	err = b.doImport(frags, clearFrags)
	if err != nil {
		t.Fatalf("doing import: %v", err)
	}

	for fidx, rowIDs := range b.rowIDs {
		// we don't know which key will get translated first, but we do know the pattern
		if fidx == 3 {
			if len(rowIDs) != 0 {
				t.Fatalf("expected no rowIDs for int field, but got: %v", rowIDs)
			}
			continue
		}
		if !reflect.DeepEqual(rowIDs, []uint64{3, 4, 3, 4, 3, 4, 3, 4, 3, 4}) && !reflect.DeepEqual(rowIDs, []uint64{4, 3, 4, 3, 4, 3, 4, 3, 4, 3}) {
			t.Fatalf("unexpected row ids for field %d: %v", fidx, rowIDs)
		}
	}

	frags, _, err = b.makeFragments(make(fragments), make(fragments))
	if err != nil {
		t.Fatalf("making fragments: %v", err)
	}

	var n int
	for key := range frags {
		if key.shard == 0 {
			n++
		}
	}
	if n != 5 { // zero, one, two, four (three is an int field so not in fragments) + _exists
		t.Fatalf("there should be 5 views, but have %d", n)
	}

	tests := []struct {
		pql string
		exp interface{}
	}{
		{
			pql: "Row(zero='a')",
			exp: []uint64{0, 2, 4, 6, 10, 12, 14, 16, 18},
		},
		{
			pql: "Row(one='b')",
			exp: []uint64{0, 2, 4, 6, 8, 10, 12, 14, 16, 18},
		},
		{
			pql: "Row(two='c')",
			exp: []uint64{0, 2, 4, 6, 10, 12, 14, 16, 18},
		},
		{
			pql: "Row(three=99)",
			exp: []uint64{0, 2, 4, 6, 10, 12, 14, 16, 18},
		},
		{
			pql: "Row(zero='d')",
			exp: []uint64{20, 22, 24, 26, 28},
		},
		{
			pql: "Row(one='e')",
			exp: []uint64{20, 22, 24, 26, 28},
		},
		{
			pql: "Row(two='f')",
			exp: []uint64{20, 22, 24, 26, 28},
		},
		{
			pql: "Row(three > -11)",
			exp: []uint64{0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28},
		},
		{
			pql: "Row(three=0)",
			exp: []uint64{19, 21, 23, 25, 27},
		},
		{
			pql: "Row(three=100)",
			exp: []uint64{20, 22, 24, 26, 28},
		},
		{
			pql: "Row(four=1, from=2019-01-01T00:00, to=2019-01-29T00:00)",
			exp: []uint64{0, 2, 4, 6, 10, 12, 14, 16, 18},
		},
		{
			pql: "Row(four=1, from=2019-02-01T00:00, to=2019-02-29T00:00)",
			exp: []uint64{1, 3, 5, 7},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
				Index: idx.Name,
				Query: test.pql,
			})
			row, ok := resp.Results[0].(*featurebase.Row)
			assert.True(t, ok, "wrong return type: %T", resp.Results[0])
			assert.Equal(t, test.exp, row.Columns())
		})
	}

	b.reset()
	r.ID = uint64(0)
	r.Values[0] = "x"
	r.Values[1] = "b"
	r.Clears[0] = "a"
	r.Clears[1] = "b" // b should get cleared
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding with clears: %v", err)
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing w/clears: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Row(zero='a') Row(zero='x') Row(one='b')",
	})
	assert.Equal(t, 3, len(resp.Results))

	for i, result := range resp.Results {
		row, ok := result.(*featurebase.Row)
		assert.True(t, ok, "wrong return type: %T", result)
		switch i {
		case 0:
			if arow := row.Columns(); arow[0] == 0 {
				t.Errorf("shouldn't have id 0 in row a after clearing! %v", arow)
			}
		case 1:
			if xrow := row.Columns(); xrow[0] != 0 {
				t.Errorf("should have id 0 in row x after setting %v", xrow)
			}
		case 2:
			if brow := row.Columns(); brow[0] == 0 {
				t.Errorf("shouldn't have id 0 in row b after clearing! %v", brow)
			}
		}
	}

	// TODO test importing across multiple shards
}

func testBatchesStringIDs(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "batches-strings-ids",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "zero",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeSet,
					Keys:      true,
					CacheType: featurebase.CacheTypeRanked,
					CacheSize: 100,
				},
			},
			{
				Name: "one",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeMutex,
					Keys:      true,
					CacheType: featurebase.CacheTypeNone,
					CacheSize: 0,
				},
			},
			{
				Name: "two",
				Options: featurebase.FieldOptions{
					Type:        featurebase.FieldTypeTime,
					Keys:        true,
					TimeQuantum: "YMDH",
				},
			},
		},
		Options: featurebase.IndexOptions{
			Keys:           true,
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 3, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("getting new batch: %v", err)
	}

	r := Row{Values: make([]interface{}, 3)}
	r.Time.Set(time.Date(2019, time.January, 2, 15, 45, 0, 0, time.UTC))

	for i := 0; i < 3; i++ {
		r.ID = strconv.Itoa(i)
		if i%2 == 0 {
			r.Values[0] = "a"
			r.Values[1] = "b"
			r.Values[2] = "c"
			r.Time.SetMonth("01")
		} else {
			r.Values[0] = "x"
			r.Values[1] = "y"
			r.Values[2] = "z"
			r.Time.SetMonth("02")
		}
		err := b.Add(r)
		if err != nil && err != ErrBatchNowFull {
			t.Fatalf("unexpected err adding record: %v", err)
		}
	}

	if len(b.toTranslateID) != 3 {
		t.Fatalf("id translation table unexpected size: %v", b.toTranslateID)
	}
	for i, k := range b.toTranslateID {
		if ik, err := strconv.Atoi(k); err != nil || ik != i {
			t.Errorf("unexpected toTranslateID key %s at index %d", k, i)
		}
	}

	err = b.doTranslation()
	if err != nil {
		t.Fatalf("translating: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	tests := []struct {
		pql string
		exp interface{}
	}{
		{
			pql: "Row(zero='a')",
			exp: []string{"0", "2"},
		},
		{
			pql: "Row(zero='x')",
			exp: []string{"1"},
		},
		{
			pql: "Row(one='b')",
			exp: []string{"0", "2"},
		},
		{
			pql: "Row(one='y')",
			exp: []string{"1"},
		},
		{
			pql: "Row(two='c')",
			exp: []string{"0", "2"},
		},
		{
			pql: "Row(two='z')",
			exp: []string{"1"},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-part1-%d", i), func(t *testing.T) {
			resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
				Index: idx.Name,
				Query: test.pql,
			})
			row, ok := resp.Results[0].(*featurebase.Row)
			assert.True(t, ok, "wrong return type: %T", resp.Results[0])
			assert.ElementsMatch(t, test.exp, row.Keys)
		})
	}

	b.reset()

	r.ID = "1"
	r.Values[0] = "a"
	err = b.Add(r)
	if err != nil {
		t.Fatalf("unexpected err adding record: %v", err)
	}

	r.ID = "3"
	r.Values[0] = "z"
	err = b.Add(r)
	if err != nil {
		t.Fatalf("unexpected err adding record: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	tests = []struct {
		pql string
		exp interface{}
	}{
		{
			pql: "Row(zero='a')",
			exp: []string{"0", "1", "2"},
		},
		{
			pql: "Row(zero='z')",
			exp: []string{"3"},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-part2-%d", i), func(t *testing.T) {
			resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
				Index: idx.Name,
				Query: test.pql,
			})
			row, ok := resp.Results[0].(*featurebase.Row)
			assert.True(t, ok, "wrong return type: %T", resp.Results[0])
			assert.ElementsMatch(t, test.exp, row.Keys)
		})
	}
}

func TestQuantizedTime(t *testing.T) {
	cases := []struct {
		name    string
		time    time.Time
		year    string
		month   string
		day     string
		hour    string
		quantum featurebase.TimeQuantum
		reset   bool
		exp     []string
		expErr  string
	}{
		{
			name:   "no time quantum",
			expErr: "",
		},
		{
			name:   "no time quantum with data",
			year:   "2017",
			exp:    []string{},
			expErr: "",
		},
		{
			name:    "no data",
			quantum: "Y",
			exp:     nil,
			expErr:  "",
		},
		{
			name:    "timestamp",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			quantum: "YMDH",
			exp:     []string{"2013", "201310", "20131016", "2013101617"},
		},
		{
			name:    "timestamp-less-granular",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			quantum: "YM",
			exp:     []string{"2013", "201310"},
		},
		{
			name:    "timestamp-mid-granular",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			quantum: "MD",
			exp:     []string{"201310", "20131016"},
		},
		{
			name:    "justyear",
			year:    "2013",
			quantum: "Y",
			exp:     []string{"2013"},
		},
		{
			name:    "justyear-wantmonth",
			year:    "2013",
			quantum: "YM",
			expErr:  "no data set for month",
		},
		{
			name:    "timestamp-changeyear",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			year:    "2019",
			quantum: "YMDH",
			exp:     []string{"2019", "201910", "20191016", "2019101617"},
		},
		{
			name:    "yearmonthdayhour",
			year:    "2013",
			month:   "10",
			day:     "16",
			hour:    "17",
			quantum: "YMDH",
			exp:     []string{"2013", "201310", "20131016", "2013101617"},
		},
		{
			name:    "timestamp-changehour",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			hour:    "05",
			quantum: "MDH",
			exp:     []string{"201310", "20131016", "2013101605"},
		},
		{
			name:    "timestamp",
			time:    time.Date(2013, time.October, 16, 17, 34, 43, 0, time.FixedZone("UTC-5", -5*60*60)),
			quantum: "YMDH",
			reset:   true,
			exp:     nil,
		},
	}

	for i, test := range cases {
		t.Run(test.name+strconv.Itoa(i), func(t *testing.T) {
			tq := QuantizedTime{}
			var zt time.Time
			if zt != test.time {
				tq.Set(test.time)
			}
			if test.year != "" {
				tq.SetYear(test.year)
			}
			if test.month != "" {
				tq.SetMonth(test.month)
			}
			if test.day != "" {
				tq.SetDay(test.day)
			}
			if test.hour != "" {
				tq.SetHour(test.hour)
			}
			if test.reset {
				tq.Reset()
			}

			views, err := tq.views(test.quantum)
			if !reflect.DeepEqual(views, test.exp) {
				t.Errorf("unexpected views, got/want:\n%v\n%v\n", views, test.exp)
			}
			if (err != nil && err.Error() != test.expErr) || (err == nil && test.expErr != "") {
				t.Errorf("unexpected error, got/want:\n%v\n%s\n", err, test.expErr)
			}
		})
	}
}

func testBatchStaleness(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-batch-staleness",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "anint",
				Options: featurebase.FieldOptions{
					Type: featurebase.FieldTypeInt,
					Min:  pql.NewDecimal(-1_000_000, 0),
					Max:  pql.NewDecimal(1_000_000, 0),
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 3, tbl, idx.Fields, OptMaxStaleness(time.Millisecond))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	r := Row{ID: uint64(0), Values: []interface{}{int64(0)}}
	err = b.Add(r)
	if err != nil && err != ErrBatchNowFull {
		t.Fatalf("adding to batch: %v", err)
	}

	// sleep so batch becomes stale
	time.Sleep(time.Millisecond)

	r = Row{ID: uint64(1), Values: []interface{}{int64(0)}}
	err = b.Add(r)
	if err != ErrBatchNowStale {
		t.Fatal("batch expected to be stale")
	}
}

func testImportBatchMultipleInts(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-import-batch-multi-int",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "anint",
				Options: featurebase.FieldOptions{
					Type: featurebase.FieldTypeInt,
					Min:  pql.NewDecimal(-1_000_000, 0),
					Max:  pql.NewDecimal(1_000_000, 0),
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 6, tbl, idx.Fields, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	r := Row{Values: make([]interface{}, 1)}

	vals := []int64{16, 8, 32, 1, 2, 4}
	for i := uint64(0); i < 6; i++ {
		r.ID = uint64(1)
		r.Values[0] = vals[i]
		err := b.Add(r)
		if err != nil && err != ErrBatchNowFull {
			t.Fatalf("adding to batch: %v", err)
		}
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Row(anint=4)",
	})
	assert.Equal(t, 1, len(resp.Results))

	result := resp.Results[0]
	row, ok := result.(*featurebase.Row)
	assert.True(t, ok, "wrong return type: %T", result)
	assert.Equal(t, []uint64{1}, row.Columns())
}

// testImportBatchMultipleTimestamps tests if nils are handles correctly for TS
// in batch imports
func testImportBatchMultipleTimestamps(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	fieldName := "ts2"
	idx := &featurebase.IndexInfo{
		Name: "test-import-batch-multi-timestamp",
		Fields: []*featurebase.FieldInfo{
			{
				Name: fieldName,
				Options: featurebase.FieldOptions{
					Type:     featurebase.FieldTypeTimestamp,
					TimeUnit: "s",
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b1, err := NewBatch(importer, 6, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("getting batch 1: %v", err)
	}
	b2, err := NewBatch(importer, 6, tbl, idx.Fields, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch 2: %v", err)
	}
	batches := []*Batch{b1, b2}

	for j := 0; j < 2; j++ {
		t.Run(fmt.Sprintf("batch %d", j), func(t *testing.T) {
			b := batches[j]
			r := Row{Values: make([]interface{}, 1)}

			rawVals := []interface{}{int64(16), int64(8), int64(32), nil, int64(2), int64(4)}
			chkVals := []int64{16, 8, 32, 0, 2, 4}
			chkImport := []interface{}{time.Unix(16, 0), time.Unix(8, 0), time.Unix(32, 0), nil, time.Unix(2, 0), time.Unix(4, 0)}
			cols := []uint64{0, 1, 2, 3, 4, 5}
			for i := range cols {
				r.ID = cols[i]
				r.Values[0] = rawVals[i]
				err := b.Add(r)
				if err != nil && err != ErrBatchNowFull {
					t.Fatalf("adding to batch: %v", err)
				}
			}

			if b.nullIndices[fieldName][0] != 3 {
				t.Fatalf("unexpected nulls, got/want: %v/%v", b.nullIndices[fieldName], []uint64{3})
			}
			for i, val := range chkVals {
				if b.values[fieldName][i] != val {
					t.Fatalf("unexpected value, got/want: %v/%v", b.values[fieldName][i], val)
				}
				if b.ids[i] != cols[i] {
					t.Fatalf("unexpected id, got/want: %v/%v", b.ids[i], cols[i])
				}
			}
			err = b.Import()
			if err != nil {
				t.Fatalf("importing: %v", err)
			}

			for i := range chkImport {
				var timeStr string
				if chkImport[i] == nil {
					timeStr = "null"
				} else {
					ttime, ok := chkImport[i].(time.Time)
					assert.True(t, ok)
					timeStr = fmt.Sprintf(`"%s"`, ttime.Format(time.RFC3339))
				}
				resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
					Index: idx.Name,
					Query: fmt.Sprintf("Row(ts2 == %s)", timeStr),
				})
				assert.Equal(t, 1, len(resp.Results))

				result := resp.Results[0]
				row, ok := result.(*featurebase.Row)
				assert.True(t, ok, "wrong return type: %T", result)
				assert.Equal(t, []uint64{uint64(i)}, row.Columns())
			}
		})
	}
}

func testImportBatchSetsAndClears(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-import-batch-set-and-clear",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "aset",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeSet,
					CacheType: featurebase.DefaultCacheType,
					CacheSize: featurebase.DefaultCacheSize,
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 6, tbl, idx.Fields, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	r := Row{
		Values: make([]interface{}, 1),
		Clears: make(map[int]interface{}),
	}

	vals := []uint64{1, 2, 3, 1, 5, 6}
	clears := []interface{}{nil, uint64(1), uint64(3), nil, uint64(2), uint64(4)}
	for i := uint64(0); i < 6; i++ {
		r.ID = i%3 + 1
		r.Values[0] = vals[i]
		if clears[i] != nil {
			r.Clears[0] = clears[i]
		}
		err := b.Add(r)
		if err != nil && err != ErrBatchNowFull {
			t.Fatalf("adding to batch: %v", err)
		}
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "TopN(aset, n=6)",
	})
	pairsField, ok := resp.Results[0].(*featurebase.PairsField)
	assert.True(t, ok, "wrong return type: %T", resp.Results[0])
	assert.Equal(t, 3, len(pairsField.Pairs))

	exp := [][]uint64{
		{},
		{1},
		{},
		{},
		{},
		{2},
		{3},
	}
	for row := 0; row < 7; row++ {
		resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
			Index: idx.Name,
			Query: fmt.Sprintf("Row(aset=%d)", row),
		})
		assert.Equal(t, 1, len(resp.Results))

		result := resp.Results[0]
		fRow, ok := result.(*featurebase.Row)
		assert.True(t, ok, "wrong return type: %T", result)
		assert.Equal(t, exp[row], fRow.Columns())
	}
}

// testTopNCacheRegression recreates an issue we saw in an IDK test
// where if a value is completely removed (all bits unset from a row),
// it didn't get removed from the cache because a full recalculation
// had no way to clear the cache, it would just reset existing
// values. We added Clear on the cache interface to fix this.
func testTopNCacheRegression(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-topn-cache-regression",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "aset",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeSet,
					CacheType: featurebase.DefaultCacheType,
					CacheSize: featurebase.DefaultCacheSize,
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 3, tbl, idx.Fields, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	records := []struct {
		ID    uint64
		Set   interface{}
		Clear interface{}
	}{
		{0, 1, nil},
		{featurebase.ShardWidth, 1, nil},
		{featurebase.ShardWidth * 2, nil, 1},
		{featurebase.ShardWidth * 2, nil, 1},
		{0, nil, 1},
		{featurebase.ShardWidth, nil, 1},
		{featurebase.ShardWidth, 1, nil},
		{featurebase.ShardWidth, nil, nil},
	}

	for _, rec := range records {
		if rec.Set != nil {
			rec.Set = uint64(rec.Set.(int))
		}
		row := Row{
			ID:     rec.ID,
			Values: []interface{}{rec.Set},
		}
		if rec.Clear != nil {
			row.Clears = map[int]interface{}{0: uint64(rec.Clear.(int))}
		}

		err := b.Add(row)
		if err == ErrBatchNowFull {
			if err := b.Import(); err != nil {
				t.Fatalf("importing: %v", err)
			}
		}
	}
	if err := b.Import(); err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "TopN(aset, n=6)",
	})
	pairsField, ok := resp.Results[0].(*featurebase.PairsField)
	assert.True(t, ok, "wrong return type: %T", resp.Results[0])
	assert.Equal(t, 1, len(pairsField.Pairs))
	assert.Equal(t, uint64(1), pairsField.Pairs[0].ID)
	assert.Equal(t, uint64(1), pairsField.Pairs[0].Count)
}

// testMultipleIntSameBatch checks that if the same ID is added multiple times
// with different values that only the last value is set and the bits aren't
// mixed together. It adds a different ID in between the two same ones which
// triggered a bug because we were sorting by shard rather than ID.
func testMultipleIntSameBatch(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-multiple-int-same-batch",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "age",
				Options: featurebase.FieldOptions{
					Type: featurebase.FieldTypeInt,
					Max:  pql.NewDecimal(10_000, 0),
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 4, tbl, idx.Fields, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	if err := b.Add(Row{
		ID:     uint64(1),
		Values: []interface{}{int64(1)},
	}); err != nil {
		t.Fatalf("adding to batch: %v", err)
	}
	if err := b.Add(Row{
		ID:     uint64(2),
		Values: []interface{}{int64(0)},
	}); err != nil {
		t.Fatalf("adding to batch: %v", err)
	}
	if err := b.Add(Row{
		ID:     uint64(1),
		Values: []interface{}{int64(2)},
	}); err != nil {
		t.Fatalf("adding to batch: %v", err)
	}

	if err := b.Import(); err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Sum(field=age)",
	})
	assert.Equal(t, 1, len(resp.Results))

	result := resp.Results[0]
	row, ok := result.(featurebase.ValCount)
	assert.True(t, ok, "wrong return type: %T", result)
	assert.Equal(t, int64(2), row.Val)
}

// mutexClearRegression checks for a bug where shards beyond the first
// one in a batch did not get any bits set in their clear bitmap, and
// in fact, all the bits were set in the clear bitmap for the first
// shard.
func mutexClearRegression(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-multiple-mut-same-batch",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "mut",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeMutex,
					CacheType: featurebase.CacheTypeNone,
					CacheSize: 0,
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 11, tbl, idx.Fields, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	col := uint64(0)
	row := uint64(0)
	for i := uint64(0); i <= 21; i++ {
		col = (i%2+1)*featurebase.ShardWidth + i%5
		row = i % 3
		if err := b.Add(Row{
			ID:     col,
			Values: []interface{}{row},
		}); err == ErrBatchNowFull {
			if err := b.Import(); err != nil {
				t.Fatalf("importing: %v", err)
			}

			resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
				Index: idx.Name,
				Query: "GroupBy(Rows(field=mut), Rows(field=mut))",
			})
			assert.Equal(t, 1, len(resp.Results))

			result := resp.Results[0]
			groupCounts, ok := result.(*featurebase.GroupCounts)
			assert.True(t, ok, "wrong return type: %T", result)
			for j, gc := range groupCounts.Groups() {
				assert.Equal(t, gc.Group[0].RowID, gc.Group[1].RowID, "zmismatched group at after %d batch: %d, %v", j, i, gc)
			}

		} else if err != nil {
			t.Fatalf("adding to batch: %v", err)
		}
	}

	if err := b.Import(); err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "GroupBy(Rows(field=mut), Rows(field=mut))",
	})
	assert.Equal(t, 1, len(resp.Results))

	result := resp.Results[0]
	groupCounts, ok := result.(*featurebase.GroupCounts)
	assert.True(t, ok, "wrong return type: %T", result)
	for i, gc := range groupCounts.Groups() {
		assert.Equal(t, gc.Group[0].RowID, gc.Group[1].RowID, "bmismatched group at %d, %v", i, gc)
	}
}

// test clearing record with explict nil
func mutexNilClearID(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-mut-nil-clear-id",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "mut",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeMutex,
					CacheType: featurebase.CacheTypeNone,
					CacheSize: 0,
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 11, tbl, idx.Fields, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	col := uint64(0)
	row := uint64(0)
	// populate mutex with some data
	for i := uint64(0); i < 11; i++ {
		col = (i%2+1)*featurebase.ShardWidth + i%5
		row = i % 3
		if err := b.Add(Row{
			ID:     col,
			Values: []interface{}{row},
		}); err == ErrBatchNowFull {
			if err := b.Import(); err != nil {
				t.Fatalf("importing: %v", err)
			}
		} else if err != nil {
			t.Fatalf("adding to batch: %v", err)
		}

	}

	// example data just copyied from test above
	// confirm expected data
	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Row(mut=0)",
	})
	assert.Equal(t, 1, len(resp.Results))

	result := resp.Results[0]
	fRow, ok := result.(*featurebase.Row)
	assert.True(t, ok, "wrong return type: %T", result)
	items := fRow.Columns()

	// delete item 0
	b.Add(
		Row{
			ID:     items[0],
			Values: []interface{}{nil},
			Clears: map[int]interface{}{0: nil},
		},
	)
	b.Import()
	items = items[1:]

	// confirm record removed
	resp = tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Row(mut=0)",
	})
	assert.Equal(t, 1, len(resp.Results))

	result = resp.Results[0]
	fRow, ok = result.(*featurebase.Row)
	assert.True(t, ok, "wrong return type: %T", result)
	assert.Equal(t, items, fRow.Columns())
}

// similar test to above but with string keys
func mutexNilClearKey(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-mut-nil-clear-key",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "mut",
				Options: featurebase.FieldOptions{
					Type:      featurebase.FieldTypeMutex,
					CacheType: featurebase.CacheTypeNone,
					CacheSize: 0,
					Keys:      true,
				},
			},
		},
		Options: featurebase.IndexOptions{
			Keys:           true,
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 3, tbl, idx.Fields)
	if err != nil {
		t.Fatalf("getting new batch: %v", err)
	}

	r := Row{Values: make([]interface{}, 1)}

	for i := 0; i < 3; i++ {
		r.ID = strconv.Itoa(i)
		if i%2 == 0 {
			r.Values[0] = "a"
		} else {
			r.Values[0] = "x"
		}
		err := b.Add(r)
		if err != nil && err != ErrBatchNowFull {
			t.Fatalf("unexpected err adding record: %v", err)
		}
	}

	if len(b.toTranslateID) != 3 {
		t.Fatalf("id translation table unexpected size: %v", b.toTranslateID)
	}
	for i, k := range b.toTranslateID {
		if ik, err := strconv.Atoi(k); err != nil || ik != i {
			t.Errorf("unexpected toTranslateID key %s at index %d", k, i)
		}
	}

	err = b.doTranslation()
	if err != nil {
		t.Fatalf("translating: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Row(mut='a')",
	})
	assert.Equal(t, 1, len(resp.Results))

	result := resp.Results[0]
	fRow, ok := result.(*featurebase.Row)
	assert.True(t, ok, "wrong return type: %T", result)
	assert.Equal(t, []string{"0", "2"}, fRow.Keys)

	r.ID = "2"
	r.Values[0] = nil
	r.Clears = map[int]interface{}{0: nil}
	err = b.Add(r)
	if err != nil {
		t.Fatalf("unexpected err adding record: %v", err)
	}
	err = b.Import()
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp = tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Row(mut='a')",
	})
	assert.Equal(t, 1, len(resp.Results))

	result = resp.Results[0]
	fRow, ok = result.(*featurebase.Row)
	assert.True(t, ok, "wrong return type: %T", result)
	assert.Equal(t, []string{"0"}, fRow.Keys)
}

func testImportBatchBools(t *testing.T, importer featurebase.Importer, sapi featurebase.SchemaAPI, qapi featurebase.QueryAPI) {
	ctx := context.Background()

	idx := &featurebase.IndexInfo{
		Name: "test-import-batch-bools",
		Fields: []*featurebase.FieldInfo{
			{
				Name: "boolcol",
				Options: featurebase.FieldOptions{
					Type: featurebase.FieldTypeBool,
				},
			},
		},
		Options: featurebase.IndexOptions{
			TrackExistence: true,
		},
	}

	tbl := featurebase.IndexInfoToTable(idx)
	assert.NoError(t, sapi.CreateTable(ctx, tbl))
	defer func() {
		assert.NoError(t, sapi.DeleteTable(ctx, tbl.Name))
	}()

	b, err := NewBatch(importer, 3, tbl, idx.Fields, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting new batch: %v", err)
	}

	r := Row{Values: make([]interface{}, 1)}

	r.ID = uint64(0)
	r.Values[0] = bool(false)
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding after import: %v", err)
	}
	r.ID = uint64(1)
	r.Values[0] = bool(true)
	err = b.Add(r)
	if err != nil {
		t.Fatalf("adding second after import: %v", err)
	}

	err = b.Import()
	if err != nil {
		t.Fatalf("second import: %v", err)
	}

	resp := tq(t, ctx, qapi, &featurebase.QueryRequest{
		Index: idx.Name,
		Query: "Count(All())",
	})
	count, ok := resp.Results[0].(uint64)
	assert.True(t, ok, "wrong return type: %T", resp.Results[0])
	assert.Equal(t, uint64(2), count)
}

func TestConvert(t *testing.T) {
	t.Run("timestampToInt", func(t *testing.T) {
		tests := []struct {
			unit TimeUnit
			ts   string
			exp  int64
		}{
			{unit: "s", ts: "2022-01-01T00:00:00Z", exp: 1640995200},
			{unit: "ms", ts: "2022-01-01T00:00:00Z", exp: 1640995200000},
			{unit: "us", ts: "2022-01-01T00:00:00Z", exp: 1640995200000000},
			{unit: "ns", ts: "2022-01-01T00:00:00Z", exp: 1640995200000000000},
			{unit: "x", ts: "2022-01-01T00:00:00Z", exp: 0},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				ts, err := time.Parse(time.RFC3339, test.ts)
				assert.NoError(t, err)
				v := timestampToInt(test.unit, ts)
				assert.Equal(t, test.exp, v)
			})
		}
	})

	t.Run("Int64ToTimestamp", func(t *testing.T) {
		tests := []struct {
			unit  TimeUnit
			epoch string
			val   int64
			exp   time.Time
		}{
			{
				unit:  "ms",
				epoch: "2022-01-01T00:00:00Z",
				val:   0,
				exp:   time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			{
				unit:  "s",
				epoch: "2022-01-01T00:00:00Z",
				val:   86400,
				exp:   time.Date(2022, 1, 2, 0, 0, 0, 0, time.UTC),
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				epoch, err := time.Parse(time.RFC3339, test.epoch)
				assert.NoError(t, err)
				ts, err := Int64ToTimestamp(test.unit, epoch, test.val)
				assert.NoError(t, err)
				assert.Equal(t, test.exp, ts)
			})
		}
	})
}
