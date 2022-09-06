// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/test"

	"github.com/pkg/errors"
)

func NewTestClient(t *testing.T, c *test.Cluster) *Client {
	client, err := NewClient(c.Nodes[0].URL())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestAgainstCluster(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	client := NewTestClient(t, c)
	t.Run("string-slice-combos", func(t *testing.T) { testStringSliceCombos(t, c, client) })
	t.Run("import-batch-ints", func(t *testing.T) { testImportBatchInts(t, c, client) })
	t.Run("import-batch-sorting", func(t *testing.T) { testImportBatchSorting(t, c, client) })
	t.Run("test-trim-null", func(t *testing.T) { testTrimNull(t, c, client) })
	t.Run("test-string-slice-empty-and-nil", func(t *testing.T) { testStringSliceEmptyAndNil(t, c, client) })
	t.Run("test-string-slice", func(t *testing.T) { testStringSlice(t, c, client) })
	t.Run("test-single-clear-batch-regression", func(t *testing.T) { testSingleClearBatchRegression(t, c, client) })
	t.Run("test-batches", func(t *testing.T) { testBatches(t, c, client) })
	t.Run("batches-strings-ids", func(t *testing.T) { testBatchesStringIDs(t, c, client) })
	t.Run("test-batch-staleness", func(t *testing.T) { testBatchStaleness(t, c, client) })
	t.Run("test-import-batch-multiple-ints", func(t *testing.T) { testImportBatchMultipleInts(t, c, client) })
	t.Run("test-import-batch-multiple-timestamps", func(t *testing.T) { testImportBatchMultipleTimestamps(t, c, client) })
	t.Run("test-import-batch-sets-clears", func(t *testing.T) { testImportBatchSetsAndClears(t, c, client) })
	t.Run("test-topn-cache-regression", func(t *testing.T) { testTopNCacheRegression(t, c, client) })
	t.Run("test-multiple-int-same-batch", func(t *testing.T) { testMultipleIntSameBatch(t, c, client) })
	t.Run("test-mutex-clearing-regression", func(t *testing.T) { mutexClearRegression(t, c, client) })
	t.Run("test-mutex-nil-clear-id", func(t *testing.T) { mutexNilClearID(t, c, client) })
	t.Run("test-mutex-nil-clear-key", func(t *testing.T) { mutexNilClearKey(t, c, client) })
}

func testStringSliceCombos(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-string-slice-combos")
	fields := make([]*Field, 1)
	fields[0] = idx.Field("a1", OptFieldKeys(true), OptFieldTypeSet(CacheTypeRanked, 100))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()

	b, err := NewBatch(client, 5, idx, fields)
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

	err = ingestRecords(records, b)
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	a1 := fields[0]

	result := tq(t, client, a1.TopN(10))
	rez := sortableCRI(result.CountItems())
	sort.Sort(rez)
	exp := sortableCRI{
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

	result = tq(t, client, a1.Row("a"))
	errorIfNotEqual(t, result.Row().Columns, []uint64{0, 5, 6, 11})
	result = tq(t, client, a1.Row("b"))
	errorIfNotEqual(t, result.Row().Columns, []uint64{0, 5, 6, 11})
	result = tq(t, client, a1.Row("c"))
	errorIfNotEqual(t, result.Row().Columns, []uint64{0, 3, 5, 6, 11})
	result = tq(t, client, a1.Row("z"))
	errorIfNotEqual(t, result.Row().Columns, []uint64{1, 7})
	result = tq(t, client, a1.Row("q"))
	errorIfNotEqual(t, result.Row().Columns, []uint64{3, 9})
	result = tq(t, client, a1.Row("r"))
	errorIfNotEqual(t, result.Row().Columns, []uint64{3, 9})
	result = tq(t, client, a1.Row("s"))
	errorIfNotEqual(t, result.Row().Columns, []uint64{3, 9})
	result = tq(t, client, a1.Row("t"))
	errorIfNotEqual(t, result.Row().Columns, []uint64{3, 9})

	result = tq(t, client, idx.RawQuery("Count(All())"))
	errorIfNotEqual(t, result.Count(), int64(14))
}

func errorIfNotEqual(t *testing.T, exp, got interface{}) {
	t.Helper()
	if !reflect.DeepEqual(exp, got) {
		t.Errorf("unequal exp/got:\n%v\n%v", exp, got)
	}
}

type sortableCRI []CountResultItem

func (s sortableCRI) Len() int { return len(s) }
func (s sortableCRI) Less(i, j int) bool {
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
func (s sortableCRI) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func tq(t *testing.T, client *Client, query PQLQuery) QueryResult {
	resp, err := client.Query(query)
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	return resp.Results()[0]
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

func testImportBatchInts(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-import-batch-ints")
	field := idx.Field("anint", OptFieldTypeInt())
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	b, err := NewBatch(client, 3, idx, []*Field{field})
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

	resp, err := client.Query(idx.BatchQuery(field.Equals(0), field.Equals(7), field.Equals(2)))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	for i, result := range resp.Results() {
		if !reflect.DeepEqual(result.Row().Columns, []uint64{uint64(i)}) {
			t.Errorf("expected %v for %d, but got %v", []uint64{uint64(i)}, i, result.Row().Columns)
		}
	}
}

func testImportBatchSorting(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-import-batch-sorting")
	field := idx.Field("anint", OptFieldTypeInt())
	field2 := idx.Field("amutex", OptFieldTypeMutex(CacheTypeNone, 0))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	b, err := NewBatch(client, 100, idx, []*Field{field, field2})
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

	resp, err := client.Query(idx.RawQuery("Count(All())"))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	if res := resp.Results()[0]; res.Count() != 100 {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func testTrimNull(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-trim-null")
	field := idx.Field("empty", OptFieldTypeInt())
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()
	b, err := NewBatch(client, 3, idx, []*Field{field})
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}
	b.nullIndices = make(map[string][]uint64, 1)
	b.nullIndices[field.Name()] = []uint64{0, 1, 2}
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
	resp, err := client.Query(idx.BatchQuery(field.Equals(0), field.Equals(1), field.Equals(2)))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	for i, result := range resp.Results() {
		if !reflect.DeepEqual(result.Row().Columns, []uint64(nil)) {
			t.Errorf("expected %#v for %d, but got %#v", []uint64(nil), i, result.Row().Columns)
		}
	}

	b, err = NewBatch(client, 4, idx, []*Field{field})
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

	resp, err = client.Query(idx.BatchQuery(field.Equals(10), field.Equals(40), field.Equals(20), field.Equals(30)))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	for i, result := range resp.Results() {
		if i == 1 {
			if !reflect.DeepEqual(result.Row().Columns, []uint64(nil)) {
				t.Errorf("expected %#v for %d, but got %#v", []uint64(nil), i, result.Row().Columns)
			}
		} else {
			if !reflect.DeepEqual(result.Row().Columns, []uint64{result.Row().Columns[0]}) {
				t.Errorf("expected %#v for %d, but got %#v", []uint64{result.Row().Columns[0]}, i, result.Row().Columns)
			}
		}
	}

}

func testStringSliceEmptyAndNil(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-string-slice-nil")
	fields := make([]*Field, 1)
	fields[0] = idx.Field("strslice", OptFieldKeys(true), OptFieldTypeSet(CacheTypeRanked, 100))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()

	// first create a batch and test adding a single value with empty
	// string - this failed with a translation error at one point, and
	// how we catch it and treat it like a nil.
	b, err := NewBatch(client, 2, idx, fields)
	if err != nil {
		t.Fatalf("creating new batch: %v", err)
	}
	r := Row{Values: make([]interface{}, len(fields))}
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
	b, err = NewBatch(client, 6, idx, fields)
	if err != nil {
		t.Fatalf("creating new batch: %v", err)
	}
	r = Row{Values: make([]interface{}, len(fields))}
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

	rows := []interface{}{"a", "b", "c", "z"}
	resp, err := client.Query(idx.BatchQuery(fields[0].Row(rows[0]), fields[0].Row(rows[1]), fields[0].Row(rows[2]), fields[0].Row(rows[3])))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	// TODO test is flaky because we can't guarantee what a,b,c map to
	expectations := [][]uint64{{0, 2}, {2, 3}, {3}, {2}}
	for i, re := range resp.Results() {
		if !reflect.DeepEqual(re.Row().Columns, expectations[i]) {
			t.Errorf("expected row %v to have columns %v, but got %v", rows[i], expectations[i], re.Row().Columns)
		}
	}

}

func testStringSlice(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-string-slice")
	fields := make([]*Field, 1)
	fields[0] = idx.Field("strslice", OptFieldKeys(true), OptFieldTypeSet(CacheTypeRanked, 100))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()

	b, err := NewBatch(client, 3, idx, fields)
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

	r := Row{Values: make([]interface{}, len(fields))}
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

	resp, err := client.Query(idx.BatchQuery(fields[0].Row("a")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	result := resp.Result()
	if !reflect.DeepEqual(result.Row().Columns, []uint64{0, 1}) {
		t.Fatalf("expected a to be [0,1], got %v", result.Row().Columns)
	}
}

func testSingleClearBatchRegression(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-single-clear-batch-regression")
	numFields := 1
	fields := make([]*Field, numFields)
	fields[0] = idx.Field("zero", OptFieldKeys(true))

	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()

	_, err = client.Query(fields[0].Set("row1", 1))
	if err != nil {
		t.Fatalf("setting bit: %v", err)
	}

	b, err := NewBatch(client, 1, idx, fields)
	if err != nil {
		t.Fatalf("getting new batch: %v", err)
	}
	r := Row{ID: uint64(1), Values: make([]interface{}, numFields), Clears: make(map[int]interface{})}
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

	resp, err := client.Query(fields[0].Row("row1"))
	if err != nil {
		t.Fatalf("error querying: %v", err)
	}
	result := resp.Results()[0].Row().Columns
	if len(result) != 0 {
		t.Fatalf("unexpected values in row: result %+v", result)
	}

}

func testBatches(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-batches")
	numFields := 5
	fields := make([]*Field, numFields)
	fields[0] = idx.Field("zero", OptFieldKeys(true))
	fields[1] = idx.Field("one", OptFieldKeys(true))
	fields[2] = idx.Field("two", OptFieldKeys(true))
	fields[3] = idx.Field("three", OptFieldTypeInt())
	fields[4] = idx.Field("four", OptFieldTypeTime(TimeQuantumYearMonthDay))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()
	b, err := NewBatch(client, 10, idx, fields)
	if err != nil {
		t.Fatalf("getting new batch: %v", err)
	}
	r := Row{Values: make([]interface{}, numFields), Clears: make(map[int]interface{})}
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

	resp, err := client.Query(idx.BatchQuery(fields[0].Row("a"),
		fields[1].Row("b"),
		fields[2].Row("c"),
		fields[3].Equals(99)))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	results := resp.Results()
	for _, j := range []int{0, 2, 3} {
		cols := results[j].Row().Columns
		if !reflect.DeepEqual(cols, []uint64{0, 2, 4, 6, 10, 12, 14, 16, 18}) {
			t.Fatalf("unexpected columns for a: %v", cols)
		}
	}
	res := results[1]

	if cols := res.Row().Columns; !reflect.DeepEqual(cols, []uint64{0, 2, 4, 6, 8, 10, 12, 14, 16, 18}) {
		t.Fatalf("unexpected columns for field 1 row b: %v", cols)
	}

	resp, err = client.Query(idx.BatchQuery(fields[0].Row("d"),
		fields[1].Row("e"),
		fields[2].Row("f")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	results = resp.Results()
	for _, res := range results {
		cols := res.Row().Columns
		if !reflect.DeepEqual(cols, []uint64{20, 22, 24, 26, 28}) {
			t.Fatalf("unexpected columns: %v", cols)
		}
	}

	resp, err = client.Query(idx.BatchQuery(fields[3].GT(-11),
		fields[3].Equals(0),
		fields[3].Equals(100),
		fields[4].Range(1, time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, time.January, 29, 0, 0, 0, 0, time.UTC)),
		fields[4].Range(1, time.Date(2019, time.February, 1, 0, 0, 0, 0, time.UTC), time.Date(2019, time.February, 29, 0, 0, 0, 0, time.UTC))))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}
	results = resp.Results()

	if cols := results[0].Row().Columns; !reflect.DeepEqual(cols, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28}) {
		t.Fatalf("all columns (but 8) should be greater than -11, but got: %v", cols)
	}

	if cols := results[1].Row().Columns; !reflect.DeepEqual(cols, []uint64{19, 21, 23, 25, 27}) {
		t.Fatalf("wrong cols for ==0: %v", cols)
	}

	if cols := results[2].Row().Columns; !reflect.DeepEqual(cols, []uint64{20, 22, 24, 26, 28}) {
		t.Fatalf("wrong cols for ==100: %v", cols)
	}

	cols := results[3].Row().Columns
	exp := []uint64{0, 2, 4, 6, 10, 12, 14, 16, 18}
	if !reflect.DeepEqual(cols, exp) {
		t.Fatalf("wrong cols for January: got/want\n%v\n%v", cols, exp)
	}

	cols = results[4].Row().Columns
	exp = []uint64{1, 3, 5, 7}
	if !reflect.DeepEqual(cols, exp) {
		t.Fatalf("wrong cols for January: got/want\n%v\n%v", cols, exp)
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
	resp, err = client.Query(idx.BatchQuery(
		fields[0].Row("a"),
		fields[0].Row("x"),
		fields[1].Row("b"),
	))
	if err != nil {
		t.Fatalf("querying after clears: %v", err)
	}
	if arow := resp.Results()[0].Row().Columns; arow[0] == 0 {
		t.Errorf("shouldn't have id 0 in row a after clearing! %v", arow)
	}
	if xrow := resp.Results()[1].Row().Columns; xrow[0] != 0 {
		t.Errorf("should have id 0 in row x after setting %v", xrow)
	}
	if brow := resp.Results()[2].Row().Columns; brow[0] == 0 {
		t.Errorf("shouldn't have id 0 in row b after clearing! %v", brow)
	}

	// TODO test importing across multiple shards
}

func testBatchesStringIDs(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("batches-strings-ids", OptIndexKeys(true))
	fields := make([]*Field, 3)
	fields[0] = idx.Field("zero", OptFieldKeys(true))
	fields[1] = idx.Field("one", OptFieldTypeMutex(CacheTypeNone, 0), OptFieldKeys(true))
	fields[2] = idx.Field("two", OptFieldTypeTime("YMDH"), OptFieldKeys(true))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()

	b, err := NewBatch(client, 3, idx, fields)
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

	resp, err := client.Query(idx.BatchQuery(fields[0].Row("a"), fields[0].Row("x"), fields[1].Row("b"), fields[1].Row("y"), fields[2].Row("c"), fields[2].Row("z")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	results := resp.Results()
	for i, res := range results {
		cols := res.Row().Keys
		if i%2 == 0 && !reflect.DeepEqual(cols, []string{"0", "2"}) && !reflect.DeepEqual(cols, []string{"2", "0"}) {
			t.Fatalf("unexpected columns: %v", cols)
		}
		if i%2 == 1 && !reflect.DeepEqual(cols, []string{"1"}) {
			t.Fatalf("unexpected columns: %v", cols)
		}
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

	resp, err = client.Query(idx.BatchQuery(fields[0].Row("a"), fields[0].Row("z")))
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	results = resp.Results()
	for i, res := range results {
		cols := res.Row().Keys
		if err := isPermutationOf(cols, []string{"0", "1", "2"}); i == 0 && err != nil {
			t.Fatalf("unexpected columns: %v: %v", cols, err)
		}
		if i == 1 && !reflect.DeepEqual(cols, []string{"3"}) {
			t.Fatalf("unexpected columns: %v", cols)
		}
	}

}

func isPermutationOf(one, two []string) error {
	if len(one) != len(two) {
		return errors.Errorf("different lengths %d and %d", len(one), len(two))
	}
outer:
	for _, vOne := range one {
		for j, vTwo := range two {
			if vOne == vTwo {
				two = append(two[:j], two[j+1:]...)
				continue outer
			}
		}
		return errors.Errorf("%s in one but not two", vOne)
	}
	if len(two) != 0 {
		return errors.Errorf("vals in two but not one: %v", two)
	}
	return nil
}

func TestQuantizedTime(t *testing.T) {
	cases := []struct {
		name    string
		time    time.Time
		year    string
		month   string
		day     string
		hour    string
		quantum TimeQuantum
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
			quantum: TimeQuantumYear,
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

func testBatchStaleness(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-batch-staleness")
	field := idx.Field("anint", OptFieldTypeInt())
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()

	b, err := NewBatch(client, 3, idx, []*Field{field}, OptMaxStaleness(time.Millisecond))
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

func testImportBatchMultipleInts(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-import-batch-multi-int")
	field := idx.Field("anint", OptFieldTypeInt())
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	b, err := NewBatch(client, 6, idx, []*Field{field}, OptUseShardTransactionalEndpoint(true))
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

	if resp, err := client.Query(field.Equals(4)); err != nil {
		t.Fatalf("querying: %v", err)
	} else if res := resp.Results()[0].Row().Columns; len(res) != 1 || res[0] != 1 {
		t.Fatalf("unepxected result: %v", res)
	}

}

// testImportBatchMultipleTimestamps tests if nils are handles correctly for TS in batch imports
func testImportBatchMultipleTimestamps(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-import-batch-multi-timestamp")
	field := idx.Field("ts2", OptFieldTypeTimestamp(time.Unix(0, 0), "s"))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	b1, err := NewBatch(client, 6, idx, []*Field{field})
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}
	b2, err := NewBatch(client, 6, idx, []*Field{field}, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
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

			if b.nullIndices[field.name][0] != 3 {
				t.Fatalf("unexpected nulls, got/want: %v/%v", b.nullIndices[field.name], []uint64{3})
			}
			for i, val := range chkVals {
				if b.values[field.name][i] != val {
					t.Fatalf("unexpected value, got/want: %v/%v", b.values[field.name][i], val)
				}
				if b.ids[i] != cols[i] {
					t.Fatalf("unexpected id, got/want: %v/%v", b.ids[i], cols[i])
				}
			}
			err = b.Import()
			if err != nil {
				t.Fatalf("importing: %v", err)
			}

			qr := c.Query(t, idx.name, `Extract(All(), Rows(ts2))`)
			results := qr.Results[0].(featurebase.ExtractedTable)
			for k, res := range results.Columns {
				if chkImport[k] != nil {
					if res.Rows[0] != chkImport[k].(time.Time).UTC() {
						t.Fatalf("unexpected result, got/want: %v/%v", res.Rows[0], chkImport[k].(time.Time).UTC())
					}
				} else {
					if res.Rows[0] != nil {
						t.Fatalf("unexpected result, got/want: %v/%v", res.Rows[0], nil)
					}
				}
			}
		})
	}
}

func testImportBatchSetsAndClears(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-import-batch-set-and-clear")
	field := idx.Field("aset", OptFieldTypeSet(featurebase.DefaultCacheType, featurebase.DefaultCacheSize))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	b, err := NewBatch(client, 6, idx, []*Field{field}, OptUseShardTransactionalEndpoint(true))
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

	if resp, err := client.Query(field.TopN(6)); err != nil {
		t.Fatalf("querying topn: %v", err)
	} else if res := resp.Result().CountItems(); len(res) != 3 {
		t.Fatalf("unexpected topn: %+v", res)
	}

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
		resp, err := client.Query(field.Row(row))
		if err != nil {
			t.Fatalf("querying: %v", err)
		}
		res := resp.Results()[0].Row().Columns
		if !reflect.DeepEqual(exp[row], res) && !(len(exp[row]) == 0 && len(res) == 0) {
			t.Errorf("row: %d, exp: %v, got %v", row, exp[row], res)
		}
	}

}

// testTopNCacheRegression recreates an issue we saw in an IDK test
// where if a value is completely removed (all bits unset from a row),
// it didn't get removed from the cache beacuse a full recalculation
// had no way to clear the cache, it would just reset existing
// values. We added Clear on the cache interface to fix this.
func testTopNCacheRegression(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-topn-cache-regression")
	field := idx.Field("aset", OptFieldTypeSet(featurebase.DefaultCacheType, featurebase.DefaultCacheSize))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	b, err := NewBatch(client, 3, idx, []*Field{field}, OptUseShardTransactionalEndpoint(true))
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

	if resp, err := client.Query(field.TopN(6)); err != nil {
		t.Fatalf("querying topn: %v", err)
	} else if res := resp.Result().CountItems(); len(res) != 1 {
		t.Fatalf("unexpected topn: %+v", res)
	} else if res[0].ID != 1 || res[0].Count != 1 {
		t.Fatalf("unexpected topn result: %v", res)
	}
}

// testMultipleIntSameBatch checks that if the same ID is added multiple times with different values that only the last value is set and the bits aren't mixed together. It adds a different ID in between the two same ones which triggered a bug because we were sorting by shard rather than ID.
func testMultipleIntSameBatch(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-multiple-int-same-batch")
	field := idx.Field("age", OptFieldTypeInt(0, 10000))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	b, err := NewBatch(client, 4, idx, []*Field{field}, OptUseShardTransactionalEndpoint(true))
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

	if resp, err := client.Query(field.Sum(nil)); err != nil {
		t.Fatalf("querying sum: %v", err)
	} else if res := resp.Result().Value(); res != 2 {
		t.Errorf("unexpected sum: %+v", res)
	}
}

// mutexClearRegression checks for a bug where shards beyond the first
// one in a batch did not get any bits set in their clear bitmap, and
// in fact, all the bits were set in the clear bitmap for the first
// shard.
func mutexClearRegression(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-multiple-mut-same-batch")
	field := idx.Field("mut", OptFieldTypeMutex(CacheTypeNone, 0))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	b, err := NewBatch(client, 11, idx, []*Field{field}, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	col := uint64(0)
	row := uint64(1)
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
			resp, err := client.Query(idx.GroupBy(field.Rows(), field.Rows()))
			if err != nil {
				t.Fatalf("querying groupby: %v", err)
			}
			groupCounts := resp.Result().GroupCounts()
			for j, gc := range groupCounts {
				if gc.Groups[0].RowID != gc.Groups[1].RowID {
					t.Errorf("zmismatched group at after %d batch: %d, %v", j, i, gc)
				}
			}
		} else if err != nil {
			t.Fatalf("adding to batch: %v", err)
		}

	}
	if err := b.Import(); err != nil {
		t.Fatalf("importing: %v", err)
	}

	resp, err := client.Query(idx.GroupBy(field.Rows(), field.Rows()))
	if err != nil {
		t.Fatalf("querying groupby: %v", err)
	}
	groupCounts := resp.Result().GroupCounts()
	for i, gc := range groupCounts {
		if gc.Groups[0].RowID != gc.Groups[1].RowID {
			t.Fatalf("bmismatched group at %d, %v", i, gc)
		}
	}
}

// test clearing record with explict nil
func mutexNilClearID(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-mut-nil-clear-id")
	field := idx.Field("mut", OptFieldTypeMutex(CacheTypeNone, 0))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}

	b, err := NewBatch(client, 11, idx, []*Field{field}, OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}

	col := uint64(0)
	row := uint64(1)
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
	resp, err := client.Query(idx.RawQuery("Row(mut=0)"))
	if err != nil {
		t.Fatalf("Fetching data: %v", err)
	}
	items := resp.Result().Row().Columns
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
	resp, err = client.Query(idx.RawQuery("Row(mut=0)"))
	if err != nil {
		t.Fatalf("Fetching data: %v", err)
	}
	errorIfNotEqual(t, resp.Result().Row().Columns, items)

}

// similar test to above but with string keys
func mutexNilClearKey(t *testing.T, c *test.Cluster, client *Client) {
	schema := NewSchema()
	idx := schema.Index("test-mut-nil-clear-key", OptIndexKeys(true))
	fields := make([]*Field, 1)
	fields[0] = idx.Field("mut", OptFieldTypeMutex(CacheTypeNone, 0), OptFieldKeys(true))
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatalf("syncing schema: %v", err)
	}
	defer func() {
		err := client.DeleteIndex(idx)
		if err != nil {
			t.Logf("problem cleaning up from test: %v", err)
		}
	}()

	b, err := NewBatch(client, 3, idx, fields)
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
	resp, err := client.Query(idx.RawQuery(`Row(mut="a")`))
	errorIfNotEqual(t, resp.Result().Row().Keys, []string{"0", "2"})

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
	resp, err = client.Query(idx.RawQuery(`Row(mut="a")`))
	if err != nil {
		t.Fatalf("importing: %v", err)
	}
	errorIfNotEqual(t, resp.Result().Row().Keys, []string{"0"})
}
