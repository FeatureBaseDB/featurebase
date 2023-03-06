// Copyright 2023 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/batch"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/stretchr/testify/require"
)

// the new "handle nulls for non-BSI fields" logic has a lot of
// implications, and needs to test things across multiple ways
// of importing data, so we want to have some consistent
// behavior.
//
// We're not testing keyed indexes, because the way index keys
// work doesn't have any relation to the code that's used to
// track existence, but we do want to test both keyed and
// unkeyed fields, and we want to test direct Set/Clear
// operations, SetRow, ClearRow (maybe? for mutexes?),
// Import operations, and the batch code. Note that direct
// use of ImportRoaring, outside of the batch code, makes
// no guarantees; it's up to a user providing roaring bitmaps
// to handle existence views.
//
// This is necessary because it's simply not *possible* to
// detect the distinction between "no bits provided" and
// "an empty set" from the bits written to a view other than
// an existence view. We could, in principle, spend a lot
// of time computing a best-guess that all non-empty sets
// are non-null, but this would be less accurate and much
// more expensive than doing that work on the batch side.

// setupNullHandlingSchema yields a cluster, and API, which point at an
// index with the given name, containing fields {mu, mk, su, sk, tu, tk}
// which are mutex/set/timequantum fields which are unkeyed/keyed respectively.
func setupNullHandlingSchema(t *testing.T, indexSuffix string) (*test.Cluster, string, *pilosa.API) {
	c := test.MustRunCluster(t, 3)
	api := c.GetNode(0).API
	index := c.Idx(indexSuffix)
	// we have six cases we care about; {mutex,set,timequantum} * {keyed, unkeyed}
	c.CreateField(t, index, pilosa.IndexOptions{TrackExistence: true}, "mu", pilosa.OptFieldTypeMutex(pilosa.CacheTypeNone, 0))
	c.CreateField(t, index, pilosa.IndexOptions{TrackExistence: true}, "mk", pilosa.OptFieldTypeMutex(pilosa.CacheTypeNone, 0), pilosa.OptFieldKeys())
	c.CreateField(t, index, pilosa.IndexOptions{TrackExistence: true}, "su", pilosa.OptFieldTypeSet(pilosa.CacheTypeNone, 0))
	c.CreateField(t, index, pilosa.IndexOptions{TrackExistence: true}, "sk", pilosa.OptFieldTypeSet(pilosa.CacheTypeNone, 0), pilosa.OptFieldKeys())
	c.CreateField(t, index, pilosa.IndexOptions{TrackExistence: true}, "tu", pilosa.OptFieldTypeTime("YMD", "0"))
	c.CreateField(t, index, pilosa.IndexOptions{TrackExistence: true}, "tk", pilosa.OptFieldTypeTime("YMD", "0"), pilosa.OptFieldKeys())
	return c, index, api
}

var nullHandlingFieldMasks = map[string]int{
	"mu": 1,
	"mk": 2,
	"su": 4,
	"sk": 8,
	"tu": 16,
	"tk": 32,
}
var nullHandlingExpectedResults = generateNullHandlingExpectedResults()

type nullSet map[bool][]uint64

func (n nullSet) null(v uint64) {
	n[true] = append(n[true], v)
}

func (n nullSet) notNull(v uint64) {
	n[false] = append(n[false], v)
}

func (n nullSet) clone() nullSet {
	f := n[false]
	t := n[true]
	nf := make([]uint64, len(f))
	nt := make([]uint64, len(t))
	copy(nf, f)
	copy(nt, t)
	return nullSet{false: nf, true: nt}
}

// trimSlice removes elements of s for which func returns
// true, returning the modified slice. it is destructive.
func trimSlice(s []uint64, fn func(uint64) bool) []uint64 {
	n := 0
	for i, v := range s {
		if fn(v) {
			continue
		} else {
			s[n] = s[i]
			n++
		}
	}
	return s[:n]
}

// trim removes values matching f
func (ns nullSet) trim(fn func(uint64) bool) {
	ns[false] = trimSlice(ns[false], fn)
	ns[true] = trimSlice(ns[true], fn)
}

// This outlines the expected results of a series of steps.
// We have six fields, keyed/unkeyed sets, mutexes, and time
// quantums. We only ever set value 0/"a" in them.
//
// First, we set those bits for the records 0..63, by treating
// each field as having a bitmask 1/2/4/8/16/32, and setting the
// bits for each field that's a 1 in the record's ID.
//
// Then, we *clear* every bit for every 5th record. I used to do
// every 3rd, but this worked out poorly, because 63%3 == 0. This
// means that all those entries should still exist, but should be
// considered nulls.
//
// Then, we delete everything that has either the 16 or the 32
// bit (or both) set. This means that all the records which are
// *not* divisible by 5 should no longer be considered null, because
// the records don't exist. The ones which are divisible by 5 are
// still null.
//
// Then, we set a bit in "tk" for record 63. At this point, the
// others should all be null. Remember that, prior to the delete,
// they were all non-null; 63 was the record that had every bit set.
// So we're verifying that, after a delete, recreating the record
// does not show things as not-null because they had been set *prior*
// to the delete.
func generateNullHandlingExpectedResults() map[int]map[string]nullSet {
	out := map[int]map[string]nullSet{}
	for phase := 0; phase < 4; phase++ {
		out[phase] = map[string]nullSet{}
		for k := range nullHandlingFieldMasks {
			out[phase][k] = nullSet{}
		}
	}
	// We can't combine these two inner loops, because a field is
	// only null for records which exist, so record 0, having no
	// values set at all, isn't even a null.
	phase := 0
	for i := 0; i < (1 << 6); i++ {
		madeAny := false
		for k, v := range nullHandlingFieldMasks {
			if i&v != 0 {
				out[phase][k].notNull(uint64(i))
				madeAny = true
			}
		}
		if madeAny {
			for k, v := range nullHandlingFieldMasks {
				if i&v == 0 {
					out[phase][k].null(uint64(i))
				}
			}
		}
	}
	// phase 1: we clear every bit in records divisible by 5.
	// this should not change nullness of sets or time quantums,
	// but should change their results. it should make mutexes null.
	// we also clear several bits in row 1/"b" -- bits which were
	// never set. this should have no impact on anything, so we don't
	// reflect it here.
	phase = 1
	for i := 0; i < (1 << 6); i++ {
		madeAny := false
		for k, v := range nullHandlingFieldMasks {
			if i&v != 0 {
				if k[0] == 'm' && (i%5) == 0 {
					out[phase][k].null(uint64(i))
				} else {
					out[phase][k].notNull(uint64(i))
				}
				// even if the only value was the mutex field, the record
				// was ever created, so the record still exists even if
				// every field is null.
				madeAny = true
			}
		}
		if madeAny {
			for k, v := range nullHandlingFieldMasks {
				if i&v == 0 {
					out[phase][k].null(uint64(i))
				}
			}
		}
	}

	// phase 2: delete the &16 and &32 rows. we expect deleted records
	// to be neither null nor not null. We delete both the &16 and &32
	// rows because the delete flow is different for keys and no-keys.
	phase = 2
	for k := range nullHandlingFieldMasks {
		ns := out[1][k].clone()
		ns.trim(func(v uint64) bool { return v >= 16 && (v%5) != 0 })
		out[phase][k] = ns
	}

	// phase 3: create a record which had previously existed.
	// we expect previously-existing fields to now show as null,
	// unless we created them again.
	phase = 3
	for k, v := range nullHandlingFieldMasks {
		ns := out[2][k].clone()
		if v == 32 {
			ns.notNull(63)
		} else {
			ns.null(63)
		}
		out[phase][k] = ns
	}
	return out
}

func nullTestQuery(t *testing.T, api *pilosa.API, req *pilosa.QueryRequest) pilosa.QueryResponse {
	t.Helper()
	resp, err := api.Query(context.Background(), req)
	if err != nil {
		t.Fatalf("running request: %v", err)
	}
	if resp.Err != nil {
		t.Fatalf("request returned unexpected error: %v", resp.Err)
	}
	return resp
}

func nullTestRows(t *testing.T, api *pilosa.API, req *pilosa.QueryRequest) [][]uint64 {
	t.Helper()
	resp := nullTestQuery(t, api, req)
	out := make([][]uint64, 0, len(resp.Results))
	for _, result := range resp.Results {
		row, ok := result.(*pilosa.Row)
		if !ok {
			t.Fatalf("expected row result from query, got %T", result)
		}
		out = append(out, row.Columns())
	}
	return out
}

func nullTestImport(t *testing.T, api *pilosa.API, req *pilosa.ImportRequest) {
	t.Helper()
	qcx := api.Txf().NewQcx()
	defer qcx.Abort()
	err := api.Import(context.Background(), qcx, req)
	if err != nil {
		t.Fatalf("importing: %v", err)
	}
	err = qcx.Finish()
	if err != nil {
		t.Fatalf("committing: %v", err)
	}
}

func nullTestExpectResults(t *testing.T, api *pilosa.API, index string, phase int) {
	t.Helper()
	for k, expected := range nullHandlingExpectedResults[phase] {
		t.Run(fmt.Sprintf("%s-phase-%d", k, phase), func(t *testing.T) {
			req := &pilosa.QueryRequest{
				Index: index,
				Query: fmt.Sprintf(`Row(%s == null)
	Row(%s != null)`, k, k),
			}
			rows := nullTestRows(t, api, req)
			t.Run("true", func(t *testing.T) {
				require.Equal(t, expected[true], rows[0])
			})
			t.Run("false", func(t *testing.T) {
				require.Equal(t, expected[false], rows[1])
			})
		})
	}
}

// TestNullHandlingSet only tests the behavior of null values
// in non-BSI fields. It tests this using set/clear.
func TestNullHandlingSet(t *testing.T) {
	c, index, api := setupNullHandlingSchema(t, "i")
	defer c.Close()

	phase := 0
	var reqs []string
	for i := 0; i < (1 << 6); i++ {
		for k, v := range nullHandlingFieldMasks {
			if i&v != 0 {
				if k[1] == 'k' {
					reqs = append(reqs, fmt.Sprintf(`Set(%d, %s="a")`, i, k))
				} else {
					reqs = append(reqs, fmt.Sprintf(`Set(%d, %s=0)`, i, k))
				}
			}
		}
	}
	req := &pilosa.QueryRequest{
		Index: index,
		Query: strings.Join(reqs, "\n"),
	}
	resp := nullTestQuery(t, api, req)
	for i, v := range resp.Results {
		if v != true {
			t.Fatalf("result %d (request %s): %#v", i, reqs[i], v)
		}
	}
	nullTestExpectResults(t, api, index, phase)

	phase = 1
	reqs = reqs[:0]
	for i := 0; i < (1 << 6); i += 5 {
		for k := range nullHandlingFieldMasks {
			if k[1] == 'k' {
				reqs = append(reqs, fmt.Sprintf(`Clear(%d, %s="a")`, i, k))
			} else {
				reqs = append(reqs, fmt.Sprintf(`Clear(%d, %s=0)`, i, k))
			}
		}
	}
	// clear a lot of bits that weren't set in the first place. this should have
	// no effect on null/not-null state.
	for i := 16; i < 48; i++ {
		for k := range nullHandlingFieldMasks {
			if k[1] == 'k' {
				reqs = append(reqs, fmt.Sprintf(`Clear(%d, %s="b")`, i, k))
			} else {
				reqs = append(reqs, fmt.Sprintf(`Clear(%d, %s=1)`, i, k))
			}
		}
	}
	req = &pilosa.QueryRequest{
		Index: index,
		Query: strings.Join(reqs, "\n"),
	}
	resp = nullTestQuery(t, api, req)
	for i, v := range resp.Results {
		// it's okay to get either true or false, because some of those bits
		// wouldn't have existed before, so the clear would fail. that's fine.
		if v != true && v != false {
			t.Fatalf("result %d (request %s): %#v", i, reqs[i], v)
		}
	}
	nullTestExpectResults(t, api, index, phase)

	phase = 2
	req = &pilosa.QueryRequest{
		Index: index,
		Query: `Delete(Row(tk="a")) Delete(Row(tu=0))`,
	}
	resp = nullTestQuery(t, api, req)
	if len(resp.Results) != 2 || resp.Results[0] != true || resp.Results[1] != true {
		t.Fatalf("expected two trues, got %#v", resp.Results)
	}
	nullTestExpectResults(t, api, index, phase)

	phase = 3
	req = &pilosa.QueryRequest{
		Index: index,
		Query: `Set(63, tk="a")`,
	}
	resp = nullTestQuery(t, api, req)
	if len(resp.Results) != 1 || resp.Results[0] != true {
		t.Fatalf("expected single true result, got %#v", resp.Results)
	}
	nullTestExpectResults(t, api, index, phase)
}

// TestNullHandlingImport only tests the behavior of null values
// in non-BSI fields. It tests this using the old Import API to
// import values.
func TestNullHandlingImport(t *testing.T) {
	c, index, api := setupNullHandlingSchema(t, "i")
	defer c.Close()

	phase := 0
	reqs := map[string]*pilosa.ImportRequest{}
	for k := range nullHandlingFieldMasks {
		reqs[k] = &pilosa.ImportRequest{
			Index: index,
			Field: k,
			Shard: ^uint64(0),
		}
	}
	for i := 0; i < (1 << 6); i++ {
		for k, v := range nullHandlingFieldMasks {
			if i&v != 0 {
				if k[1] == 'k' {
					reqs[k].ColumnIDs = append(reqs[k].ColumnIDs, uint64(i))
					reqs[k].RowKeys = append(reqs[k].RowKeys, "a")
				} else {
					reqs[k].ColumnIDs = append(reqs[k].ColumnIDs, uint64(i))
					reqs[k].RowIDs = append(reqs[k].RowIDs, 0)
				}
			}
		}
	}
	for _, req := range reqs {
		nullTestImport(t, api, req)
	}
	nullTestExpectResults(t, api, index, phase)

	phase = 1
	for k := range nullHandlingFieldMasks {
		reqs[k].ColumnIDs = reqs[k].ColumnIDs[:0]
		if k[1] == 'k' {
			reqs[k].RowKeys = reqs[k].RowKeys[:0]
			// the import stashed its computed RowIDs in the req,
			// remove them.
			reqs[k].RowIDs = nil
		} else {
			reqs[k].RowIDs = reqs[k].RowIDs[:0]
		}
		reqs[k].Clear = true
		reqs[k].Shard = ^uint64(0)
	}

	for i := 0; i < (1 << 6); i += 5 {
		for k := range nullHandlingFieldMasks {
			if k[1] == 'k' {
				reqs[k].ColumnIDs = append(reqs[k].ColumnIDs, uint64(i))
				reqs[k].RowKeys = append(reqs[k].RowKeys, "a")
				reqs[k].ColumnIDs = append(reqs[k].ColumnIDs, uint64(i))
				reqs[k].RowKeys = append(reqs[k].RowKeys, "b")
			} else {
				reqs[k].ColumnIDs = append(reqs[k].ColumnIDs, uint64(i))
				reqs[k].RowIDs = append(reqs[k].RowIDs, 0)
				reqs[k].ColumnIDs = append(reqs[k].ColumnIDs, uint64(i))
				reqs[k].RowIDs = append(reqs[k].RowIDs, 0)
			}
		}
	}
	// clear a lot of bits that never previously got set, expecting no impact.
	for i := 16; i < 48; i++ {
		for k := range nullHandlingFieldMasks {
			if k[1] == 'k' {
				reqs[k].ColumnIDs = append(reqs[k].ColumnIDs, uint64(i))
				reqs[k].RowKeys = append(reqs[k].RowKeys, "b")
			} else {
				reqs[k].ColumnIDs = append(reqs[k].ColumnIDs, uint64(i))
				reqs[k].RowIDs = append(reqs[k].RowIDs, 1)
			}
		}
	}
	for _, req := range reqs {
		t.Logf("req: %#v", req)
		nullTestImport(t, api, req)
	}
	nullTestExpectResults(t, api, index, phase)

	phase = 2
	// no Delete in Import API, we use the old API for it.
	req := &pilosa.QueryRequest{
		Index: index,
		Query: `Delete(Row(tk="a")) Delete(Row(tu=0))`,
	}
	resp := nullTestQuery(t, api, req)
	if len(resp.Results) != 2 || resp.Results[0] != true || resp.Results[1] != true {
		t.Fatalf("expected two trues, got %#v", resp.Results)
	}
	nullTestExpectResults(t, api, index, phase)
	phase = 3
	reqImport := &pilosa.ImportRequest{
		Index:     index,
		Field:     "tk",
		ColumnIDs: []uint64{63},
		RowKeys:   []string{"a"},
		Shard:     ^uint64(0),
	}
	nullTestImport(t, api, reqImport)
	nullTestExpectResults(t, api, index, phase)
}

func TestNullHandlingBatch(t *testing.T) {
	c, index, api := setupNullHandlingSchema(t, "i")
	defer c.Close()
	ctx := context.Background()
	fapi := pilosa.NewOnPremSchema(api)
	imp := pilosa.NewOnPremImporter(api)
	tbl, err := fapi.TableByName(ctx, dax.TableName(index))
	if err != nil {
		t.Fatalf("getting table defs: %v", err)
	}
	idxInfoBase := pilosa.TableToIndexInfo(tbl)
	fields := make([]*pilosa.FieldInfo, 0, len(nullHandlingFieldMasks))
	for k := range nullHandlingFieldMasks {
		fields = append(fields, idxInfoBase.Field(k))
	}

	phase := 0
	b, err := batch.NewBatch(imp, 10000, tbl, fields, batch.OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}
	var row batch.Row
	row.Values = make([]interface{}, len(fields))
	// we don't add the all-null row for ID 0
	for i := 1; i < (1 << 6); i++ {
		row.ID = uint64(i)
		row.Values = row.Values[:0]
		for _, fld := range fields {
			if i&nullHandlingFieldMasks[fld.Name] != 0 {
				if fld.Name[1] == 'k' {
					row.Values = append(row.Values, "a")
				} else {
					row.Values = append(row.Values, uint64(0))
				}
			} else {
				row.Values = append(row.Values, nil)
			}
		}
		err := b.Add(row)
		if err != nil {
			t.Fatalf("adding row: %v", err)
		}
	}
	if err := b.Import(); err != nil {
		t.Fatalf("importing batch: %v", err)
	}
	nullTestExpectResults(t, api, index, phase)

	phase = 1
	b, err = batch.NewBatch(imp, 10000, tbl, fields, batch.OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}
	// delete everything which is a multiple of 5, but
	// don't try to delete 0 because this creates an all-null
	// record.
	for i := 5; i < (1 << 6); i += 5 {
		row.ID = uint64(i)
		row.Values = row.Values[:0]
		row.Clears = map[int]interface{}{}
		for i, fld := range fields {
			row.Values = append(row.Values, nil)
			if fld.Name[0] == 'm' {
				// can't specify a particular bit to clear in a mutex
				row.Clears[i] = nil
			} else {
				if fld.Name[1] == 'k' {
					row.Clears[i] = "a"
				} else {
					row.Clears[i] = uint64(0)
				}
			}
		}
		err := b.Add(row)
		if err != nil {
			t.Fatalf("adding row: %v", err)
		}
	}
	if err := b.Import(); err != nil {
		t.Fatalf("importing batch: %v", err)
	}
	b, err = batch.NewBatch(imp, 10000, tbl, fields, batch.OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}
	// delete everything which is a multiple of 5
	for i := 16; i < 48; i++ {
		row.ID = uint64(i)
		row.Values = row.Values[:0]
		row.Clears = map[int]interface{}{}
		for i, fld := range fields {
			row.Values = append(row.Values, nil)
			// no way to specify "clear a specific bit", so we don't clear the mutexes.
			if fld.Name[0] != 'm' {
				if fld.Name[1] == 'k' {
					row.Clears[i] = "b"
				} else {
					row.Clears[i] = uint64(1)
				}
			}
		}
		err := b.Add(row)
		if err != nil {
			t.Fatalf("adding row: %v", err)
		}
	}
	if err := b.Import(); err != nil {
		t.Fatalf("importing batch: %v", err)
	}
	nullTestExpectResults(t, api, index, phase)

	phase = 2
	// no Delete in batch API, we use the old API for it.
	req := &pilosa.QueryRequest{
		Index: index,
		Query: `Delete(Row(tk="a")) Delete(Row(tu=0))`,
	}
	resp := nullTestQuery(t, api, req)
	if len(resp.Results) != 2 || resp.Results[0] != true || resp.Results[1] != true {
		t.Fatalf("expected two trues, got %#v", resp.Results)
	}
	nullTestExpectResults(t, api, index, phase)

	phase = 3
	// truncate fields to only have the one field in it
	for _, f := range fields {
		if f.Name == "tk" {
			fields[0] = f
			fields = fields[:1]
			break
		}
	}
	b, err = batch.NewBatch(imp, 10000, tbl, fields, batch.OptUseShardTransactionalEndpoint(true))
	if err != nil {
		t.Fatalf("getting batch: %v", err)
	}
	row.ID = uint64(63)
	row.Values = []interface{}{[]string{"a"}}
	err = b.Add(row)
	if err != nil {
		t.Fatalf("adding row: %v", err)
	}
	if err := b.Import(); err != nil {
		t.Fatalf("importing batch: %v", err)
	}
	nullTestExpectResults(t, api, index, phase)
}
