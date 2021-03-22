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

//+build integration

package client

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto" //nolint:staticcheck
	pnet "github.com/pilosa/pilosa/v2/net"
	"github.com/pilosa/pilosa/v2/pb"
	"github.com/pkg/errors"
)

var schema = NewSchema()
var index *Index
var indexName = "go-testindex"
var keysIndex *Index
var schemaTestIndex *Index
var testField *Field

// AtomicRecord import test stuff
var indexAR *Index
var indexARname = "i"
var fieldAcct0 = "acct0"
var fieldAcct1 = "acct1"
var field0 *Field
var field1 *Field

func TestMain(m *testing.M) {
	Setup()
	r := m.Run()
	TearDown()
	os.Exit(r)
}

func Setup() {
	client := getClient()

	// Make sure the existing schema is empty.
	if existingSchema, err := client.Schema(); err != nil {
		panic(err)
	} else if indexes := existingSchema.Indexes(); len(indexes) > 0 {
		TearDown()
		//panic(fmt.Sprintf("Pilosa data isn't clean, found indexes: %v", indexes))
	}

	testSchema := NewSchema()
	index = testSchema.Index(indexName)
	keysIndex = testSchema.Index("go-testinindex-keys", OptIndexKeys(true))
	schemaTestIndex = testSchema.Index("schema-test-index",
		OptIndexKeys(true),
		OptIndexTrackExistence(false))
	testField = index.Field("test-field")

	indexAR = testSchema.Index(indexARname)
	field0 = indexAR.Field(fieldAcct0, OptFieldTypeInt(-1000, 1000))
	field1 = indexAR.Field(fieldAcct1, OptFieldTypeInt(-1000, 1000))

	err := client.SyncSchema(testSchema)
	if err != nil {
		panic(err)
	}
	_ = client.Close()
}

func TearDown() {
	client := getClient()
	if client == nil {
		return
	}
	defer client.Close()
	if err := client.DeleteIndex(index); err != nil {
		panic(err)
	}
	if err := client.DeleteIndex(indexAR); err != nil {
		panic(err)
	}

	if err := client.DeleteIndex(keysIndex); err != nil {
		panic(err)
	}
	if err := client.DeleteIndex(schemaTestIndex); err != nil {
		panic(err)
	}
}

func Reset() {
	TearDown()
	Setup()
}

func TestCreateDefaultClient(t *testing.T) {
	client := DefaultClient()
	if client == nil {
		t.Fatal()
	}
}

func TestClientReturnsResponse(t *testing.T) {
	client := getClient()
	defer client.Close()
	response, err := client.Query(testField.Row(1))
	if err != nil {
		t.Fatalf("Error querying: %s", err)
	}
	if response == nil {
		t.Fatalf("Response should not be nil")
	}
}

func TestQueryWithShards(t *testing.T) {
	Reset()
	const shardWidth = 1048576
	client := getClient()
	defer client.Close()
	if _, err := client.Query(testField.Set(1, 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(testField.Set(1, shardWidth)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(testField.Set(1, shardWidth*3)); err != nil {
		t.Fatal(err)
	}

	response, err := client.Query(testField.Row(1), OptQueryShards(0, 3))
	if err != nil {
		t.Fatal(err)
	}
	if columns := response.Result().Row().Columns; !reflect.DeepEqual(columns, []uint64{100, shardWidth * 3}) {
		t.Fatalf("Unexpected results: %#v", columns)
	}
}

func TestQueryWithColumns(t *testing.T) {
	Reset()
	client := getClient()
	defer client.Close()
	targetAttrs := map[string]interface{}{
		"name":       "some string",
		"age":        int64(95),
		"registered": true,
		"height":     1.83,
	}
	_, err := client.Query(testField.Set(1, 100))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(index.SetColumnAttrs(100, targetAttrs))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(response.Column(), ColumnItem{}) {
		t.Fatalf("No columns should be returned if it wasn't explicitly requested")
	}
	response, err = client.Query(testField.Row(1), &QueryOptions{ColumnAttrs: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.ColumnAttrs()) != 1 {
		t.Fatalf("Column count should be == 1")
	}
	columns := response.Columns()
	if len(columns) != 1 {
		t.Fatalf("Column count should be == 1")
	}
	if columns[0].ID != 100 {
		t.Fatalf("Column ID should be == 100")
	}
	if !reflect.DeepEqual(columns[0].Attributes, targetAttrs) {
		t.Fatalf("Column attrs does not match")
	}

	if !reflect.DeepEqual(response.Column(), columns[0]) {
		t.Fatalf("Columns() should be equivalent to first column in the response")
	}
}

func TestSetRowAttrs(t *testing.T) {
	Reset()
	client := getClient()
	defer client.Close()
	targetAttrs := map[string]interface{}{
		"name":       "some string",
		"age":        int64(95),
		"registered": true,
		"height":     1.83,
	}
	_, err := client.Query(testField.Set(1, 100))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(testField.SetRowAttrs(1, targetAttrs))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(testField.Row(1), &QueryOptions{ColumnAttrs: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(targetAttrs, response.Result().Row().Attributes) {
		t.Fatalf("Row attributes should be set")
	}
}

func TestOrmCount(t *testing.T) {
	client := getClient()
	defer client.Close()
	countField := index.Field("count-test")
	err := client.EnsureField(countField)
	if err != nil {
		t.Fatal(err)
	}
	qry := index.BatchQuery(
		countField.Set(10, 20),
		countField.Set(10, 21),
		countField.Set(15, 25),
	)
	_, err = client.Query(qry)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(index.Count(countField.Row(10)))
	if err != nil {
		t.Fatal(err)
	}
	if response.Result().Count() != 2 {
		t.Fatalf("Count should be 2")
	}
}

func TestDecimalField(t *testing.T) {
	client := getClient()
	defer client.Close()
	decField := index.Field("a-decimal", OptFieldTypeDecimal(3))
	err := client.EnsureField(decField)
	if err != nil {
		t.Fatal(err)
	}

	sch, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	idx := sch.indexes["go-testindex"]
	if opts := idx.Field("a-decimal").Options(); opts.scale != 3 {
		t.Fatalf("scale should be 3, but: %v", opts)
	}

}

func TestIntersectReturns(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("segments")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	qry1 := index.BatchQuery(
		field.Set(2, 10),
		field.Set(2, 15),
		field.Set(3, 10),
		field.Set(3, 20),
	)
	_, err = client.Query(qry1)
	if err != nil {
		t.Fatal(err)
	}

	qry2 := index.Intersect(field.Row(2), field.Row(3))
	response, err := client.Query(qry2)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results()) != 1 {
		t.Fatal("There must be 1 result")
	}
	if !reflect.DeepEqual(response.Result().Row().Columns, []uint64{10}) {
		t.Fatal("Returned columns must be: [10]")
	}
}

func TestTopNReturns(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("topn_test")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	qry := index.BatchQuery(
		field.Set(10, 5),
		field.Set(10, 10),
		field.Set(10, 15),
		field.Set(20, 5),
		field.Set(30, 5),
	)
	_, err = client.Query(qry)
	if err != nil {
		t.Fatal(err)
	}

	// XXX: The following is required to make this test pass. See: https://github.com/pilosa/pilosa/issues/625
	_, _, err = client.HTTPRequest("POST", "/recalculate-caches", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err := client.Query(field.TopN(2))
	if err != nil {
		t.Fatal(err)
	}
	items := response.Result().CountItems()
	if len(items) != 2 {
		t.Fatalf("There should be 2 count items: %v", items)
	}
	item := items[0]
	if item.ID != 10 {
		t.Fatalf("Item[0] ID should be 10")
	}
	if item.Count != 3 {
		t.Fatalf("Item[0] Count should be 3")
	}

	_, err = client.Query(field.SetRowAttrs(10, map[string]interface{}{"foo": "bar"}))
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.Query(field.FilterAttrTopN(5, nil, "foo", "bar"))
	if err != nil {
		t.Fatal(err)
	}
	items = response.Result().CountItems()
	if len(items) != 1 {
		t.Fatalf("There should be 1 count item: %v", items)
	}
	item = items[0]
	if item.ID != 10 {
		t.Fatalf("Item[0] ID should be 10")
	}
	if item.Count != 3 {
		t.Fatalf("Item[0] Count should be 3")
	}
}

func TestMinMaxRow(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("test-minmaxrow-field")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	qry := index.BatchQuery(
		field.Set(10, 5),
		field.Set(10, 10),
		field.Set(10, 15),
		field.Set(20, 5),
		field.Set(30, 5),
	)
	_, err = client.Query(qry)
	if err != nil {
		t.Fatalf("error setting bits: %v", err)
	}

	response, err := client.Query(field.MinRow())
	if err != nil {
		t.Fatalf("error executing min: %v", err)
	}
	min := response.Result().CountItem().ID
	response, err = client.Query(field.MaxRow())
	if err != nil {
		t.Fatalf("error executing max: %v", err)
	}
	max := response.Result().CountItem().ID

	if min != 10 {
		t.Fatalf("Min should be 10, got %v instead", min)
	}
	if max != 30 {
		t.Fatalf("Max should be 30, got %v instead", max)
	}
}

func TestSetMutexField(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("mutex-test", OptFieldTypeMutex(CacheTypeDefault, 0))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}

	// can set mutex
	_, err = client.Query(field.Set(1, 100))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(field.Row(1))
	if err != nil {
		t.Fatal(err)
	}
	target := []uint64{100}
	if !reflect.DeepEqual(target, response.Result().Row().Columns) {
		t.Fatalf("%v != %v", target, response.Result().Row().Columns)
	}

	// setting another row removes the previous
	_, err = client.Query(field.Set(42, 100))
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.Query(index.BatchQuery(
		field.Row(1),
		field.Row(42),
	))
	if err != nil {
		t.Fatal(err)
	}
	target1 := []uint64(nil)
	target42 := []uint64{100}
	if !reflect.DeepEqual(target1, response.Results()[0].Row().Columns) {
		t.Fatalf("%#v != %#v", target1, response.Results()[0].Row().Columns)
	}
	if !reflect.DeepEqual(target42, response.Results()[1].Row().Columns) {
		t.Fatalf("%#v != %#v", target42, response.Results()[1].Row().Columns)
	}
}

func TestSetBoolField(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("bool-test", OptFieldTypeBool())
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}

	// can set bool
	_, err = client.Query(field.Set(true, 100))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(field.Row(true))
	if err != nil {
		t.Fatal(err)
	}
	target := []uint64{100}
	if !reflect.DeepEqual(target, response.Result().Row().Columns) {
		t.Fatalf("%v != %v", target, response.Result().Row().Columns)
	}
}

func TestClearRowQuery(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("clear-row-test")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Query(index.BatchQuery(
		field.Set(1, 100),
		field.Set(1, 200),
	))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(field.Row(1))
	if err != nil {
		t.Fatal(err)
	}
	target := []uint64{100, 200}
	if !reflect.DeepEqual(target, response.Result().Row().Columns) {
		t.Fatalf("%v != %v", target, response.Result().Row().Columns)
	}

	_, err = client.Query(field.ClearRow(1))
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.Query(field.Row(1))
	if err != nil {
		t.Fatal(err)
	}
	target = []uint64(nil)
	if !reflect.DeepEqual(target, response.Result().Row().Columns) {
		t.Fatalf("%v != %v", target, response.Result().Row().Columns)
	}
}

func TestRowsQuery(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("rows-test")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.BatchQuery(
		field.Set(1, 100),
		field.Set(1, 200),
		field.Set(2, 200),
	))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Query(field.Rows())
	if err != nil {
		t.Fatal(err)
	}
	target := RowIdentifiersResult{
		IDs: []uint64{1, 2},
	}
	if !reflect.DeepEqual(target, resp.Result().RowIdentifiers()) {
		t.Fatalf("%v != %v", target, resp.Result().RowIdentifiers())
	}
}

func TestUnionRowsQuery(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("rows-test")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.BatchQuery(
		field.Set(1, 100),
		field.Set(1, 200),
		field.Set(2, 200),
	))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Query(field.Rows().Union())
	if err != nil {
		t.Fatal(err)
	}
	target := []uint64{100, 200}
	if !reflect.DeepEqual(target, resp.Result().Row().Columns) {
		t.Fatalf("%v != %v", target, resp.Result().Row().Columns)
	}
}

func TestLikeQuery(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("like-test", OptFieldKeys(true))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.BatchQuery(
		field.Set("a", 100),
		field.Set("b", 200),
		field.Set("bc", 200),
	))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Query(field.Like("b%"))
	if err != nil {
		t.Fatal(err)
	}
	target := RowIdentifiersResult{
		Keys: []string{"b", "bc"},
	}
	if !reflect.DeepEqual(target, resp.Result().RowIdentifiers()) {
		t.Fatalf("%v != %v", target, resp.Result().RowIdentifiers())
	}
}

func TestGroupByQuery(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("group-by-test")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.BatchQuery(
		field.Set(1, 100),
		field.Set(1, 200),
		field.Set(2, 200),
	))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Query(index.GroupBy(field.Rows()))
	if err != nil {
		t.Fatal(err)
	}
	target := []GroupCount{
		{Groups: []FieldRow{{FieldName: "group-by-test", RowID: 1}}, Count: 2},
		{Groups: []FieldRow{{FieldName: "group-by-test", RowID: 2}}, Count: 1},
	}

	checkGroupBy(t, target, resp.Result().GroupCounts())
}

func TestGroupByIntQuery(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("fint", OptFieldTypeInt(-10, 10))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.RawQuery(`
	Set(0, fint=1)
	Set(1, fint=2)

	Set(2,fint=-2)
	Set(3,fint=-1)

	Set(4,fint=4)

	Set(10, fint=0)
	Set(100, fint=0)
	Set(1000, fint=0)
	Set(10000,fint=0)
	Set(100000,fint=0)
	`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Query(index.GroupBy(field.Rows()))
	if err != nil {
		t.Fatal(err)
	}
	var a, b, c, d, e, f int64 = -2, -1, 0, 1, 2, 4
	target := []GroupCount{
		{Groups: []FieldRow{{FieldName: "fint", Value: &a}}, Count: 1},
		{Groups: []FieldRow{{FieldName: "fint", Value: &b}}, Count: 1},
		{Groups: []FieldRow{{FieldName: "fint", Value: &c}}, Count: 5},
		{Groups: []FieldRow{{FieldName: "fint", Value: &d}}, Count: 1},
		{Groups: []FieldRow{{FieldName: "fint", Value: &e}}, Count: 1},
		{Groups: []FieldRow{{FieldName: "fint", Value: &f}}, Count: 1},
	}

	checkGroupBy(t, target, resp.Result().GroupCounts())
}

func checkGroupBy(t *testing.T, expected, results []GroupCount) {
	t.Helper()
	if len(results) != len(expected) {
		t.Fatalf("number of groupings mismatch:\n got:%+v\nwant:%+v\n", results, expected)
	}
	for i, result := range results {
		if !reflect.DeepEqual(expected[i], result) {
			t.Fatalf("unexpected result at %d: \n got:%+v\nwant:%+v\n", i, result, expected[i])
		}
	}
}

func TestCreateDeleteIndexField(t *testing.T) {
	client := getClient()
	defer client.Close()
	index1 := NewIndex("to-be-deleted")
	field1 := index1.Field("foo")
	err := client.CreateIndex(index1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateField(field1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.DeleteField(field1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.DeleteIndex(index1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnsureIndexExists(t *testing.T) {
	client := getClient()
	defer client.Close()
	err := client.EnsureIndex(index)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnsureFieldExists(t *testing.T) {
	client := getClient()
	defer client.Close()
	err := client.EnsureField(testField)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateFieldWithTimeQuantum(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("field-with-timequantum", OptFieldTypeTime(TimeQuantumYear))
	err := client.CreateField(field)
	if err != nil {
		t.Fatal(err)
	}
}

func TestErrorCreatingIndex(t *testing.T) {
	client := getClient()
	defer client.Close()
	err := client.CreateIndex(index)
	if err == nil {
		t.Fatal()
	}
}

func TestErrorCreatingField(t *testing.T) {
	client := getClient()
	defer client.Close()
	err := client.CreateField(testField)
	if err == nil {
		t.Fatal()
	}
}

func TestIndexAlreadyExists(t *testing.T) {
	client := getClient()
	defer client.Close()
	err := client.CreateIndex(index)
	if err != ErrIndexExists {
		t.Fatal(err)
	}
}

func TestQueryWithEmptyClusterFails(t *testing.T) {
	client, _ := NewClient(DefaultCluster(), OptClientRetries(0))
	attrs := map[string]interface{}{"a": 1}
	_, err := client.Query(index.SetColumnAttrs(0, attrs))
	if errors.Cause(err) != ErrEmptyCluster {
		t.Fatal(err)
	}
}

func TestFailoverFail(t *testing.T) {
	uri, _ := pnet.NewURIFromAddress("does-not-resolve.foo.bar")
	cluster := NewClusterWithHost(uri, uri, uri, uri)
	client, _ := NewClient(cluster, OptClientRetries(0))
	attrs := map[string]interface{}{"a": 1}
	_, err := client.Query(index.SetColumnAttrs(0, attrs))
	if !strings.Contains(err.Error(), ErrTriedMaxHosts.Error()) {
		t.Fatalf("ErrTriedMaxHosts error should be returned. Got: %v", err)
	}
}

func TestQueryFailsIfAddressNotResolved(t *testing.T) {
	uri, _ := pnet.NewURIFromAddress("nonexisting.domain.pilosa.com:3456")
	client, _ := NewClient(uri, OptClientRetries(0))
	_, err := client.Query(index.RawQuery("bar"))
	if err == nil {
		t.Fatal()
	}
}

func TestQueryFails(t *testing.T) {
	client := getClient()
	defer client.Close()
	_, err := client.Query(index.RawQuery("Invalid query"))
	if err == nil {
		t.Fatal()
	}
}

func TestInvalidHttpRequest(t *testing.T) {
	client := getClient()
	defer client.Close()
	_, _, err := client.HTTPRequest("INVALID METHOD", "/foo", nil, nil)
	if err == nil {
		t.Fatal()
	}
}

func TestErrorResponseNotRead(t *testing.T) {
	server := getMockServer(500, []byte("Unknown error"), 512)
	defer server.Close()
	uri, err := pnet.NewURIFromAddress(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := NewClient(uri, OptClientRetries(0))
	response, err := client.Query(testField.Row(1))
	if err == nil {
		t.Fatalf("Got response: %v", response)
	}
}

func TestResponseNotRead(t *testing.T) {
	server := getMockServer(200, []byte("some content"), 512)
	defer server.Close()
	uri, err := pnet.NewURIFromAddress(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := NewClient(uri, OptClientRetries(0))
	response, err := client.Query(testField.Row(1))
	if err == nil {
		t.Fatalf("Got response: %v", response)
	}
}

func TestSchema(t *testing.T) {
	client := getClient()
	defer client.Close()
	schema, err := client.Schema()
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.indexes) < 1 {
		t.Fatalf("There should be at least 1 index in the schema")
	}
	f := schemaTestIndex.Field("schema-test-field",
		OptFieldTypeSet(CacheTypeLRU, 9999),
		OptFieldKeys(true),
	)
	if f == nil {
		t.Fatal("f should not be nil")
	}
	if err := client.EnsureField(f); err != nil {
		t.Fatalf("ensuring field: %v", err)
	}
	err = client.SyncSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	schema, err = client.Schema()
	if err != nil {
		t.Fatal(err)
	}
	i2 := schema.indexes[schemaTestIndex.Name()]
	if !reflect.DeepEqual(schemaTestIndex.options, i2.options) {
		t.Fatalf("%v != %v", schemaTestIndex.options, i2.options)
	}

	f2 := schema.indexes[schemaTestIndex.Name()].fields["schema-test-field"]
	if f2 == nil {
		t.Fatal("Field should not be nil")
	}
	if f2 != nil { // happy linter
		opt := f2.options
		if opt.cacheType != CacheTypeLRU {
			t.Fatalf("cache type %s != %s", CacheTypeLRU, opt.cacheType)
		}
		if opt.cacheSize != 9999 {
			t.Fatalf("cache size 9999 != %d", opt.cacheSize)
		}
		if !opt.keys {
			t.Fatalf("keys true != %v", opt.keys)
		}
		if !reflect.DeepEqual(f.options, f2.options) {
			t.Fatalf("%v != %v", f.options, f2.options)
		}
	}
}

func TestSync(t *testing.T) {
	client := getClient()
	defer client.Close()
	remoteIndex := NewIndex("remote-index-1")
	err := client.EnsureIndex(remoteIndex)
	if err != nil {
		t.Fatal(err)
	}
	remoteField := remoteIndex.Field("remote-field-1")
	err = client.EnsureField(remoteField)
	if err != nil {
		t.Fatal(err)
	}
	schema1 := NewSchema()
	index11 := schema1.Index("diff-index1")
	index11.Field("field1-1")
	index11.Field("field1-2")
	index12 := schema1.Index("diff-index2")
	index12.Field("field2-1")
	schema1.Index(remoteIndex.Name())

	err = client.SyncSchema(schema1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.DeleteIndex(remoteIndex)
	if err != nil {
		t.Fatal(err)
	}

	err = client.DeleteIndex(index11)
	if err != nil {
		t.Fatal(err)
	}

	err = client.DeleteIndex(index12)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSyncFailure(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	uri, err := pnet.NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client, _ := NewClient(uri, OptClientRetries(0))
	err = client.SyncSchema(NewSchema())
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestErrorRetrievingSchema(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	uri, err := pnet.NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client, _ := NewClient(uri, OptClientRetries(0))
	_, err = client.Schema()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestExportReaderFailure(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	uri, err := pnet.NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	field := index.Field("exportfield")
	shardURIs := map[uint64]*pnet.URI{
		0: uri,
	}
	client, _ := NewClient(uri, OptClientRetries(0))
	reader := newExportReader(client, shardURIs, field)
	buf := make([]byte, 1000)
	_, err = reader.Read(buf)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestExportReaderReadBodyFailure(t *testing.T) {
	server := getMockServer(200, []byte("not important"), 100)
	defer server.Close()
	uri, err := pnet.NewURIFromAddress(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	field := index.Field("exportfield")
	shardURIs := map[uint64]*pnet.URI{0: uri}
	client, _ := NewClient(uri, OptClientRetries(0))
	reader := newExportReader(client, shardURIs, field)
	buf := make([]byte, 1000)
	_, err = reader.Read(buf)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestFetchFragmentNodes(t *testing.T) {
	client := getClient()
	defer client.Close()
	nodes, err := client.fetchFragmentNodes(index.Name(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("1 node should be returned")
	}
	// running the same for coverage
	nodes, err = client.fetchFragmentNodes(index.Name(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("1 node should be returned")
	}
}

func TestFetchStatus(t *testing.T) {
	client := getClient()
	defer client.Close()
	status, err := client.Status()
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Nodes) == 0 {
		t.Fatalf("There should be at least 1 host in the status")
	}
}

func TestFetchInfo(t *testing.T) {
	client := getClient()
	defer client.Close()
	info, err := client.Info()
	if err != nil {
		t.Fatal(err)
	}
	if info.ShardWidth == 0 {
		t.Fatalf("shard width should not be zero")
	}
	if info.Memory < (512 * 1024 * 1024) {
		t.Fatalf("server memory [%d bytes] under 512MB seems highly improbable", info.Memory)
	}
	if info.CPUPhysicalCores < 1 || info.CPULogicalCores < 1 {
		t.Fatalf("server did not detect any CPU cores")
	}
	if info.CPUType == "" {
		t.Fatalf("server reported empty string for CPU type")
	}
	if info.CPUMHz == 0 {
		t.Fatalf("server reported 0MHz processor")
	}
}

func TestRowRangeQuery(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("test-rowrangefield", OptFieldTypeTime(TimeQuantumMonthDayHour))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.BatchQuery(
		field.SetTimestamp(10, 100, time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)),
		field.SetTimestamp(10, 100, time.Date(2018, time.January, 1, 0, 0, 0, 0, time.UTC)),
		field.SetTimestamp(10, 100, time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)),
	))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2017, time.January, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2018, time.January, 5, 0, 0, 0, 0, time.UTC)
	resp, err := client.Query(field.RowRange(10, start, end))
	if err != nil {
		t.Fatal(err)
	}
	target := []uint64{100}
	if !reflect.DeepEqual(resp.Result().Row().Columns, target) {
		t.Fatalf("%v != %v", target, resp.Result().Row().Columns)
	}
}

func TestRangeField(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("rangefield", OptFieldTypeInt())
	field2 := index.Field("rangefield-set")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.BatchQuery(
		field2.Set(1, 10),
		field2.Set(1, 100),
		field.SetIntValue(10, 11),
		field.SetIntValue(100, 15),
	))
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Query(field.Sum(field2.Row(1)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result().Value() != 26 {
		t.Fatalf("Sum 26 != %d", resp.Result().Value())
	}
	if resp.Result().Count() != 2 {
		t.Fatalf("Count 2 != %d", resp.Result().Count())
	}
}

func TestRangeField2(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("rangefield", OptFieldTypeInt(10, 20))
	field2 := index.Field("rangefield-set")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field2)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Query(field.Min(field2.Row(1)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result().Value() != 11 {
		t.Fatalf("Min 11 != %d", resp.Result().Value())
	}
	if resp.Result().Count() != 1 {
		t.Fatalf("Count 1 != %d", resp.Result().Count())
	}

	resp, err = client.Query(field.Max(field2.Row(1)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result().Value() != 15 {
		t.Fatalf("Max 15 != %d", resp.Result().Value())
	}
	if resp.Result().Count() != 1 {
		t.Fatalf("Count 1 != %d", resp.Result().Count())
	}

	resp, err = client.Query(field.LT(15))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Result().Row().Columns) != 1 {
		t.Fatalf("Count 1 != %d", len(resp.Result().Row().Columns))
	}
	if resp.Result().Row().Columns[0] != 10 {
		t.Fatalf("Column 10 != %d", resp.Result().Row().Columns[0])
	}
}

func TestNotQuery(t *testing.T) {
	client := getClient()
	defer client.Close()
	index := schema.Index("not-query-index", OptIndexTrackExistence(true))
	field := index.Field("not-field")
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cerr := client.DeleteIndex(index)
		if cerr != nil {
			t.Errorf("failed to delete index: %v", cerr)
		}
	}()

	_, err = client.Query(index.BatchQuery(
		field.Set(1, 10),
		field.Set(1, 11),
		field.Set(2, 11),
		field.Set(2, 12),
		field.Set(2, 13),
	))
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Query(index.Not(field.Row(1)))
	if err != nil {
		t.Fatal(err)
	}
	target := []uint64{12, 13}
	if !reflect.DeepEqual(target, resp.Result().Row().Columns) {
		t.Fatalf("%v != %v", target, resp.Result().Row().Columns)
	}
}

func TestStoreQuery(t *testing.T) {
	client := getClient()
	defer client.Close()
	schema := NewSchema()
	index := schema.Index("store-test")
	fromField := index.Field("x-from-field")
	toField := index.Field("x-to-field")
	err := client.SyncSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cerr := client.DeleteIndex(index)
		if cerr != nil {
			t.Errorf("failed to delete index: %v", cerr)
		}
	}()

	_, err = client.Query(index.BatchQuery(
		fromField.Set(10, 100),
		fromField.Set(10, 200),
		toField.Store(fromField.Row(10), 1),
	))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Query(toField.Row(1))
	if err != nil {
		t.Fatal(err)
	}
	target := []uint64{100, 200}
	if !reflect.DeepEqual(target, resp.Result().Row().Columns) {
		t.Fatalf("%v != %v", target, resp.Result().Row().Columns)
	}
}

func TestExcludeAttrsColumns(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("excludecolumnsattrsfield")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	attrs := map[string]interface{}{
		"foo": "bar",
	}
	_, err = client.Query(index.BatchQuery(
		field.Set(1, 100),
		field.SetRowAttrs(1, attrs),
	))
	if err != nil {
		t.Fatal(err)
	}

	// test exclude columns.
	resp, err := client.Query(field.Row(1), &QueryOptions{ExcludeColumns: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Result().Row().Columns) != 0 {
		t.Fatalf("columns should be excluded")
	}
	if len(resp.Result().Row().Attributes) != 1 {
		t.Fatalf("attributes should be included")
	}

	// test exclude attributes.
	resp, err = client.Query(field.Row(1), &QueryOptions{ExcludeRowAttrs: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Result().Row().Columns) != 1 {
		t.Fatalf("columns should be included")
	}
	if len(resp.Result().Row().Attributes) != 0 {
		t.Fatalf("attributes should be excluded")
	}
}

func TestMultipleClientKeyQuery(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := keysIndex.Field("multiple-client-field")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}

	const goroutineCount = 10
	wg := &sync.WaitGroup{}
	wg.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func(rowID uint64) {
			if _, e := client.Query(field.Set(rowID, "col")); e != nil {
				err = e
			}
			wg.Done()
		}(uint64(i))
	}
	wg.Wait()

	if err != nil {
		t.Fatal(err)
	}
}

func TestDecodingFragmentNodesFails(t *testing.T) {
	server := getMockServer(200, []byte("notjson"), 7)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, err := client.fetchFragmentNodes("foo", 0)
	if err == nil {
		t.Fatalf("fetchFragmentNodes should fail when response from /fragment/nodes cannot be decoded")
	}
}

func TestImportNodeFails(t *testing.T) {
	server := getMockServer(500, []byte{}, 0)
	defer server.Close()
	uri, _ := pnet.NewURIFromAddress(server.URL)
	client, _ := NewClient(uri, OptClientRetries(0))
	importRequest := &pb.ImportRequest{
		ColumnIDs:  []uint64{},
		RowIDs:     []uint64{},
		Timestamps: []int64{},
		Index:      "foo",
		Field:      "bar",
		Shard:      0,
	}
	data, err := proto.Marshal(importRequest)
	if err != nil {
		t.Fatalf("marshaling importRequest: %v", err)
	}
	err = client.importData(uri, "/index/foo/field/bar/import?clear=false", data)
	if err == nil {
		t.Fatalf("importNode should fail when posting to /import fails")
	}
}

func TestQueryUnmarshalFails(t *testing.T) {
	server := getMockServer(200, []byte(`{}`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	field := NewSchema().Index("foo").Field("bar")
	_, err := client.Query(field.Row(1))
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func TestResponseWithInvalidType(t *testing.T) {
	qr := &pb.QueryResponse{
		Err: "",
		ColumnAttrSets: []*pb.ColumnAttrSet{
			{
				ID: 0,
				Attrs: []*pb.Attr{
					{
						Type:        9999,
						StringValue: "NOVAL",
					},
				},
			},
		},
		Results: []*pb.QueryResult{},
	}
	data, err := proto.Marshal(qr)
	if err != nil {
		t.Fatal(err)
	}
	server := getMockServer(200, data, -1)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, err = client.Query(testField.Row(1))
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestStatusFails(t *testing.T) {
	server := getMockServer(404, nil, 0)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, err := client.Status()
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestStatusUnmarshalFails(t *testing.T) {
	server := getMockServer(200, []byte("foo"), 3)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, err := client.Status()
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestStatusToNodeShardsForIndex(t *testing.T) {
	client := getClient()
	defer client.Close()
	status := Status{
		Nodes: []StatusNode{
			{
				URI: StatusURI{
					Scheme: "https",
					Host:   "localhost",
					Port:   10101,
				},
			},
		},
		indexMaxShard: map[string]uint64{
			index.Name(): 0,
		},
	}
	shardMap, err := client.statusToNodeShardsForIndex(status, index.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(shardMap) != 1 {
		t.Fatalf("len(shardMap) %d != %d", 1, len(shardMap))
	}
	if _, ok := shardMap[0]; !ok {
		t.Fatalf("shard map should have the correct shard")
	}
}

func TestHttpRequest(t *testing.T) {
	client := getClient()
	defer client.Close()
	_, _, err := client.HTTPRequest("GET", "/status", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSyncSchemaCantCreateIndex(t *testing.T) {
	server := getMockServer(404, nil, 0)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	schema = NewSchema()
	schema.Index("foo")
	err := client.syncSchema(schema, NewSchema())
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestSyncSchemaCantCreateField(t *testing.T) {
	server := getMockServer(404, nil, 0)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	schema = NewSchema()
	index := schema.Index("foo")
	index.Field("foofield")
	serverSchema := NewSchema()
	serverSchema.Index("foo")
	err := client.syncSchema(schema, serverSchema)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestExportFieldFailure(t *testing.T) {
	paths := map[string]mockResponseItem{
		"/status": {
			content:       []byte(`{"state":"NORMAL","nodes":[{"scheme":"http","host":"localhost","port":10101}]}`),
			statusCode:    404,
			contentLength: -1,
		},
		"/pb.shards/max": {
			content:       []byte(`{"standard":{"go-testindex": 0}}`),
			statusCode:    404,
			contentLength: -1,
		},
	}
	server := getMockPathServer(paths)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, err := client.ExportField(testField)
	if err == nil {
		t.Fatal("should have failed")
	}
	statusItem := paths["/status"]
	statusItem.statusCode = 200
	paths["/status"] = statusItem
	_, err = client.ExportField(testField)
	if err == nil {
		t.Fatal("should have failed")
	}
	statusItem = paths["/pb.shards/max"]
	statusItem.statusCode = 200
	paths["/pb.shards/max"] = statusItem
	_, err = client.ExportField(testField)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestShardsMaxDecodeFailure(t *testing.T) {
	server := getMockServer(200, []byte(`{`), 0)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, err := client.shardsMax()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestReadSchemaDecodeFailure(t *testing.T) {
	server := getMockServer(200, []byte(`{`), 0)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, err := client.readSchema()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestStatusToNodeShardsForIndexFailure(t *testing.T) {
	server := getMockServer(200, []byte(`[]`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	// no shard
	status := Status{
		indexMaxShard: map[string]uint64{},
	}
	_, err := client.statusToNodeShardsForIndex(status, "foo")
	if err == nil {
		t.Fatal("should have failed")
	}

	// no fragment nodes
	status = Status{
		indexMaxShard: map[string]uint64{
			"foo": 0,
		},
	}
	_, err = client.statusToNodeShardsForIndex(status, "foo")
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestUserAgent(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		version := strings.TrimPrefix(Version, "v")
		targetUserAgent := fmt.Sprintf("pilosa/client/%s", version)
		if targetUserAgent != r.UserAgent() {
			t.Fatalf("UserAgent %s != %s", targetUserAgent, r.UserAgent())
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, _, err := client.HTTPRequest("GET", "/version", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientRace(t *testing.T) {
	uri, err := pnet.NewURIFromAddress(getPilosaBindAddress())
	if err != nil {
		panic(err)
	}
	client, err := NewClient(uri,
		OptClientTLSConfig(&tls.Config{InsecureSkipVerify: true}),
		OptClientRetries(0))
	if err != nil {
		panic(err)
	}
	f := func() {
		if _, e := client.Query(testField.Row(1)); e != nil {
			err = e
		}
	}
	for i := 0; i < 10; i++ {
		go f()
	}
	if err != nil {
		panic(err)
	}
}

func TestFetchPrimaryFails(t *testing.T) {
	server := getMockServer(404, []byte(`[]`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, err := client.fetchPrimaryNode()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestFetchPrimaryPrimaryNotFound(t *testing.T) {
	server := getMockServer(200, []byte(`{"state":"NORMAL","nodes":[{"id":"0f5c2ffc-1244-47d0-a83d-f5a25abba9bc","uri":{"scheme":"http","host":"localhost","port":10101}}],"localID":"0f5c2ffc-1244-47d0-a83d-f5a25abba9bc"}`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, err := client.fetchPrimaryNode()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestServerWarning(t *testing.T) {
	var herr error
	defer func() {
		if herr != nil {
			t.Errorf("error in HTTP handler: %v", herr)
		}
	}()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content, err := proto.Marshal(&pb.QueryResponse{})
		if err != nil {
			// Cannot directly interact with t from another goroutine.
			herr = err
			w.WriteHeader(500)
			return
		}
		w.Header().Set("warning", `299 pilosa/2.0 "FAKE WARNING: Deprecated PQL version: PQL v2 will remove support for SetBit() in Pilosa 2.1. Please update your client to support Set() (See https://docs.pilosa.com/pql#versioning)." "Sat, 25 Aug 2019 23:34:45 GMT"`)
		w.WriteHeader(200)
		_, err = io.Copy(w, bytes.NewReader(content))
		if err != nil {
			// Cannot directly interact with t from another goroutine.
			herr = err
			return
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	_, err := client.Query(testField.Row(1))
	if err != nil {
		t.Fatal(err)
	}
}

func TestExportRowIDColumnID(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("exportfield-rowid-colid")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.BatchQuery(
		field.Set(1, 1),
		field.Set(1, 10),
		field.Set(2, 1048577),
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.ExportField(field)
	if err != nil {
		t.Fatal(err)
	}
	s := consumeReader(t, r)
	target := "1,1\n1,10\n2,1048577\n"
	if target != s {
		t.Fatalf("%s != %s", target, s)
	}
}

func TestExportRowIDColumnKey(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := keysIndex.Field("exportfield-rowid-colkey")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(keysIndex.BatchQuery(
		field.Set(1, "one"),
		field.Set(1, "ten"),
		field.Set(2, "big-number"),
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.ExportField(field)
	if err != nil {
		t.Fatal(err)
	}
	s := consumeReader(t, r)
	target := "1,one\n1,ten\n2,big-number\n"
	if target != s {
		//t.Fatalf("%s != %s", target, s)
		t.Log("TODO: these results do not necessarily come back ordered anymore!")
	}
}

func TestExportRowKeyColumnID(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("exportfield-rowkey-colid", OptFieldKeys(true))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.BatchQuery(
		field.Set("one", 1),
		field.Set("one", 10),
		field.Set("two", 1048577),
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.ExportField(field)
	if err != nil {
		t.Fatal(err)
	}
	s := consumeReader(t, r)
	target := "one,1\none,10\ntwo,1048577\n"
	if target != s {
		t.Fatalf("%s != %s", target, s)
	}
}

func TestExportRowKeyColumnKey(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := keysIndex.Field("exportfield-rowkey-colkey", OptFieldKeys(true))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(keysIndex.BatchQuery(
		field.Set("one", "one"),
		field.Set("one", "ten"),
		field.Set("two", "big-number"),
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.ExportField(field)
	if err != nil {
		t.Fatal(err)
	}
	s := consumeReader(t, r)
	target := "one,one\none,ten\ntwo,big-number\n"
	if target != s {
		//t.Fatalf("%s != %s", target, s)
		t.Log("TODO: these results do not necessarily come back ordered anymore!")
	}
}

func TestTranslateRowKeys(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := index.Field("translate-rowkeys", OptFieldKeys(true))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.BatchQuery(
		field.Set("key1", 10),
		field.Set("key2", 1000),
	))
	if err != nil {
		t.Fatal(err)
	}
	rowIDs, err := client.TranslateRowKeys(field, []string{"key1", "key2"})
	if err != nil {
		t.Fatal(err)
	}
	target := []uint64{1, 2}
	if !reflect.DeepEqual(target, rowIDs) {
		t.Fatalf("%v != %v", target, rowIDs)
	}
}

func TestTranslateColKeys(t *testing.T) {
	client := getClient()
	defer client.Close()
	field := keysIndex.Field("translate-colkeys")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(keysIndex.BatchQuery(
		field.Set(10, "ten"),
		field.Set(1000, "one-thousand"),
	))
	if err != nil {
		t.Fatal(err)
	}
	colIDs, err := client.TranslateColumnKeys(keysIndex, []string{"ten", "one-thousand"})
	if err != nil {
		t.Fatal(err)
	}
	target := []uint64{5242881, 82837505}
	if !reflect.DeepEqual(target, colIDs) {
		t.Fatalf("%v != %v", target, colIDs)
	}
}

func TestCSVExportFailure(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	client, _ := NewClient(server.URL, OptClientRetries(0))
	field := index.Field("exportfield")
	_, err := client.ExportField(field)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestTransactions(t *testing.T) {
	client := getClient()
	defer client.Close()

	if trns, err := client.StartTransaction("blah", time.Minute, false, time.Minute); err != nil {
		t.Errorf("%v", err)
	} else if trns.ID != "blah" || trns.Timeout != time.Minute || !trns.Active {
		t.Errorf("unexpected returned transaction: %+v", trns)
	}

	if trnsMap, err := client.Transactions(); err != nil {
		t.Errorf("listing transactions: %v", err)
	} else if len(trnsMap) != 1 || !trnsMap["blah"].Active {
		t.Errorf("unexpected trnsMap: %+v", trnsMap)
	}

	if trns, err := client.GetTransaction("blah"); err != nil {
		t.Errorf("%v", err)
	} else if trns.ID != "blah" || trns.Timeout != time.Minute || !trns.Active {
		t.Errorf("unexpected returned transaction: %+v", trns)
	}

	if trns, err := client.FinishTransaction("blah"); err != nil {
		t.Errorf("%v", err)
	} else if trns.ID != "blah" || trns.Timeout != time.Minute || !trns.Active {
		t.Errorf("unexpected returned transaction: %+v", trns)
	}

}

func getMockServer(statusCode int, response []byte, contentLength int) *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		if contentLength >= 0 {
			w.Header().Set("Content-Length", strconv.Itoa(contentLength))
		}
		w.WriteHeader(statusCode)
		if response != nil {
			_, _ = io.Copy(w, bytes.NewReader(response))
		}
	})
	return httptest.NewServer(handler)
}

type mockResponseItem struct {
	content       []byte
	contentLength int
	statusCode    int
}

func getMockPathServer(responses map[string]mockResponseItem) *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		if item, ok := responses[r.RequestURI]; ok {
			if item.contentLength >= 0 {
				w.Header().Set("Content-Length", strconv.Itoa(item.contentLength))
			} else {
				w.Header().Set("Content-Length", strconv.Itoa(len(item.content)))
			}
			statusCode := item.statusCode
			if statusCode == 0 {
				statusCode = 200
			}
			w.WriteHeader(statusCode)
			if item.content != nil {
				_, _ = io.Copy(w, bytes.NewReader(item.content))
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.Copy(w, bytes.NewReader([]byte("not found")))
	})
	return httptest.NewServer(handler)
}

func getClient(options ...ClientOption) *Client {
	var client *Client
	var err error
	uri, err := pnet.NewURIFromAddress(getPilosaBindAddress())
	if err != nil {
		panic(err)
	}
	options = append([]ClientOption{
		OptClientTLSConfig(&tls.Config{InsecureSkipVerify: true}),
		OptClientRetries(0),
	}, options...)
	client, err = NewClient(uri, options...)
	if err != nil {
		panic(err)
	}
	return client
}

func getPilosaBindAddress() string {
	for _, kvStr := range os.Environ() {
		kv := strings.SplitN(kvStr, "=", 2)
		if kv[0] == "PILOSA_BIND" {
			return kv[1]
		}
	}
	return "http://:10101"
}

func consumeReader(t *testing.T, r io.Reader) string {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

//////////// new stuff

func queryBalances(client *Client, acctOwnerID uint64, fieldAcct0, fieldAcct1 string) (acct0bal, acct1bal int64) {

	q := fmt.Sprintf("FieldValue(field=%v, column=%v)", fieldAcct0, acctOwnerID)
	pql := NewPQLBaseQuery(q, indexAR, nil)
	r, err := client.Query(pql)
	panicOn(err)
	acct0bal = r.ResultList[0].(*ValCountResult).Val

	q = fmt.Sprintf("FieldValue(field=%v, column=%v)", fieldAcct1, acctOwnerID)
	pql = NewPQLBaseQuery(q, indexAR, nil)
	r, err = client.Query(pql)
	panicOn(err)
	acct1bal = r.ResultList[0].(*ValCountResult).Val

	return
}

func skipForRoaring(t *testing.T) {
	src := os.Getenv("PILOSA_TXSRC")
	if src == "" || strings.Contains(src, "roaring") {
		t.Skip("skip if roaring pseudo-txn involved -- won't show transactional rollback/atomic commit")
	}
}

// Check the classic "bank balance transfer between accounts"
// to prevent read-anomalies when two writes are split by a read.
//
func TestImportAtomicRecord(t *testing.T) {
	skipForRoaring(t)
	Reset()
	client := getClient()
	defer client.Close()
	uri := &pnet.URI{Scheme: "http", Port: 10101, Host: "localhost"}
	acctOwnerID := uint64(78) // ColumnID
	shard := uint64(0)
	transferUSD := int64(100)

	setBal := func(bal0, bal1 int64) (data []byte, err error) {
		ivr0 := &pb.ImportValueRequest{
			Index:     indexARname,
			Field:     fieldAcct0,
			Shard:     shard,
			ColumnIDs: []uint64{acctOwnerID},
			Values:    []int64{bal0},
		}
		ivr1 := &pb.ImportValueRequest{
			Index:     indexARname,
			Field:     fieldAcct1,
			Shard:     shard,
			ColumnIDs: []uint64{acctOwnerID},
			Values:    []int64{bal1},
		}

		ar := &pb.AtomicRecord{
			Index: indexARname,
			Shard: shard,
			Ivr: []*pb.ImportValueRequest{
				ivr0, ivr1,
			},
		}

		data, err = proto.Marshal(ar)
		panicOn(err)

		return
	}

	// setup 500 USD in acct1 and 700 USD in acct2.
	// transfer 100 USD.
	// should see 400 USD in acct, and 800 USD in acct2.
	//
	expectedBalStartingAcct0 := int64(500)
	expectedBalStartingAcct1 := int64(700)

	data, err := setBal(expectedBalStartingAcct0, expectedBalStartingAcct1)
	panicOn(err)
	err = client.importData(uri, "/import-atomic-record", data)
	panicOn(err)

	// start the main test, reading two balances and writing two updates.

	startingBalanceAcct0, startingBalanceAcct1 := queryBalances(client, acctOwnerID, fieldAcct0, fieldAcct1)

	//vv("starting balance: acct0=%v,  acct1=%v", startingBalanceAcct0, startingBalanceAcct1)

	if startingBalanceAcct0 != expectedBalStartingAcct0 {
		panic(fmt.Sprintf("expected %v, observed %v starting acct0 balance", expectedBalStartingAcct0, startingBalanceAcct0))
	}
	if startingBalanceAcct1 != expectedBalStartingAcct1 {
		panic(fmt.Sprintf("expected %v, observed %v starting acct1 balance", expectedBalStartingAcct1, startingBalanceAcct1))
	}

	data, err = setBal(expectedBalStartingAcct0-transferUSD, expectedBalStartingAcct1+transferUSD)
	panicOn(err)

	//vv("sad path: transferUSD %v from %v -> %v, with power loss half-way through", transferUSD, fieldAcct0, fieldAcct1)
	err = client.importData(uri, "/import-atomic-record?simPowerLossAfter=1", data)
	if err == nil {
		panic("expected to get 'update was aborted'")
	} else {
		if !strings.Contains(err.Error(), "update was aborted") {
			panic(err)
		}
	}

	endingBalanceAcct0, endingBalanceAcct1 := queryBalances(client, acctOwnerID, fieldAcct0, fieldAcct1)

	// should not have been applied
	if endingBalanceAcct0 != startingBalanceAcct0 ||
		endingBalanceAcct1 != startingBalanceAcct1 {
		panic(fmt.Sprintf("problem: transaction did not abort atomically. Should have same start and end balances in both accounts, but we see: startingBalanceAcct0=%v -> endingBalanceAcct0=%v; startingBalanceAcct1=%v -> endingBalanceAcct1=%v", startingBalanceAcct0, endingBalanceAcct0, startingBalanceAcct1, endingBalanceAcct1))
	}
	//vv("good: with power loss half-way, no change in account balances; acct0=%v; acct1=%v", endingBalanceAcct0, endingBalanceAcct1)

	// next part of the test, just make sure we do the update.
	//vv("happy path: transferUSD %v from %v -> %v, with no interruption.", transferUSD, fieldAcct0, fieldAcct1)

	// happy path with no power failure half-way through.

	err = client.importData(uri, "/import-atomic-record?simPowerLossAfter=0", data)
	panicOn(err)
	endingBalanceAcct0, endingBalanceAcct1 = queryBalances(client, acctOwnerID, fieldAcct0, fieldAcct1)

	// should have been applied this time.
	if endingBalanceAcct0 != startingBalanceAcct0-transferUSD ||
		endingBalanceAcct1 != startingBalanceAcct1+transferUSD {
		panic(fmt.Sprintf("problem: transaction did not get committed/applied. transferUSD=%v, but we see: startingBalanceAcct0=%v -> endingBalanceAcct0=%v; startingBalanceAcct1=%v -> endingBalanceAcct1=%v", transferUSD, startingBalanceAcct0, endingBalanceAcct0, startingBalanceAcct1, endingBalanceAcct1))
	}
	//vv("ending balance: acct0=%v,  acct1=%v", endingBalanceAcct0, endingBalanceAcct1)

}
