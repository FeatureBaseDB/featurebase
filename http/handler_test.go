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

package http_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	gohttp "net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/http"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/test"
)

func TestHandlerPanics(t *testing.T) {
	h := test.MustNewHandler()
	bufLogger := test.NewBufferLogger()
	h.Handler.Logger = bufLogger

	w := httptest.NewRecorder()
	// will panic since Handler has no Holder set up
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/index/taxi", nil))
	bufbytes, err := bufLogger.ReadAll()
	if err != nil {
		t.Fatalf("reading all logoutput: %v", err)
	}
	if !bytes.Contains(bufbytes, []byte("PANIC: runtime error: invalid memory address or nil pointer dereference")) {
		t.Fatalf("expected panic in log, but got: %s", bufbytes)
	}
	if w.Code != gohttp.StatusInternalServerError {
		t.Fatalf("expected internal server error, but got: %v", w.Code)
	}
	bodyBytes := w.Body.Bytes()
	if !bytes.Contains(bodyBytes, []byte("PANIC: runtime error: invalid memory address or nil pointer dereference")) {
		t.Fatalf("response to client should have panic, but got %s", bodyBytes)
	}
}

// Ensure the handler returns "not found" for invalid paths.
func TestHandler_NotFound(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/no_such_path", nil))
	if w.Code != gohttp.StatusNotFound {
		t.Fatalf("invalid status: %d", w.Code)
	}
}

// Ensure the handler can return the schema.
func TestHandler_Schema(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	i0 := hldr.MustCreateIndexIfNotExists("i0", pilosa.IndexOptions{})
	i1 := hldr.MustCreateIndexIfNotExists("i1", pilosa.IndexOptions{})

	if f, err := i0.CreateFieldIfNotExists("f1", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewStandard, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if f, err := i1.CreateFieldIfNotExists("f0", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewStandard, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := i0.CreateFieldIfNotExists("f0", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}

	h := test.MustNewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema", nil))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"indexes":[{"name":"i0","fields":[{"name":"f0"},{"name":"f1","views":[{"name":"standard"}]}]},{"name":"i1","fields":[{"name":"f0","views":[{"name":"standard"}]}]}]}`+"\n" {
	} else if body := w.Body.String(); body != `{"indexes":[{"name":"i0","fields":[{"name":"f0","options":{"cacheType":"ranked","cacheSize":50000}},{"name":"f1","options":{"cacheType":"ranked","cacheSize":50000},"views":[{"name":"standard"}]}]},{"name":"i1","fields":[{"name":"f0","options":{"cacheType":"ranked","cacheSize":50000},"views":[{"name":"standard"}]}]}]}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can return the status.
func TestHandler_Status(t *testing.T) {
	s := test.NewServer()
	hldr := test.MustOpenHolder()
	defer s.Close()
	defer hldr.Close()

	i0 := hldr.MustCreateIndexIfNotExists("i0", pilosa.IndexOptions{})
	i1 := hldr.MustCreateIndexIfNotExists("i1", pilosa.IndexOptions{})

	if f, err := i0.CreateFieldIfNotExists("f1", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewStandard, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if f, err := i1.CreateFieldIfNotExists("f0", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewStandard, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := i0.CreateFieldIfNotExists("f0", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}

	h := test.MustNewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	h.API.Cluster.SetState(pilosa.ClusterStateNormal)
	h.API.StatusHandler = s
	s.Handler = h

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/status", nil))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"state":"NORMAL","nodes":[{"id":"node0","uri":{"scheme":"http","host":"host0"},"isCoordinator":false}],"localID":"node0"}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_Info(t *testing.T) {
	s := test.NewServer()
	defer s.Close()
	h := test.MustNewHandler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/info", nil))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != fmt.Sprintf("{\"sliceWidth\":%d}\n", pilosa.SliceWidth) {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can abort a cluster resize.
func TestHandler_ClusterResizeAbort(t *testing.T) {

	t.Run("No resize job", func(t *testing.T) {
		h := test.MustNewHandler()
		h.API.Cluster = test.NewCluster(1)
		h.API.Cluster.SetState(pilosa.ClusterStateResizing)

		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/cluster/resize/abort", nil))
		if w.Code != gohttp.StatusOK {
			bod, err := ioutil.ReadAll(w.Body)
			t.Fatalf("unexpected status code: %d, bod: %s, readerr: %v", w.Code, bod, err)
		} else if body := w.Body.String(); body != `{"info":"complete current job: no resize job currently running"}`+"\n" {
			t.Fatalf("unexpected body: %s", body)
		}
	})

}

// Ensure the handler can return the maxslice map.
func TestHandler_MaxSlices(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	hldr.MustCreateFragmentIfNotExists("i0", "f0", pilosa.ViewStandard, 1).MustSetBits(30, (1*pilosa.SliceWidth)+1)
	hldr.MustCreateFragmentIfNotExists("i0", "f0", pilosa.ViewStandard, 1).MustSetBits(30, (1*pilosa.SliceWidth)+2)
	hldr.MustCreateFragmentIfNotExists("i0", "f0", pilosa.ViewStandard, 3).MustSetBits(30, (3*pilosa.SliceWidth)+4)

	hldr.MustCreateFragmentIfNotExists("i1", "f1", pilosa.ViewStandard, 0).MustSetBits(40, (0*pilosa.SliceWidth)+1)
	hldr.MustCreateFragmentIfNotExists("i1", "f1", pilosa.ViewStandard, 0).MustSetBits(40, (0*pilosa.SliceWidth)+2)
	hldr.MustCreateFragmentIfNotExists("i1", "f1", pilosa.ViewStandard, 0).MustSetBits(40, (0*pilosa.SliceWidth)+8)

	h := test.MustNewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/slices/max", nil))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"standard":{"i0":3,"i1":0}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can accept URL arguments.
func TestHandler_Query_Args_URL(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if index != "idx0" {
			t.Fatalf("unexpected index: %s", index)
		} else if query.String() != `Count(Bitmap(id=100))` {
			t.Fatalf("unexpected query: %s", query.String())
		} else if !reflect.DeepEqual(slices, []uint64{0, 1}) {
			t.Fatalf("unexpected slices: %+v", slices)
		}
		return []interface{}{uint64(100)}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?slices=0,1", strings.NewReader("Count( Bitmap( id=100))")))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d %s", w.Code, w.Body.String())
	} else if body := w.Body.String(); body != `{"results":[100]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can accept arguments via protobufs.
func TestHandler_Query_Args_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if index != "idx0" {
			t.Fatalf("unexpected index: %s", index)
		} else if query.String() != `Count(Bitmap(id=100))` {
			t.Fatalf("unexpected query: %s", query.String())
		} else if !reflect.DeepEqual(slices, []uint64{0, 1}) {
			t.Fatalf("unexpected slices: %+v", slices)
		}
		return []interface{}{uint64(100)}, nil
	}

	// Generate request body.
	reqBody, err := proto.Marshal(&internal.QueryRequest{
		Query:  "Count(Bitmap(id=100))",
		Slices: []uint64{0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Generate protobuf request.
	req := test.MustNewHTTPRequest("POST", "/index/idx0/query", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/x-protobuf")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
}

// Ensure the handler returns an error when parsing bad arguments.
func TestHandler_Query_Args_Err(t *testing.T) {
	w := httptest.NewRecorder()
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder

	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?slices=a,b", strings.NewReader("Bitmap(id=100)")))
	if w.Code != gohttp.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"invalid slice argument"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}
func TestHandler_Query_Params_Err(t *testing.T) {
	w := httptest.NewRecorder()
	test.MustNewHandler().ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?slices=0,1&db=sample", strings.NewReader("Bitmap(id=100)")))
	if w.Code != gohttp.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"db is not a valid argument"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}

}

// Ensure the handler can execute a query with a uint64 response as JSON.
func TestHandler_Query_Uint64_JSON(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{uint64(100)}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?slices=0,1", strings.NewReader("Count( Bitmap( id=100))")))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[100]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can execute a query with a uint64 response as protobufs.
func TestHandler_Query_Uint64_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{uint64(100)}, nil
	}

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader("Count(Bitmap(id=100))"))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if rt := resp.Results[0].Type; rt != http.QueryResultTypeUint64 {
		t.Fatalf("unexpected response type: %d", resp.Results[0].Type)
	} else if n := resp.Results[0].N; n != 100 {
		t.Fatalf("unexpected n: %d", n)
	}
}

// Ensure the handler can execute a query that returns a bitmap as JSON.
func TestHandler_Query_Bitmap_JSON(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		r := pilosa.NewRow(1, 3, 66, pilosa.SliceWidth+1)
		r.Attrs = map[string]interface{}{"a": "b", "c": 1, "d": true}
		return []interface{}{r}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader("Bitmap(id=100)")))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[{"attrs":{"a":"b","c":1,"d":true},"columns":[1,3,66,1048577]}]}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can execute a query that returns a row with column attributes as JSON.
func TestHandler_Query_Row_ColumnAttrs_JSON(t *testing.T) {
	hldr := test.NewHolder()
	defer hldr.Close()

	// Create index and set column attributes.
	index, err := hldr.CreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	} else if err := index.ColumnAttrStore().SetAttrs(3, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	} else if err := index.ColumnAttrStore().SetAttrs(66, map[string]interface{}{"y": 123, "z": false}); err != nil {
		t.Fatal(err)
	}

	h := test.MustNewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		r := pilosa.NewRow(1, 3, 66, pilosa.SliceWidth+1)
		r.Attrs = map[string]interface{}{"a": "b", "c": 1, "d": true}
		return []interface{}{r}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query?columnAttrs=true", strings.NewReader("Bitmap(id=100)")))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[{"attrs":{"a":"b","c":1,"d":true},"columns":[1,3,66,1048577]}],"columnAttrs":[{"id":3,"attrs":{"x":"y"}},{"id":66,"attrs":{"y":123,"z":false}}]}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can execute a query that returns a row as protobuf.
func TestHandler_Query_Row_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		r := pilosa.NewRow(1, pilosa.SliceWidth+1)
		r.Attrs = map[string]interface{}{"a": "b", "c": int64(1), "d": true}
		return []interface{}{r}, nil
	}

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader("Bitmap(id=100)"))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if rt := resp.Results[0].Type; rt != http.QueryResultTypeRow {
		t.Fatalf("unexpected response type: %d", resp.Results[0].Type)
	} else if columns := resp.Results[0].Row.Columns; !reflect.DeepEqual(columns, []uint64{1, pilosa.SliceWidth + 1}) {
		t.Fatalf("unexpected columns: %+v", columns)
	} else if attrs := resp.Results[0].Row.Attrs; len(attrs) != 3 {
		t.Fatalf("unexpected attr length: %d", len(attrs))
	} else if k, v := attrs[0].Key, attrs[0].StringValue; k != "a" || v != "b" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	} else if k, v := attrs[1].Key, attrs[1].IntValue; k != "c" || v != int64(1) {
		t.Fatalf("unexpected attr[1]: %s=%v", k, v)
	} else if k, v := attrs[2].Key, attrs[2].BoolValue; k != "d" || !v {
		t.Fatalf("unexpected attr[2]: %s=%v", k, v)
	}
}

// Ensure the handler can execute a query that returns a row with column attributes as protobuf.
func TestHandler_Query_Row_ColumnAttrs_Protobuf(t *testing.T) {
	hldr := test.NewHolder()
	defer hldr.Close()

	// Create index and set column attributes.
	index, err := hldr.CreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	} else if err := index.ColumnAttrStore().SetAttrs(1, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	}

	h := test.MustNewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		r := pilosa.NewRow(1, pilosa.SliceWidth+1)
		r.Attrs = map[string]interface{}{"a": "b", "c": int64(1), "d": true}
		return []interface{}{r}, nil
	}

	// Encode request body.
	buf, err := proto.Marshal(&internal.QueryRequest{
		Query:       "Bitmap(id=100)",
		ColumnAttrs: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("POST", "/index/i/query", bytes.NewReader(buf))
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if columns := resp.Results[0].Row.Columns; !reflect.DeepEqual(columns, []uint64{1, pilosa.SliceWidth + 1}) {
		t.Fatalf("unexpected columns: %+v", columns)
	} else if rt := resp.Results[0].Type; rt != http.QueryResultTypeRow {
		t.Fatalf("unexpected response type: %d", resp.Results[0].Type)
	} else if attrs := resp.Results[0].Row.Attrs; len(attrs) != 3 {
		t.Fatalf("unexpected attr length: %d", len(attrs))
	} else if k, v := attrs[0].Key, attrs[0].StringValue; k != "a" || v != "b" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	} else if k, v := attrs[1].Key, attrs[1].IntValue; k != "c" || v != int64(1) {
		t.Fatalf("unexpected attr[1]: %s=%v", k, v)
	} else if k, v := attrs[2].Key, attrs[2].BoolValue; k != "d" || !v {
		t.Fatalf("unexpected attr[2]: %s=%v", k, v)
	}

	if a := resp.ColumnAttrSets; len(a) != 1 {
		t.Fatalf("unexpected column attributes length: %d", len(a))
	} else if a[0].ID != 1 {
		t.Fatalf("unexpected id: %d", a[0].ID)
	} else if len(a[0].Attrs) != 1 {
		t.Fatalf("unexpected column attr length: %d", len(a))
	} else if k, v := a[0].Attrs[0].Key, a[0].Attrs[0].StringValue; k != "x" || v != "y" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	}
}

// Ensure the handler can execute a query that returns pairs as JSON.
func TestHandler_Query_Pairs_JSON(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{[]pilosa.Pair{
			{ID: 1, Count: 2},
			{ID: 3, Count: 4},
		}}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader(`TopN(field=x, n=2)`)))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[[{"id":1,"count":2},{"id":3,"count":4}]]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can execute a query that returns pairs as protobuf.
func TestHandler_Query_Pairs_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{[]pilosa.Pair{
			{ID: 1, Count: 2},
			{ID: 3, Count: 4},
		}}, nil
	}

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader(`TopN(field=x, n=2)`))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if rt := resp.Results[0].Type; rt != http.QueryResultTypePairs {
		t.Fatalf("unexpected response type: %d", resp.Results[0].Type)
	} else if a := resp.Results[0].GetPairs(); len(a) != 2 {
		t.Fatalf("unexpected pair length: %d", len(a))
	}
}

// Ensure the handler can return an error as JSON.
func TestHandler_Query_Err_JSON(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return nil, errors.New("marker")
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader(`Bitmap(id=100)`)))
	if w.Code != gohttp.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"executing: marker"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can return an error as protobuf.
func TestHandler_Query_Err_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return nil, errors.New("marker")
	}

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader(`TopN(field=x, n=2)`))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != gohttp.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if s := resp.Err; s != `executing: marker` {
		t.Fatalf("unexpected error: %s", s)
	}
}

// Ensure the handler returns "method not allowed" for non-POST queries.
func TestHandler_Query_MethodNotAllowed(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/index/i/query", nil))
	if w.Code != gohttp.StatusMethodNotAllowed {
		t.Fatalf("invalid status: %d", w.Code)
	}
}

// Ensure the handler returns an error if there is a parsing error..
func TestHandler_Query_ErrParse(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?slices=0,1", strings.NewReader("bad_fn(")))
	if w.Code != gohttp.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"parsing: expected comma, right paren, or identifier, found \"\" occurred at line 1, char 8"}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can delete an index.
func TestHandler_Index_Delete(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	// Create index.
	if _, err := hldr.CreateIndexIfNotExists("i", pilosa.IndexOptions{}); err != nil {
		t.Fatal(err)
	}

	// Send request to delete index.
	resp, err := gohttp.DefaultClient.Do(test.MustNewHTTPRequest("DELETE", s.URL+"/index/i", strings.NewReader("")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Verify body response.
	if resp.StatusCode != gohttp.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	} else if buf, err := ioutil.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	} else if string(buf) != "{}\n" {
		t.Fatalf("unexpected response body: %s", buf)
	}

	// Verify index is gone.
	if hldr.Index("i") != nil {
		t.Fatal("expected nil index")
	}
}

// Ensure handler can delete a field.
func TestHandler_DeleteField(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	i0 := hldr.MustCreateIndexIfNotExists("i0", pilosa.IndexOptions{})
	if _, err := i0.CreateFieldIfNotExists("f1", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}

	h := test.MustNewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("DELETE", "/index/i0/field/f1", strings.NewReader("")))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	} else if f := hldr.Index("i0").Field("f1"); f != nil {
		t.Fatal("expected nil field")
	}
}

// Ensure the handler can return data in differing blocks for an index.
func TestHandler_Index_AttrStore_Diff(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	// Set attributes on the index.
	index, err := hldr.CreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := index.ColumnAttrStore().SetAttrs(1, map[string]interface{}{"foo": 1, "bar": 2}); err != nil {
		t.Fatal(err)
	} else if err := index.ColumnAttrStore().SetAttrs(100, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	} else if err := index.ColumnAttrStore().SetAttrs(200, map[string]interface{}{"snowman": "☃"}); err != nil {
		t.Fatal(err)
	}

	// Retrieve block checksums.
	blks, err := index.ColumnAttrStore().Blocks()
	if err != nil {
		t.Fatal(err)
	}

	// Remove block #0 and alter block 2's checksum.
	blks = blks[1:]
	blks[1].Checksum = []byte("MISMATCHED_CHECKSUM")

	// Send block checksums to determine diff.
	resp, err := gohttp.Post(
		s.URL+"/index/i/attr/diff",
		"application/json",
		strings.NewReader(`{"blocks":`+string(test.MustMarshalJSON(blks))+`}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Read and validate body.
	if body := string(test.MustReadAll(resp.Body)); body != `{"attrs":{"1":{"bar":2,"foo":1},"200":{"snowman":"☃"}}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can return data in differing blocks for a field.
func TestHandler_Field_AttrStore_Diff(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	// Set attributes on the index.
	idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists("meta", pilosa.FieldOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.RowAttrStore().SetAttrs(1, map[string]interface{}{"foo": 1, "bar": 2}); err != nil {
		t.Fatal(err)
	} else if err := f.RowAttrStore().SetAttrs(100, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	} else if err := f.RowAttrStore().SetAttrs(200, map[string]interface{}{"snowman": "☃"}); err != nil {
		t.Fatal(err)
	}

	// Retrieve block checksums.
	blks, err := f.RowAttrStore().Blocks()
	if err != nil {
		t.Fatal(err)
	}

	// Remove block #0 and alter block 2's checksum.
	blks = blks[1:]
	blks[1].Checksum = []byte("MISMATCHED_CHECKSUM")

	// Send block checksums to determine diff.
	resp, err := gohttp.Post(
		s.URL+"/index/i/field/meta/attr/diff",
		"application/json",
		strings.NewReader(`{"blocks":`+string(test.MustMarshalJSON(blks))+`}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Read and validate body.
	if body := string(test.MustReadAll(resp.Body)); body != `{"attrs":{"1":{"bar":2,"foo":1},"200":{"snowman":"☃"}}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can retrieve the version.
func TestHandler_Version(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/version", nil)
	h.ServeHTTP(w, r)
	version := pilosa.Version
	if strings.HasPrefix(version, "v") {
		version = version[1:]
	}
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if w.Body.String() != `{"version":"`+version+`"}`+"\n" {
		t.Fatalf("unexpected body: %q", w.Body.String())
	}
}

// Ensure the handler can return a list of nodes for a fragment.
func TestHandler_Fragment_Nodes(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(3)
	h.API.Cluster.ReplicaN = 2

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/fragment/nodes?index=X&slice=0", nil)
	h.ServeHTTP(w, r)
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `[{"id":"node2","uri":{"scheme":"http","host":"host2"},"isCoordinator":false},{"id":"node0","uri":{"scheme":"http","host":"host0"},"isCoordinator":false}]`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}

	// invalid argument should return BadRequest
	w = httptest.NewRecorder()
	r = test.MustNewHTTPRequest("GET", "/fragment/nodes?db=X&slice=0", nil)
	h.ServeHTTP(w, r)
	if w.Code != gohttp.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	// index is required
	w = httptest.NewRecorder()
	r = test.MustNewHTTPRequest("GET", "/fragment/nodes?slice=0", nil)
	h.ServeHTTP(w, r)
	if w.Code != gohttp.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
}

// Ensure the handler can return expvars without panicking.
func TestHandler_Expvars(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/debug/vars", nil)
	h.ServeHTTP(w, r)
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
}

func MustReadAll(r io.Reader) []byte {
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return buf
}

func TestHandler_RecalculateCaches(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.MustNewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/recalculate-caches", nil))
	if w.Code != gohttp.StatusNoContent {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

}

func TestHandler_CORS(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	// No CORS config present, so should fail
	handler := test.MustNewHandler()

	req := test.MustNewHTTPRequest("OPTIONS", "/index/foo/query", nil)
	req.Header.Add("Origin", "http://test/")
	req.Header.Add("Access-Control-Request-Method", "POST")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	result := w.Result()

	// This handler does not support CORS, return Method Not Allowed (405)
	if result.StatusCode != 405 {
		t.Fatalf("CORS preflight status should be 405, but is %v", result.StatusCode)
	}

	// CORS config should allow preflight response
	handler = test.MustNewHandler(http.OptHandlerAllowedOrigins([]string{"http://test/"}))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	result = w.Result()

	if result.StatusCode != 200 {
		t.Fatalf("CORS preflight status should be 200, but is %v", result.StatusCode)
	}
	if w.HeaderMap["Access-Control-Allow-Origin"][0] != "http://test/" {
		t.Fatal("CORS header not present")
	}
}
