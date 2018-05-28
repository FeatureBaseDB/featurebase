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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/statik"
	"github.com/pilosa/pilosa/test"
)

func TestHandlerPanics(t *testing.T) {
	h := test.NewHandler()
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
	if w.Code != http.StatusInternalServerError {
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

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/no_such_path", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("invalid status: %d", w.Code)
	}
}

// Ensure the handler can return the schema.
func TestHandler_Schema(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	i0 := hldr.MustCreateIndexIfNotExists("i0", pilosa.IndexOptions{})
	i1 := hldr.MustCreateIndexIfNotExists("i1", pilosa.IndexOptions{})

	if f, err := i0.CreateFrameIfNotExists("f1", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewStandard, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if f, err := i1.CreateFrameIfNotExists("f0", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewStandard, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := i0.CreateFrameIfNotExists("f0", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	h := test.NewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"indexes":[{"name":"i0","frames":[{"name":"f0"},{"name":"f1","views":[{"name":"standard"}]}]},{"name":"i1","frames":[{"name":"f0","views":[{"name":"standard"}]}]}]}`+"\n" {
	} else if body := w.Body.String(); body != `{"indexes":[{"name":"i0","frames":[{"name":"f0","options":{"cacheType":"ranked","cacheSize":50000}},{"name":"f1","options":{"cacheType":"ranked","cacheSize":50000},"views":[{"name":"standard"}]}]},{"name":"i1","frames":[{"name":"f0","options":{"cacheType":"ranked","cacheSize":50000},"views":[{"name":"standard"}]}]}]}`+"\n" {
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

	if f, err := i0.CreateFrameIfNotExists("f1", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewStandard, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if f, err := i1.CreateFrameIfNotExists("f0", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewStandard, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := i0.CreateFrameIfNotExists("f0", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	h := test.NewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	h.API.Cluster.SetState(pilosa.ClusterStateNormal)
	h.API.StatusHandler = s
	s.Handler = h

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/status", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"state":"NORMAL","nodes":[{"id":"node0","uri":{"scheme":"http","host":"host0"},"isCoordinator":false}],"localID":"node0"}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

func TestHandler_Info(t *testing.T) {
	s := test.NewServer()
	defer s.Close()
	h := test.NewHandler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/info", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != fmt.Sprintf("{\"sliceWidth\":%d}\n", SliceWidth) {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can abort a cluster resize.
func TestHandler_ClusterResizeAbort(t *testing.T) {

	t.Run("No resize job", func(t *testing.T) {
		h := test.NewHandler()
		h.API.Cluster = test.NewCluster(1)
		h.API.Cluster.SetState(pilosa.ClusterStateResizing)

		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/cluster/resize/abort", nil))
		if w.Code != http.StatusOK {
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

	hldr.MustCreateFragmentIfNotExists("i0", "f0", pilosa.ViewStandard, 1).MustSetBits(30, (1*SliceWidth)+1)
	hldr.MustCreateFragmentIfNotExists("i0", "f0", pilosa.ViewStandard, 1).MustSetBits(30, (1*SliceWidth)+2)
	hldr.MustCreateFragmentIfNotExists("i0", "f0", pilosa.ViewStandard, 3).MustSetBits(30, (3*SliceWidth)+4)

	hldr.MustCreateFragmentIfNotExists("i1", "f1", pilosa.ViewStandard, 0).MustSetBits(40, (0*SliceWidth)+1)
	hldr.MustCreateFragmentIfNotExists("i1", "f1", pilosa.ViewStandard, 0).MustSetBits(40, (0*SliceWidth)+2)
	hldr.MustCreateFragmentIfNotExists("i1", "f1", pilosa.ViewStandard, 0).MustSetBits(40, (0*SliceWidth)+8)

	h := test.NewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/slices/max", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"standard":{"i0":3,"i1":0}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can accept URL arguments.
func TestHandler_Query_Args_URL(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
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
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d %s", w.Code, w.Body.String())
	} else if body := w.Body.String(); body != `{"results":[100]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can accept arguments via protobufs.
func TestHandler_Query_Args_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
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
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
}

// Ensure the handler returns an error when parsing bad arguments.
func TestHandler_Query_Args_Err(t *testing.T) {
	w := httptest.NewRecorder()
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder

	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?slices=a,b", strings.NewReader("Bitmap(id=100)")))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"invalid slice argument"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}
func TestHandler_Query_Params_Err(t *testing.T) {
	w := httptest.NewRecorder()
	test.NewHandler().ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?slices=0,1&db=sample", strings.NewReader("Bitmap(id=100)")))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"db is not a valid argument"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}

}

// Ensure the handler can execute a query with a uint64 response as JSON.
func TestHandler_Query_Uint64_JSON(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{uint64(100)}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?slices=0,1", strings.NewReader("Count( Bitmap( id=100))")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[100]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can execute a query with a uint64 response as protobufs.
func TestHandler_Query_Uint64_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{uint64(100)}, nil
	}

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader("Count(Bitmap(id=100))"))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if rt := resp.Results[0].Type; rt != pilosa.QueryResultTypeUint64 {
		t.Fatalf("unexpected response type: %d", resp.Results[0].Type)
	} else if n := resp.Results[0].N; n != 100 {
		t.Fatalf("unexpected n: %d", n)
	}
}

// Ensure the handler can execute a query that returns a bitmap as JSON.
func TestHandler_Query_Bitmap_JSON(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		r := pilosa.NewRow(1, 3, 66, pilosa.SliceWidth+1)
		r.Attrs = map[string]interface{}{"a": "b", "c": 1, "d": true}
		return []interface{}{r}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader("Bitmap(id=100)")))
	if w.Code != http.StatusOK {
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

	h := test.NewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		r := pilosa.NewRow(1, 3, 66, pilosa.SliceWidth+1)
		r.Attrs = map[string]interface{}{"a": "b", "c": 1, "d": true}
		return []interface{}{r}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query?columnAttrs=true", strings.NewReader("Bitmap(id=100)")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[{"attrs":{"a":"b","c":1,"d":true},"columns":[1,3,66,1048577]}],"columnAttrs":[{"id":3,"attrs":{"x":"y"}},{"id":66,"attrs":{"y":123,"z":false}}]}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can execute a query that returns a row as protobuf.
func TestHandler_Query_Row_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
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
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if rt := resp.Results[0].Type; rt != pilosa.QueryResultTypeRow {
		t.Fatalf("unexpected response type: %d", resp.Results[0].Type)
	} else if columns := resp.Results[0].Row.Columns; !reflect.DeepEqual(columns, []uint64{1, SliceWidth + 1}) {
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

	h := test.NewHandler()
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
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if columns := resp.Results[0].Row.Columns; !reflect.DeepEqual(columns, []uint64{1, SliceWidth + 1}) {
		t.Fatalf("unexpected columns: %+v", columns)
	} else if rt := resp.Results[0].Type; rt != pilosa.QueryResultTypeRow {
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

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{[]pilosa.Pair{
			{ID: 1, Count: 2},
			{ID: 3, Count: 4},
		}}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader(`TopN(frame=x, n=2)`)))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[[{"id":1,"count":2},{"id":3,"count":4}]]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can execute a query that returns pairs as protobuf.
func TestHandler_Query_Pairs_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{[]pilosa.Pair{
			{ID: 1, Count: 2},
			{ID: 3, Count: 4},
		}}, nil
	}

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader(`TopN(frame=x, n=2)`))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if rt := resp.Results[0].Type; rt != pilosa.QueryResultTypePairs {
		t.Fatalf("unexpected response type: %d", resp.Results[0].Type)
	} else if a := resp.Results[0].GetPairs(); len(a) != 2 {
		t.Fatalf("unexpected pair length: %d", len(a))
	}
}

// Ensure the handler can return an error as JSON.
func TestHandler_Query_Err_JSON(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return nil, errors.New("marker")
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader(`Bitmap(id=100)`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"executing: marker"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can return an error as protobuf.
func TestHandler_Query_Err_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return nil, errors.New("marker")
	}

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader(`TopN(frame=x, n=2)`))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
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

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/index/i/query", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("invalid status: %d", w.Code)
	}
}

// Ensure the handler returns an error if there is a parsing error..
func TestHandler_Query_ErrParse(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?slices=0,1", strings.NewReader("bad_fn(")))
	if w.Code != http.StatusBadRequest {
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
	resp, err := http.DefaultClient.Do(test.MustNewHTTPRequest("DELETE", s.URL+"/index/i", strings.NewReader("")))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Verify body response.
	if resp.StatusCode != http.StatusOK {
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

// Ensure handler can delete a frame.
func TestHandler_DeleteFrame(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	i0 := hldr.MustCreateIndexIfNotExists("i0", pilosa.IndexOptions{})
	if _, err := i0.CreateFrameIfNotExists("f1", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	h := test.NewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("DELETE", "/index/i0/frame/f1", strings.NewReader("")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	} else if f := hldr.Index("i0").Frame("f1"); f != nil {
		t.Fatal("expected nil frame")
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
	resp, err := http.Post(
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

// Ensure the handler can return data in differing blocks for a frame.
func TestHandler_Frame_AttrStore_Diff(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	// Set attributes on the index.
	idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	f, err := idx.CreateFrameIfNotExists("meta", pilosa.FrameOptions{})
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
	resp, err := http.Post(
		s.URL+"/index/i/frame/meta/attr/diff",
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

// Ensure the handler can create a new field on an existing frame.
func TestHandler_Frame_AddField(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	t.Run("OK", func(t *testing.T) {
		idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		f, err := idx.CreateFrameIfNotExists("f", pilosa.FrameOptions{})
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.Post(
			s.URL+"/index/i/frame/f/field/x",
			"application/json",
			strings.NewReader(`{"type":"int","min":100,"max":200}`),
		)
		if err != nil {
			t.Fatal(err)
		} else if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		} else if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status code: %d", resp.StatusCode)
		}

		if field := f.Field("x"); !reflect.DeepEqual(field, &pilosa.Field{Name: "x", Type: "int", Min: 100, Max: 200}) {
			t.Fatalf("unexpected field: %#v", field)
		}
	})

	t.Run("ErrInvalidFieldType", func(t *testing.T) {
		idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := idx.CreateFrameIfNotExists("f", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		}

		resp, err := http.Post(
			s.URL+"/index/i/frame/f/field/x",
			"application/json",
			strings.NewReader(`{"type":"bad_type","min":100,"max":200}`),
		)
		if err != nil {
			t.Fatal(err)
		} else if body := MustReadAll(resp.Body); string(body) != `creating field: validating field: invalid field type`+"\n" {
			t.Fatalf("unexpected body: %q", body)
		} else if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		} else if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("unexpected status code: %d", resp.StatusCode)
		}
	})

	t.Run("ErrInvalidFieldRange", func(t *testing.T) {
		idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := idx.CreateFrameIfNotExists("f", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		}

		resp, err := http.Post(
			s.URL+"/index/i/frame/f/field/x",
			"application/json",
			strings.NewReader(`{"type":"int","min":200,"max":100}`),
		)
		if err != nil {
			t.Fatal(err)
		} else if body := MustReadAll(resp.Body); string(body) != `creating field: validating field: invalid field range`+"\n" {
			t.Fatalf("unexpected body: %q", body)
		} else if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		} else if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("unexpected status code: %d", resp.StatusCode)
		}
	})

	t.Run("ErrFieldAlreadyExists", func(t *testing.T) {
		idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := idx.CreateFrameIfNotExists("f", pilosa.FrameOptions{
			Fields: []*pilosa.Field{{Name: "x", Type: pilosa.FieldTypeInt, Min: 0, Max: 100}},
		}); err != nil {
			t.Fatal(err)
		}

		resp, err := http.Post(
			s.URL+"/index/i/frame/f/field/x",
			"application/json",
			strings.NewReader(`{"type":"int","min":0,"max":100}`),
		)
		if err != nil {
			t.Fatal(err)
		} else if body := MustReadAll(resp.Body); string(body) != `creating field: field already exists`+"\n" {
			t.Fatalf("unexpected body: %q", body)
		} else if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		} else if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("unexpected status code: %d", resp.StatusCode)
		}
	})
}

// Ensure the handler can delete existing fields.
func TestHandler_Frame_DeleteField(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	t.Run("OK", func(t *testing.T) {
		idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		f, err := idx.CreateFrameIfNotExists("f", pilosa.FrameOptions{})
		if err != nil {
			t.Fatal(err)
		} else if err := f.CreateField(&pilosa.Field{Name: "x", Type: pilosa.FieldTypeInt, Min: 0, Max: 100}); err != nil {
			t.Fatal(err)
		}

		req, err := http.NewRequest("DELETE", s.URL+"/index/i/frame/f/field/x", nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		} else if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		} else if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status code: %d", resp.StatusCode)
		}

		if field := f.Field("x"); field != nil {
			t.Fatalf("expected nil field, got: %#v", field)
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		f, err := idx.CreateFrameIfNotExists("f", pilosa.FrameOptions{})
		if err != nil {
			t.Fatal(err)
		} else if err := f.CreateField(&pilosa.Field{Name: "x", Type: pilosa.FieldTypeInt, Min: 0, Max: 100}); err != nil {
			t.Fatal(err)
		}

		req, err := http.NewRequest("DELETE", s.URL+"/index/i/frame/f/field/y", nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		} else if body, err := ioutil.ReadAll(resp.Body); err != nil {
			t.Fatal(err)
		} else if strings.TrimSpace(string(body)) != `deleting field: field not found` {
			t.Fatalf("unexpected body: %q", body)
		} else if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		} else if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("unexpected status code: %d", resp.StatusCode)
		}
	})
}

func TestHandler_Frame_GetFields(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	t.Run("OK", func(t *testing.T) {
		idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		f, err := idx.CreateFrameIfNotExists("f", pilosa.FrameOptions{})
		if err != nil {
			t.Fatal(err)
		} else if err := f.CreateField(&pilosa.Field{Name: "x", Type: pilosa.FieldTypeInt, Min: 1, Max: 100}); err != nil {
			t.Fatal(err)
		}
		resp, err := http.Get(s.URL + "/index/i/frame/f/fields")
		if err != nil {
			t.Fatal(err)
		} else if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status code: %d", resp.StatusCode)
		}

		var fields FrameFields
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal([]byte(body), &fields); err != nil {
			t.Fatal(err)
		}
		field := fields.Fields[0]
		if field.Name != "x" {
			t.Fatalf("expected field's name: x, actuall name: %v", field.Name)
		} else if field.Min != 1 {
			t.Fatalf("expected field's min: x, actuall min: %v", field.Min)
		} else if field.Max != 100 {
			t.Fatalf("expected field's max: x, actuall max: %v", field.Max)
		}

	})

	t.Run("ErrFrameFieldNotAllowed", func(t *testing.T) {
		idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		_, err := idx.CreateFrameIfNotExists("f1", pilosa.FrameOptions{})
		if err != nil {
			t.Fatalf("creating frame: %v", err)
		}

		resp, err := http.Get(s.URL + "/index/i/frame/f1/fields")
		if err != nil {
			t.Fatal(err)
		}
		if err != nil {
			t.Fatal(err)
		} else if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status code: %d", resp.StatusCode)
		} else if body, err := ioutil.ReadAll(resp.Body); err != nil {
			t.Fatal(err)
		} else if strings.TrimSpace(string(body)) == `frame fields not allowed` {
			t.Fatalf("shouldn't get frame fields not allowed error: %q", body)
		}
	})

}

type FrameFields struct {
	Fields []pilosa.Field
}

// Ensure the handler can backup a fragment and then restore it.
func TestHandler_Fragment_BackupRestore(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	// Set bits in the index.
	f0 := hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0)
	f0.MustSetBits(100, 1, 2, 3)

	// Begin backing up from slice i/f/0.
	resp, err := http.Get(s.URL + "/fragment/data?index=i&frame=f&view=standard&slice=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Ensure response came back OK.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected backup status code: %d", resp.StatusCode)
	}

	// Create frame.
	if _, err := hldr.MustCreateIndexIfNotExists("x", pilosa.IndexOptions{}).CreateFrame("y", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Restore backup to slice x/y/0.
	if resp, err := http.Post(s.URL+"/fragment/data?index=x&frame=y&view=standard&slice=0", "application/octet-stream", resp.Body); err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("unexpected restore status code: %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// Verify data is correctly restored.
	f1 := hldr.Fragment("x", "y", pilosa.ViewStandard, 0)
	if f1 == nil {
		t.Fatal("fragment x/y/standard/0 not created")
	} else if columns := f1.Row(100).Columns(); !reflect.DeepEqual(columns, []uint64{1, 2, 3}) {
		t.Fatalf("unexpected restored columns: %+v", columns)
	}
}

// Ensure the handler can retrieve the version.
func TestHandler_Version(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/version", nil)
	h.ServeHTTP(w, r)
	version := pilosa.Version
	if strings.HasPrefix(version, "v") {
		version = version[1:]
	}
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if w.Body.String() != `{"version":"`+version+`"}`+"\n" {
		t.Fatalf("unexpected body: %q", w.Body.String())
	}
}

// Ensure the handler can return a list of nodes for a fragment.
func TestHandler_Fragment_Nodes(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(3)
	h.API.Cluster.ReplicaN = 2

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/fragment/nodes?index=X&slice=0", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `[{"id":"node2","uri":{"scheme":"http","host":"host2"},"isCoordinator":false},{"id":"node0","uri":{"scheme":"http","host":"host0"},"isCoordinator":false}]`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}

	// invalid argument should return BadRequest
	w = httptest.NewRecorder()
	r = test.MustNewHTTPRequest("GET", "/fragment/nodes?db=X&slice=0", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	// index is required
	w = httptest.NewRecorder()
	r = test.MustNewHTTPRequest("GET", "/fragment/nodes?slice=0", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
}

// Ensure the handler can return expvars without panicking.
func TestHandler_Expvars(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Cluster = test.NewCluster(1)
	h.API.Holder = hldr.Holder
	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/debug/vars", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
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

	h := test.NewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/recalculate-caches", nil))
	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

}

func TestHandler_WebUI(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.API.Holder = hldr.Holder
	h.API.Cluster = test.NewCluster(1)
	h.FileSystem = &statik.FileSystem{}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "<title>Pilosa WebUI</title>") {
		t.Fatalf("WebUI is not being served correctly.")
	}

	// If curl is the client, the response should be different
	w = httptest.NewRecorder()
	req := test.MustNewHTTPRequest("GET", "/", nil)
	req.Header.Add("User-Agent", "curl/7.54.0")
	h.ServeHTTP(w, req)
	if !strings.Contains(w.Body.String(), "try the WebUI") {
		t.Fatalf("WebUI is not being served correctly.")
	}
}
