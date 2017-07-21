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
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"encoding/json"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/test"
)

// Ensure the handler returns "not found" for invalid paths.
func TestHandler_NotFound(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder

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

	if f, err := i0.CreateFrameIfNotExists("f1", pilosa.FrameOptions{InverseEnabled: true}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewStandard, 0, 0, nil); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewInverse, 0, 0, nil); err != nil {
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
	h.Holder = hldr.Holder
	h.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"indexes":[{"name":"i0","frames":[{"name":"f0"},{"name":"f1","views":[{"name":"inverse"},{"name":"standard"}]}]},{"name":"i1","frames":[{"name":"f0","views":[{"name":"standard"}]}]}]}`+"\n" {
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

	if f, err := i0.CreateFrameIfNotExists("f1", pilosa.FrameOptions{InverseEnabled: true}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewStandard, 0, 0, nil); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.ViewInverse, 0, 0, nil); err != nil {
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
	h.Holder = hldr.Holder
	h.Cluster = test.NewCluster(1)
	h.StatusHandler = s
	s.Handler = h

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/status", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"status":{"State":"UP","Indexes":[{"Name":"i0","Meta":{"ColumnLabel":"columnID"},"Frames":[{"Name":"f0","Meta":{"RowLabel":"rowID","CacheType":"ranked","CacheSize":50000}},{"Name":"f1","Meta":{"RowLabel":"rowID","InverseEnabled":true,"CacheType":"ranked","CacheSize":50000}}]},{"Name":"i1","Meta":{"ColumnLabel":"columnID"},"Frames":[{"Name":"f0","Meta":{"RowLabel":"rowID","CacheType":"ranked","CacheSize":50000}}]}]}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
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
	h.Holder = hldr.Holder
	h.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/slices/max", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"maxSlices":{"i0":3,"i1":0}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can return the maxslice map for the inverse views.
func TestHandler_MaxSlices_Inverse(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	f0, err := hldr.MustCreateIndexIfNotExists("i0", pilosa.IndexOptions{}).CreateFrame("f0", pilosa.FrameOptions{InverseEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f0.SetBit(pilosa.ViewInverse, 30, (1*SliceWidth)+1, nil); err != nil {
		t.Fatal(err)
	} else if _, err := f0.SetBit(pilosa.ViewInverse, 30, (1*SliceWidth)+2, nil); err != nil {
		t.Fatal(err)
	} else if _, err := f0.SetBit(pilosa.ViewInverse, 30, (3*SliceWidth)+4, nil); err != nil {
		t.Fatal(err)
	}

	f1, err := hldr.MustCreateIndexIfNotExists("i1", pilosa.IndexOptions{}).CreateFrame("f1", pilosa.FrameOptions{InverseEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f1.SetBit(pilosa.ViewStandard, 40, (0*SliceWidth)+1, nil); err != nil {
		t.Fatal(err)
	} else if _, err := f1.SetBit(pilosa.ViewInverse, 40, (0*SliceWidth)+2, nil); err != nil {
		t.Fatal(err)
	} else if _, err := f1.SetBit(pilosa.ViewInverse, 40, (0*SliceWidth)+4, nil); err != nil {
		t.Fatal(err)
	}

	h := test.NewHandler()
	h.Holder = hldr.Holder
	h.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/slices/max?inverse=true", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"maxSlices":{"i0":3,"i1":0}}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can accept URL arguments.
func TestHandler_Query_Args_URL(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
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
		t.Fatalf("unexpected status code: %d", w.Code, w.Body.String())
	} else if body := w.Body.String(); body != `{"results":[100]}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can accept arguments via protobufs.
func TestHandler_Query_Args_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
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
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder

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
	} else if body := w.Body.String(); body != `{"error":"invalid query params"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}

}

// Ensure the handler can execute a query with a uint64 response as JSON.
func TestHandler_Query_Uint64_JSON(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
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
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
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
	} else if n := resp.Results[0].N; n != 100 {
		t.Fatalf("unexpected n: %d", n)
	}
}

// Ensure the handler can execute a query that returns a bitmap as JSON.
func TestHandler_Query_Bitmap_JSON(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		bm := pilosa.NewBitmap(1, 3, 66, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": 1, "d": true}
		return []interface{}{bm}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader("Bitmap(id=100)")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[{"attrs":{"a":"b","c":1,"d":true},"bits":[1,3,66,1048577]}]}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can execute a query that returns a bitmap with column attributes as JSON.
func TestHandler_Query_Bitmap_ColumnAttrs_JSON(t *testing.T) {
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
	h.Holder = hldr.Holder
	h.Cluster = test.NewCluster(1)
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		bm := pilosa.NewBitmap(1, 3, 66, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": 1, "d": true}
		return []interface{}{bm}, nil
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query?columnAttrs=true", strings.NewReader("Bitmap(id=100)")))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"results":[{"attrs":{"a":"b","c":1,"d":true},"bits":[1,3,66,1048577]}],"columnAttrs":[{"id":3,"attrs":{"x":"y"}},{"id":66,"attrs":{"y":123,"z":false}}]}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can execute a query that returns a bitmap as protobuf.
func TestHandler_Query_Bitmap_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		bm := pilosa.NewBitmap(1, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": int64(1), "d": true}
		return []interface{}{bm}, nil
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
	} else if bits := resp.Results[0].Bitmap.Bits; !reflect.DeepEqual(bits, []uint64{1, SliceWidth + 1}) {
		t.Fatalf("unexpected bits: %+v", bits)
	} else if attrs := resp.Results[0].Bitmap.Attrs; len(attrs) != 3 {
		t.Fatalf("unexpected attr length: %d", len(attrs))
	} else if k, v := attrs[0].Key, attrs[0].StringValue; k != "a" || v != "b" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	} else if k, v := attrs[1].Key, attrs[1].IntValue; k != "c" || v != int64(1) {
		t.Fatalf("unexpected attr[1]: %s=%v", k, v)
	} else if k, v := attrs[2].Key, attrs[2].BoolValue; k != "d" || v != true {
		t.Fatalf("unexpected attr[2]: %s=%v", k, v)
	}
}

// Ensure the handler can execute a query that returns a bitmap with column attributes as protobuf.
func TestHandler_Query_Bitmap_ColumnAttrs_Protobuf(t *testing.T) {
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
	h.Holder = hldr.Holder
	h.Cluster = test.NewCluster(1)
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		bm := pilosa.NewBitmap(1, pilosa.SliceWidth+1)
		bm.Attrs = map[string]interface{}{"a": "b", "c": int64(1), "d": true}
		return []interface{}{bm}, nil
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
	if bits := resp.Results[0].Bitmap.Bits; !reflect.DeepEqual(bits, []uint64{1, SliceWidth + 1}) {
		t.Fatalf("unexpected bits: %+v", bits)
	} else if attrs := resp.Results[0].Bitmap.Attrs; len(attrs) != 3 {
		t.Fatalf("unexpected attr length: %d", len(attrs))
	} else if k, v := attrs[0].Key, attrs[0].StringValue; k != "a" || v != "b" {
		t.Fatalf("unexpected attr[0]: %s=%v", k, v)
	} else if k, v := attrs[1].Key, attrs[1].IntValue; k != "c" || v != int64(1) {
		t.Fatalf("unexpected attr[1]: %s=%v", k, v)
	} else if k, v := attrs[2].Key, attrs[2].BoolValue; k != "d" || v != true {
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
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
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
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
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
	} else if a := resp.Results[0].GetPairs(); len(a) != 2 {
		t.Fatalf("unexpected pair length: %d", len(a))
	}
}

// Ensure the handler can return an error as JSON.
func TestHandler_Query_Err_JSON(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return nil, errors.New("marker")
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader(`Bitmap(id=100)`)))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"marker"}`+"\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

// Ensure the handler can return an error as protobuf.
func TestHandler_Query_Err_Protobuf(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
	h.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return nil, errors.New("marker")
	}

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("POST", "/index/i/query", strings.NewReader(`TopN(frame=x, n=2)`))
	r.Header.Set("Accept", "application/x-protobuf")
	h.ServeHTTP(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status code: %d", w.Code)
	}

	var resp internal.QueryResponse
	if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	} else if s := resp.Err; s != `marker` {
		t.Fatalf("unexpected error: %s", s)
	}
}

// Ensure the handler returns "method not allowed" for non-POST queries.
func TestHandler_Query_MethodNotAllowed(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
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
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?slices=0,1", strings.NewReader("bad_fn(")))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{"error":"expected comma, right paren, or identifier, found \"\" occurred at line 1, char 8"}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	}
}

// Ensure the handler can delete an index.
func TestHandler_Index_Delete(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.Holder = hldr.Holder
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
	h.Holder = hldr.Holder
	h.Cluster = test.NewCluster(1)
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

// Ensure handler can set the Index time quantum.
func TestHandler_SetIndexTimeQuantum(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateIndexIfNotExists("i0", pilosa.IndexOptions{})

	h := test.NewHandler()
	h.Holder = hldr.Holder
	h.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("PATCH", "/index/i0/time-quantum", strings.NewReader(`{"timeQuantum":"ymdh"}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	} else if q := hldr.Index("i0").TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected time quantum: %s", q)
	}
}

// Ensure handler can set the frame time quantum.
func TestHandler_SetFrameTimeQuantum(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	// Create frame.
	if _, err := hldr.MustCreateIndexIfNotExists("i0", pilosa.IndexOptions{}).CreateFrame("f1", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	h := test.NewHandler()
	h.Holder = hldr.Holder
	h.Cluster = test.NewCluster(1)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, test.MustNewHTTPRequest("PATCH", "/index/i0/frame/f1/time-quantum", strings.NewReader(`{"timeQuantum":"ymdh"}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if body := w.Body.String(); body != `{}`+"\n" {
		t.Fatalf("unexpected body: %s", body)
	} else if q := hldr.Index("i0").Frame("f1").TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected time quantum: %s", q)
	}
}

// Ensure the handler can return data in differing blocks for an index.
func TestHandler_Index_AttrStore_Diff(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.Holder = hldr.Holder
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
	s.Handler.Holder = hldr.Holder
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

// Ensure the handler can backup a fragment and then restore it.
func TestHandler_Fragment_BackupRestore(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.Holder = hldr.Holder
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
	} else if bits := f1.Row(100).Bits(); !reflect.DeepEqual(bits, []uint64{1, 2, 3}) {
		t.Fatalf("unexpected restored bits: %+v", bits)
	}
}

// Ensure the handler can retrieve the version.
func TestHandler_Version(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/version", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if w.Body.String() != `{"version":"`+pilosa.Version+`"}`+"\n" {
		t.Fatalf("unexpected body: %q", w.Body.String())
	}
}

// Ensure the handler can return a list of nodes for a fragment.
func TestHandler_Fragment_Nodes(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Holder = hldr.Holder
	h.Cluster = test.NewCluster(3)
	h.Cluster.ReplicaN = 2

	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/fragment/nodes?index=X&slice=0", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	} else if w.Body.String() != `[{"host":"host2","internalHost":""},{"host":"host0","internalHost":""}]`+"\n" {
		t.Fatalf("unexpected body: %q", w.Body.String())
	}
}

// Ensure the handler can return expvars without panicking.
func TestHandler_Expvars(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/debug/vars", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
}
func TestHandler_GetBlockColumnAttrs(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	f := hldr.MustCreateFragmentIfNotExists("i", "f", "standard", 0)
	f.SetBit(1, 10000)
	attrStore := hldr.Index("i").ColumnAttrStore()
	attrStore.SetAttrs(10000, map[string]interface{}{"A": "col-10000"})

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/block/column-attrs?index=i&block=100", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
	response := map[string]interface{}{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	if err != nil {
		t.Fatal(err)
	}
	target := map[string]interface{}{
		"index": "i",
		"block": float64(100),
		"attrs": map[string]interface{}{
			"10000": map[string]interface{}{
				"A": "col-10000",
			},
		},
	}
	if !reflect.DeepEqual(target, response) {
		t.Fatal("correct attributes should be returned")
	}
}

func TestHandler_GetBlockRowAttrs(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	f := hldr.MustCreateFragmentIfNotExists("i", "f", "standard", 0)
	f.SetBit(1, 10000)
	attrStore := hldr.Frame("i", "f").RowAttrStore()
	attrStore.SetAttrs(1, map[string]interface{}{"A": "row-1"})

	h := test.NewHandler()
	h.Cluster = test.NewCluster(1)
	h.Holder = hldr.Holder
	w := httptest.NewRecorder()
	r := test.MustNewHTTPRequest("GET", "/block/row-attrs?index=i&frame=f&block=0", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", w.Code)
	}
	response := map[string]interface{}{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	if err != nil {
		t.Fatal(err)
	}
	target := map[string]interface{}{
		"index": "i",
		"frame": "f",
		"block": float64(0),
		"attrs": map[string]interface{}{
			"1": map[string]interface{}{
				"A": "row-1",
			},
		},
	}
	if !reflect.DeepEqual(target, response) {
		t.Fatal("correct attributes should be returned")
	}
}
