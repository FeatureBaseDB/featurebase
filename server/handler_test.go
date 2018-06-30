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

package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	gohttp "net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/http"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/server"
	"github.com/pilosa/pilosa/test"
)

// Ensure the handler returns "not found" for invalid paths.
func TestHandler_Endpoints(t *testing.T) {
	cmd := test.MustRunCluster(t, 1)[0]
	h := cmd.Handler.(*http.Handler).Handler
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	t.Run("Not Found", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/no_such_path", nil))
		if w.Code != gohttp.StatusNotFound {
			t.Fatalf("invalid status: %d", w.Code)
		}
	})

	t.Run("Info", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/info", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != fmt.Sprintf("{\"shardWidth\":%d}\n", pilosa.ShardWidth) {
			t.Fatalf("unexpected body: %s", body)
		}
	})

	i0 := hldr.MustCreateIndexIfNotExists("i0", pilosa.IndexOptions{})
	i1 := hldr.MustCreateIndexIfNotExists("i1", pilosa.IndexOptions{})
	if f, err := i0.CreateFieldIfNotExists("f1", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if f, err := i1.CreateFieldIfNotExists("f0", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := i0.CreateFieldIfNotExists("f0", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}

	t.Run("Schema", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"indexes":[{"name":"i0","fields":[{"name":"f0"},{"name":"f1","views":[{"name":"standard"}]}]},{"name":"i1","fields":[{"name":"f0","views":[{"name":"standard"}]}]}]}`+"\n" {
		} else if body := w.Body.String(); body != `{"indexes":[{"name":"i0","fields":[{"name":"f0","options":{"cacheType":"ranked","cacheSize":50000}},{"name":"f1","options":{"cacheType":"ranked","cacheSize":50000},"views":[{"name":"standard"}]}]},{"name":"i1","fields":[{"name":"f0","options":{"cacheType":"ranked","cacheSize":50000},"views":[{"name":"standard"}]}]}]}`+"\n" {
			t.Fatalf("unexpected body: %s", body)
		}
	})

	t.Run("Status", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/status", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		ret := mustJSONDecode(t, w.Body)
		if ret["state"].(string) != "NORMAL" {
			t.Fatalf("wrong state from /status: %#v", ret)
		}
		if len(ret["nodes"].([]interface{})) != 1 {
			t.Fatalf("wrong length nodes list: %#v", ret)
		}
	})

	t.Run("Abort no resize job", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/cluster/resize/abort", nil))
		if w.Code != gohttp.StatusInternalServerError {
			bod, err := ioutil.ReadAll(w.Body)
			t.Fatalf("unexpected status code: %d, bod: %s, readerr: %v", w.Code, bod, err)
		}
		// TODO need to test aborting a cluster resize job. this may not be the right place
	})

	hldr.SetBit("i0", "f0", 30, (1*pilosa.ShardWidth)+1)
	hldr.SetBit("i0", "f0", 30, (1*pilosa.ShardWidth)+2)
	hldr.SetBit("i0", "f0", 30, (3*pilosa.ShardWidth)+4)

	hldr.SetBit("i0", "f0", 31, 1)

	hldr.SetBit("i1", "f1", 40, (0*pilosa.ShardWidth)+1)
	hldr.SetBit("i1", "f1", 40, (0*pilosa.ShardWidth)+2)
	hldr.SetBit("i1", "f1", 40, (0*pilosa.ShardWidth)+8)

	t.Run("Max Shard", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/shards/max", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"standard":{"i0":3,"i1":0}}`+"\n" {
			t.Fatalf("unexpected body: %s", body)
		}
	})

	t.Run("Shards args", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query?shards=0,1", strings.NewReader("Count(Row(f0=30))")))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d %s", w.Code, w.Body.String())
		} else if body := w.Body.String(); body != `{"results":[2]}`+"\n" {
			t.Fatalf("unexpected body: %q", body)
		}
	})

	t.Run("Shards args protobuf", func(t *testing.T) {
		// Generate request body.
		reqBody, err := proto.Marshal(&internal.QueryRequest{
			Query:  "Count(Row(f0=30))",
			Shards: []uint64{0, 1},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Generate protobuf request.
		req := test.MustNewHTTPRequest("POST", "/index/i0/query", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Accept", "application/json")

		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"results":[2]}`+"\n" {
			t.Fatalf("unexpected body: %q", body)
		}

	})

	t.Run("Query args error", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query?shards=a,b", strings.NewReader("Count(Row(f0=30))")))
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"error":"invalid shard argument"}`+"\n" {
			t.Fatalf("unexpected body: %q", body)
		}
	})

	t.Run("Query params err", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query?shards=0,1&db=sample", strings.NewReader("Count(Row(f0=30))")))
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"error":"db is not a valid argument"}`+"\n" {
			t.Fatalf("unexpected body: %q", body)
		}
	})

	t.Run("Uint64 protobuf", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader("Count(Row(f0=30))"))
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
		} else if n := resp.Results[0].N; n != 3 {
			t.Fatalf("unexpected n: %d", n)
		}
	})

	t.Run("Row JSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader("Row(f0=30)")))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"results":[{"attrs":{},"columns":[1048577,1048578,3145732]}]}`+"\n" {
			t.Fatalf("unexpected body: %s", body)
		}
	})

	f0 := i0.Field("f0")
	if err := i0.ColumnAttrStore().SetAttrs((1*pilosa.ShardWidth)+1, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	} else if err := i0.ColumnAttrStore().SetAttrs((1*pilosa.ShardWidth)+2, map[string]interface{}{"y": 123, "z": false}); err != nil {
		t.Fatal(err)
	} else if err := f0.RowAttrStore().SetAttrs(30, map[string]interface{}{"a": "b", "c": 1, "d": true}); err != nil {
		t.Fatal(err)
	}

	t.Run("ColumnAttrs_JSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query?columnAttrs=true", strings.NewReader("Row(f0=30)")))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d. body: %s", w.Code, w.Body.String())
		} else if body := w.Body.String(); body != `{"results":[{"attrs":{"a":"b","c":1,"d":true},"columns":[1048577,1048578,3145732]}],"columnAttrs":[{"id":1048577,"attrs":{"x":"y"}},{"id":1048578,"attrs":{"y":123,"z":false}}]}`+"\n" {
			t.Fatalf("unexpected body: %s", body)
		}
	})

	t.Run("Row pbuf", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader("Row(f0=30)"))
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
		} else if columns := resp.Results[0].Row.Columns; !reflect.DeepEqual(columns, []uint64{pilosa.ShardWidth + 1, pilosa.ShardWidth + 2, (3 * pilosa.ShardWidth) + 4}) {
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
	})

	t.Run("Row columnattrs protobuf", func(t *testing.T) {
		// Encode request body.
		buf, err := proto.Marshal(&internal.QueryRequest{
			Query:       "Row(f0=30)",
			ColumnAttrs: true,
		})
		if err != nil {
			t.Fatal(err)
		}

		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("POST", "/index/i0/query", bytes.NewReader(buf))
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
		if columns := resp.Results[0].Row.Columns; !reflect.DeepEqual(columns, []uint64{pilosa.ShardWidth + 1, pilosa.ShardWidth + 2, (3 * pilosa.ShardWidth) + 4}) {
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

		if a := resp.ColumnAttrSets; len(a) != 2 {
			t.Fatalf("unexpected column attributes length: %d", len(a))
		} else if a[0].ID != pilosa.ShardWidth+1 {
			t.Fatalf("unexpected id: %d", a[0].ID)
		} else if len(a[0].Attrs) != 1 {
			t.Fatalf("unexpected column attr length: %d", len(a))
		} else if k, v := a[0].Attrs[0].Key, a[0].Attrs[0].StringValue; k != "x" || v != "y" {
			t.Fatalf("unexpected attr[0]: %s=%v", k, v)
		}
	})

	t.Run("Query Pairs JSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader(`TopN(f0, n=2)`)))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"results":[[{"id":30,"count":3},{"id":31,"count":1}]]}`+"\n" {
			t.Fatalf("unexpected body: %q", body)
		}
	})

	t.Run("Query Pairs protobuf", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader(`TopN(f0, n=2)`))
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
	})

	t.Run("Query err JSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader(`Row(row=30)`)))
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"error":"executing: field not found"}`+"\n" {
			t.Fatalf("unexpected body: %q", body)
		}
	})

	t.Run("Query err protobuf", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader(`Row(row=30)`))
		r.Header.Set("Accept", "application/x-protobuf")
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		}

		var resp internal.QueryResponse
		if err := proto.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		} else if s := resp.Err; s != `executing: field not found` {
			t.Fatalf("unexpected error: %s", s)
		}
	})

	t.Run("Method not allowed", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/index/i0/query", nil))
		if w.Code != gohttp.StatusMethodNotAllowed {
			t.Fatalf("invalid status: %d", w.Code)
		}
	})

	t.Run("Err Parse", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/idx0/query?shards=0,1", strings.NewReader("bad_fn(")))
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"error":"parsing: parsing: \nparse error near IDENT (line 1 symbol 1 - line 1 symbol 4):\n\"bad\"\n"}`+"\n" {
			t.Fatalf("unexpected body: %s", body)
		}
	})

	t.Run("delete index", func(t *testing.T) {
		hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("DELETE", "/index/i", strings.NewReader("")))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d, body: %s", w.Code, w.Body.String())
		} else if w.Body.String() != `{"success":true}`+"\n" {
			t.Fatalf("unexpected response body: %s", w.Body.String())
		}
		// Verify index is gone.
		if hldr.Index("i") != nil {
			t.Fatal("expected nil index")
		}
	})

	t.Run("Field delete", func(t *testing.T) {
		i := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := i.CreateFieldIfNotExists("f1", pilosa.FieldOptions{}); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("DELETE", "/index/i/field/f1", strings.NewReader("")))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d, body: %s", w.Code, w.Body.String())
		} else if body := w.Body.String(); body != `{"success":true}`+"\n" {
			t.Fatalf("unexpected body: %s", body)
		} else if f := hldr.Index("i").Field("f1"); f != nil {
			t.Fatal("expected nil field")
		}
	})

	i := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if err := i.ColumnAttrStore().SetAttrs(1, map[string]interface{}{"foo": 1, "bar": 2}); err != nil {
		t.Fatal(err)
	} else if err := i.ColumnAttrStore().SetAttrs(100, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	} else if err := i.ColumnAttrStore().SetAttrs(200, map[string]interface{}{"snowman": "☃"}); err != nil {
		t.Fatal(err)
	}

	t.Run("AttrStore Diff", func(t *testing.T) {
		blks, err := i.ColumnAttrStore().Blocks()
		if err != nil {
			t.Fatal(err)
		}

		blks = blks[1:]
		blks[1].Checksum = []byte("MISMATCHED_CHECKSUM")

		// Send block checksums to determine diff.
		req := test.MustNewHTTPRequest(
			"POST",
			"/index/i/attr/diff",
			strings.NewReader(`{"blocks":`+string(test.MustMarshalJSON(blks))+`}`),
		)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d, body: %s", w.Code, w.Body.String())
		}

		// Read and validate body.
		if w.Body.String() != `{"attrs":{"1":{"bar":2,"foo":1},"200":{"snowman":"☃"}}}`+"\n" {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	meta, err := i.CreateFieldIfNotExists("meta", pilosa.FieldOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.RowAttrStore().SetAttrs(1, map[string]interface{}{"foo": 1, "bar": 2}); err != nil {
		t.Fatal(err)
	} else if err := meta.RowAttrStore().SetAttrs(100, map[string]interface{}{"x": "y"}); err != nil {
		t.Fatal(err)
	} else if err := meta.RowAttrStore().SetAttrs(200, map[string]interface{}{"snowman": "☃"}); err != nil {
		t.Fatal(err)
	}

	t.Run("field attrstore diff", func(t *testing.T) {
		blks, err := meta.RowAttrStore().Blocks()
		if err != nil {
			t.Fatal(err)
		}
		blks = blks[1:]
		blks[1].Checksum = []byte("MISMATCHED_CHECKSUM")

		// Send block checksums to determine diff.
		req := test.MustNewHTTPRequest(
			"POST",
			"/index/i/field/meta/attr/diff",
			strings.NewReader(`{"blocks":`+string(test.MustMarshalJSON(blks))+`}`),
		)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d, body: %s", w.Code, w.Body.String())
		}

		// Read and validate body.
		if w.Body.String() != `{"attrs":{"1":{"bar":2,"foo":1},"200":{"snowman":"☃"}}}`+"\n" {
			t.Fatalf("unexpected body: %s", w.Body.String())
		}
	})

	t.Run("Version", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("GET", "/version", nil)
		h.ServeHTTP(w, r)
		version := strings.TrimPrefix(pilosa.Version, "v")
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"version":"`+version+`"}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}
	})

	t.Run("Fragment Nodes", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("GET", "/fragment/nodes?index=i&shard=0", nil)
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		body := mustJSONDecodeSlice(t, w.Body)
		bmap := body[0].(map[string]interface{})
		if bmap["isCoordinator"] != true {
			t.Fatalf("expected true coordinator")
		}

		// invalid argument should return BadRequest
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("GET", "/fragment/nodes?db=X&shard=0", nil)
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		}

		// index is required
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("GET", "/fragment/nodes?shard=0", nil)
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
	})

	t.Run("Expvars", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("GET", "/debug/vars", nil)
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
	})

	t.Run("Recalculate Caches", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/recalculate-caches", nil))
		if w.Code != gohttp.StatusNoContent {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
	})

	t.Run("CORS", func(t *testing.T) {
		req := test.MustNewHTTPRequest("OPTIONS", "/index/foo/query", nil)
		req.Header.Add("Origin", "http://test/")
		req.Header.Add("Access-Control-Request-Method", "POST")

		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		result := w.Result()

		// This handler does not support CORS, return Method Not Allowed (405)
		if result.StatusCode != 405 {
			t.Fatalf("CORS preflight status should be 405, but is %v", result.StatusCode)
		}

		clus := test.MustRunCluster(t, 1, []server.CommandOption{test.OptAllowedOrigins([]string{"http://test/"})})
		w = httptest.NewRecorder()
		h := clus[0].Handler.(*http.Handler).Handler
		h.ServeHTTP(w, req)
		result = w.Result()

		if result.StatusCode != 200 {
			t.Fatalf("CORS preflight status should be 200, but is %v", result.StatusCode)
		}
		if w.HeaderMap["Access-Control-Allow-Origin"][0] != "http://test/" {
			t.Fatal("CORS header not present")
		}
	})

	t.Run("index handlers", func(t *testing.T) {
		// create index
		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("POST", "/index/idx1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":true}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}

		// create index again
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/index/idx1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusConflict {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":false,"error":{"message":"index already exists"}}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}

		// create field
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/index/idx1/field/fld1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":true}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}

		// create field again
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/index/idx1/field/fld1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusConflict {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":false,"error":{"message":"field already exists"}}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}

		// delete field
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("DELETE", "/index/idx1/field/fld1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":true}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}

		// delete field again
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("DELETE", "/index/idx1/field/fld1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusNotFound {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":false,"error":{"message":"field not found"}}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}

		// delete index
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("DELETE", "/index/idx1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":true}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}

		// delete index again
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("DELETE", "/index/idx1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusNotFound {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":false,"error":{"message":"index not found"}}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}
	})
}

func mustJSONDecode(t *testing.T, r io.Reader) (ret map[string]interface{}) {
	dec := json.NewDecoder(r)
	err := dec.Decode(&ret)
	if err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	return ret
}

func mustJSONDecodeSlice(t *testing.T, r io.Reader) (ret []interface{}) {
	dec := json.NewDecoder(r)
	err := dec.Decode(&ret)
	if err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	return ret
}
