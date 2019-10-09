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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	gohttp "net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/encoding/proto"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/test"
)

func TestHandler_PostSchemaCluster(t *testing.T) {
	cluster := test.MustRunCluster(t, 3)
	defer cluster.Close()
	cmd := cluster[0]
	h := cmd.Handler.(*http.Handler).Handler

	t.Run("PostSchema", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/schema", strings.NewReader(`{"indexes":[{"name":"blah","options":{"keys":false,"trackExistence":true},"fields":[{"name":"f1","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false}}],"shardWidth":1048576}]}`)))
		if w.Code != gohttp.StatusNoContent {
			bod, err := ioutil.ReadAll(w.Result().Body)
			if err != nil {
				t.Errorf("reading body: %v", err)
			}
			t.Fatalf("unexpected code: %v, bod: %s", w.Code, bod)
		}
		for i := 0; i < len(cluster); i++ {
			cmd = cluster[i]
			idx, err := cmd.API.Index(context.Background(), "blah")
			if err != nil {
				t.Fatalf("getting index: %v", err)
			}
			if idx.Name() != "blah" {
				t.Fatalf("index did not get set, got %v", idx.Name())
			}

			fld, err := cmd.API.Field(context.Background(), "blah", "f1")
			if err != nil {
				t.Fatalf("getting field: %v", err)
			}
			if fld.Name() != "f1" {
				t.Fatalf("unexpected field: %v", fld.Name())
			}
		}

		h.ServeHTTP(w, test.MustNewHTTPRequest("DELETE", "/index/blah", nil))
	})
}

func TestHandler_Endpoints(t *testing.T) {
	cluster := test.MustRunCluster(t, 1)
	defer cluster.Close()
	cmd := cluster[0]
	h := cmd.Handler.(*http.Handler).Handler
	holder := cmd.Server.Holder()
	hldr := test.Holder{Holder: holder}

	// Ensure the handler returns "not found" for invalid paths.
	t.Run("Not Found", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/no_such_path", nil))
		if w.Code != gohttp.StatusNotFound {
			t.Fatalf("invalid status: %d", w.Code)
		}
	})

	t.Run("SchemaEmpty", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		body := w.Body.String()
		if body != "{\"indexes\":null}\n" {
			t.Fatalf("unexpected empty schema: '%v'", body)
		}

	})

	t.Run("PostSchema", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/schema", strings.NewReader(`{"indexes":[{"name":"blah","options":{"keys":false,"trackExistence":true},"fields":[{"name":"f1","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false}}],"shardWidth":1048576}]}`)))
		if w.Code != gohttp.StatusNoContent {
			bod, err := ioutil.ReadAll(w.Result().Body)
			if err != nil {
				t.Errorf("reading body: %v", err)
			}
			t.Fatalf("unexpected code: %v, bod: %s", w.Code, bod)
		}
		idx, err := cmd.API.Index(context.Background(), "blah")
		if err != nil {
			t.Fatalf("getting index: %v", err)
		}
		if idx.Name() != "blah" {
			t.Fatalf("index did not get set, got %v", idx.Name())
		}

		fld, err := cmd.API.Field(context.Background(), "blah", "f1")
		if err != nil {
			t.Fatalf("getting field: %v", err)
		}
		if fld.Name() != "f1" {
			t.Fatalf("unexpected field: %v", fld.Name())
		}

		h.ServeHTTP(w, test.MustNewHTTPRequest("DELETE", "/index/blah", nil))
	})

	t.Run("Info", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/info", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		var details map[string]interface{}
		body := w.Body.Bytes()
		err := json.Unmarshal(body, &details)
		if err != nil {
			t.Fatalf("error unmarshalling json body [%s]: %v", body, err)
		}
		sw := details["shardWidth"]
		if sw == nil {
			t.Fatalf("no shardWidth in json body [%s]", body)
		}
		var n float64
		var ok bool
		if n, ok = sw.(float64); !ok {
			t.Fatalf("shardWidth not float64 (%T) in json body [%s]", sw, body)
		}
		if uint64(n) != pilosa.ShardWidth {
			t.Fatalf("incorrect shard width: got %d, expected %d", uint64(n), pilosa.ShardWidth)
		}
		count := details["cpuPhysicalCores"]
		if count == nil {
			t.Fatalf("no cpuPhysicalCores in json body [%s]", body)
		}
		if n, ok = count.(float64); !ok {
			t.Fatalf("cpuPhysicalCores not float64 (%T) in json body [%s]", count, body)
		}
		if int(n) == 0 {
			t.Fatal("cpu count should not be 0")
		}
	})

	i0 := hldr.MustCreateIndexIfNotExists("i0", pilosa.IndexOptions{})
	i1 := hldr.MustCreateIndexIfNotExists("i1", pilosa.IndexOptions{})
	if f, err := i0.CreateFieldIfNotExists("f1", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if f, err := i1.CreateFieldIfNotExists("f0", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := i0.CreateFieldIfNotExists("f0", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	}

	t.Run("Schema", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		body := w.Body.String()
		target := fmt.Sprintf(`{"indexes":[{"name":"i0","options":{"keys":false,"trackExistence":false},"fields":[{"name":"f0","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false}},{"name":"f1","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false}}],"shardWidth":%d},{"name":"i1","options":{"keys":false,"trackExistence":false},"fields":[{"name":"f0","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false}}],"shardWidth":%[1]d}]}
`, pilosa.ShardWidth)
		if body != target {
			t.Fatalf("%s != %s", target, body)
		}
	})

	t.Run("ImportRoaring", func(t *testing.T) {
		w := httptest.NewRecorder()
		roaringData, _ := hex.DecodeString("3B3001000100000900010000000100010009000100")
		msg := pilosa.ImportRoaringRequest{
			Clear: false,
			Views: map[string][]byte{
				"": roaringData,
			},
		}
		ser := proto.Serializer{}
		data, err := ser.Marshal(&msg)
		if err != nil {
			t.Fatal(err)
		}
		httpReq := test.MustNewHTTPRequest("POST", "/index/i0/field/f1/import-roaring/0", bytes.NewBuffer(data))
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		httpReq.Header.Set("Accept", "application/x-protobuf")
		h.ServeHTTP(w, httpReq)
		resp, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i0", Query: "TopN(f1)"})
		if err != nil {
			t.Fatalf("querying: %v", err)
		}
		if !reflect.DeepEqual(resp.Results[0], []pilosa.Pair{{Count: 12, ID: 0}}) {
			t.Fatalf("Unexpected result %v", resp.Results[0])
		}

	})

	t.Run("ImportRoaringFieldTypeFail", func(t *testing.T) {
		// Roaring import into a non-set field should fail.
		if _, err := i0.CreateFieldIfNotExists("int-field", pilosa.OptFieldTypeInt(0, 1)); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		roaringData, _ := hex.DecodeString("3B3001000100000900010000000100010009000100")
		msg := pilosa.ImportRoaringRequest{
			Clear: false,
			Views: map[string][]byte{
				"": roaringData,
			},
		}
		ser := proto.Serializer{}
		data, err := ser.Marshal(&msg)
		if err != nil {
			t.Fatal(err)
		}
		httpReq := test.MustNewHTTPRequest("POST", "/index/i0/field/int-field/import-roaring/0", bytes.NewBuffer(data))
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		httpReq.Header.Set("Accept", "application/x-protobuf")
		h.ServeHTTP(w, httpReq)
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
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
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/internal/shards/max", nil))
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
		reqBody, err := cmd.API.Serializer.Marshal(&pilosa.QueryRequest{
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
		} else if w.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("unexpected header: %q", w.Header().Get("Content-Type"))
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

		var resp pilosa.QueryResponse
		if err := cmd.API.Serializer.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		} else if rt, ok := resp.Results[0].(uint64); !ok || rt != 3 {
			t.Fatalf("unexpected response type: %#v", resp.Results[0])
		} else if w.Header().Get("Content-Type") != "application/protobuf" {
			t.Fatalf("unexpected header: %q", w.Header().Get("Content-Type"))
		}
	})

	t.Run("Row JSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader("Row(f0=30)")))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != fmt.Sprintf(`{"results":[{"attrs":{},"columns":[%d,%d,%d]}]}`, pilosa.ShardWidth+1, pilosa.ShardWidth+2, 3*pilosa.ShardWidth+4)+"\n" {
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
		exp := fmt.Sprintf(`{"results":[{"attrs":{"a":"b","c":1,"d":true},"columns":[%[1]d,%[2]d,%[3]d]}],"columnAttrs":[{"id":%[1]d,"attrs":{"x":"y"}},{"id":%[2]d,"attrs":{"y":123,"z":false}}]}`, pilosa.ShardWidth+1, pilosa.ShardWidth+2, 3*pilosa.ShardWidth+4) + "\n"
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d. body: %s", w.Code, w.Body.String())
		} else if body := w.Body.String(); body != exp {
			t.Fatalf("unexpected body: \n%s\ngot:\n%s", body, exp)
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

		var resp pilosa.QueryResponse
		if err := cmd.API.Serializer.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		} else if columns := resp.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{pilosa.ShardWidth + 1, pilosa.ShardWidth + 2, (3 * pilosa.ShardWidth) + 4}) {
			t.Fatalf("unexpected columns: %+v", columns)
		} else if attrs := resp.Results[0].(*pilosa.Row).Attrs; len(attrs) != 3 {
			t.Fatalf("unexpected attr length: %d", len(attrs))
		} else if attrs["a"] != "b" {
			t.Fatalf("unexpected attr[a]: %v", attrs["a"])
		} else if attrs["c"] != int64(1) {
			t.Fatalf("unexpected attr[c]: %v", attrs["c"])
		} else if !attrs["d"].(bool) {
			t.Fatalf("unexpected attr[d]: %v", attrs["d"])
		}
	})

	t.Run("Row columnattrs protobuf", func(t *testing.T) {
		// Encode request body.
		buf, err := cmd.API.Serializer.Marshal(&pilosa.QueryRequest{
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

		var resp pilosa.QueryResponse
		if err := cmd.API.Serializer.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if columns := resp.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{pilosa.ShardWidth + 1, pilosa.ShardWidth + 2, (3 * pilosa.ShardWidth) + 4}) {
			t.Fatalf("unexpected columns: %+v", columns)
		} else if _, ok := resp.Results[0].(*pilosa.Row); !ok {
			t.Fatalf("unexpected response type: %#v", resp.Results[0])
		} else if attrs := resp.Results[0].(*pilosa.Row).Attrs; len(attrs) != 3 {
			t.Fatalf("unexpected attr length: %d", len(attrs))
		} else if attrs["a"] != "b" {
			t.Fatalf("unexpected attr[a]: %v", attrs["a"])
		} else if attrs["c"] != int64(1) {
			t.Fatalf("unexpected attr[c]: %v", attrs["c"])
		} else if !attrs["d"].(bool) {
			t.Fatalf("unexpected attr[d]: %v", attrs["d"])
		}

		if a := resp.ColumnAttrSets; len(a) != 2 {
			t.Fatalf("unexpected column attributes length: %d", len(a))
		} else if a[0].ID != pilosa.ShardWidth+1 {
			t.Fatalf("unexpected id: %d", a[0].ID)
		} else if len(a[0].Attrs) != 1 {
			t.Fatalf("unexpected column attr length: %d", len(a))
		} else if a[0].Attrs["x"] != "y" {
			t.Fatalf("unexpected attr[x]: %v", a[0].Attrs["x"])
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

		var resp pilosa.QueryResponse
		if err := cmd.API.Serializer.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		} else if a := resp.Results[0].([]pilosa.Pair); len(a) != 2 {
			t.Fatalf("unexpected pair length: %d", len(a))
		}
	})

	t.Run("Query err JSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader(`Row(row=30)`)))
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"error":"executing: map reduce: field not found"}`+"\n" {
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

		var resp pilosa.QueryResponse
		if err := cmd.API.Serializer.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		} else if s := resp.Err.Error(); s != `executing: map reduce: field not found` {
			t.Fatalf("unexpected error: %s", s)
		}
	})

	t.Run("Query empty", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader("")))
		if body := w.Body.String(); body != `{"results":[]}`+"\n" {
			t.Fatalf("unexpected body: %q", body)
		}
	})

	t.Run("Query int field unbounded", func(t *testing.T) {
		w := httptest.NewRecorder()
		fieldName := "f-int-ubound"
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", fmt.Sprintf("/index/i0/field/%s", fieldName),
			strings.NewReader(`{"options":{"type":"int"}}`)))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema", strings.NewReader("")))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		rsp := getSchemaResponse{}
		if err := json.Unmarshal(w.Body.Bytes(), &rsp); err != nil {
			t.Fatalf("json decode: %s", err)
		}
		field := rsp.findField("i0", fieldName)
		if field == nil {
			t.Fatalf("field not found: %s", fieldName)
		}
		if math.MinInt64 != field.Options.Min {
			t.Fatalf("field min %d != %d", int64(math.MinInt64), field.Options.Min)
		}
		if math.MaxInt64 != field.Options.Max {
			t.Fatalf("field max %d != %d", int64(math.MaxInt64), field.Options.Max)
		}
	})

	t.Run("Query int field unbounded min", func(t *testing.T) {
		w := httptest.NewRecorder()
		fieldName := "f-int-ubound-min"
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", fmt.Sprintf("/index/i0/field/%s", fieldName),
			strings.NewReader(`{"options":{"type":"int", "max": 10}}`)))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema", strings.NewReader("")))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		rsp := getSchemaResponse{}
		if err := json.Unmarshal(w.Body.Bytes(), &rsp); err != nil {
			t.Fatalf("json decode: %s", err)
		}
		field := rsp.findField("i0", fieldName)
		if field == nil {
			t.Fatalf("field not found: %s", fieldName)
		}
		if math.MinInt64 != field.Options.Min {
			t.Fatalf("field min %d != %d", int64(math.MinInt64), field.Options.Min)
		}
		if 10 != field.Options.Max {
			t.Fatalf("field max %d != %d", 10, field.Options.Max)
		}
	})

	t.Run("Query int field unbounded max", func(t *testing.T) {
		w := httptest.NewRecorder()
		fieldName := "f-int-ubound-max"
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", fmt.Sprintf("/index/i0/field/%s", fieldName),
			strings.NewReader(`{"options":{"type":"int", "min": -10}}`)))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema", strings.NewReader("")))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		rsp := getSchemaResponse{}
		if err := json.Unmarshal(w.Body.Bytes(), &rsp); err != nil {
			t.Fatalf("json decode: %s", err)
		}
		field := rsp.findField("i0", fieldName)
		if field == nil {
			t.Fatalf("field not found: %s", fieldName)
		}
		if -10 != field.Options.Min {
			t.Fatalf("field min %d != %d", 10, field.Options.Min)
		}
		if math.MaxInt64 != field.Options.Max {
			t.Fatalf("field max %d != %d", int64(math.MaxInt64), field.Options.Max)
		}
	})

	t.Run("Query int field min > max return 400", func(t *testing.T) {
		w := httptest.NewRecorder()
		fieldName := "f-int-ubound-err"
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", fmt.Sprintf("/index/i0/field/%s", fieldName),
			strings.NewReader(`{"options":{"type":"int", "min": 10, "max": -10}}`)))
		fmt.Println("body", w.Body.String())
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
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
		if _, err := i.CreateFieldIfNotExists("f1", pilosa.OptFieldTypeDefault()); err != nil {
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
			"/internal/index/i/attr/diff",
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

	meta, err := i.CreateFieldIfNotExists("meta", pilosa.OptFieldTypeDefault())
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
			"/internal/index/i/field/meta/attr/diff",
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
		r := test.MustNewHTTPRequest("GET", "/internal/fragment/nodes?index=i&shard=0", nil)
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
		r = test.MustNewHTTPRequest("GET", "/internal/fragment/nodes?db=X&shard=0", nil)
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		}

		// index is required
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("GET", "/internal/fragment/nodes?shard=0", nil)
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
		defer clus.Close()
		w = httptest.NewRecorder()
		h := clus[0].Handler.(*http.Handler).Handler
		h.ServeHTTP(w, req)
		result = w.Result()

		if result.StatusCode != 200 {
			t.Fatalf("CORS preflight status should be 200, but is %v", result.StatusCode)
		}

		if result.Header["Access-Control-Allow-Origin"][0] != "http://test/" {
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
			t.Errorf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":false,"error":{"message":"creating index: index already exists"}}`+"\n" {
			t.Errorf("unexpected body: %q", w.Body.String())
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
			t.Errorf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":false,"error":{"message":"creating field: field already exists"}}`+"\n" {
			t.Errorf("unexpected body: %q", w.Body.String())
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
			t.Errorf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":false,"error":{"message":"deleting field: field not found"}}`+"\n" {
			t.Errorf("unexpected body: %q", w.Body.String())
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
			t.Errorf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":false,"error":{"message":"deleting index: index not found"}}`+"\n" {
			t.Errorf("unexpected body: %q", w.Body.String())
		}
	})

	t.Run("translate keys", func(t *testing.T) {
		// create index
		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("POST", "/index/i1-tr", strings.NewReader(`{"options":{"keys":true}}`))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":true}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}

		// create field
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/index/i1-tr/field/f1", strings.NewReader(`{"options":{"keys":true}}`))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":true}`+"\n" {
			t.Fatalf("unexpected body: %q", w.Body.String())
		}

		// set some bits
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/index/i1-tr/query", strings.NewReader(`Set("col1", f1="row1")`))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}

		// Generate request body for translate column keys request
		reqBody, err := cmd.API.Serializer.Marshal(&pilosa.TranslateKeysRequest{
			Index: "i1-tr",
			Keys:  []string{"col1", "col2", "col3"},
		})
		if err != nil {
			t.Fatal(err)
		}
		// Generate protobuf request.
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/internal/translate/keys", bytes.NewReader(reqBody))
		r.Header.Set("Content-Type", "application/x-protobuf")
		r.Header.Set("Accept", "application/x-protobuf")
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		target := []uint64{1, 2, 3}
		resp := pilosa.TranslateKeysResponse{}
		err = cmd.API.Serializer.Unmarshal(w.Body.Bytes(), &resp)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(target, resp.IDs) {
			t.Fatalf("%v != %v", target, resp.IDs)
		}

		// Generate request body for translate row keys request
		reqBody, err = cmd.API.Serializer.Marshal(&pilosa.TranslateKeysRequest{
			Index: "i1-tr",
			Field: "f1",
			Keys:  []string{"row1", "row2"},
		})
		if err != nil {
			t.Fatal(err)
		}
		// Generate protobuf request.
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/internal/translate/keys", bytes.NewReader(reqBody))
		r.Header.Set("Content-Type", "application/x-protobuf")
		r.Header.Set("Accept", "application/x-protobuf")
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		target = []uint64{1, 2}
		resp = pilosa.TranslateKeysResponse{}
		err = cmd.API.Serializer.Unmarshal(w.Body.Bytes(), &resp)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(target, resp.IDs) {
			t.Fatalf("%v != %v", target, resp.IDs)
		}
	})
}

func TestClusterTranslator(t *testing.T) {
	cluster := make(test.Cluster, 2)
	cluster[0] = test.NewCommandNode(true)
	cluster[0].Config.Gossip.Port = "0"
	err := cluster[0].Start()
	if err != nil {
		t.Fatalf("starting cluster 1: %v", err)
	}
	httpTranslateStore := http.NewTranslateStore(cluster[0].URL())
	cluster[1] = test.NewCommandNode(false,
		server.OptCommandServerOptions(
			pilosa.OptServerPrimaryTranslateStore(httpTranslateStore),
		),
	)
	cluster[1].Config.Gossip.Port = "0"
	cluster[1].Config.Gossip.Seeds = []string{cluster[0].GossipAddress()}
	err = cluster[1].Start()
	if err != nil {
		t.Fatalf("starting cluster 1: %v", err)
	}

	test.MustDo("POST", cluster[0].URL()+"/index/i0", "{\"options\": {\"keys\": true}}")
	test.MustDo("POST", cluster[0].URL()+"/index/i0/field/f0", "{\"options\": {\"keys\": true}}")

	test.MustDo("POST", cluster[0].URL()+"/index/i0/query", "Set(\"foo\", f0=\"bar\")")

	// wait for key to replicate to second node
	time.Sleep(500 * time.Millisecond)

	result0 := test.MustDo("POST", cluster[0].URL()+"/index/i0/query", "Row(f0=\"bar\")").Body
	result1 := test.MustDo("POST", cluster[1].URL()+"/index/i0/query", "Row(f0=\"bar\")").Body

	if result0 != result1 {
		t.Fatalf("`%s` != `%s`", result0, result1)
	}

	for _, i := range []string{result0, result1} {
		var resp map[string]interface{}
		err := json.Unmarshal([]byte(i), &resp)
		if err != nil {
			t.Fatalf("json unmarshal error: %s", err)
		}
		if results, ok := resp["results"].([]interface{}); ok {
			if result, ok := results[0].(map[string]interface{}); ok {
				if keys, ok := result["keys"].([]interface{}); ok {
					if key, ok := keys[0].(string); ok {
						if key != "foo" {
							t.Fatalf("Key is %s but should be 'foo'", key)
						}
					}
				}
			}
		}
	}
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

type getSchemaResponse struct {
	Indexes []*pilosa.IndexInfo `json:"indexes"`
}

func (r getSchemaResponse) findField(indexName, fieldName string) *pilosa.FieldInfo {
	for _, index := range r.Indexes {
		if index.Name == indexName {
			for _, field := range index.Fields {
				if field.Name == fieldName {
					return field
				}
			}
		}
	}
	return nil
}
