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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/boltdb"
	"github.com/molecula/featurebase/v2/encoding/proto"
	"github.com/molecula/featurebase/v2/http"
	"github.com/molecula/featurebase/v2/pql"
	pb "github.com/molecula/featurebase/v2/proto"
	"github.com/molecula/featurebase/v2/server"
	"github.com/molecula/featurebase/v2/test"
	"google.golang.org/grpc"
)

func TestHandler_PostSchemaCluster(t *testing.T) {
	cluster := test.MustRunCluster(t, 3)
	defer cluster.Close()
	cmd := cluster.GetNode(0)
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
		for i := 0; i < cluster.Len(); i++ {
			cmd = cluster.GetNode(i)
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
	cmd := cluster.GetNode(0)
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
		if body != "{\"indexes\":[]}\n" {
			t.Fatalf("unexpected empty schema: '%v'", body)
		}

	})

	t.Run("SchemaDetailsEmpty", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema/details", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		body := w.Body.String()
		if body != "{\"indexes\":[]}\n" {
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
	const shard = 0
	tx0, err := holder.BeginTx(true, i0.Index, shard)
	if err != nil {
		t.Fatal(err)
	}
	defer tx0.Rollback()
	if f, err := i0.CreateFieldIfNotExists("f1", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(tx0, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := i0.CreateFieldIfNotExists("f0", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	}
	if err := tx0.Commit(); err != nil {
		t.Fatal(err)
	}

	i1 := hldr.MustCreateIndexIfNotExists("i1", pilosa.IndexOptions{})
	tx1, err := holder.BeginTx(true, i1.Index, shard)
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback()
	if f, err := i1.CreateFieldIfNotExists("f0", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(tx1, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatal(err)
	}

	t.Run("Schema", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}

		var bodySchema pilosa.Schema
		if err := json.Unmarshal(w.Body.Bytes(),
			&bodySchema); err != nil {
			t.Fatalf("unexpected unmarshalling error: %v", err)
		}
		// DO NOT COMPARE `CreatedAt` - reset to 0
		for _, i := range bodySchema.Indexes {
			i.CreatedAt = 0
			for _, f := range i.Fields {
				f.CreatedAt = 0
			}
		}
		//

		var targetSchema pilosa.Schema
		if err := json.Unmarshal([]byte(fmt.Sprintf(`{"indexes":[{"name":"i0","options":{"keys":false,"trackExistence":false},"fields":[{"name":"f0","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false}},{"name":"f1","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false}}],"shardWidth":%d},{"name":"i1","options":{"keys":false,"trackExistence":false},"fields":[{"name":"f0","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false}}],"shardWidth":%[1]d}]}`, pilosa.ShardWidth)),
			&targetSchema); err != nil {
			t.Fatalf("unexpected unmarshalling error: %v", err)
		}

		if !reflect.DeepEqual(targetSchema, bodySchema) {
			t.Fatalf("target: %+v\nbody: %+v\n", targetSchema, bodySchema)
		}
	})

	// i2 is for SchemaDetails
	i2 := hldr.MustCreateIndexIfNotExists("i2", pilosa.IndexOptions{})
	tx2, err := holder.BeginTx(true, i2.Index, shard)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()
	if f, err := i2.CreateFieldIfNotExists("f0", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(tx2, 0, 0, nil); err != nil {
		t.Fatal(err)
	}

	f, err := i2.CreateFieldIfNotExists("f1", pilosa.OptFieldTypeInt(-100, 100))
	if err != nil {
		t.Fatal(err)
	}

	for n := 0; n < 4; n++ {
		if _, err := f.SetValue(tx2, uint64(n), int64(n)); err != nil {
			t.Fatal(err)
		}
	}

	f, err = i2.CreateFieldIfNotExists("f2", pilosa.OptFieldTypeDecimal(1, pql.Decimal{Value: -10}, pql.Decimal{Value: 10}))
	if err != nil {
		t.Fatal(err)
	}

	for n := 0; n < 5; n++ {
		if _, err := f.SetValue(tx2, uint64(n), int64(n)); err != nil {
			t.Fatal(err)
		}
	}

	if f, err := i2.CreateFieldIfNotExists("f3", pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH"))); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(tx2, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if f, err := i2.CreateFieldIfNotExists("f4", pilosa.OptFieldTypeMutex(pilosa.CacheTypeRanked, 5000)); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(tx2, 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if f, err := i2.CreateFieldIfNotExists("f5", pilosa.OptFieldTypeBool()); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(tx2, 0, 0, nil); err != nil {
		t.Fatal(err)
	}

	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	t.Run("SchemaDetails", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema/details", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}

		var bodySchema pilosa.Schema
		if err := json.Unmarshal(w.Body.Bytes(),
			&bodySchema); err != nil {
			t.Fatalf("unexpected unmarshalling error: %v", err)
		}
		// DO NOT COMPARE `CreatedAt` - reset to 0
		for _, i := range bodySchema.Indexes {
			i.CreatedAt = 0
			for _, f := range i.Fields {
				f.CreatedAt = 0
			}
		}
		//

		var targetSchema pilosa.Schema
		target := fmt.Sprintf(`{"indexes":[{"name":"i0","options":{"keys":false,"trackExistence":false},"fields":[{"name":"f0","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false},"cardinality":0},{"name":"f1","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false},"cardinality":1,"views":[{"name":"standard"}]}],"shardWidth":%[1]d},{"name":"i1","options":{"keys":false,"trackExistence":false},"fields":[{"name":"f0","options":{"type":"set","cacheType":"ranked","cacheSize":50000,"keys":false},"cardinality":1,"views":[{"name":"standard"}]}],"shardWidth":%[1]d},{"name":"i2","options":{"keys":false,"trackExistence":false},"fields":[{"name":"f0","options":{"type":"set","cacheType":"ranked","cacheSize":1000,"keys":false},"cardinality":1,"views":[{"name":"standard"}]},{"name":"f1","options":{"type":"int","base":0,"bitDepth":0,"min":-100,"max":100,"keys":false,"foreignIndex":""},"cardinality":4,"views":[{"name":"bsig_f1"}]},{"name":"f2","options":{"type":"decimal","base":0,"scale":1,"bitDepth":0,"min":-10,"max":10,"keys":false},"cardinality":5,"views":[{"name":"bsig_f2"}]},{"name":"f3","options":{"type":"time","timeQuantum":"YMDH","keys":false,"noStandardView":false},"cardinality":1,"views":[{"name":"standard"}]},{"name":"f4","options":{"type":"mutex","cacheType":"ranked","cacheSize":5000,"keys":false},"cardinality":1,"views":[{"name":"standard"}]},{"name":"f5","options":{"type":"bool"},"cardinality":1,"views":[{"name":"standard"}]}],"shardWidth":%[1]d}]}`, pilosa.ShardWidth)
		if err := json.Unmarshal([]byte(target),
			&targetSchema); err != nil {
			t.Fatalf("unexpected unmarshalling error: %v", err)
		}

		if !reflect.DeepEqual(targetSchema, bodySchema) {
			t.Fatalf("target: %+v\nbody: %+v\n", targetSchema, bodySchema)
		}
	})

	t.Run("SchemaDetailsOff", func(t *testing.T) {
		err := cmd.API.SetAPIOptions(pilosa.OptAPISchemaDetailsOn(false))
		if err != nil {
			t.Fatalf("setting schema details option")
		}

		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/schema/details", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}

		var bodySchema pilosa.Schema
		if err := json.Unmarshal(w.Body.Bytes(),
			&bodySchema); err != nil {
			t.Fatalf("unexpected unmarshalling error: %v", err)

		}
		for _, i := range bodySchema.Indexes {
			for _, f := range i.Fields {
				if f.Cardinality != nil {
					t.Fatalf("expected nil cardinality, got: %v", *f.Cardinality)
				}
			}
		}

		err = cmd.API.SetAPIOptions(pilosa.OptAPISchemaDetailsOn(true))
		if err != nil {
			t.Fatalf("could not toggle schema details to on: %v", err)
		}
	})

	t.Run("Import", func(t *testing.T) {
		indexInfo, err := cmd.API.Schema(context.Background(), false)
		if err != nil {
			t.Fatalf("getting schema: %v", err)
		}

		idx := indexInfo[0]
		fld := indexInfo[0].Fields[0]
		msg := pilosa.ImportRequest{
			Index:          idx.Name,
			IndexCreatedAt: idx.CreatedAt,
			Field:          fld.Name,
			FieldCreatedAt: fld.CreatedAt,
			Shard:          0,
		}
		ser := proto.Serializer{}
		data, err := ser.Marshal(&msg)
		if err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("/index/%s/field/%s/import", idx.Name, fld.Name)
		httpReq := test.MustNewHTTPRequest("POST", path, bytes.NewBuffer(data))
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		httpReq.Header.Set("Accept", "application/x-protobuf")

		w := httptest.NewRecorder()
		h.ServeHTTP(w, httpReq)
		if w.Code != 200 {
			t.Fatalf(w.Body.String())
		}

		msg.IndexCreatedAt = -idx.CreatedAt
		msg.FieldCreatedAt = -fld.CreatedAt
		data, err = ser.Marshal(&msg)
		if err != nil {
			t.Fatal(err)
		}
		httpReq = test.MustNewHTTPRequest("POST", path, bytes.NewBuffer(data))
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		httpReq.Header.Set("Accept", "application/x-protobuf")

		w = httptest.NewRecorder()
		h.ServeHTTP(w, httpReq)

		if w.Code != 412 {
			t.Fatalf("expected: Precondition Failed, got: %d", w.Code)
		}
	})

	t.Run("ImportRoaring", func(t *testing.T) {
		w := httptest.NewRecorder()
		roaringData, _ := hex.DecodeString("3B3001000100000900010000000100010009000100")

		idx, err := cmd.API.Index(context.Background(), "i0")
		if err != nil {
			t.Fatal(err)
		}
		fld, err := cmd.API.Field(context.Background(), "i0", "f1")
		if err != nil {
			t.Fatal(err)
		}

		msg := pilosa.ImportRoaringRequest{
			IndexCreatedAt: idx.CreatedAt(),
			FieldCreatedAt: fld.CreatedAt(),
			Clear:          false,
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
		if w.Code != 200 {
			t.Fatalf("Unexpected response body: %s", w.Body.String())
		}
		resp, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i0", Query: "TopN(f1)"})
		if err != nil {
			t.Fatalf("querying: %v", err)
		}
		if !reflect.DeepEqual(resp.Results[0], &pilosa.PairsField{
			Pairs: []pilosa.Pair{
				{Count: 12, ID: 0},
			},
			Field: "f1",
		}) {
			t.Fatalf("Unexpected result %v", resp.Results[0])
		}
	})

	t.Run("ImportRoaringOverwrite", func(t *testing.T) {
		if _, err := i0.CreateFieldIfNotExists("int-field", pilosa.OptFieldTypeInt(0, 10)); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()

		// byShardWidth is a map of the same roaring (fragment) data generated
		// with different shard widths.
		// TODO: a better approach may be to generate this in the test based
		// on shard width.
		byShardWidth := make(map[uint64][]byte)
		// col/val: 3/3, 8/8
		byShardWidth[1<<20] = []byte{60, 48, 0, 1, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 32, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 48, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 80, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 72, 0, 0, 0, 76, 0, 0, 0, 78, 0, 0, 0, 80, 0, 0, 0, 3, 0, 8, 0, 3, 0, 3, 0, 8, 0}
		byShardWidth[1<<22] = []byte{60, 48, 0, 1, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 128, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 192, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 64, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 72, 0, 0, 0, 76, 0, 0, 0, 78, 0, 0, 0, 80, 0, 0, 0, 3, 0, 8, 0, 3, 0, 3, 0, 8, 0}

		var roaringData []byte
		if data, ok := byShardWidth[pilosa.ShardWidth]; ok {
			roaringData = data
		}

		msg := pilosa.ImportRoaringRequest{
			Action: pilosa.RequestActionOverwrite,
			Block:  0,
			Views: map[string][]byte{
				"bsig_int-field": roaringData,
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
		resp, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i0", Query: "Row(int-field>0)"})
		if err != nil {
			t.Fatalf("querying: %v", err)
		}
		if row := resp.Results[0].(*pilosa.Row); !reflect.DeepEqual(row.Columns(), []uint64{3, 8}) {
			t.Fatalf("Unexpected result %v", row.Columns())
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

	// UI/usage returns disk and memory usage from a precalculated cache.
	// Since the cache calculates the cache on server startup, and tests create indexes thereafter
	// the cache initially has 0 indexes when the test suite is ran. Therefore, this test first
	// resets the cache.
	t.Run("UI/usage", func(t *testing.T) {
		if cmd.API.ResetUsageCache() != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/ui/usage", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		nodeUsages := make(map[string]pilosa.NodeUsage)
		if err := json.Unmarshal(w.Body.Bytes(), &nodeUsages); err != nil {
			t.Fatalf("unmarshal")
		}

		for _, nodeUsage := range nodeUsages {
			if nodeUsage.Disk.TotalUse < 1 {
				t.Fatalf("expected some disk use, got %d", nodeUsage.Disk.TotalUse)
			}
			if nodeUsage.Disk.Capacity < 1 {
				t.Fatalf("expected some disk capacity, got %d", nodeUsage.Disk.Capacity)
			}
			if nodeUsage.Memory.TotalUse < 1 {
				t.Fatalf("expected some memory use, got %d", nodeUsage.Memory.TotalUse)
			}
			if nodeUsage.Memory.Capacity < 1 {
				t.Fatalf("expected some memory capacity, got %d", nodeUsage.Memory.Capacity)
			}
			numIndexes := len(nodeUsage.Disk.IndexUsage)
			if numIndexes != 3 {
				t.Fatalf("wrong length index usage list: expected %d, got %d", 3, numIndexes)
			}
			numFields := len(nodeUsage.Disk.IndexUsage["i1"].Fields)
			if numFields != len(i1.Fields()) {
				t.Fatalf("wrong length field usage list: expected %d, got %d", len(i1.Fields()), numFields)
			}
		}
	})

	t.Run("UI/shard-distribution", func(t *testing.T) {
		// This tests the response structure, not the cluster behavior.
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/ui/shard-distribution", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}

		ret := mustJSONDecode(t, w.Body)

		for indexName := range ret {
			indexData := ret[indexName].(map[string]interface{})
			for nodeName := range indexData {
				nodeData := indexData[nodeName].(map[string]interface{})
				_, hasPrimary := nodeData["primary-shards"]
				_, hasReplica := nodeData["replica-shards"]

				responseOK := hasPrimary && hasReplica
				if !responseOK {
					t.Fatalf("unexpected response structure")
				}
			}

		}
	})

	t.Run("Metrics", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/metrics", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
	})

	t.Run("Metrics.json", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/metrics.json", nil))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
		mustJSONDecode(t, w.Body)
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
		} else if body := w.Body.String(); body != `{"standard":{"i0":3,"i1":0,"i2":0}}`+"\n" {
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
		} else if body := w.Body.String(); body != fmt.Sprintf(`{"results":[{"columns":[%d,%d,%d]}]}`, pilosa.ShardWidth+1, pilosa.ShardWidth+2, 3*pilosa.ShardWidth+4)+"\n" {
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

		var resp pilosa.QueryResponse
		if err := cmd.API.Serializer.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		} else if columns := resp.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{pilosa.ShardWidth + 1, pilosa.ShardWidth + 2, (3 * pilosa.ShardWidth) + 4}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("Query Pairs JSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader(`TopN(f0, n=2)`)))
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"results":[[{"id":30,"key":"","count":3},{"id":31,"key":"","count":1}]]}`+"\n" {
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
		} else if a := resp.Results[0].(*pilosa.PairsField); len(a.Pairs) != 2 {
			t.Fatalf("unexpected pair length: %d", len(a.Pairs))
		}
	})

	t.Run("Query err JSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader(`Row(row=30)`)))
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if body := w.Body.String(); body != `{"error":"executing: translating call: validating value for field \"row\": field not found"}`+"\n" {
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
		} else if s := resp.Err.Error(); s != `executing: translating call: validating value for field "row": field not found` {
			t.Fatalf("unexpected error: %s", s)
		}
	})

	t.Run("Query empty", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", "/index/i0/query", strings.NewReader("")))
		if body := w.Body.String(); body != `{"results":[]}`+"\n" && body != `{"results":null}`+"\n" {
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
		if field != nil { // happy linter
			if !reflect.DeepEqual(pql.NewDecimal(math.MinInt64, 0), field.Options.Min) {
				t.Fatalf("field min %d != %d", int64(math.MinInt64), field.Options.Min)
			}
			if !reflect.DeepEqual(pql.NewDecimal(math.MaxInt64, 0), field.Options.Max) {
				t.Fatalf("field max %d != %d", int64(math.MaxInt64), field.Options.Max)
			}
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
		if field != nil { // happy linter
			if !reflect.DeepEqual(pql.NewDecimal(math.MinInt64, 0), field.Options.Min) {
				t.Fatalf("field min %d != %d", int64(math.MinInt64), field.Options.Min)
			}
			if !reflect.DeepEqual(pql.NewDecimal(1, -1), field.Options.Max) {
				t.Fatalf("field max %d != %d", 10, field.Options.Max)
			}
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
		if field != nil { // happy linter
			if !reflect.DeepEqual(pql.NewDecimal(-1, -1), field.Options.Min) {
				t.Fatalf("field min %d != %d", 10, field.Options.Min)
			}
			if !reflect.DeepEqual(pql.NewDecimal(math.MaxInt64, 0), field.Options.Max) {
				t.Fatalf("field max %d != %d", int64(math.MaxInt64), field.Options.Max)
			}
		}
	})

	t.Run("Query int field min > max return 400", func(t *testing.T) {
		w := httptest.NewRecorder()
		fieldName := "f-int-ubound-err"
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", fmt.Sprintf("/index/i0/field/%s", fieldName),
			strings.NewReader(`{"options":{"type":"int", "min": 10, "max": -10}}`)))
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		}
	})

	t.Run("Query decimal field unbounded", func(t *testing.T) {
		w := httptest.NewRecorder()
		fieldName := "f-decimal-ubound"
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", fmt.Sprintf("/index/i0/field/%s", fieldName),
			strings.NewReader(`{"options":{"type":"decimal", "scale": 0}}`)))
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
		if field != nil { // happy linter
			if !reflect.DeepEqual(pql.NewDecimal(math.MinInt64, 0), field.Options.Min) {
				t.Fatalf("field min %d != %d", int64(math.MinInt64), field.Options.Min)
			}
			if !reflect.DeepEqual(pql.NewDecimal(math.MaxInt64, 0), field.Options.Max) {
				t.Fatalf("field max %d != %d", int64(math.MaxInt64), field.Options.Max)
			}
		}
	})

	t.Run("Query decimal field unbounded min", func(t *testing.T) {
		w := httptest.NewRecorder()
		fieldName := "f-decimal-ubound-min"
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", fmt.Sprintf("/index/i0/field/%s", fieldName),
			strings.NewReader(`{"options":{"type":"decimal", "scale": 1, "max": 10.5}}`)))
		if w.Code != gohttp.StatusOK {
			fmt.Println(w.Body.String())
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
		if field != nil { // happy linter
			if !reflect.DeepEqual(pql.NewDecimal(math.MinInt64, 1), field.Options.Min) {
				t.Fatalf("field min %d != %d", pql.NewDecimal(math.MinInt64, 1), field.Options.Min)
			}
			if !reflect.DeepEqual(pql.NewDecimal(105, 1), field.Options.Max) {
				t.Fatalf("field max %s != %d", pql.NewDecimal(105, 1), field.Options.Max)
			}
		}
	})

	// Ensure that decimal fields error when scale is not provided.
	t.Run("Query decimal field scale error", func(t *testing.T) {
		w := httptest.NewRecorder()
		fieldName := "f-decimal-ubound"
		h.ServeHTTP(w, test.MustNewHTTPRequest("POST", fmt.Sprintf("/index/i0/field/%s", fieldName),
			strings.NewReader(`{"options":{"type":"decimal"}}`)))
		expErr := "decimal field requires a scale argument"
		if w.Code != gohttp.StatusBadRequest {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else if !strings.Contains(w.Body.String(), expErr) {
			t.Fatalf("expected error to contain: %s, but got: %s", expErr, w.Body.String())
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
		} else {
			var resp struct {
				Success bool `json:"success"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)

			if !resp.Success {
				t.Fatalf("unexpected body: %q", w.Body.String())
			}
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
		} else {
			var resp struct {
				Success bool `json:"success"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if !resp.Success {
				t.Fatalf("unexpected body: %q", w.Body.String())
			}

			if f := hldr.Index("i").Field("f1"); f != nil {
				t.Fatal("expected nil field")
			}
		}
	})

	hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})

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
		if bmap["isPrimary"] != true {
			t.Fatalf("expected true primary, got: %+v", bmap)
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
		h1 := clus.GetNode(0).Handler.(*http.Handler).Handler
		h1.ServeHTTP(w, req)
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
		} else {
			var resp struct {
				Success bool `json:"success"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if !resp.Success {
				t.Fatalf("unexpected body: %q", w.Body.String())
			}
		}

		// create index again
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/index/idx1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusConflict {
			t.Errorf("unexpected status code: %d", w.Code)
		} else {
			var resp struct {
				Success   bool   `json:"success"`
				Name      string `json:"name,omitempty"`
				CreatedAt int64  `json:"createdAt,omitempty"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if resp.Success || resp.Name == "" || resp.CreatedAt == 0 {
				t.Errorf("unexpected body: %q", w.Body.String())
			}
		}

		// create field
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/index/idx1/field/fld1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else {
			var resp struct {
				Success   bool   `json:"success"`
				Name      string `json:"name,omitempty"`
				CreatedAt int64  `json:"createdAt,omitempty"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if !resp.Success || resp.Name == "" || resp.CreatedAt == 0 {
				t.Fatalf("unexpected body: %q", w.Body.String())
			}
		}

		// create field again
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/index/idx1/field/fld1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusConflict {
			t.Errorf("unexpected status code: %d", w.Code)
		} else {
			var resp struct {
				Success   bool   `json:"success"`
				Name      string `json:"name,omitempty"`
				CreatedAt int64  `json:"createdAt,omitempty"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if resp.Success || resp.Name == "" || resp.CreatedAt == 0 {
				t.Errorf("unexpected body: %q", w.Body.String())
			}
		}

		// delete field
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("DELETE", "/index/idx1/field/fld1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else {
			var resp struct {
				Success bool `json:"success"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if !resp.Success {
				t.Fatalf("unexpected body: %q", w.Body.String())
			}
		}

		// delete field again
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("DELETE", "/index/idx1/field/fld1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusNotFound {
			t.Errorf("unexpected status code: %d", w.Code)
		} else if w.Body.String() != `{"success":false,"error":{"message":"deleting field: fld1: field not found"}}`+"\n" {
			t.Errorf("unexpected body: %q", w.Body.String())
		}

		// delete index
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("DELETE", "/index/idx1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else {
			var resp struct {
				Success bool `json:"success"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if !resp.Success {
				t.Fatalf("unexpected body: %q", w.Body.String())
			}
		}

		// delete index again
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("DELETE", "/index/idx1", strings.NewReader(""))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusNotFound {
			t.Errorf("unexpected status code: %d", w.Code)
		} else {
			var resp struct {
				Success bool `json:"success"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if resp.Success {
				t.Fatalf("unexpected body: %q", w.Body.String())
			}
		}
	})

	t.Run("translate keys", func(t *testing.T) {
		// create index
		w := httptest.NewRecorder()
		r := test.MustNewHTTPRequest("POST", "/index/i1-tr", strings.NewReader(`{"options":{"keys":true}}`))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else {
			var resp struct {
				Success bool `json:"success"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if !resp.Success {
				t.Fatalf("unexpected body: %q", w.Body.String())
			}
		}

		// create field
		w = httptest.NewRecorder()
		r = test.MustNewHTTPRequest("POST", "/index/i1-tr/field/f1", strings.NewReader(`{"options":{"keys":true}}`))
		h.ServeHTTP(w, r)
		if w.Code != gohttp.StatusOK {
			t.Fatalf("unexpected status code: %d", w.Code)
		} else {
			var resp struct {
				Success bool `json:"success"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &resp)
			if !resp.Success {
				t.Fatalf("unexpected body: %q", w.Body.String())
			}
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
		var target []uint64
		if pilosa.ShardWidth == 1<<22 {
			target = []uint64{650117121, 637534209, 641728513}
		} else {
			target = []uint64{162529281, 159383553, 160432129}
		}
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

	t.Run("grpc-web-cors", func(t *testing.T) {
		req := test.MustNewHTTPRequest("OPTIONS", "/pilosa.Pilosa/QueryPQL", nil)
		req.Header.Add("Origin", "http://test/")
		req.Header.Add("Access-Control-Request-Method", "POST")

		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		result := w.Result()

		// Fail CORS preflight
		if result.Header["Access-Control-Allow-Origin"] != nil {
			t.Fatalf("CORS preflight includes Access-Control-Allow-Origin but should not.")
		}

		clus := test.MustRunCluster(t, 1, []server.CommandOption{test.OptAllowedOrigins([]string{"http://test/"})})
		defer clus.Close()
		w = httptest.NewRecorder()
		h := clus.GetNode(0).Handler.(*http.Handler).Handler
		h.ServeHTTP(w, req)
		result = w.Result()

		if result.Header["Access-Control-Allow-Origin"] == nil {
			t.Fatalf("CORS preflight does not include Access-Control-Allow-Origin.")
		}

		if result.Header["Access-Control-Allow-Origin"][0] != "http://test/" {
			t.Fatal("CORS header not present")
		}
	})
}

func TestCluster_TranslateStore(t *testing.T) {
	cluster := test.MustNewCluster(t, 1)
	cluster.Nodes[0] = test.NewCommandNode(t,
		server.OptCommandServerOptions(
			pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
			pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderWithLockerFunc(nil, &sync.Mutex{})),
		),
	)

	if err := cluster.GetIdleNode(0).Start(); err != nil {
		t.Fatalf("starting node 0: %v", err)
	}
	defer cluster.GetIdleNode(0).Close() // nolint: errcheck

	test.Do(t, "POST", cluster.GetIdleNode(0).URL()+"/index/i0", "{\"options\": {\"keys\": true}}")
}

func TestClusterTranslator(t *testing.T) {
	cluster := test.MustRunCluster(t, 3,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderWithLockerFunc(nil, &sync.Mutex{})),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
			)},
	)
	defer cluster.Close()

	test.Do(t, "POST", cluster.GetNode(0).URL()+"/index/i0", "{\"options\": {\"keys\": true}}")
	test.Do(t, "POST", cluster.GetNode(0).URL()+"/index/i0/field/f0", "{\"options\": {\"keys\": true}}")

	test.Do(t, "POST", cluster.GetNode(0).URL()+"/index/i0/query", "Set(\"foo\", f0=\"bar\")")

	var result0, result1 string
	if err := test.RetryUntil(2*time.Second, func() error {
		result0 = test.Do(t, "POST", cluster.GetNode(0).URL()+"/index/i0/query", "Row(f0=\"bar\")").Body
		result1 = test.Do(t, "POST", cluster.GetNode(1).URL()+"/index/i0/query", "Row(f0=\"bar\")").Body
		if result0 != result1 {
			return fmt.Errorf("`%s` != `%s`", result0, result1)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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

func TestQueryHistory(t *testing.T) {
	cluster := test.MustRunCluster(t, 3,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("1"),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("0"),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("2"),
			)},
	)
	defer cluster.Close()

	cmd := cluster.GetNode(0)
	h := cmd.Handler.(*http.Handler).Handler

	w := httptest.NewRecorder()

	test.Do(t, "POST", cmd.URL()+"/index/i0", "")
	test.Do(t, "POST", cmd.URL()+"/index/i0/field/f0", "")

	gh := server.NewGRPCHandler(cmd.API)
	stream := &MockServerTransportStream{}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
	_, err := gh.QuerySQLUnary(ctx, &pb.QuerySQLRequest{
		Sql: `select * from i0`,
	})

	if err != nil {
		t.Fatalf("QuerySQLUnary failed: %v", err)
	}

	test.Do(t, "POST", cmd.URL()+"/index/i0/query", "Set(0, f0=0)")
	test.Do(t, "POST", cmd.URL()+"/index/i0/query", "Set(3000000, f0=0)")
	test.Do(t, "POST", cmd.URL()+"/index/i0/query", "TopN(f0)")

	h.ServeHTTP(w, test.MustNewHTTPRequest("GET", "/query-history", nil))
	if w.Code != gohttp.StatusOK {
		t.Fatalf("unexpected status code: %d %s", w.Code, w.Body.String())
	}

	ret := make([]pilosa.PastQueryStatus, 4)
	b, err := ioutil.ReadAll(w.Body)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	err = json.Unmarshal(b, &ret)
	if err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}

	// verify result length
	if len(ret) != 4 {
		// each set query executes on both nodes once
		// topn query gets added to history on node0 once, node1 twice
		t.Fatalf("expected list of length 4, got %d\n%+v", len(ret), ret)
	}

	// verify sort order
	if !sort.SliceIsSorted(ret, func(i, j int) bool {
		// must match the sort in api.PastQueries
		return ret[i].Start.After(ret[j].Start)
	}) {
		t.Fatalf("response list not sorted correctly")
	}

	// verify some response values
	if ret[0].Index != "i0" {
		t.Fatalf("response value for 'Index' was '%s', expected 'i0'", ret[0].Index)
	}
	if ret[0].Node != cluster.GetNode(0).Server.NodeID() {
		t.Fatalf("response value for 'Node' was '%s', expected '%s'", ret[0].Node, cluster.GetNode(0).Server.NodeID())
	}
	if ret[3].PQL != "Extract(All(),Rows(f0))" {
		t.Fatalf("response value for 'PQL' was '%s', expected 'Extract(All(),Rows(f0))'", ret[0].PQL)
	}
	if ret[3].SQL != "select * from i0" {
		t.Fatalf("response value for 'SQL' was '%s', expected 'select * from i0'", ret[0].SQL)
	}
	if ret[0].PQL != "TopN(f0)" {
		t.Fatalf("response value for 'PQL' was '%s', expected 'TopN(f0)'", ret[0].PQL)
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
