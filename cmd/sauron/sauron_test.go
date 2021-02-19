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

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"

	pb "github.com/golang/protobuf/proto" //nolint:staticcheck
	pbuf "github.com/molecula/go-pilosa/v2/gopilosa_pbuf"
	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/encoding/proto"
	"github.com/pkg/errors"
)

func TestSauron(t *testing.T) {

	cfg := NewSauronTestConfig(t)
	proxy := NewSauron(cfg)
	proxy.sql.MustRemove() //remove the existing db file if present
	url, err := proxy.Start()
	panicOn(err)
	defer proxy.Stop()

	schemaJson := `{"indexes":[{"name":"scratch","createdAt":1611185870149882000,"options":{"keys":false,"trackExistence":true}, "fields":[{"name":"luminosity","createdAt":1595896639332730413,"options":{"type":"int","base":0,"bitDepth":31,"min":-9223372036854775808,"max":9223372036854775807,"keys":false,"foreignIndex":""}}]}]}`

	client := &http.Client{}
	t.Run("Schema", func(t *testing.T) {
		_, err := client.Post(url+"/schema", "application/json", bytes.NewBufferString(schemaJson))
		panicOn(err)
		// verify that we can fetch it back
		resp, err := client.Get(url + "/schema")
		panicOn(err)
		body, err := ioutil.ReadAll(resp.Body)
		panicOn(err)
		sbody := string(body)
		if sbody[:62] != schemaJson[:62] {
			fmt.Printf("did not get posted schema back: observed:\n\n%v\n\nexpected:\n\n%v\n\n", sbody, schemaJson)
			panic("unexpected schema back")
		}
	})
	// add a set field "color"
	//POST localhost:10101/index/scratch/field/color
	t.Run("Create Non-Keyed SetField", func(t *testing.T) {
		resp, err := client.Post(url+"/index/scratch/field/rating", "application/text", bytes.NewBuffer(nil))
		panicOn(err)
		if resp.StatusCode != 200 {
			panic(fmt.Sprintf("expected 200 status, got '%v'", resp))
		}
	})
	t.Run("Create Keyed SetField", func(t *testing.T) {
		resp, err := client.Post(url+"/index/scratch/field/color", "application/text", bytes.NewBuffer([]byte(`{"options": {"keys": true}}`)))
		panicOn(err)
		if resp.StatusCode != 200 {
			panic(fmt.Sprintf("expected 200 status, got '%v'", resp))
		}
	})
	t.Run("Create Time Field", func(t *testing.T) {
		resp, err := client.Post(url+"/index/scratch/field/event", "application/text", bytes.NewBuffer([]byte(`{"options": { "type": "time", "timeQuantum": "YMDH" }}`)))
		panicOn(err)
		if resp.StatusCode != 200 {
			panic(fmt.Sprintf("expected 200 status, got '%v'", resp))
		}
	})

	// set some bits

	// set some bits

	t.Run("Set", func(t *testing.T) {
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(0, color=red)`))
		panicOn(err)
		// test for dups
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(0, color=red)`))
		panicOn(err)
		//
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(1, color=red)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(2, color=red)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(3, color=red)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(4, color=red)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(5, color=red)`))
		panicOn(err)

		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(2, color=green)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(4, color=green)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(6, color=green)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(8, color=green)`))
		panicOn(err)

		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(1, color=yellow)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(3, color=yellow)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(5, color=yellow)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(7, color=yellow)`))
		panicOn(err)

		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(5, color=orange)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(6, color=orange)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(7, color=orange)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(8, color=orange)`))
		panicOn(err)

		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(1, luminosity=1)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(2, luminosity=2)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(3, luminosity=4)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(4, luminosity=5)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(5, luminosity=4)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(6, luminosity=3)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(7, luminosity=2)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(8, luminosity=1)`))
		panicOn(err)

		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(7, rating=1)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(10, rating=1)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(6, rating=2)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(5, rating=2)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(4, rating=3)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(3, rating=4)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(2, rating=4)`))
		panicOn(err)

		//time fields
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(5, event=1,2021-02-05T01:00)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(5, event=1,2021-02-05T02:00)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(4, event=1,2021-02-05T02:05)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(3, event=1,2021-02-05T03:00)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Set(2, event=1,2021-02-05T04:00)`))
		panicOn(err)
	})
	t.Run("Clear", func(t *testing.T) {
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Clear(1, color=purple)`))
		panicOn(err)
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`ClearRow(color=purple)`))
		panicOn(err)
	})
	t.Run("Store", func(t *testing.T) {
		_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(`Store(Row(color=red), color=purple)`))
		panicOn(err)
	})
	// verify that we can fetch it back
	var schema pilosa.Schema
	t.Run("Fetch Schema", func(t *testing.T) {
		resp, err := client.Get(url + "/schema")
		panicOn(err)
		body, err := ioutil.ReadAll(resp.Body)
		panicOn(err)
		err = json.Unmarshal(body, &schema)
		if err != nil {
			panic(fmt.Sprintf("decoding request as JSON Pilosa schema: %v", err))
		}
	})
	t.Run("Import Roaring", func(t *testing.T) {
		//sets 12 bits in shard 0
		roaringData, _ := hex.DecodeString("3B3001000100000900010000000100010009000100")
		idx := schema.Indexes[0]
		var fld *pilosa.FieldInfo
		for _, f := range idx.Fields {
			if f.Name == "rating" {
				fld = f
			}
		}
		if fld == nil {
			panic("field color not found")
		}
		msg := pilosa.ImportRoaringRequest{
			IndexCreatedAt: idx.CreatedAt,
			FieldCreatedAt: fld.CreatedAt,
			Clear:          false,
			Views: map[string][]byte{
				"": roaringData,
			},
			UpdateExistence: true,
		}
		ser := proto.Serializer{}
		data, err := ser.Marshal(&msg)
		if err != nil {
			t.Fatal(err)
		}
		furl := url + "/index/scratch/field/rating/import-roaring/0"
		req, err := http.NewRequest("POST", furl, bytes.NewBuffer(data))
		panicOn(err)
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Accept", "application/x-protobuf")

		// maybe need to Accept: "application/x-protobuf"
		resp, err := client.Do(req)
		panicOn(err)
		_ = resp
	})
	t.Run("Read Query", func(t *testing.T) {
		for _, tst := range GetTests() {
			_, err = client.Post(url+"/index/scratch/query", "application/text", bytes.NewBufferString(tst.Pql))
			panicOn(err)
		}
	})
}

func makeImportRequest(field *FieldInfo2, shard uint64, rows, columns []uint64, clear bool) (path string, data []byte, err error) {
	msg := &pbuf.ImportRequest{
		Index:          field.GetIndexName(),
		IndexCreatedAt: field.GetIndexCreatedAt(),
		Field:          field.GetName(),
		FieldCreatedAt: field.GetCreatedAt(),
		Shard:          shard,
		RowIDs:         rows,
		ColumnIDs:      columns,
	}
	data, err = pb.Marshal(msg)
	if err != nil {
		return "", nil, errors.Wrap(err, "marshaling Import to protobuf")
	}
	path = fmt.Sprintf("/index/%s/field/%s/import?clear=%s&ignoreKeyCheck=true", field.GetIndexName(), field.GetName(), strconv.FormatBool(clear))
	return path, data, nil
}
func makeImportKeyRequest(field *FieldInfo2, shard uint64, rows []string, columns []uint64, clear bool) (path string, data []byte, err error) {
	msg := &pbuf.ImportRequest{
		Index:          field.GetIndexName(),
		IndexCreatedAt: field.GetIndexCreatedAt(),
		Field:          field.GetName(),
		FieldCreatedAt: field.GetCreatedAt(),
		Shard:          shard,
		RowKeys:        rows,
		ColumnIDs:      columns,
	}
	data, err = pb.Marshal(msg)
	if err != nil {
		return "", nil, errors.Wrap(err, "marshaling Import to protobuf")
	}
	path = fmt.Sprintf("/index/%s/field/%s/import?clear=%s&ignoreKeyCheck=true", field.GetIndexName(), field.GetName(), strconv.FormatBool(clear))
	return path, data, nil
}

func makeImportValueRequest(field *FieldInfo2, shard uint64, values []int64, columns []uint64, clear bool) (path string, data []byte, err error) {
	msg := &pbuf.ImportValueRequest{
		Index:          field.GetIndexName(),
		IndexCreatedAt: field.GetIndexCreatedAt(),
		Field:          field.GetName(),
		FieldCreatedAt: field.GetCreatedAt(),
		Shard:          shard,
		Values:         values,
		ColumnIDs:      columns,
	}
	data, err = pb.Marshal(msg)
	if err != nil {
		return "", nil, errors.Wrap(err, "marshaling Import to protobuf")
	}
	path = fmt.Sprintf("/index/%s/field/%s/import?clear=%s&ignoreKeyCheck=true", field.GetIndexName(), field.GetName(), strconv.FormatBool(clear))
	return path, data, nil
}
func TestSauronImport(t *testing.T) {

	cfg := NewSauronTestConfig(t)
	proxy := NewSauron(cfg)
	proxy.sql.MustRemove() //remove the existing db file if present
	url, err := proxy.Start()
	panicOn(err)
	defer proxy.Stop()
	schemaJson := `{
	"indexes": [{
		"name": "scratch",
		"createdAt": 1611185870149882000,
		"options": {
			"keys": false,
			"trackExistence": true
		},
		"fields": [{
			"name": "bsi",
			"createdAt": 1595896639332730413,
			"options": {
				"type": "int",
				"base": 0,
				"bitDepth": 31,
				"min": -9223372036854775808,
				"max": 9223372036854775807,
				"keys": false,
				"foreignIndex": ""
			}
		}, {
			"name": "decimal",
			"createdAt": 1613177295654228860,
			"options": {
				"type": "decimal",
				"base": 0,
				"scale": 1,
				"bitDepth": 0,
				"min": -922337203685477580.8,
				"max": 922337203685477580.7,
				"keys": false
			}
		},{
			"name": "setkeyfield",
			"createdAt": 1613345793598357200,
			"options": {
			  "type": "set",
			  "cacheType": "ranked",
			  "cacheSize": 50000,
			  "keys": true
			}
		  },{
			"name": "setfield",
			"createdAt": 1613345793598357200,
			"options": {
			  "type": "set",
			  "cacheType": "ranked",
			  "cacheSize": 50000,
			  "keys": false
			}
		  }]
	}]
}`
	client := &http.Client{}
	_, err = client.Post(url+"/schema", "application/json", bytes.NewBufferString(schemaJson))
	panicOn(err)

	t.Run("basic row/column", func(t *testing.T) {
		f, err := proxy.sql.GetField("scratch", "setfield")
		panicOn(err)
		clear := false
		cols := []uint64{0, 1, 2, 2, 3, 4, 1, 2, 3}
		rows := []uint64{0, 0, 0, 1, 1, 1, 2, 2, 2}
		shard := uint64(0)
		path, payload, err := makeImportRequest(f, shard, rows, cols, clear)
		_, err = client.Post(url+path, "application/json", bytes.NewBuffer(payload))
		panicOn(err)
	})
	t.Run("basic rowkey/column", func(t *testing.T) {
		f, err := proxy.sql.GetField("scratch", "setkeyfield")
		panicOn(err)
		clear := false
		cols := []uint64{0, 1, 2, 2, 3, 4, 1, 2, 3}
		rows := []string{"red", "red", "red", "blue", "blue", "blue", "green", "green", "green"}
		shard := uint64(0)
		path, payload, err := makeImportKeyRequest(f, shard, rows, cols, clear)
		_, err = client.Post(url+path, "application/json", bytes.NewBuffer(payload))
		panicOn(err)
	})
	t.Run("basic import values", func(t *testing.T) {
		f, err := proxy.sql.GetField("scratch", "bsi")
		panicOn(err)
		clear := false
		cols := []uint64{0, 1, 2, 3, 4}
		values := []int64{40, 30, 20, 10, 5}
		shard := uint64(0)
		path, payload, err := makeImportValueRequest(f, shard, values, cols, clear)
		_, err = client.Post(url+path, "application/json", bytes.NewBuffer(payload))
		panicOn(err)
	})

}
