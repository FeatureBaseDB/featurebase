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
	"fmt"
	"io/ioutil"
	"net"
	gohttp "net/http"
	"net/url"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/encoding/proto"
	"github.com/pilosa/pilosa/http"
	"github.com/pilosa/pilosa/test"
)

func TestHandlerOptions(t *testing.T) {
	_, err := http.NewHandler()
	if err == nil {
		t.Fatalf("expected error making handler without options, got nil")
	}
	_, err = http.NewHandler(http.OptHandlerAPI(&pilosa.API{}))
	if err == nil {
		t.Fatalf("expected error making handler without options, got nil")
	}
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	_, err = http.NewHandler(http.OptHandlerListener(ln))
	if err == nil {
		t.Fatalf("expected error making handler without options, got nil")
	}
}

func TestHandlerImport(t *testing.T) {
	m := test.MustRunCommand()
	defer m.Close()

	///////////////////////////////////////

	indexName := "i"
	fieldName := "f"

	_ = m.MustCreateIndex(t, indexName, pilosa.IndexOptions{})
	_ = m.MustCreateField(t, indexName, fieldName)

	/*
	     |--------+--------+---------|-----------------------------------------------------------------------------------
	     | remote | shardS | hasKeys | action
	     |--------+--------+---------|-----------------------------------------------------------------------------------
	   0 | false  |        |         | unmarshal, call api.Import(false, req.Shard, "")
	   1 | false  |        | yes     | ->coordinator
	   2 | false  |        | no      | unmarshal, call api.Import(false, req.Shard, "no")
	   3 | false  | 0-n    |         | unmarshal, call api.Import(false, shard, ""); return error if shard != req.Shard
	   4 | false  | 0-n    | yes     | ->coordinator
	     | false  | 0-n    | no      | ->replicas
	   6 | true   |        |         | unmarshal, call api.Import(true, req.Shard, "")
	     | true   |        | yes     | return error
	   8 | true   |        | no      | unmarshal, call api.Import(true, req.Shard, "no")
	   9 | true   | 0-n    |         | unmarshal, call api.Import(false, shard, ""); return error if shard != req.Shard
	     | true   | 0-n    | yes     | return error
	   b | true   | 0-n    | no      | unmarshal, call api.Import(true, shard, "no"); return error if shard != req.Shard
	     |--------+--------+---------|-----------------------------------------------------------------------------------
	*/

	tests := []struct {
		remote        string
		shard         string
		hasKeys       string
		expStatusCode int
		expBody       string
	}{
		{"false", "0", "yes", 200, ""},
		{"false", "0", "no", 200, ""},
		{"false", "3", "no", 400, "shards do not match\n"},
		{"true", "", "yes", 400, "remote cannot be combined with hasKeys\n"},
		{"true", "3", "yes", 400, "remote cannot be combined with hasKeys\n"},
	}

	for _, test := range tests {

		v := url.Values{}
		if test.remote != "" {
			v.Set("remote", test.remote)
		}
		if test.shard != "" {
			v.Set("shard", test.shard)
		}
		if test.hasKeys != "" {
			v.Set("hasKeys", test.hasKeys)
		}

		url := fmt.Sprintf("%s/index/%s/field/%s/import?%s", m.URL(), indexName, fieldName, v.Encode())

		// Create protobuf import payload.
		serializer := proto.Serializer{}
		/*
			type ImportRequest struct {
				Index      string
				Field      string
				Shard      uint64
				RowIDs     []uint64
				ColumnIDs  []uint64
				RowKeys    []string
				ColumnKeys []string
				Timestamps []int64
			}
		*/
		importReq := pilosa.ImportRequest{
			Index:     indexName,
			Field:     fieldName,
			Shard:     0,
			RowIDs:    []uint64{1, 2, 3},
			ColumnIDs: []uint64{8, 8, 8},
		}
		protoReq, err := serializer.Marshal(&importReq)
		if err != nil {
			t.Fatal(err)
		}

		req, err := gohttp.NewRequest("POST", url, bytes.NewBuffer(protoReq))
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Accept", "application/x-protobuf")

		client := &gohttp.Client{}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		if resp.StatusCode != test.expStatusCode {
			t.Fatalf("expected status code: %d, but got: %d", test.expStatusCode, resp.StatusCode)
		} else if string(body) != test.expBody {
			t.Fatalf("expected body: %s, but got: %s", test.expBody, body)
		}
	}
}
