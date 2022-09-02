// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	gohttp "net/http"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/encoding/proto"
	"github.com/molecula/featurebase/v3/server"
	"github.com/molecula/featurebase/v3/test"
)

func TestHandlerOptions(t *testing.T) {
	_, err := pilosa.NewHandler()
	if err == nil {
		t.Fatalf("expected error making handler without options, got nil")
	}
	_, err = pilosa.NewHandler(pilosa.OptHandlerAPI(&pilosa.API{}))
	if err == nil {
		t.Fatalf("expected error making handler without options, got nil")
	}

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("creating listener: %v", err)
	}

	_, err = pilosa.NewHandler(pilosa.OptHandlerListener(ln, ln.Addr().String()))
	if err == nil {
		t.Fatalf("expected error making handler without options, got nil")
	}

	_, err = pilosa.NewHandler(pilosa.OptHandlerListener(ln, ln.Addr().String()), pilosa.OptHandlerSerializer(proto.Serializer{}), pilosa.OptHandlerSerializer(proto.RoaringSerializer))
	if err == nil {
		t.Fatalf("expected error making handler without enough options, got nil")
	}

}

func TestMarshalUnmarshalTransactionResponse(t *testing.T) {
	tests := []struct {
		name string
		tr   *pilosa.TransactionResponse
	}{
		{
			name: "nil transaction",
			tr:   &pilosa.TransactionResponse{},
		},
		{
			name: "empty transaction",
			tr:   &pilosa.TransactionResponse{Transaction: &pilosa.Transaction{}},
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			data, err := json.Marshal(tst.tr)
			if err != nil {
				t.Fatalf("marshaling: %v", err)
			}

			mytr := &pilosa.TransactionResponse{}
			err = json.Unmarshal(data, mytr)
			if err != nil {
				t.Fatalf("unmarshalling: %v", err)
			}

			if mytr.Error != tst.tr.Error {
				t.Errorf("errors mismatch:exp/got \n%v\n%v", tst.tr.Error, mytr.Error)
			}
			test.CompareTransactions(t, tst.tr.Transaction, mytr.Transaction)
		})
	}
}

func TestUpdateFieldTTL(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	tests := []struct {
		name      string
		field     string
		ttl       string
		ttlOption string
		expStatus int
		expErr    string
		expTTL    time.Duration
	}{
		{
			// test update ttl: set initial ttl to 10s, then update ttl to 48h
			name:      "t1_48h",
			field:     "t1_48h",
			ttl:       "10s",
			ttlOption: `{"option": "ttl", "value": "48h"}`,
			expStatus: 200,
			expErr:    "",
			expTTL:    time.Hour * 48,
		},
		{
			// test unknown unit for ttl value
			name:      "t2_unknown_unit",
			field:     "t2_unknown_unit",
			ttl:       "20s",
			ttlOption: `{"option": "ttl", "value": "24abc"}`,
			expStatus: 400,
			expErr:    `unknown unit "abc"`,
			expTTL:    time.Second * 20,
		},
		{
			// test invalid ttl value
			name:      "t3_invalid",
			field:     "t3_invalid",
			ttl:       "30s",
			ttlOption: `{"option": "ttl", "value": "abcdef"}`,
			expStatus: 400,
			expErr:    `invalid duration "abcdef"`,
			expTTL:    time.Second * 30,
		},
		{
			// test empty ttl value
			name:      "t4_invalid_empty",
			field:     "t4_invalid_empty",
			ttl:       "40s",
			ttlOption: `{"option": "ttl", "value": ""}`,
			expStatus: 400,
			expErr:    `invalid duration ""`,
			expTTL:    time.Second * 40,
		},
		{
			// test non existent field name
			name:      "t5_diff_field_name",
			field:     "t5_diff_field",
			ttl:       "50s",
			ttlOption: `{"option": "ttl", "value": ""}`,
			expStatus: 404,
			expErr:    `key does not exist`,
			expTTL:    time.Second * 50,
		},
		{
			// test without field name
			name:      "t6_empty_field_name",
			field:     "",
			ttl:       "60s",
			ttlOption: `{"option": "ttl", "value": "12h"}`,
			expStatus: 405,
			expErr:    `405 Method Not Allowed`,
			expTTL:    time.Second * 60,
		},
		{
			// test negative value for ttl
			name:      "t7_negative",
			field:     "t7_negative",
			ttl:       "70s",
			ttlOption: `{"option": "ttl", "value": "-12h"}`,
			expStatus: 400,
			expErr:    `ttl can't be negative`,
			expTTL:    time.Second * 70,
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c.CreateField(t, "ttltest", pilosa.IndexOptions{}, test.name, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD"), test.ttl))
			nodeURL := c.Nodes[0].URL() + "/index/ttltest/field/" + test.field
			req, err := gohttp.NewRequest("PATCH", nodeURL, strings.NewReader(test.ttlOption))
			if err != nil {
				t.Fatal(err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := gohttp.DefaultClient.Do(req)

			if err != nil {
				t.Fatalf("doing option request: %v", err)
			}

			if resp.StatusCode != test.expStatus {
				t.Errorf("expected status: '%d', got: '%d'", test.expStatus, resp.StatusCode)
			}

			if resp.StatusCode == 400 || resp.StatusCode == 404 {
				// unmarshal error message to check against expErr
				var respBody map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&respBody)
				errMsg := respBody["error"].(map[string]interface{})["message"].(string)

				if !strings.Contains(errMsg, test.expErr) {
					t.Errorf("expected error: '%s', got: '%s'", test.expErr, errMsg)
				}
			} else if resp.StatusCode == 405 {
				if !strings.Contains(resp.Status, test.expErr) {
					t.Errorf("expected error: '%s', got: '%s'", test.expErr, resp.Status)
				}
			}

			// find updated field and check its TTL
			for _, node := range c.Nodes {
				ii, err := node.API.Schema(context.Background(), false)
				if err != nil {
					t.Fatalf("getting schema: %v", err)
				}
				if ii[0].Fields[i].Name == test.name {
					if ii[0].Fields[i].Options.TTL != test.expTTL {
						t.Errorf("expected TTL: '%s', got: '%s'", test.expTTL, ii[0].Fields[i].Options.TTL)
					}
				} else {
					t.Errorf("unexpected field: '%s', got: '%s'", test.name, ii[0].Fields[i].Name)
				}
			}

		})
	}

}

func TestUpdateFieldNoStandardView(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	tests := []struct {
		name              string
		field             string
		fieldOption       string
		expStatus         int
		expErr            string
		expNoStandardView bool
	}{
		{
			// test update noStandardView to true
			name:              "t1_true",
			field:             "t1_true",
			fieldOption:       `{"option": "noStandardView", "value": "true"}`,
			expStatus:         200,
			expErr:            "",
			expNoStandardView: true,
		},
		{
			// test update noStandardView to false
			name:              "t2_false",
			field:             "t2_false",
			fieldOption:       `{"option": "noStandardView", "value": "false"}`,
			expStatus:         200,
			expErr:            "",
			expNoStandardView: false,
		},
		{
			// test update noStandardView with invalid value
			name:              "t3_invalid",
			field:             "t3_invalid",
			fieldOption:       `{"option": "noStandardView", "value": "123"}`,
			expStatus:         400,
			expErr:            `invalid value for noStandardView: '123'`,
			expNoStandardView: false,
		},
		{
			// test udpate noStandardView with empty value
			name:              "t4_empty",
			field:             "t4_empty",
			fieldOption:       `{"option": "noStandardView", "value": ""}`,
			expStatus:         400,
			expErr:            `invalid value for noStandardView: ''`,
			expNoStandardView: false,
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c.CreateField(t, "ttltest", pilosa.IndexOptions{}, test.name, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD"), "0"))
			nodeURL := c.Nodes[0].URL() + "/index/ttltest/field/" + test.field
			req, err := gohttp.NewRequest("PATCH", nodeURL, strings.NewReader(test.fieldOption))
			if err != nil {
				t.Fatal(err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := gohttp.DefaultClient.Do(req)

			if err != nil {
				t.Fatalf("doing option request: %v", err)
			}

			if resp.StatusCode != test.expStatus {
				t.Errorf("expected status: '%d', got: '%d'", test.expStatus, resp.StatusCode)
			}

			if resp.StatusCode == 400 {
				// unmarshal error message to check against expErr
				var respBody map[string]interface{}
				json.NewDecoder(resp.Body).Decode(&respBody)
				errMsg := respBody["error"].(map[string]interface{})["message"].(string)

				if !strings.Contains(errMsg, test.expErr) {
					t.Errorf("expected error: '%s', got: '%s'", test.expErr, errMsg)
				}
			}

			// find updated field and check its noStandardView
			for _, node := range c.Nodes {
				ii, err := node.API.Schema(context.Background(), false)
				if err != nil {
					t.Fatalf("getting schema: %v", err)
				}
				if ii[0].Fields[i].Name == test.name {
					if ii[0].Fields[i].Options.NoStandardView != test.expNoStandardView {
						t.Errorf("expected noStandardView value: '%t', got: '%t'", test.expNoStandardView, ii[0].Fields[i].Options.NoStandardView)
					}
				} else {
					t.Errorf("unexpected field: '%s', got: '%s'", test.name, ii[0].Fields[i].Name)
				}
			}

		})
	}
}

func TestIngestSchemaHandler(t *testing.T) {
	c := test.MustRunCluster(t, 3,
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node0"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node1"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node2"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
	)
	defer c.Close()

	schema := `
{
   "index-name": "example",
   "primary-key-type": "string",
   "index-action": "create",
   "fields": [
       {
           "field-name": "idset",
           "field-type": "id",
           "field-options": {
				"cache-type": "none"
           }
       },
       {
           "field-name": "id",
           "field-type": "id",
           "field-options": {
               "enforce-mutual-exclusion": true
           }
       },
       {
           "field-name": "bool",
           "field-type": "bool"
       },
       {
           "field-name": "stringset",
           "field-type": "string",
           "field-options": {
				"cache-type": "ranked",
           		"cache-size": 100000
           }
       },
       {
           "field-name": "string",
           "field-type": "string",
           "field-options": {
               "enforce-mutual-exclusion": true
           }
       },
       {
           "field-name": "int",
           "field-type": "int"
       },
       {
           "field-name": "decimal",
           "field-type": "decimal",
           "field-options": {
               "scale": 2
           }
       },
       {
           "field-name": "timestamp",
           "field-type": "timestamp",
           "field-options": {
               "epoch": "1996-12-19T16:39:57-08:00",
               "unit": "µs"
           }
       },
       {
           "field-name": "quantum",
           "field-type": "string",
           "field-options": {
               "time-quantum": "YMDH"
           }
       }
   ]
}
`
	m := c.GetPrimary()
	schemaURL := fmt.Sprintf("%s/internal/schema", m.URL())
	resp := test.Do(t, "POST", schemaURL, string(schema))
	if resp.StatusCode != gohttp.StatusOK {
		t.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	// now, try again, expecting a failure:
	resp = test.Do(t, "POST", schemaURL, string(schema))
	if resp.StatusCode != gohttp.StatusConflict {
		t.Errorf("invalid status: expected 409, got %d, body=%s", resp.StatusCode, resp.Body)
	}
}

func TestPostFieldWithTTL(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	schema := `
	{
		"index-name": "ttl_test",
		"primary-key-type": "string",
		"index-action": "create",
		"fields":[]
	 }
	`
	m := c.GetPrimary()
	schemaURL := fmt.Sprintf("%s/internal/schema", m.URL())
	resp := test.Do(t, "POST", schemaURL, string(schema))
	if resp.StatusCode != gohttp.StatusOK {
		t.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}

	tests := []struct {
		name      string
		url       string
		option    string
		expStatus int
		expErr    string
		expTTL    time.Duration
	}{
		{
			name:      "t1_48h",
			url:       fmt.Sprintf("%s/index/ttl_test/field/t1_48h", m.URL()),
			option:    `{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"48h" }}`,
			expStatus: 200,
			expErr:    `"success":true`,
			expTTL:    time.Hour * 48,
		},
		{
			name:      "t2_unknown_unit",
			url:       fmt.Sprintf("%s/index/ttl_test/field/t2_unknown_unit", m.URL()),
			option:    `{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"24abc" }}`,
			expStatus: 400,
			expErr:    "cannot parse ttl",
		},
		{
			name:      "t3_invalid",
			url:       fmt.Sprintf("%s/index/ttl_test/field/t3_invalid", m.URL()),
			option:    `{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"abcdef" }}`,
			expStatus: 400,
			expErr:    "cannot parse ttl",
		},
		{
			name:      "t4_invalid_empty",
			url:       fmt.Sprintf("%s/index/ttl_test/field/t4_invalid_empty", m.URL()),
			option:    `{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"" }}`,
			expStatus: 400,
			expErr:    "cannot parse ttl",
		},
		{
			name:      "t5_negative",
			url:       fmt.Sprintf("%s/index/ttl_test/field/t5_negative", m.URL()),
			option:    `{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"-24h" }}`,
			expStatus: 400,
			expErr:    "ttl can't be negative",
		},
	}

	for i, test_i := range tests {
		t.Run(test_i.name, func(t *testing.T) {
			respField := test.Do(t, "POST", test_i.url, test_i.option)
			if respField.StatusCode != test_i.expStatus {
				t.Errorf("expected status: '%d', got: '%d'", test_i.expStatus, respField.StatusCode)
			}

			if !strings.Contains(respField.Body, test_i.expErr) {
				t.Errorf("expected error: '%s', got: '%s'", test_i.expErr, respField.Body)
			}

			// find the created field and check its TTL
			// since only first test (t1_48h) is successful in creating a field,
			// dont find other fields from other test cases, it will cause index out of bound
			if test_i.name == "t1_48h" {
				for _, node := range c.Nodes {
					ii, err := node.API.Schema(context.Background(), false)
					if err != nil {
						t.Fatalf("getting schema: %v", err)
					}

					if ii[0].Fields[i].Name == test_i.name {
						if ii[0].Fields[i].Options.TTL != test_i.expTTL {
							t.Errorf("expected TTL: '%s', got: '%s'", test_i.expTTL, ii[0].Fields[i].Options.TTL)
						}
					} else {
						t.Errorf("unexpected field: '%s', got: '%s'", test_i.name, ii[0].Fields[i].Name)
					}
				}
			}
		})
	}
}

func TestGetViewAndDelete(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	schema := `
	{
		"index-name": "example",
		"primary-key-type": "string",
		"index-action": "create",
		"fields": [
			{
				"field-name": "test_view",
				"field-type": "time",
				"field-options": {
					"time-quantum": "YMDH"
				}
			}
		]
	 }
	 `
	m := c.GetPrimary()
	schemaURL := fmt.Sprintf("%s/internal/schema", m.URL())
	resp := test.Do(t, "POST", schemaURL, string(schema))
	if resp.StatusCode != gohttp.StatusOK {
		t.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}

	// Send sample data
	postQueryUrl := fmt.Sprintf("%s/index/example/query", m.URL())
	queryOption := `
	Set(1,test_view=1,2001-02-03T04:05)
	`
	respQuery := test.Do(t, "POST", postQueryUrl, string(queryOption))
	if respQuery.StatusCode != gohttp.StatusOK {
		t.Errorf("posting query, status: %d, body=%s", respQuery.StatusCode, respQuery.Body)
	}

	// The above sample data should create these views:
	expectedViewNames := []string{
		"standard",
		"standard_2001",
		"standard_200102",
		"standard_20010203",
		"standard_2001020304",
	}

	// Call view to get data
	viewUrl := fmt.Sprintf("%s/index/example/field/test_view/view", m.URL())
	respView := test.Do(t, "GET", viewUrl, "")
	if respView.StatusCode != gohttp.StatusOK {
		t.Errorf("view handler, status: %d, body=%s", respView.StatusCode, respView.Body)
	}

	type viewReponse struct {
		Name  string `json:"name"`
		Type  string `json:"type"`
		Field string `json:"field"`
		Index string `json:"index"`
	}

	var parsedViews []viewReponse
	if err := json.Unmarshal([]byte(respView.Body), &parsedViews); err != nil {
		t.Errorf("parsing view, err: %s", err)
	}

	// check if data from view matches with expectedViewNames
	parseViewNames := []string{}
	for _, view := range parsedViews {
		parseViewNames = append(parseViewNames, view.Name)
	}
	sort.Strings(parseViewNames)

	if !reflect.DeepEqual(expectedViewNames, parseViewNames) {
		t.Fatalf("expected %v, but got %v", expectedViewNames, parseViewNames)
	}

	// call delete on view standard_2001020304
	deleteViewUrl := fmt.Sprintf("%s/index/example/field/test_view/view/standard_2001020304", m.URL())
	respDelete := test.Do(t, "DELETE", deleteViewUrl, "")
	if respDelete.StatusCode != gohttp.StatusOK {
		t.Errorf("delete handler, status: %d, body=%s", respDelete.StatusCode, respDelete.Body)
	}

	// remove view that was deleted (standard_2001020304) from expectedViewNames
	expectedViewNames = expectedViewNames[:len(expectedViewNames)-1]

	// call view again
	viewUrl = fmt.Sprintf("%s/index/example/field/test_view/view", m.URL())
	respView = test.Do(t, "GET", viewUrl, "")
	if respView.StatusCode != gohttp.StatusOK {
		t.Errorf("view handler after delete, status: %d, body=%s", respView.StatusCode, respView.Body)
	}

	if err := json.Unmarshal([]byte(respView.Body), &parsedViews); err != nil {
		t.Errorf("parsing view, err: %s", err)
	}

	// check if data from view matches with expectedViewNames
	parseViewNames = []string{}
	for _, view := range parsedViews {
		parseViewNames = append(parseViewNames, view.Name)
	}
	sort.Strings(parseViewNames)

	if !reflect.DeepEqual(expectedViewNames, parseViewNames) {
		t.Fatalf("after delete, expected %v, but got %v", expectedViewNames, parseViewNames)
	}
}

func TestTranslationHandlers(t *testing.T) {
	// reusable data for the tests
	nameBytes, err := json.Marshal([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("marshalling json: %v", err)
	}
	names := string(nameBytes)

	c := test.MustRunCluster(t, 1)
	defer c.Close()

	schema := `
{
   "index-name": "example",
   "primary-key-type": "string",
   "index-action": "create",
   "fields": [
       {
           "field-name": "stringset",
           "field-type": "string",
           "field-options": {
				"cache-type": "ranked",
           		"cache-size": 100000
           }
       }
   ]
}
`
	m := c.GetPrimary()
	schemaURL := fmt.Sprintf("%s/internal/schema", m.URL())
	resp := test.Do(t, "POST", schemaURL, string(schema))
	if resp.StatusCode != gohttp.StatusOK {
		t.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	baseURLs := []string{
		fmt.Sprintf("%s/internal/translate/index/example/", m.URL()),
		fmt.Sprintf("%s/internal/translate/field/example/stringset/", m.URL()),
		fmt.Sprintf("%s/internal/translate/field/example/nonexistent/", m.URL()),
	}
	for _, url := range baseURLs {
		expectFailure := strings.HasSuffix(url, "/nonexistent/")
		createURL := url + "keys/create"
		findURL := url + "keys/find"
		var results map[string]uint64

		if expectFailure {
			resp := test.Do(t, "POST", findURL, names)
			if resp.StatusCode != gohttp.StatusInternalServerError {
				t.Fatalf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
			}
			resp = test.Do(t, "POST", createURL, names)
			if resp.StatusCode != gohttp.StatusInternalServerError {
				t.Fatalf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
			}
			continue
		}

		// try to find them when they don't exist
		resp := test.Do(t, "POST", findURL, names)
		if resp.StatusCode != gohttp.StatusOK {
			t.Fatalf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
		}
		err := json.Unmarshal([]byte(resp.Body), &results)
		if err != nil {
			t.Fatalf("unmarshalling result: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("finding keys before any were set: expected no results, got %d (%q)", len(results), results)
		}

		// try to create them, but malformed, so we expect an error
		resp = test.Do(t, "POST", createURL, names[:6])
		if resp.StatusCode != gohttp.StatusBadRequest {
			t.Fatalf("invalid status: expected 400, got %d, body=%s", resp.StatusCode, resp.Body)
		}

		// try to create them
		resp = test.Do(t, "POST", createURL, names)
		if resp.StatusCode != gohttp.StatusOK {
			t.Fatalf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
		}
		err = json.Unmarshal([]byte(resp.Body), &results)
		if err != nil {
			t.Fatalf("unmarshalling result: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("finding keys before any were set: expected 3 results, got %d (%q)", len(results), results)
		}

		// try to find them now that they exist
		resp = test.Do(t, "POST", findURL, names)
		if resp.StatusCode != gohttp.StatusOK {
			t.Fatalf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
		}
		err = json.Unmarshal([]byte(resp.Body), &results)
		if err != nil {
			t.Fatalf("unmarshalling result: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("finding keys before any were set: expected 3 results, got %d (%q)", len(results), results)
		}
	}
}

func TestAuthAllowedNetworks(t *testing.T) {
	permissions1 := `
"user-groups":
  "dca35310-ecda-4f23-86cd-876aee55906b":
    "test": "read"
admin: "ac97c9e2-346b-42a2-b6da-18bcb61a32fe"`

	tmpDir := t.TempDir()
	permissionsPath := path.Join(tmpDir, "test-permissions.yaml")
	err := ioutil.WriteFile(permissionsPath, []byte(permissions1), 0600)
	if err != nil {
		t.Fatalf("failed to write permissions file: %v", err)
	}

	queryLogPath := path.Join(tmpDir, "query.log")
	_, err = os.Create(queryLogPath)
	if err != nil {
		t.Fatal(err)
	}

	validIP := "10.0.0.2"

	clusterSize := 3
	commandOpts := make([][]server.CommandOption, clusterSize)
	configs := make([]*server.Config, clusterSize)
	for i := range configs {
		conf := server.NewConfig()
		configs[i] = conf
		conf.TLS.CertificatePath = "./testdata/certs/localhost.crt"
		conf.TLS.CertificateKeyPath = "./testdata/certs/localhost.key"
		conf.Auth.Enable = true
		conf.Auth.ClientId = "e9088663-eb08-41d7-8f65-efb5f54bbb71"
		conf.Auth.ClientSecret = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		conf.Auth.AuthorizeURL = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize"
		conf.Auth.TokenURL = "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize"
		conf.Auth.GroupEndpointURL = "https://graph.microsoft.com/v1.0/me/transitiveMemberOf/microsoft.graph.group?$count=true"
		conf.Auth.LogoutURL = "https://login.microsoftonline.com/common/oauth2/v2.0/logout"
		conf.Auth.RedirectBaseURL = "https://localhost:10101/"
		conf.Auth.QueryLogPath = queryLogPath
		conf.Auth.SecretKey = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
		conf.Auth.PermissionsFile = permissionsPath
		conf.Auth.Scopes = []string{"https://graph.microsoft.com/.default", "offline_access"}
		conf.Auth.ConfiguredIPs = []string{validIP}
		commandOpts[i] = append(commandOpts[i], server.OptCommandConfig(conf))
	}

	c := test.MustRunCluster(t, clusterSize, commandOpts...)
	defer c.Close()

	m := c.GetPrimary()
	index := "allowed-networks-index"
	keyedIndex := "allowed-networks-index-keyed"
	field := "field1"

	// needed for key translation
	nameBytes, err := json.Marshal([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("marshalling json: %v", err)
	}
	names := string(nameBytes)

	schema := `
	{
	   "index-name": "allowed-networks-index-keyed",
	   "primary-key-type": "string",
	   "index-action": "create",
	   "fields": [
		   {
			   "field-name": "stringset",
			   "field-type": "string",
			   "field-options": {
					"cache-type": "ranked",
					   "cache-size": 100000
			   }
		   }
	   ]
	}
	`

	IPTests := []struct {
		TestName   string
		ClientIP   string
		StatusCode int
	}{
		{TestName: "ValidIP", ClientIP: validIP, StatusCode: http.StatusOK},
		{TestName: "InvalidIP", ClientIP: "10.0.1.1", StatusCode: http.StatusForbidden},
	}

	tests := []struct {
		testName string
		method   string
		url      string
		body     string
	}{
		{
			testName: "Post-Index",
			method:   "POST",
			url:      fmt.Sprintf("%s/index/%s", m.URL(), index),
			body:     "",
		},
		{
			testName: "Post-field",
			method:   "POST",
			url:      fmt.Sprintf("%s/index/%s/field/%s", m.URL(), index, field),
			body:     "",
		},
		{
			testName: "Get-Schema",
			method:   "GET",
			url:      fmt.Sprintf("%s/schema", m.URL()),
			body:     "",
		},
		{
			testName: "Post-Schema",
			method:   "POST",
			url:      fmt.Sprintf("%s/internal/schema", m.URL()),
			body:     schema,
		},
		{
			testName: "Get-Shards",
			method:   "GET",
			url:      fmt.Sprintf("%s/internal/index/%s/shards", m.URL(), keyedIndex),
			body:     "",
		},
		{
			testName: "Post-CreateKeys",
			method:   "POST",
			url:      fmt.Sprintf("%s/internal/translate/index/%s/keys/create", m.URL(), keyedIndex),
			body:     names,
		},
		{
			testName: "Get-MemoryUsage",
			method:   "GET",
			url:      fmt.Sprintf("%s/internal/mem-usage", m.URL()),
			body:     "",
		},
		{
			testName: "Get-Status",
			method:   "GET",
			url:      fmt.Sprintf("%s/status", m.URL()),
			body:     "",
		},
	}

	for _, ipTest := range IPTests {
		for _, test := range tests {
			t.Run(ipTest.TestName+"-"+test.testName, func(t *testing.T) {
				var req *gohttp.Request
				if test.body != "" {
					req, err = gohttp.NewRequest(test.method, test.url, strings.NewReader(test.body))
					if err != nil {
						t.Fatal(err)
					}
				} else {
					req, err = gohttp.NewRequest(test.method, test.url, nil)
					if err != nil {
						t.Fatal(err)
					}
				}

				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Forwarded-For", ipTest.ClientIP)
				resp, err := gohttp.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("failed to send request: %v", err)
				}
				if resp.StatusCode != ipTest.StatusCode {
					t.Fatalf("expected %v response code, got %v, body: %v", ipTest.StatusCode, resp.StatusCode, resp.Body)
				}

				if ipTest.StatusCode == 200 {
					body, err := ioutil.ReadAll(resp.Body)
					if err != nil {
						t.Fatalf("reading resp body :%v", err)
					}

					if test.testName == "Post-CreateKeys" {
						var results map[string]uint64
						err = json.Unmarshal([]byte(body), &results)
						if err != nil {
							t.Fatalf("unmarshalling result: %v", err)
						}
						if len(results) != 3 {
							t.Fatalf("finding keys before any were set: expected 3 results, got %d (%q)", len(results), results)
						}
					}
				}
			})
		}
	}
}
