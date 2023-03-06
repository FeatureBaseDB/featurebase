// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/encoding/proto"
	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/stretchr/testify/assert"
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

	indexName := c.Idx("s")
	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c.CreateField(t, indexName, pilosa.IndexOptions{}, test.name, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD"), test.ttl))
			nodeURL := fmt.Sprintf("%s/index/%s/field/%s", c.Nodes[0].URL(), c, test.field)
			req, err := http.NewRequest("PATCH", nodeURL, strings.NewReader(test.ttlOption))
			if err != nil {
				t.Fatal(err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)

			if err != nil {
				t.Fatalf("doing option request: %v", err)
			}

			if resp.StatusCode != test.expStatus {
				t.Errorf("expected status: '%d', got: '%d'", test.expStatus, resp.StatusCode)
			}

			if resp.StatusCode == 400 || resp.StatusCode == 404 {
				// unmarshal error message to check against expErr
				var respBody map[string]interface{}
				err := json.NewDecoder(resp.Body).Decode(&respBody)
				assert.NoError(t, err)
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
				for _, idx := range ii {
					if idx.Name == indexName {
						if len(idx.Fields) <= i {
							t.Fatalf("expected %d fields, last %s, got %d fields",
								i, test.name, len(idx.Fields))
						}
						if idx.Fields[i].Name == test.name {
							if idx.Fields[i].Options.TTL != test.expTTL {
								t.Errorf("expected noStandardView value: '%s', got: '%s'", test.expTTL, idx.Fields[i].Options.TTL)
							}
						} else {
							t.Errorf("unexpected field: '%s', got: '%s'", test.name, idx.Fields[i].Name)
						}
						break
					}
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
	// "s" to make it match %s behavior of a cluster
	indexName := c.Idx("s")

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c.CreateField(t, indexName, pilosa.IndexOptions{}, test.name, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD"), "0"))
			nodeURL := fmt.Sprintf("%s/index/%s/field/%s", c.Nodes[0].URL(), c, test.field)
			req, err := http.NewRequest("PATCH", nodeURL, strings.NewReader(test.fieldOption))
			if err != nil {
				t.Fatal(err)
			}

			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)

			if err != nil {
				t.Fatalf("doing option request: %v", err)
			}

			if resp.StatusCode != test.expStatus {
				t.Errorf("expected status: '%d', got: '%d'", test.expStatus, resp.StatusCode)
			}

			if resp.StatusCode == 400 {
				// unmarshal error message to check against expErr
				var respBody map[string]interface{}
				err := json.NewDecoder(resp.Body).Decode(&respBody)
				assert.NoError(t, err)
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
				for _, idx := range ii {
					if idx.Name == indexName {
						if len(idx.Fields) <= i {
							t.Fatalf("expected %d fields, last %s, got %d fields",
								i, test.name, len(idx.Fields))
						}
						if idx.Fields[i].Name == test.name {
							if idx.Fields[i].Options.NoStandardView != test.expNoStandardView {
								t.Errorf("expected noStandardView value: '%t', got: '%t'", test.expNoStandardView, idx.Fields[i].Options.NoStandardView)
							}
						} else {
							t.Errorf("unexpected field: '%s', got: '%s'", test.name, idx.Fields[i].Name)
						}
						break
					}
				}
			}
		})
	}
}

func TestPostFieldWithTTL(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	indexName := c.Idx("s")
	_, err := c.GetNode(0).API.CreateIndex(context.Background(), indexName, pilosa.IndexOptions{Keys: true})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	m := c.GetPrimary()

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
			url:       fmt.Sprintf("%s/index/%s/field/t1_48h", m.URL(), c),
			option:    `{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"48h" }}`,
			expStatus: 200,
			expErr:    `"success":true`,
			expTTL:    time.Hour * 48,
		},
		{
			name:      "t2_unknown_unit",
			url:       fmt.Sprintf("%s/index/%s/field/t2_unknown_unit", m.URL(), c),
			option:    `{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"24abc" }}`,
			expStatus: 400,
			expErr:    "cannot parse ttl",
		},
		{
			name:      "t3_invalid",
			url:       fmt.Sprintf("%s/index/%s/field/t3_invalid", m.URL(), c),
			option:    `{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"abcdef" }}`,
			expStatus: 400,
			expErr:    "cannot parse ttl",
		},
		{
			name:      "t4_invalid_empty",
			url:       fmt.Sprintf("%s/index/%s/field/t4_invalid_empty", m.URL(), c),
			option:    `{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"" }}`,
			expStatus: 400,
			expErr:    "cannot parse ttl",
		},
		{
			name:      "t5_negative",
			url:       fmt.Sprintf("%s/index/%s/field/t5_negative", m.URL(), c),
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

					for _, idx := range ii {
						if idx.Name == indexName {
							if len(idx.Fields) <= i {
								t.Fatalf("expected %d fields, last %s, got %d fields",
									i, test_i.name, len(idx.Fields))
							}
							if idx.Fields[i].Name == test_i.name {
								if idx.Fields[i].Options.TTL != test_i.expTTL {
									t.Errorf("expected TTL: '%s', got: '%s'", test_i.expTTL, idx.Fields[i].Options.TTL)
								}
							} else {
								t.Errorf("unexpected field: '%s', got: '%s'", test_i.name, idx.Fields[i].Name)
							}
							break
						}
					}
				}
			}
		})
	}
}

func TestGetViewAndDelete(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	m := c.GetPrimary()

	_, err := m.API.CreateIndex(context.Background(), c.Idx("s"), pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = m.API.CreateField(context.Background(), c.Idx("s"), "test_view", pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH"), "0"))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}

	// Send sample data
	postQueryUrl := fmt.Sprintf("%s/index/%s/query", m.URL(), c)
	queryOption := `
	Set(1,test_view=1,2001-02-03T04:05)
	`
	respQuery := test.Do(t, "POST", postQueryUrl, string(queryOption))
	if respQuery.StatusCode != http.StatusOK {
		t.Errorf("posting query, status: %d, body=%s", respQuery.StatusCode, respQuery.Body)
	}

	// The above sample data should create these views:
	expectedViewNames := []string{
		"existence",
		"standard",
		"standard_2001",
		"standard_200102",
		"standard_20010203",
		"standard_2001020304",
	}

	// Call view to get data
	viewUrl := fmt.Sprintf("%s/index/%s/field/test_view/view", m.URL(), c)
	respView := test.Do(t, "GET", viewUrl, "")
	if respView.StatusCode != http.StatusOK {
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
	deleteViewUrl := fmt.Sprintf("%s/index/%s/field/test_view/view/standard_2001020304", m.URL(), c)
	respDelete := test.Do(t, "DELETE", deleteViewUrl, "")
	if respDelete.StatusCode != http.StatusOK {
		t.Errorf("delete handler, status: %d, body=%s", respDelete.StatusCode, respDelete.Body)
	}

	// remove view that was deleted (standard_2001020304) from expectedViewNames
	expectedViewNames = expectedViewNames[:len(expectedViewNames)-1]

	// call view again
	viewUrl = fmt.Sprintf("%s/index/%s/field/test_view/view", m.URL(), c)
	respView = test.Do(t, "GET", viewUrl, "")
	if respView.StatusCode != http.StatusOK {
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

// TestHandlerSQL tests that the json coming back from a POST /sql request has
// the expected json tags.
func TestHandlerSQL(t *testing.T) {
	cfg := server.NewConfig()
	c := test.MustRunCluster(t, 1, []server.CommandOption{
		server.OptCommandConfig(cfg),
	})
	defer c.Close()

	m := c.GetPrimary()

	tests := []struct {
		name    string
		url     string
		sql     string
		expKeys []string
	}{
		{
			name:    "sql",
			url:     "/sql",
			sql:     "show tables",
			expKeys: []string{"schema", "data", "execution-time"},
		},
		{
			name:    "sql-with-plan",
			url:     "/sql?plan=1",
			sql:     "show tables",
			expKeys: []string{"schema", "data", "query-plan", "execution-time"},
		},
		{
			name:    "invalid-sql",
			url:     "/sql",
			sql:     "invalid sql",
			expKeys: []string{"error", "execution-time"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlURL := fmt.Sprintf("%s%s", m.URL(), tt.url)
			resp := test.Do(t, "POST", sqlURL, tt.sql)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("post sql, status: %d, body=%s", resp.StatusCode, resp.Body)
			}

			out := make(map[string]interface{})
			assert.NoError(t, json.Unmarshal([]byte(resp.Body), &out))

			keys := make([]string, 0, len(out))
			for k := range out {
				keys = append(keys, k)
			}

			assert.ElementsMatch(t, tt.expKeys, keys)
		})
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
	m := c.GetPrimary()
	_, err = m.API.CreateIndex(context.Background(), c.Idx("s"), pilosa.IndexOptions{Keys: true})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = m.API.CreateField(context.Background(), c.Idx("s"), "stringset", pilosa.OptFieldTypeSet("ranked", 100000), pilosa.OptFieldKeys())
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}

	baseURLs := []string{
		fmt.Sprintf("%s/internal/translate/index/%s/", m.URL(), c),
		fmt.Sprintf("%s/internal/translate/field/%s/stringset/", m.URL(), c),
		fmt.Sprintf("%s/internal/translate/field/%s/nonexistent/", m.URL(), c),
	}
	for _, url := range baseURLs {
		expectFailure := strings.HasSuffix(url, "/nonexistent/")
		createURL := url + "keys/create"
		findURL := url + "keys/find"
		var results map[string]uint64

		if expectFailure {
			resp := test.Do(t, "POST", findURL, names)
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
			}
			resp = test.Do(t, "POST", createURL, names)
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
			}
			continue
		}

		// try to find them when they don't exist
		resp := test.Do(t, "POST", findURL, names)
		if resp.StatusCode != http.StatusOK {
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
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid status: expected 400, got %d, body=%s", resp.StatusCode, resp.Body)
		}

		// try to create them
		resp = test.Do(t, "POST", createURL, names)
		if resp.StatusCode != http.StatusOK {
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
		if resp.StatusCode != http.StatusOK {
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
	err := os.WriteFile(permissionsPath, []byte(permissions1), 0600)
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
	index := c.Idx("s")
	keyedIndex := c.Idx("k")
	field := "field1"

	// This keyed index used to be created by the Post-Schema subtest, but that doesn't
	// exist anymore, so we create it up here. We don't create the other one because it's
	// supposed to get created by Post-Index.
	_, err = m.API.CreateIndex(context.Background(), c.Idx("k"), pilosa.IndexOptions{Keys: true})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}

	// needed for key translation
	nameBytes, err := json.Marshal([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("marshalling json: %v", err)
	}
	names := string(nameBytes)

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
				var req *http.Request
				if test.body != "" {
					req, err = http.NewRequest(test.method, test.url, strings.NewReader(test.body))
					if err != nil {
						t.Fatal(err)
					}
				} else {
					req, err = http.NewRequest(test.method, test.url, nil)
					if err != nil {
						t.Fatal(err)
					}
				}

				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Forwarded-For", ipTest.ClientIP)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("failed to send request: %v", err)
				}
				if resp.StatusCode != ipTest.StatusCode {
					t.Fatalf("expected %v response code, got %v, body: %v", ipTest.StatusCode, resp.StatusCode, resp.Body)
				}

				if ipTest.StatusCode == 200 {
					body, err := io.ReadAll(resp.Body)
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
