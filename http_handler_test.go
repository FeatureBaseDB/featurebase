// Copyright 2021 Molecula Corp. All rights reserved.
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

	c.CreateField(t, "ttltest", pilosa.IndexOptions{}, "timefield", pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD"), "0"))

	nodeURL := c.Nodes[0].URL() + "/index/ttltest/field/timefield"
	fmt.Println(nodeURL)

	req, err := gohttp.NewRequest("PATCH", nodeURL, strings.NewReader(`{"option": "ttl", "value": "48h"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := gohttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("doing option request: %v", err)
	} else if resp.StatusCode != 200 {
		t.Fatalf("unexpected status updating TTL")
	}

	for _, node := range c.Nodes {
		ii, err := node.API.Schema(context.Background(), false)
		if err != nil {
			t.Fatalf("getting schema: %v", err)
		}
		if ii[0].Fields[0].Options.TTL != time.Hour*48 {
			t.Fatalf("unexpected TTL after update: %s", ii[0].Fields[0].Options.TTL)
		}
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
		"index-name": "example",
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

	postFieldTTLUrl := fmt.Sprintf("%s/index/example/field/with_ttl", m.URL())

	// Create new field with ttl but in invalid format
	fieldOptionInvalidTTL := `
 	{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"24hour" }}
	`
	respField := test.Do(t, "POST", postFieldTTLUrl, string(fieldOptionInvalidTTL))
	if (respField.StatusCode != gohttp.StatusBadRequest) &&
		(respField.Body != "applying option: cannot parse ttl: 24hour") {
		t.Errorf("expected ttl parse error, got status: %d, body=%s", respField.StatusCode, respField.Body)
	}

	// Create new field with ttl in invalid format
	fieldOptionValidTTL := `
	{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"24h" }}
	`
	respField = test.Do(t, "POST", postFieldTTLUrl, string(fieldOptionValidTTL))
	if resp.StatusCode != gohttp.StatusOK {
		t.Errorf("creating field with ttl, got status: %d, body=%s", respField.StatusCode, respField.Body)
	}

	// Create new field without ttl
	postFieldNoTTLUrl := fmt.Sprintf("%s/index/example/field/no_ttl", m.URL())
	fieldOptionNoTTL := `
	{ "options": {"timeQuantum":"YMDH","type":"time" }}
	`
	respField = test.Do(t, "POST", postFieldNoTTLUrl, string(fieldOptionNoTTL))
	if resp.StatusCode != gohttp.StatusOK {
		t.Errorf("creating field without ttl, status: %d, body=%s", respField.StatusCode, respField.Body)
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
	err := ioutil.WriteFile(permissionsPath, []byte(permissions1), 0666)
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
