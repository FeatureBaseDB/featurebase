// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"encoding/json"
	"fmt"
	"net"
	gohttp "net/http"
	"reflect"
	"sort"
	"strings"
	"testing"

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

	ln, err := net.Listen("tcp", ":0")
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

func TestPostFieldWithTtl(t *testing.T) {
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

	postFieldTtlUrl := fmt.Sprintf("%s/index/example/field/with_ttl", m.URL())

	// Create new field with ttl but in invalid format
	fieldOptionInvalidTtl := `
 	{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"24hour" }}
	`
	respField := test.Do(t, "POST", postFieldTtlUrl, string(fieldOptionInvalidTtl))
	if (respField.StatusCode != gohttp.StatusBadRequest) &&
		(respField.Body != "applying option: cannot parse ttl: 24hour") {
		t.Errorf("expected ttl parse error, got status: %d, body=%s", respField.StatusCode, respField.Body)
	}

	// Create new field with ttl in invalid format
	fieldOptionValidTtl := `
	{ "options": {"timeQuantum":"YMDH","type":"time","ttl":"24h" }}
	`
	respField = test.Do(t, "POST", postFieldTtlUrl, string(fieldOptionValidTtl))
	if resp.StatusCode != gohttp.StatusOK {
		t.Errorf("creating field with ttl, got status: %d, body=%s", respField.StatusCode, respField.Body)
	}

	// Create new field without ttl
	postFieldNoTtlUrl := fmt.Sprintf("%s/index/example/field/no_ttl", m.URL())
	fieldOptionNoTtl := `
	{ "options": {"timeQuantum":"YMDH","type":"time" }}
	`
	respField = test.Do(t, "POST", postFieldNoTtlUrl, string(fieldOptionNoTtl))
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
