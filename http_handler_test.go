// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"encoding/json"
	"fmt"
	"net"
	gohttp "net/http"
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
