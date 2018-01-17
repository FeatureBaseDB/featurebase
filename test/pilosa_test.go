package test_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

func TestNewCluster(t *testing.T) {
	cluster := test.MustNewServerCluster(t, 3)

	response, err := http.Get("http://" + cluster.Servers[0].Server.Addr().String() + "/status")
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	dec := json.NewDecoder(response.Body)
	body := struct {
		State string
		Nodes []struct {
			Scheme string
			Host   string
			Port   int
		}
	}{}

	err = dec.Decode(&body)
	if err != nil {
		t.Fatalf("decoding status response: %v", err)
	}

	bytes, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}

	if len(body.Nodes) != 3 {
		t.Fatalf("wrong number of nodes in status: %s", bytes)
	}

	if body.State != pilosa.ClusterStateNormal {
		t.Fatalf("cluster state should be %s but is %s", pilosa.ClusterStateNormal, body.State)
	}
}
