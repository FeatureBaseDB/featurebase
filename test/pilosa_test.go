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

package test_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

func TestNewCluster(t *testing.T) {
	numNodes := 3
	cluster := test.MustRunMainWithCluster(t, numNodes)
	coordinator := cluster[0].Server.Cluster.Coordinator
	for i := 1; i < numNodes; i++ {
		if coordi := cluster[i].Server.Cluster.Coordinator; coordi != coordinator {
			t.Fatalf("node %d does not have the same coordinator as node 0. '%v' and '%v' respectively", i, coordi, coordinator)
		}
	}

	response, err := http.Get("http://" + cluster[0].Server.Addr().String() + "/status")
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
