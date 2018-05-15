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

package pilosa

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/pilosa/pilosa/pql"
)

// Ensure executor returns an error if too many writes are in a single request.
func TestExecutor_Execute_ErrMaxWritesPerRequest(t *testing.T) {

	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	hldr := NewHolder()
	hldr.Path = path
	if err := hldr.Open(); err != nil {
		panic(err)
	}
	defer hldr.Close()

	client := &http.Client{}

	cluster := NewCluster()
	id := fmt.Sprintf("node%d", 0)
	uri, err := NewURIFromAddress("http://host0:0")
	if err != nil {
		panic(err)
	}
	node, err := NewNode(
		id,
		OptNodeURI(uri),
		OptNodeRemoteAPI(uri, client),
	)
	if err != nil {
		panic(err)
	}
	cluster.Nodes = append(cluster.Nodes, node)
	cluster.Node = cluster.Nodes[0]
	cluster.Coordinator = cluster.Nodes[0].ID
	cluster.SetState(ClusterStateNormal)

	e := NewExecutor()
	e.Holder = hldr
	e.Cluster = cluster
	e.Node = cluster.Nodes[0]

	e.maxWritesPerRequest = 3

	s := "SetBit() ClearBit() SetBit() SetBit()"
	q, err := pql.NewParser(strings.NewReader(s)).Parse()
	if err != nil {
		panic(err)
	}

	if _, err := e.Execute(context.Background(), "i", q, nil, nil); err != ErrTooManyWrites {
		t.Fatalf("unexpected error: %s", err)
	}
}
