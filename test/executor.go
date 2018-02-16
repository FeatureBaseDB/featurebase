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

package test

import (
	"net/http"
	"strings"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

// Executor represents a test wrapper for pilosa.Executor.
type Executor struct {
	*pilosa.Executor
}

var remoteClient *http.Client

func init() {
	remoteClient = pilosa.GetHTTPClient(nil)
}

// NewExecutor returns a new instance of Executor.
// The executor always matches the uri of the first cluster node.
func NewExecutor(holder *pilosa.Holder, cluster *pilosa.Cluster) *Executor {
	executor := pilosa.NewExecutor(remoteClient)
	e := &Executor{Executor: executor}
	e.Holder = holder
	e.Cluster = cluster
	e.Node = cluster.Nodes[0]
	return e
}

// MustParse parses s into a PQL query. Panic on error.
func MustParse(s string) *pql.Query {
	q, err := pql.NewParser(strings.NewReader(s)).Parse()
	if err != nil {
		panic(err)
	}
	return q
}
