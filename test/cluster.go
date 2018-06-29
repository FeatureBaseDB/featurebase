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
	"fmt"
	"io/ioutil"

	"github.com/pilosa/pilosa"
)

// NewCluster returns a cluster with n nodes and uses a mod-based hasher.
func NewCluster(n int) *pilosa.Cluster {
	path, err := ioutil.TempDir("", "pilosa-cluster-")
	if err != nil {
		panic(err)
	}

	c := pilosa.NewCluster()
	c.ReplicaN = 1
	c.Hasher = &modHasher{}
	c.Path = path
	c.Topology = pilosa.NewTopology()

	for i := 0; i < n; i++ {
		c.Nodes = append(c.Nodes, &pilosa.Node{
			ID:  fmt.Sprintf("node%d", i),
			URI: newURI("http", fmt.Sprintf("host%d", i), uint16(0)),
		})
	}

	c.Node = c.Nodes[0]
	c.Coordinator = c.Nodes[0].ID
	c.SetState(pilosa.ClusterStateNormal)

	return c
}

// newURI is a test URI creator that intentionally swallows errors.
func newURI(scheme, host string, port uint16) pilosa.URI {
	uri := pilosa.DefaultURI()
	uri.SetScheme(scheme)
	uri.SetHost(host)
	uri.SetPort(port)
	return *uri
}

// modHasher represents a simple, mod-based hashing.
type modHasher struct{}

func (*modHasher) Hash(key uint64, n int) int { return int(key) % n }
