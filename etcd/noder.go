// Copyright 2021 Pilosa Corp.
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

package etcd

import (
	"context"
	"encoding/json"
	"log"
	"sort"

	"github.com/pilosa/pilosa/v2/topology"
)

var _ topology.Noder = &Noder{}

type Noder struct {
	*EtcdWithCache
}

func NewNoder(opt Options, replicas int) *Noder {
	return &Noder{
		EtcdWithCache: NewEtcdWithCache(opt, replicas),
	}
}

// Nodes implements the Noder interface.
func (n *Noder) Nodes() []*topology.Node {
	// If we have looked up nodes within a certain time, then we're going to
	// use the cached value for now. This is temporary and will be addressed
	// correctly in #1133.
	peers := n.Peers()
	nodes := make([]*topology.Node, len(peers))
	for i, peer := range peers {
		node := &topology.Node{}
		if meta, err := n.Metadata(context.Background(), peer.ID); err != nil {
			log.Println(err, "getting metadata") // TODO: handle this with a logger
		} else if err := json.Unmarshal(meta, node); err != nil {
			log.Println(err, "unmarshaling json metadata")
		}

		node.ID = peer.ID

		nodes[i] = node
	}

	// Nodes must be sorted.
	sort.Sort(topology.ByID(nodes))

	return nodes
}

// SetNodes implements the Noder interface as NOP
// (because we can't force to set nodes for etcd).
func (n *Noder) SetNodes(nodes []*topology.Node) {}

// AppendNode implements the Noder interface as NOP
// (because resizer is responsible for adding new nodes).
func (n *Noder) AppendNode(node *topology.Node) {}

// RemoveNode implements the Noder interface as NOP
// (because resizer is responsible for removing existing nodes)
func (n *Noder) RemoveNode(nodeID string) bool {
	return false
}
