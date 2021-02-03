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

package topology

import (
	"sort"
)

// Noder is an interface which abstracts the Node slice so that the list of
// nodes in a cluster can be maintained outside of the cluster struct.
type Noder interface {
	Nodes() []*Node // Remember: this has to be sorted correctly!!
	PrimaryNodeID(hasher Hasher) string
	SetNodes([]*Node)
	AppendNode(*Node)
	RemoveNode(nodeID string) bool
}

// localNoder is a simple implementation of the Noder interface
// which maintains an instance of the `nodes` slice.
type localNoder struct {
	nodes []*Node
}

// NewLocalNoder is a helper function for wrapping an existing slice of Nodes
// with something which implements Noder.
func NewLocalNoder(nodes []*Node) *localNoder {
	return &localNoder{
		nodes: nodes,
	}
}

// NewEmptyLocalNoder is an empty Noder used for testing.
func NewEmptyLocalNoder() *localNoder {
	return &localNoder{}
}

// Nodes implements the Noder interface.
func (n *localNoder) Nodes() []*Node {
	return n.nodes
}

// PrimaryNodeID implements the Noder interface.
func (n *localNoder) PrimaryNodeID(hasher Hasher) string {
	snap := NewClusterSnapshot(NewLocalNoder(n.nodes), hasher, 1)
	primaryNode := snap.PrimaryFieldTranslationNode()
	if primaryNode == nil {
		return ""
	}
	return primaryNode.ID
}

// SetNodes implements the Noder interface.
func (n *localNoder) SetNodes(nodes []*Node) {
	n.nodes = nodes
}

// AppendNode implements the Noder interface.
func (n *localNoder) AppendNode(node *Node) {
	n.nodes = append(n.nodes, node)

	// All hosts must be merged in the same order on all nodes in the cluster.
	sort.Sort(ByID(n.nodes))
}

// RemoveNode implements the Noder interface.
func (n *localNoder) RemoveNode(nodeID string) bool {
	i := NodePositionByID(n.nodes, nodeID)
	if i < 0 {
		return false
	}

	copy(n.nodes[i:], n.nodes[i+1:])
	n.nodes[len(n.nodes)-1] = nil
	n.nodes = n.nodes[:len(n.nodes)-1]

	return true
}
