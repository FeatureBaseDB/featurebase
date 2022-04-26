// Copyright 2021 Molecula Corp. All rights reserved.
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

// NewIDNoder is a helper function for wrapping an existing slice of Node IDs
// with something which implements Noder.
func NewIDNoder(ids []string) *localNoder {
	nodes := make([]*Node, len(ids))
	for i, id := range ids {
		node := &Node{
			ID: id,
		}
		nodes[i] = node
	}

	// Nodes must be sorted.
	sort.Sort(ByID(nodes))

	return &localNoder{
		nodes: nodes,
	}
}

// Nodes implements the Noder interface.
func (n *localNoder) Nodes() []*Node {
	return n.nodes
}

// PrimaryNodeID implements the Noder interface.
func (n *localNoder) PrimaryNodeID(hasher Hasher) string {
	snap := NewClusterSnapshot(NewLocalNoder(n.nodes), hasher, "jmp-hash", 1)
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
