// Copyright 2021 Molecula Corp. All rights reserved.
package disco

import (
	"fmt"

	"github.com/molecula/featurebase/v3/net"
)

// Node represents a node in the cluster.
type Node struct {
	ID        string    `json:"id"`
	URI       net.URI   `json:"uri"`
	GRPCURI   net.URI   `json:"grpc-uri"`
	IsPrimary bool      `json:"isPrimary"`
	State     NodeState `json:"state"`
}

func (n *Node) Clone() *Node {
	if n == nil {
		return nil
	}
	var other Node
	other.ID = n.ID
	other.URI = n.URI
	other.GRPCURI = n.GRPCURI
	other.IsPrimary = n.IsPrimary
	other.State = n.State
	return &other
}

func (n *Node) String() string {
	return fmt.Sprintf("Node:%s:%s:%s(%v)", n.URI, n.State, n.ID, n.IsPrimary)
}

// Nodes represents a list of nodes.
type Nodes []*Node

// Contains returns true if a node exists in the list.
func (a Nodes) Contains(n *Node) bool {
	for i := range a {
		if a[i] == n {
			return true
		}
	}
	return false
}

// ContainsID returns true if host matches one of the node's id.
func (a Nodes) ContainsID(id string) bool {
	for _, n := range a {
		if n.ID == id {
			return true
		}
	}
	return false
}

// NodeByID returns the node for an ID. If the ID is not found,
// it returns nil.
func (a Nodes) NodeByID(id string) *Node {
	for _, n := range a {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// Filter returns a new list of nodes with node removed.
func (a Nodes) Filter(n *Node) []*Node {
	other := make([]*Node, 0, len(a))
	for i := range a {
		if a[i] != n {
			other = append(other, a[i])
		}
	}
	return other
}

// FilterID returns a new list of nodes with ID removed.
func (a Nodes) FilterID(id string) []*Node {
	other := make([]*Node, 0, len(a))
	for _, node := range a {
		if node.ID != id {
			other = append(other, node)
		}
	}
	return other
}

// FilterURI returns a new list of nodes with URI removed.
func (a Nodes) FilterURI(uri net.URI) []*Node {
	other := make([]*Node, 0, len(a))
	for _, node := range a {
		if node.URI != uri {
			other = append(other, node)
		}
	}
	return other
}

// IDs returns a list of all node IDs.
func (a Nodes) IDs() []string {
	ids := make([]string, len(a))
	for i, n := range a {
		ids[i] = n.ID
	}
	return ids
}

// URIs returns a list of all uris.
func (a Nodes) URIs() []net.URI {
	uris := make([]net.URI, len(a))
	for i, n := range a {
		uris[i] = n.URI
	}
	return uris
}

// Clone returns a shallow copy of nodes.
func (a Nodes) Clone() []*Node {
	other := make([]*Node, len(a))
	copy(other, a)
	return other
}

// ByID implements sort.Interface for []Node based on
// the ID field.
type ByID []*Node

func (h ByID) Len() int           { return len(h) }
func (h ByID) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h ByID) Less(i, j int) bool { return h[i].ID < h[j].ID }
