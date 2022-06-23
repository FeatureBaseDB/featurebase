// Copyright 2021 Molecula Corp. All rights reserved.
package disco

import (
	"context"
	"sort"
)

// Noder is an interface which abstracts the Node slice so that the list of
// nodes in a cluster can be maintained outside of the cluster struct.
type Noder interface {
	Nodes() []*Node // Remember: this has to be sorted correctly!!
	PrimaryNodeID(hasher Hasher) string

	// SetMetadata records the local node's metadata.
	SetMetadata(ctx context.Context, node *Node) error

	// SetState changes a node to a given state.
	SetState(ctx context.Context, state NodeState) error

	// ClusterState considers the state of all nodes and gives
	// a general cluster state. The output calculation is as follows:
	// - If any of the nodes are still starting: "STARTING"
	// - If all nodes are up and running: "NORMAL"
	// - If number of DOWN nodes is lower than number of replicas: "DEGRADED"
	// - If number of unresponsive nodes is greater than (or equal to) the number of replicas: "DOWN"
	ClusterState(context.Context) (ClusterState, error)
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

// ClusterState is a no-op implementation of the Stator ClusterState method.
func (n *localNoder) ClusterState(context.Context) (ClusterState, error) {
	return ClusterStateUnknown, nil
}

func (n *localNoder) SetState(ctx context.Context, state NodeState) error {
	return nil
}

// localNoder doesn't really implement metadata support.
func (*localNoder) SetMetadata(context.Context, *Node) error {
	return nil
}
