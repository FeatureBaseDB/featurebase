package dax

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/errors"
)

// Node is used in API requests, like RegisterNode (before being assigned
// roles).
type Node struct {
	Address Address `json:"address"`

	RoleTypes []RoleType `json:"role-types"`
}

// AssignedNode is used in API responses.
type AssignedNode struct {
	Address Address `json:"address"`
	Role    Role    `json:"role"`
}

// NodeService represents a service for managing Nodes.
type NodeService interface {
	CreateNode(context.Context, Address, *Node) error
	ReadNode(context.Context, Address) (*Node, error)
	DeleteNode(context.Context, Address) error
	Nodes(context.Context) ([]*Node, error)
}

// ComputeNode represents a compute node and the table/shards for which it is
// responsible.
type ComputeNode struct {
	Address Address   `json:"address"`
	Table   TableKey  `json:"table"`
	Shards  ShardNums `json:"shards"`
}

// TranslateNode represents a translate node and the table/partitions for which
// it is responsible.
type TranslateNode struct {
	Address    Address       `json:"address"`
	Table      TableKey      `json:"table"`
	Partitions PartitionNums `json:"partitions"`
}

type Noder interface {
	ComputeNodes(ctx context.Context, qtid QualifiedTableID, shards ...ShardNum) ([]ComputeNode, error)
	TranslateNodes(ctx context.Context, qtid QualifiedTableID, partitions ...PartitionNum) ([]TranslateNode, error)

	// IngestPartition is effectively the "write" version of TranslateNodes. Its
	// implementations will return the same Address that TranslateNodes would,
	// but it includes the logic to create/assign the partition if it is not
	// already being handled by a computer.
	IngestPartition(ctx context.Context, qtid QualifiedTableID, partition PartitionNum) (Address, error)

	// IngestShard is effectively the "write" version of ComputeNodes. Its
	// implementations will return the same Address that ComputeNodes would, but
	// it includes the logic to create/assign the shard if it is not already
	// being handled by a computer.
	IngestShard(ctx context.Context, qtid QualifiedTableID, shard ShardNum) (Address, error)
}

// Ensure type implements interface.
var _ Noder = &nopNoder{}

// NopMDS is a no-op implementation of the MDS interface.
type nopNoder struct{}

func NewNopNoder() *nopNoder {
	return &nopNoder{}
}

func (n *nopNoder) ComputeNodes(ctx context.Context, qtid QualifiedTableID, shards ...ShardNum) ([]ComputeNode, error) {
	return nil, nil
}
func (n *nopNoder) IngestPartition(ctx context.Context, qtid QualifiedTableID, partition PartitionNum) (Address, error) {
	return "", nil
}
func (n *nopNoder) IngestShard(ctx context.Context, qtid QualifiedTableID, shard ShardNum) (Address, error) {
	return "", nil
}
func (n *nopNoder) TranslateNodes(ctx context.Context, qtid QualifiedTableID, partitions ...PartitionNum) ([]TranslateNode, error) {
	return nil, nil
}

////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////

const (
	ErrNodeDoesNotExist errors.Code = "NodeDoesNotExist"
)

func NewErrNodeDoesNotExist(addr Address) error {
	return errors.New(
		ErrNodeDoesNotExist,
		fmt.Sprintf("node '%s' does not exist", addr),
	)
}
