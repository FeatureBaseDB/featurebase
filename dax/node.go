package dax

import (
	"context"
	"fmt"
	"strings"

	"github.com/featurebasedb/featurebase/v3/errors"
)

// Node is used in API requests, like RegisterNode (before being assigned
// roles).
type Node struct {
	Address Address `json:"address"`

	// RoleTypes allows a registering node to specify which role type(s) it is
	// capable of filling. The controller will not assign a role to this node
	// with a type not included in RoleTypes.
	RoleTypes []RoleType `json:"role-types"`

	// HasDirective will be true when the node has received at least one
	// directive from the controller. This can be used to instruct the
	// controller that it should send a directive regardless of whether it
	// already knows about this node. This can happen in an on-prem, serverless
	// setup (when the controller and computer are both running in the same
	// process) and the node is restarted. In that case, the controller comes
	// up, reads the meta data, and assumes that the local computer registering
	// with it has already registered. But we really want the controller to
	// treat this as a new node registration so the computer can load data from
	// snapshotter/writelogger.
	HasDirective bool `json:"has-directive"`
}

// Nodes is a slice of *Node. It's useful for printing the nodes as a list of
// node.Addresses via its String() method.
type Nodes []*Node

// String prints the slice of node addresses in Nodes.
func (n Nodes) String() string {
	out := make([]string, 0, len(n))
	for i := range n {
		out = append(out, string(n[i].Address))
	}
	return "[" + strings.Join(out, ",") + "]"
}

// AssignedNode represents a Worker which has been assigned a role. Note that
// the worker which it represents might be responsible for multiple roles, but
// AssignedNode only ever represents one of those roles at a time. This is
// because it is always the response of a RoleType-specific request.
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

// Ensure type implements interface.
var _ NodeService = &nopNodeService{}

// nopNoder is a no-op implementation of the Noder interface.
type nopNodeService struct{}

func NewNopNodeService() *nopNodeService {
	return &nopNodeService{}
}

func (n *nopNodeService) CreateNode(context.Context, Address, *Node) error {
	return nil
}
func (n *nopNodeService) ReadNode(context.Context, Address) (*Node, error) {
	return nil, nil
}
func (n *nopNodeService) DeleteNode(context.Context, Address) error {
	return nil
}
func (n *nopNodeService) Nodes(context.Context) ([]*Node, error) {
	return []*Node{}, nil
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

// nopNoder is a no-op implementation of the Noder interface.
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
