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
