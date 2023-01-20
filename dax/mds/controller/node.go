package controller

import (
	"github.com/featurebasedb/featurebase/v3/dax"
)

// NodeService represents a service for managing Nodes.
type NodeService interface {
	CreateNode(dax.Transaction, dax.Address, *dax.Node) error
	ReadNode(dax.Transaction, dax.Address) (*dax.Node, error)
	DeleteNode(dax.Transaction, dax.Address) error
	Nodes(dax.Transaction) ([]*dax.Node, error)
}

// Ensure type implements interface.
var _ NodeService = &nopNodeService{}

// nopNoder is a no-op implementation of the Noder interface.
type nopNodeService struct{}

func NewNopNodeService() *nopNodeService {
	return &nopNodeService{}
}

func (n *nopNodeService) CreateNode(dax.Transaction, dax.Address, *dax.Node) error {
	return nil
}
func (n *nopNodeService) ReadNode(dax.Transaction, dax.Address) (*dax.Node, error) {
	return nil, nil
}
func (n *nopNodeService) DeleteNode(dax.Transaction, dax.Address) error {
	return nil
}
func (n *nopNodeService) Nodes(dax.Transaction) ([]*dax.Node, error) {
	return []*dax.Node{}, nil
}
