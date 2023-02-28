package sqldb

import (
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/pkg/errors"
)

var _ controller.NodeService = (*NodeService)(nil)

type NodeService struct {
}

func (n *NodeService) CreateNode(tx dax.Transaction, addr dax.Address, node *dax.Node) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	mnode := &models.Node{Address: node.Address}
	err := dt.C.Create(mnode)
	if err != nil {
		return errors.Wrap(err, "creating node")
	}

	nodeRoles := make(models.NodeRoles, len(node.RoleTypes))
	mnode.NodeRoles = nodeRoles
	for i, rt := range node.RoleTypes {
		mnode.NodeRoles[i] = models.NodeRole{
			NodeID: mnode.ID,
			Role:   rt,
		}
	}
	err = dt.C.Create(&(mnode.NodeRoles))
	if err != nil {
		return errors.Wrap(err, "creating node roles")
	}

	return nil
}

func (n *NodeService) ReadNode(tx dax.Transaction, addr dax.Address) (*dax.Node, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction()
	}

	node := &models.Node{}
	err := dt.C.Eager().Where("address = ?", addr).First(node)
	if err != nil {
		return nil, errors.Wrap(err, "getting node")
	}

	roleTypes := make([]dax.RoleType, len(node.NodeRoles))
	for i, nr := range node.NodeRoles {
		roleTypes[i] = nr.Role
	}

	return &dax.Node{
		Address:   node.Address,
		RoleTypes: roleTypes,
	}, nil
}

func (n *NodeService) DeleteNode(tx dax.Transaction, addr dax.Address) error {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return dax.NewErrInvalidTransaction()
	}

	node := &models.Node{}
	err := dt.C.Eager().Where("address = ?", addr).First(node)
	if err != nil {
		return errors.Wrap(err, "getting node")
	}
	err = dt.C.Destroy(node)
	return errors.Wrap(err, "destroying node")
}

func (n *NodeService) Nodes(tx dax.Transaction) ([]*dax.Node, error) {
	dt, ok := tx.(*DaxTransaction)
	if !ok {
		return nil, dax.NewErrInvalidTransaction()
	}

	nodes := []*models.Node{}
	dt.C.Eager().All(&nodes)

	ret := make([]*dax.Node, len(nodes))
	for i, node := range nodes {
		ret[i] = &dax.Node{
			Address:   node.Address,
			RoleTypes: make([]dax.RoleType, len(node.NodeRoles)),
		}
		for j, nr := range node.NodeRoles {
			ret[i].RoleTypes[j] = nr.Role
		}
	}

	return ret, nil
}
