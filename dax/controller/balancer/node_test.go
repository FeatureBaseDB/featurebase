package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/gobuffalo/pop/v6"
	"github.com/stretchr/testify/assert"
)

const (
	nodeAddr  = "myaddress"
	nodeAddr2 = "myaddress2"
	nodeAddr3 = "myaddress3"
)

func TestNodeService(t *testing.T) {
	// TODO: currently you must start w/ a clean test database
	// soda drop -e test; soda create -e test; soda migrate -e test
	conn, err := pop.Connect("test")
	assert.NoError(t, err, "connecting")

	trans := sqldb.Transactor{Connection: conn}

	tx, err := trans.BeginTx(context.Background(), true)
	assert.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	nodeSvc := &sqldb.NodeService{}

	err = nodeSvc.CreateNode(tx, dax.Address(""), &dax.Node{Address: nodeAddr, RoleTypes: []dax.RoleType{"compute"}})
	assert.NoError(t, err)

	node, err := nodeSvc.ReadNode(tx, nodeAddr)
	assert.NoError(t, err)
	assert.EqualValues(t, nodeAddr, node.Address)
	assert.EqualValues(t, 1, len(node.RoleTypes))
	assert.EqualValues(t, "compute", node.RoleTypes[0])

	err = nodeSvc.CreateNode(tx, dax.Address(""), &dax.Node{Address: nodeAddr2, RoleTypes: []dax.RoleType{"translate", "compute"}})
	assert.NoError(t, err, "create node 2")

	err = nodeSvc.CreateNode(tx, dax.Address(""), &dax.Node{Address: nodeAddr3, RoleTypes: []dax.RoleType{"compute"}})
	assert.NoError(t, err, "create node 3")

	nodes, err := nodeSvc.Nodes(tx)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, len(nodes))
	for _, node := range nodes {
		assert.Contains(t, node.RoleTypes, dax.RoleType("compute"), "node should have compute role but is: %+v", node)
	}

	err = nodeSvc.DeleteNode(tx, nodeAddr2)
	assert.NoError(t, err, "deleting node")

	nodes, err = nodeSvc.Nodes(tx)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(nodes))
	for _, node := range nodes {
		assert.Contains(t, node.RoleTypes, dax.RoleType("compute"), "node should have compute role but is: %+v", node)
	}
}
