package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/stretchr/testify/require"
)

const (
	nodeAddr  = "myaddress"
	nodeAddr2 = "myaddress2"
	nodeAddr3 = "myaddress3"
	nodeAddr4 = "myaddress4"
	nodeAddr5 = "myaddress5"
)

func TestNodeService(t *testing.T) {
	// TODO: currently you must start w/ a clean test database
	// soda drop -e test; soda create -e test; soda migrate -e test
	trans, err := sqldb.Connect(sqldb.GetTestConfig())
	require.NoError(t, err, "connecting")

	tx, err := trans.BeginTx(context.Background(), true)
	require.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	nodeSvc := sqldb.NewNodeService(nil)

	err = nodeSvc.CreateNode(tx, dax.Address(""), &dax.Node{Address: nodeAddr, RoleTypes: []dax.RoleType{"compute"}})
	require.NoError(t, err)

	node, err := nodeSvc.ReadNode(tx, nodeAddr)
	require.NoError(t, err)
	require.EqualValues(t, nodeAddr, node.Address)
	require.EqualValues(t, 1, len(node.RoleTypes))
	require.EqualValues(t, "compute", node.RoleTypes[0])

	err = nodeSvc.CreateNode(tx, dax.Address(""), &dax.Node{Address: nodeAddr2, RoleTypes: []dax.RoleType{"translate", "compute"}})
	require.NoError(t, err, "create node 2")

	err = nodeSvc.CreateNode(tx, dax.Address(""), &dax.Node{Address: nodeAddr3, RoleTypes: []dax.RoleType{"compute"}})
	require.NoError(t, err, "create node 3")

	nodes, err := nodeSvc.Nodes(tx)
	require.NoError(t, err)
	require.EqualValues(t, 3, len(nodes))
	for _, node := range nodes {
		require.Contains(t, node.RoleTypes, dax.RoleType("compute"), "node should have compute role but is: %+v", node)
	}

	err = nodeSvc.DeleteNode(tx, nodeAddr2)
	require.NoError(t, err, "deleting node")

	nodes, err = nodeSvc.Nodes(tx)
	require.NoError(t, err)
	require.EqualValues(t, 2, len(nodes))
	for _, node := range nodes {
		require.Contains(t, node.RoleTypes, dax.RoleType("compute"), "node should have compute role but is: %+v", node)
	}
}
