package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	nodeAddr  = "myaddress"
	nodeAddr2 = "myaddress2"
	nodeAddr3 = "myaddress3"
	nodeAddr4 = "myaddress4"
	nodeAddr5 = "myaddress5"
)

func TestWorkerRegistry(t *testing.T) {
	tx, err := SQLTransactor.BeginTx(context.Background(), true)
	require.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	workerReg := sqldb.NewWorkerRegistry(nil)

	err = workerReg.AddWorker(tx, &dax.Node{Address: nodeAddr, RoleTypes: []dax.RoleType{dax.RoleTypeCompute}})
	require.NoError(t, err)

	node, err := workerReg.Worker(tx, nodeAddr)
	require.NoError(t, err)
	require.EqualValues(t, nodeAddr, node.Address)
	require.EqualValues(t, 1, len(node.RoleTypes))
	require.EqualValues(t, "compute", node.RoleTypes[0])

	err = workerReg.AddWorker(tx, &dax.Node{Address: nodeAddr2, RoleTypes: []dax.RoleType{dax.RoleTypeTranslate, dax.RoleTypeCompute}})
	require.NoError(t, err, "create node 2")

	err = workerReg.AddWorker(tx, &dax.Node{Address: nodeAddr3, RoleTypes: []dax.RoleType{dax.RoleTypeCompute}})
	require.NoError(t, err, "create node 3")

	nodes, err := workerReg.Workers(tx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, len(nodes))
	for _, node := range nodes {
		assert.Contains(t, node.RoleTypes, dax.RoleType("compute"), "node should have compute role but is: %+v", node)
	}

	err = workerReg.RemoveWorker(tx, nodeAddr2)
	require.NoError(t, err, "deleting node")

	nodes, err = workerReg.Workers(tx)
	require.NoError(t, err)
	require.EqualValues(t, 2, len(nodes))
	for _, node := range nodes {
		require.Contains(t, node.RoleTypes, dax.RoleType("compute"), "node should have compute role but is: %+v", node)
	}
}
