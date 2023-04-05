package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/stretchr/testify/require"
)

func TestFreeWorkerService(t *testing.T) {
	tx, err := SQLTransactor.BeginTx(context.Background(), true)
	require.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	node1 := &dax.Node{Address: nodeAddr, RoleTypes: dax.AllRoleTypes}
	node2 := &dax.Node{Address: nodeAddr2, RoleTypes: dax.AllRoleTypes}
	node3 := &dax.Node{Address: nodeAddr3, RoleTypes: dax.AllRoleTypes}
	node4 := &dax.Node{Address: nodeAddr4, RoleTypes: dax.AllRoleTypes}
	node5 := &dax.Node{Address: nodeAddr5, RoleTypes: dax.AllRoleTypes}

	workerReg := sqldb.NewWorkerRegistry(nil)

	// Add some workers.
	require.NoError(t, workerReg.AddWorker(tx, node1))
	require.NoError(t, workerReg.AddWorker(tx, node2))
	require.NoError(t, workerReg.AddWorker(tx, node3))
	require.NoError(t, workerReg.AddWorker(tx, node4))
	require.NoError(t, workerReg.AddWorker(tx, node5))

	// Remove one of the workers.
	require.NoError(t, workerReg.RemoveWorker(tx, node2.Address))

	fwSvc := sqldb.NewFreeWorkerService(nil)

	addrs, err := fwSvc.ListWorkers(tx, role)
	require.NoError(t, err)
	require.ElementsMatch(t, dax.Addresses{nodeAddr, nodeAddr3, nodeAddr4, nodeAddr5}, addrs)

	addrs, err = fwSvc.PopWorkers(tx, role, 2)
	require.NoError(t, err)
	require.Equal(t, 2, len(addrs))
	require.NotEqual(t, addrs[0], addrs[1])
}
