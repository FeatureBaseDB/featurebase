package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/stretchr/testify/require"
)

func TestFreeWorkerService(t *testing.T) {
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

	fwSvc := sqldb.NewFreeWorkerService(nil)
	err = fwSvc.AddWorkers(tx, role, nodeAddr, nodeAddr2, nodeAddr3, nodeAddr4, nodeAddr5)
	require.NoError(t, err)

	err = fwSvc.RemoveWorker(tx, role, nodeAddr2)
	require.NoError(t, err)

	addrs, err := fwSvc.ListWorkers(tx, role)
	require.NoError(t, err)
	require.ElementsMatch(t, dax.Addresses{nodeAddr, nodeAddr3, nodeAddr4, nodeAddr5}, addrs)

	addrs, err = fwSvc.PopWorkers(tx, role, 2)
	require.NoError(t, err)
	require.Equal(t, 2, len(addrs))
	require.NotEqual(t, addrs[0], addrs[1])
}
