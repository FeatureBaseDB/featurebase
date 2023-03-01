package balancer_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/gobuffalo/pop/v6"
	"github.com/stretchr/testify/assert"
)

func TestFreeWorkerService(t *testing.T) {
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

	fwSvc := &sqldb.FreeWorkerService{}
	err = fwSvc.AddWorkers(tx, role, nodeAddr, nodeAddr2, nodeAddr3, nodeAddr4, nodeAddr5)
	assert.NoError(t, err)

	err = fwSvc.RemoveWorker(tx, role, nodeAddr2)
	assert.NoError(t, err)

	addrs, err := fwSvc.ListWorkers(tx, role)
	assert.NoError(t, err)
	assert.ElementsMatch(t, dax.Addresses{nodeAddr, nodeAddr3, nodeAddr4, nodeAddr5}, addrs)

	addrs, err = fwSvc.PopWorkers(tx, role, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(addrs))
	assert.NotEqual(t, addrs[0], addrs[1])
}
