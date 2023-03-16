package dax_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/stretchr/testify/require"
)

func TestDirectiveVersion(t *testing.T) {
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

	dvSvc := sqldb.NewDirectiveVersion(nil)

	n, err := dvSvc.Increment(tx, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), n)
}
