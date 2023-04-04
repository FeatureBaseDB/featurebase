package sqldb_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/require"
)

func TestDirectiveVersion(t *testing.T) {
	t.Run("GetAndSet", func(t *testing.T) {
		trans, err := sqldb.NewTransactor(sqldb.GetTestConfigRandomDB("directive_version"), logger.StderrLogger) // TODO running migrations takes kind of a long time, consolidate w/ other SQL tests
		require.NoError(t, err, "connecting")
		require.NoError(t, trans.Start())

		tx, err := trans.BeginTx(context.Background(), true)
		require.NoError(t, err, "getting transaction")

		defer func() {
			err := tx.Rollback()
			if err != nil {
				t.Logf("rolling back: %v", err)
			}
		}()

		addr := dax.Address("address1")

		dvSvc := sqldb.NewDirectiveVersion(nil)

		// Get the current version; this returns 0 because a record for addr
		// didn't exist and so it was created.
		n, err := dvSvc.GetCurrent(tx, addr)
		require.NoError(t, err)
		require.Equal(t, uint64(0), n)

		// Set next version to n+1 = 1.
		require.NoError(t, dvSvc.SetNext(tx, addr, n, n+1))

		// Get the version again and make sure we get the 1 that was set.
		n, err = dvSvc.GetCurrent(tx, addr)
		require.NoError(t, err)
		require.Equal(t, uint64(1), n)

		// Try to set next version with an incorrect current version (999) and
		// ensure we get an error.
		require.Error(t, dvSvc.SetNext(tx, addr, 999, n+1))
	})
}
