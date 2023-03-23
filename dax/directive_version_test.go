package dax_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/require"
)

func TestDirectiveVersion(t *testing.T) {
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

	dvSvc := sqldb.NewDirectiveVersion(nil)

	n, err := dvSvc.Increment(tx, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), n)
}
