package dax_test

import (
	"context"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/require"
)

func TestTransaction(t *testing.T) {
	t.Run("retryWithTx", func(t *testing.T) {
		ctx := context.Background()
		log := logger.StderrLogger
		trans, err := sqldb.NewTransactor(sqldb.GetTestConfigRandomDB("retry_with_tx"), log) // TODO running migrations takes kind of a long time, consolidate w/ other SQL tests
		require.NoError(t, err, "connecting")
		require.NoError(t, trans.Start())

		orgID := dax.OrganizationID("acme")
		db := &dax.Database{
			ID:   "db1id",
			Name: "db1",
			Options: dax.DatabaseOptions{
				WorkersMin: 1,
			},
		}
		qdb := dax.NewQualifiedDatabase(orgID, db)
		qdbid := qdb.QualifiedID()
		schemar := sqldb.NewSchemar(log)

		// The purpose of this test is to ensure that we're enforcing repeatable
		// read isolation level on writes. It tests the RetryWithTx function by
		// retrying a write that fails and ensuring that it succeeds on the next
		// retry. It performs the following steps:
		//
		// tx1: create db1 with units 1
		// tx2: read db1 (units should be 1)
		//      wait... on chan "wait2"
		//      read db1 again (units should still be 1) << repeatableread
		//      set units to 2
		// tx3  set units to 3
		// tx4  read db1 (units should be 3)
		// close "wait2"
		// tx5  read db1 (units should be 2)

		wait2 := make(chan struct{})
		wait3 := make(chan struct{})
		done := make(chan struct{})

		// tx1
		tx1 := func(tx dax.Transaction, writable bool) error {
			dt, ok := tx.(*sqldb.DaxTransaction)
			require.True(t, ok)

			require.NoError(t, schemar.CreateDatabase(dt, qdb))

			return nil
		}
		require.NoError(t, dax.RetryWithTx(ctx, trans, tx1, true, 1))

		// tx2

		// tx2cnt tracks the number of times that tx2 has been called. We need
		// this because we expect it to read different values depending on which
		// call it's on. And we only want it to close channels the first time
		// through.
		var tx2cnt int

		// exp contains the values that we expect tx2 to read (for
		// "workers-min") on the respective call.
		exp := map[int]int{
			0: 1,
			1: 3,
		}
		tx2 := func(tx dax.Transaction, writable bool) error {
			dt, ok := tx.(*sqldb.DaxTransaction)
			require.True(t, ok)

			qdb, err := schemar.DatabaseByID(dt, qdbid)
			require.NoError(t, err)
			require.Equal(t, exp[tx2cnt], qdb.Options.WorkersMin)
			if tx2cnt == 0 {
				close(wait3)
			}

			// Wait until tx3 commits before trying to do anything else.
			<-wait2

			qdb, err = schemar.DatabaseByID(dt, qdbid)
			require.NoError(t, err)
			require.Equal(t, exp[tx2cnt], qdb.Options.WorkersMin)

			// Increment tx2cnt for the next time tx2 gets called.
			tx2cnt++

			return schemar.SetDatabaseOption(dt, qdbid, dax.DatabaseOptionWorkersMin, "2")
		}

		// Run the calls to tx2 in a go routine because we want to mimic
		// concurrent attempt to read/write the same data.
		go func() {
			require.NoError(t, dax.RetryWithTx(ctx, trans, tx2, true, 2))

			// After the second call of tx2 completes, close the done channel
			// so that tx5 can proceed and verify that tx2 eventually got to
			// commit its transaction.
			close(done)
		}()

		// Wait until tx2 does its first read of the data before allowing tx3 to
		// begin.
		select {
		case <-wait3:
		case <-time.After(10 * time.Second):
			t.Fatal("expected close of channel: wait3")
		}

		// tx3
		tx3 := func(tx dax.Transaction, writable bool) error {
			dt, ok := tx.(*sqldb.DaxTransaction)
			require.True(t, ok)

			qdb, err := schemar.DatabaseByID(dt, qdb.QualifiedID())
			require.NoError(t, err)
			require.Equal(t, 1, qdb.Options.WorkersMin)

			require.NoError(t, schemar.SetDatabaseOption(dt, qdbid, dax.DatabaseOptionWorkersMin, "3"))

			return nil
		}

		require.NoError(t, dax.RetryWithTx(ctx, trans, tx3, true, 1))

		// tx4
		tx4 := func(tx dax.Transaction, writable bool) error {
			dt, ok := tx.(*sqldb.DaxTransaction)
			require.True(t, ok)

			qdb, err := schemar.DatabaseByID(dt, qdb.QualifiedID())
			require.NoError(t, err)
			require.Equal(t, 3, qdb.Options.WorkersMin)

			return nil
		}

		require.NoError(t, dax.RetryWithTx(ctx, trans, tx4, false, 1))

		// Close wait2 so that tx2 can continue retrying transactions.
		close(wait2)

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("expected close of channel: done")
		}

		// tx5
		tx5 := func(tx dax.Transaction, writable bool) error {
			dt, ok := tx.(*sqldb.DaxTransaction)
			require.True(t, ok)

			qdb, err := schemar.DatabaseByID(dt, qdb.QualifiedID())
			require.NoError(t, err)
			require.Equal(t, 2, qdb.Options.WorkersMin)

			return nil
		}

		require.NoError(t, dax.RetryWithTx(ctx, trans, tx5, false, 1))
	})

}
