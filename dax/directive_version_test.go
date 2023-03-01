package dax_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/gobuffalo/pop/v6"
	"github.com/stretchr/testify/assert"
)

func TestDirectiveVersion(t *testing.T) {
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

	dvSvc := &sqldb.DirectiveVersion{}

	n, err := dvSvc.Increment(tx, 1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), n)
}
