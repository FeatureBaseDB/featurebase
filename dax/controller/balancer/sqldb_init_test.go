package balancer_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/featurebasedb/featurebase/v3/logger"
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// SQLTransactor is a global connection to a SQL database which is
// created, migrated, and destroyed for each test run. The database
// gets a randomized name and is used by all the tests in this
// package.
var SQLTransactor sqldb.Transactor

// run is a separate function so that we can defer cleanups. (deferred functions won't run if os.Exit is called)
func run(m *testing.M) int {
	// We connect to a randomized database, create it, and run migrations. Then we drop it when tests are done.
	conf := sqldb.GetTestConfigRandomDB("balancer_test")
	var err error
	SQLTransactor, err = sqldb.Connect(conf, logger.StderrLogger)
	if err != nil {
		fmt.Printf("couldn't make connection to database: %v", err)
		return -1
	}

	defer sqldb.DropDatabase(SQLTransactor)
	code := m.Run()
	return code
}
