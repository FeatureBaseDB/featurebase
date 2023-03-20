package sqldb

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax/controller"
)

func EnvOr(envName, defaultVal string) string {
	val, ok := os.LookupEnv(envName)
	if !ok {
		return defaultVal
	}
	return val
}

func GetTestConfig() *controller.SQLDBConfig {
	return &controller.SQLDBConfig{
		Dialect:  "postgres",
		Database: EnvOr("SQLDB_DB", "dax_test"),
		Host:     EnvOr("SQLDB_HOST", "127.0.0.1"),
		Port:     EnvOr("SQLDB_PORT", "5432"),
		User:     EnvOr("SQLDB_USER", "postgres"),
		Password: EnvOr("SQLDB_PASSWORD", "testpass"),
	}
}

func GetTestConfigRandomDB(dbprefix string) *controller.SQLDBConfig {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &controller.SQLDBConfig{
		Dialect:  "postgres",
		Database: fmt.Sprintf("%s_%d", dbprefix, rnd.Int()),
		Host:     EnvOr("SQLDB_HOST", "127.0.0.1"),
		Port:     EnvOr("SQLDB_PORT", "5432"),
		User:     EnvOr("SQLDB_USER", "postgres"),
		Password: EnvOr("SQLDB_PASSWORD", "testpass"),
	}
}
