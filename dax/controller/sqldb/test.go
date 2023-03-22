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
		Database: EnvOr("FEATUREBASE_CONTROLLER_CONFIG_SQLDB_DATABASE", "dax_test"),
		Host:     EnvOr("FEATUREBASE_CONTROLLER_CONFIG_SQLDB_HOST", "127.0.0.1"),
		Port:     EnvOr("FEATUREBASE_CONTROLLER_CONFIG_SQLDB_PORT", "5432"),
		User:     EnvOr("FEATUREBASE_CONTROLLER_CONFIG_SQLDB_USER", "postgres"),
		Password: EnvOr("FEATUREBASE_CONTROLLER_CONFIG_SQLDB_PASSWORD", "testpass"),
	}
}

func GetTestConfigRandomDB(dbprefix string) *controller.SQLDBConfig {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &controller.SQLDBConfig{
		Dialect:  "postgres",
		Database: fmt.Sprintf("%s_%d", dbprefix, rnd.Int()),
		Host:     EnvOr("FEATUREBASE_CONTROLLER_CONFIG_SQLDB_HOST", "127.0.0.1"),
		Port:     EnvOr("FEATUREBASE_CONTROLLER_CONFIG_SQLDB_PORT", "5432"),
		User:     EnvOr("FEATUREBASE_CONTROLLER_CONFIG_SQLDB_USER", "postgres"),
		Password: EnvOr("FEATUREBASE_CONTROLLER_CONFIG_SQLDB_PASSWORD", "testpass"),
	}
}
