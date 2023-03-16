package sqldb

import (
	"os"

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
		Database: EnvOr("POSTGRES_DB", "dax_test"),
		Host:     EnvOr("POSTGRES_HOST", "127.0.0.1"),
		Port:     EnvOr("POSTGRES_PORT", "5432"),
		User:     EnvOr("POSTGRES_USER", "postgres"),
		Password: EnvOr("POSTGRES_PASSWORD", "testpass"),
	}
}
