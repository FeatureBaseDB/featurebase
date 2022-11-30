package docker

import (
	"context"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

// DefaultClient is the default docker Client
var DefaultClient *client.Client

func init() {
	cli, err := client.NewClientWithOpts()
	if err != nil {
		panic(err)
	}
	if err := client.FromEnv(cli); err != nil {
		panic(err)
	}
	cli.RegistryLogin(context.Background(), types.AuthConfig{})

	DefaultClient = cli
}

func Getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func Setenv(key, value string) error {
	return os.Setenv(key, value)
}
