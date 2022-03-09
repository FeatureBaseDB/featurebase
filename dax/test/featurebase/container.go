package featurebase

import (
	"fmt"

	"github.com/molecula/featurebase/v3/dax/test/docker"
)

var ImageName = docker.Getenv("FEATUREBASE_DOCKER_IMAGE", "dax/featurebase-test")

const (
	DataDir     = "/data"
	NetworkName = "featurebase-network"

	HTTPPort            = "8080"
	GRPCPort            = "20101"
	AdvertisePeerAddr   = "2379"
	AdvertiseClientAddr = "2380"
)

// Ensure type implements interface.
var _ docker.Container = &Container{}

type Container struct {
	name    string
	replica int
	cmd     []string
	env     map[string]string
}

func NewContainer(name string, env map[string]string) *Container {
	// peers is just "self" because we don't want to use etcd as a cluster in
	// the case of dumb compute nodes.
	// peers := fmt.Sprintf("%s=http://%s:%s", name, name, AdvertisePeerAddr)

	c := &Container{
		name:    name,
		replica: 1,
		cmd: []string{
			"/featurebase",
			"-test.run=TestRunMain",
			fmt.Sprintf("-test.coverprofile=/results/coverage-%s.out", name),
			"dax",
		},
		env: map[string]string{
			"FEATUREBASE_BIND":      "0.0.0.0:" + HTTPPort,
			"FEATUREBASE_ADVERTISE": name + ":" + HTTPPort,
		},
	}

	// Apply given env vars.
	for k, v := range env {
		c.env[k] = v
	}

	return c
}

func (c *Container) Hostname() string {
	return c.name
}

func (c *Container) ExposedPorts() []string {
	// We don't expose HTTPPort, because it's already exposed by default by
	// featurebase docker image.
	return []string{GRPCPort, AdvertisePeerAddr, AdvertiseClientAddr}
}

func (c *Container) Cmd() []string {
	return c.cmd
}

func (c *Container) Env() map[string]string {
	return c.env
}
