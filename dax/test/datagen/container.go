package datagen

import (
	"strconv"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/test/docker"
)

var ImageName = docker.Getenv("DATAGEN_DOCKER_IMAGE", "dax/datagen")

var (
	Seed                      = "123456"
	Target                    = "mds"
	Source                    = "custom"
	StartFrom                 = 1
	EndAt                     = 1000
	BatchSize                 = 100
	PilosaIndex               = ""
	FeatureBaseOrganizationID = ""
	FeatureBaseDatabaseID     = ""
	FeatureBaseTableName      = ""

	MDSAddress = "mds:8080"
)

const (
	NetworkName = "datagen-network"
)

type Option func(*Container)

func WithEndAt(endAt int) Option {
	return func(c *Container) {
		c.endAt = endAt
	}
}

type Container struct {
	name       string
	mdsAddress dax.Address
	endAt      int
}

func NewContainer(name string, mdsAddress dax.Address) *Container {
	return &Container{
		name:       name,
		mdsAddress: mdsAddress,
	}
}

func (c *Container) Hostname() string {
	return c.name
}

func (c *Container) Env() map[string]string {
	env := map[string]string{
		"GEN_MDS_ADDRESS":            c.mdsAddress.String(),
		"GEN_SEED":                   docker.Getenv("GEN_SEED", Seed),
		"GEN_TARGET":                 Target,
		"GEN_SOURCE":                 Source,
		"GEN_START_FROM":             strconv.Itoa(StartFrom),
		"GEN_END_AT":                 strconv.Itoa(c.endAt),
		"GEN_PILOSA_BATCH_SIZE":      strconv.Itoa(BatchSize),
		"GEN_FEATUREBASE_ORG_ID":     docker.Getenv("GEN_FEATUREBASE_ORG_ID", FeatureBaseOrganizationID),
		"GEN_FEATUREBASE_DB_ID":      docker.Getenv("GEN_FEATUREBASE_DB_ID", FeatureBaseDatabaseID),
		"GEN_FEATUREBASE_TABLE_NAME": docker.Getenv("GEN_FEATUREBASE_TABLE_NAME", FeatureBaseTableName),
		"GEN_CUSTOM_CONFIG":          docker.Getenv("GEN_CUSTOM_CONFIG", ""),
	}

	return env
}

func (c *Container) Cmd() []string {
	return []string{"datagen"}
}

func (c *Container) ExposedPorts() []string {
	return []string{}
}
