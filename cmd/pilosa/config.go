package main

import (
	"time"

	"github.com/umbel/pilosa"
)

const (
	// DefaultHost is the default hostname and port to use.
	DefaultHost = "localhost:15000"
)

// Config represents the configuration for the command.
type Config struct {
	Host string `toml:"host"`

	Cluster struct {
		ReplicaN int           `toml:"replicas"`
		Nodes    []*ConfigNode `toml:"nodes"`
	} `toml:"cluster"`

	Plugins struct {
		Path string `toml:"path"`
	} `toml:"plugins"`
}

type ConfigNode struct {
	Host string `toml:"host"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		Host: DefaultHost,
	}
	c.Cluster.ReplicaN = pilosa.DefaultReplicaN
	c.Cluster.Nodes = []*ConfigNode{{Host: DefaultHost}}
	return c
}

// PilosaCluster returns a new instance of pilosa.Cluster based on the config.
func (c *Config) PilosaCluster() *pilosa.Cluster {
	cluster := pilosa.NewCluster()
	cluster.ReplicaN = c.Cluster.ReplicaN

	for _, n := range c.Cluster.Nodes {
		cluster.Nodes = append(cluster.Nodes, &pilosa.Node{Host: n.Host})
	}

	return cluster
}

// Duration is a TOML wrapper type for time.Duration.
type Duration time.Duration

// String returns the string representation of the duration.
func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalText parses a TOML value into a duration value.
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}

	*d = Duration(v)
	return nil
}
