// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import "time"

// Cluster types.
const (
	ClusterNone   = ""
	ClusterStatic = "static"
	ClusterGossip = "gossip"
)

const (
	// DefaultHost is the default hostname to use.
	DefaultHost = "localhost"

	// DefaultPort is the default port use with the hostname.
	DefaultPort = "10101"

	// DefaultClusterType sets the node intercommunication method.
	DefaultClusterType = ClusterGossip

	// DefaultGossipPort indicates the port to which pilosa should bind for internal state sharing.
	DefaultGossipPort = "14000"

	// DefaultMetrics sets the internal metrics to no-op.
	DefaultMetrics = "nop"

	// DefaultMaxWritesPerRequest is the default number of writes per request.
	DefaultMaxWritesPerRequest = 5000

	// DefaultMaxClientConnections is 0 which means no limit
	DefaultMaxClientConnections = 0
)

// ClusterTypes set of cluster types.
var ClusterTypes = []string{ClusterNone, ClusterStatic, ClusterGossip}

// Config represents the configuration for the command.
type Config struct {
	DataDir    string `toml:"data-dir"`
	Bind       string `toml:"bind"`
	GossipPort string `toml:"gossip-port"`
	GossipSeed string `toml:"gossip-seed"`

	Cluster struct {
		ReplicaN      int      `toml:"replicas"`
		Type          string   `toml:"type"`
		Hosts         []string `toml:"hosts"`
		PollInterval  Duration `toml:"poll-interval"`
		LongQueryTime Duration `toml:"long-query-time"`
	} `toml:"cluster"`

	Plugins struct {
		Path string `toml:"path"`
	} `toml:"plugins"`

	AntiEntropy struct {
		Interval Duration `toml:"interval"`
	} `toml:"anti-entropy"`

	// Limits the number of mutating commands that can be in a single request to
	// the server. This includes SetBit, ClearBit, SetRowAttrs & SetColumnAttrs.
	MaxWritesPerRequest int `toml:"max-writes-per-request"`

	LogPath string `toml:"log-path"`

	Metric struct {
		Service      string   `toml:"service"`
		Host         string   `toml:"host"`
		PollInterval Duration `toml:"poll-interval"`
	} `toml:"metric"`

	MaxClientConnections int `toml:"max-client-connections"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		Bind:                 DefaultHost + ":" + DefaultPort,
		MaxWritesPerRequest:  DefaultMaxWritesPerRequest,
		MaxClientConnections: DefaultMaxClientConnections,
	}
	c.Cluster.ReplicaN = DefaultReplicaN
	c.Cluster.Type = DefaultClusterType
	c.Cluster.PollInterval = Duration(DefaultPollingInterval)
	c.Cluster.Hosts = []string{}
	c.AntiEntropy.Interval = Duration(DefaultAntiEntropyInterval)
	c.Metric.Service = DefaultMetrics
	return c
}

// Validate that all configuration permutations are compatible with each other.
func (c *Config) Validate() error {
	if !StringInSlice(c.Cluster.Type, ClusterTypes) {
		return ErrConfigClusterTypeInvalid
	}

	if c.Cluster.Type == ClusterGossip {
		if len(c.Cluster.Hosts) > 0 {
			bindWithDefaults, err := AddressWithDefaults(c.Bind)
			if err != nil {
				return err
			}
			if !foundItem(c.Cluster.Hosts, bindWithDefaults) {
				return ErrConfigHostsMissing
			}
		}
	}

	return nil
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

// MarshalText writes duration value in text format.
func (d Duration) MarshalText() (text []byte, err error) {
	return []byte(d.String()), nil
}

// MarshalTOML write duration into valid TOML.
func (d Duration) MarshalTOML() ([]byte, error) {
	return []byte(d.String()), nil
}
