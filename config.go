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
	ClusterHTTP   = "http"
	ClusterGossip = "gossip"
)

const (
	// DefaultHost is the default hostname to use.
	DefaultHost = "localhost"

	// DefaultPort is the default port use with the hostname.
	DefaultPort = "10101"

	// DefaultClusterType sets the node intercommunication method.
	DefaultClusterType = ClusterStatic

	// DefaultInternalPort the port the nodes intercommunicate on.
	DefaultInternalPort = "14000"

	// DefaultMetrics sets the internal metrics to no op
	DefaultMetrics = "nop"

	// DefaultMaxWritesPerRequest is the default number of writes per request.
	DefaultMaxWritesPerRequest = 5000
)

// ClusterTypes set of cluster types.
var ClusterTypes = []string{ClusterNone, ClusterStatic, ClusterHTTP, ClusterGossip}

// Config represents the configuration for the command.
type Config struct {
	DataDir      string `toml:"data-dir"`
	Bind         string `toml:"bind"`
	InternalPort string `toml:"internal-port"`

	Cluster struct {
		ReplicaN      int      `toml:"replicas"`
		Type          string   `toml:"type"`
		Hosts         []string `toml:"hosts"`
		InternalHosts []string `toml:"internal-hosts"`
		PollInterval  Duration `toml:"poll-interval"`
		GossipSeed    string   `toml:"gossip-seed"`
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
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		Bind:                DefaultHost + ":" + DefaultPort,
		MaxWritesPerRequest: DefaultMaxWritesPerRequest,
	}
	c.Cluster.ReplicaN = DefaultReplicaN
	c.Cluster.Type = DefaultClusterType
	c.Cluster.PollInterval = Duration(DefaultPollingInterval)
	c.Cluster.Hosts = []string{}
	c.Cluster.InternalHosts = []string{}
	c.AntiEntropy.Interval = Duration(DefaultAntiEntropyInterval)
	c.Metric.Service = DefaultMetrics
	return c
}

// Validate that all configuration permutations are compatible with each other.
func (c *Config) Validate() error {
	if !StringInSlice(c.Cluster.Type, ClusterTypes) {
		return ErrConfigClusterTypeInvalid
	}
	if len(c.Cluster.Hosts) > 1 && !(c.Cluster.Type == ClusterHTTP || c.Cluster.Type == ClusterGossip) {
		return ErrConfigClusterTypeMissing
	}
	if c.Cluster.Type == ClusterHTTP || c.Cluster.Type == ClusterGossip {
		if c.Cluster.ReplicaN > len(c.Cluster.Hosts) {
			return ErrConfigReplicaNInvalid
		}
		if !foundItem(c.Cluster.Hosts, c.Bind) {
			return ErrConfigHostsMissing
		}
	}
	if c.Cluster.Type == ClusterHTTP {
		if len(c.Cluster.Hosts) != len(c.Cluster.InternalHosts) {
			return ErrConfigHostsMismatch
		}
		// TODO: this seems like an odd check; it's just ensuring that InternalPort
		// matches any one substring from any of the InternalHosts.
		// I suggest we either remove this completely or make it actually check
		// the port portion of the address for this node. (note that this only applies
		// to the http broadcaster, so if we simply use gossip for all implementations
		// we can remove this).
		if !ContainsSubstring(c.InternalPort, c.Cluster.InternalHosts) {
			return ErrConfigBroadcastPort
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
