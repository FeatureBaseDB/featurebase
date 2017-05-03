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

const (
	// DefaultHost is the default hostname to use.
	DefaultHost = "localhost"

	// DefaultPort is the default port use with the hostname.
	DefaultPort = "10101"

	// DefaultClusterType sets the node intercommunication method.
	DefaultClusterType = "static"

	// DefaultInternalPort the port the nodes intercommunicate on.
	DefaultInternalPort = "14000"

	// DefaultMaxWritesPerRequest is the default number of writes per request.
	DefaultMaxWritesPerRequest = 5000
)

// Config represents the configuration for the command.
type Config struct {
	DataDir string `toml:"data-dir"`
	Host    string `toml:"host"`

	Cluster struct {
		ReplicaN        int      `toml:"replicas"`
		Type            string   `toml:"type"`
		Hosts           []string `toml:"hosts"`
		InternalHosts   []string `toml:"internal-hosts"`
		PollingInterval Duration `toml:"polling-interval"`
		InternalPort    string   `toml:"internal-port"`
		GossipSeed      string   `toml:"gossip-seed"`
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
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		Host:                DefaultHost + ":" + DefaultPort,
		MaxWritesPerRequest: DefaultMaxWritesPerRequest,
	}
	c.Cluster.ReplicaN = DefaultReplicaN
	c.Cluster.Type = DefaultClusterType
	c.Cluster.PollingInterval = Duration(DefaultPollingInterval)
	c.Cluster.Hosts = []string{}
	c.Cluster.InternalHosts = []string{}
	c.AntiEntropy.Interval = Duration(DefaultAntiEntropyInterval)
	return c
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
