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

package server

import (
	"time"

	"github.com/pilosa/pilosa/gossip"
	"github.com/pilosa/pilosa/toml"
)

// TLSConfig contains TLS configuration
type TLSConfig struct {
	// CertificatePath contains the path to the certificate (.crt or .pem file)
	CertificatePath string `toml:"certificate"`
	// CertificateKeyPath contains the path to the certificate key (.key file)
	CertificateKeyPath string `toml:"key"`
	// SkipVerify disables verification for self-signed certificates
	SkipVerify bool `toml:"skip-verify"`
}

// Config represents the configuration for the command.
type Config struct {
	// DataDir is the directory where Pilosa stores both indexed data and
	// running state such as cluster topology information.
	DataDir string `toml:"data-dir"`
	// Bind is the host:port on which Pilosa will listen.
	Bind string `toml:"bind"`

	// MaxWritesPerRequest limits the number of mutating commands that can be in
	// a single request to the server. This includes Set, Clear,
	// SetRowAttrs & SetColumnAttrs.
	MaxWritesPerRequest int `toml:"max-writes-per-request"`

	// LogPath configures where Pilosa will write logs.
	LogPath string `toml:"log-path"`

	// Verbose toggles verbose logging which can be useful for debugging.
	Verbose bool `toml:"verbose"`

	// HTTP Handler options
	Handler struct {
		// CORS Allowed Origins
		AllowedOrigins []string `toml:"allowed-origins"`
	} `toml:"handler"`

	// TLS
	TLS TLSConfig `toml:"tls"`

	Cluster struct {
		// Disabled controls whether clustering functionality is enabled.
		Disabled      bool          `toml:"disabled"`
		Coordinator   bool          `toml:"coordinator"`
		ReplicaN      int           `toml:"replicas"`
		Hosts         []string      `toml:"hosts"`
		LongQueryTime toml.Duration `toml:"long-query-time"`
	} `toml:"cluster"`

	// Gossip config is based around memberlist.Config.
	Gossip gossip.Config `toml:"gossip"`

	Translation struct {
		MapSize int `toml:"map-size"`
		// DEPRECATED: Translation config supports translation store replication.
		PrimaryURL string `toml:"primary-url"`
	} `toml:"translation"`

	AntiEntropy struct {
		Interval toml.Duration `toml:"interval"`
	} `toml:"anti-entropy"`

	Metric struct {
		// Service can be statsd, expvar, or none.
		Service string `toml:"service"`
		// Host tells the statsd client where to write.
		Host         string        `toml:"host"`
		PollInterval toml.Duration `toml:"poll-interval"`
		// Diagnostics toggles sending some limited diagnostic information to
		// Pilosa's developers.
		Diagnostics bool `toml:"diagnostics"`
	} `toml:"metric"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		DataDir:             "~/.pilosa",
		Bind:                ":10101",
		MaxWritesPerRequest: 5000,
		// LogPath: "",
		// Verbose: false,
		TLS: TLSConfig{},
	}

	// Cluster config.
	c.Cluster.Disabled = false
	// c.Cluster.Coordinator = false
	c.Cluster.ReplicaN = 1
	c.Cluster.Hosts = []string{}
	c.Cluster.LongQueryTime = toml.Duration(time.Minute)

	// Gossip config.
	c.Gossip.Port = "14000"
	// c.Gossip.Seeds = []string{}
	// c.Gossip.Key = ""
	c.Gossip.StreamTimeout = toml.Duration(10 * time.Second)
	c.Gossip.SuspicionMult = 4
	c.Gossip.PushPullInterval = toml.Duration(30 * time.Second)
	c.Gossip.ProbeInterval = toml.Duration(1 * time.Second)
	c.Gossip.ProbeTimeout = toml.Duration(500 * time.Millisecond)
	c.Gossip.Interval = toml.Duration(200 * time.Millisecond)
	c.Gossip.Nodes = 3
	c.Gossip.ToTheDeadTime = toml.Duration(30 * time.Second)

	// AntiEntropy config.
	c.AntiEntropy.Interval = toml.Duration(10 * time.Minute)

	// Metric config.
	c.Metric.Service = "none"
	// c.Metric.Host = ""
	c.Metric.PollInterval = toml.Duration(0 * time.Minute)
	c.Metric.Diagnostics = true

	return c
}
