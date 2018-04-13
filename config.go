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

import (
	"time"
)

// Cluster types.
const (
	ClusterNone   = ""
	ClusterStatic = "static"
	ClusterGossip = "gossip"
)

// DefaultConfig is a Config structure that holds all the default values.
var DefaultConfig = NewConfig()

// TLSConfig contains TLS configuration
type TLSConfig struct {
	// CertificatePath contains the path to the certificate (.crt or .pem file)
	CertificatePath string `toml:"certificate-path"`
	// CertificateKeyPath contains the path to the certificate key (.key file)
	CertificateKeyPath string `toml:"certificate-key-path"`
	// SkipVerify disables verification for self-signed certificates
	SkipVerify bool `toml:"skip-verify"`
}

// Config represents the configuration for the command.
type Config struct {
	// DataDir is the default data directory.
	DataDir string `toml:"data-dir"`
	// Bind is the host:port on which Pilosa will listen.
	Bind string `toml:"bind"`

	// MaxWritesPerRequest limits the number of mutating commands that can be in
	// a single request to the server. This includes SetBit, ClearBit,
	// SetRowAttrs & SetColumnAttrs.
	MaxWritesPerRequest int `toml:"max-writes-per-request"`

	// LogPath configures where Pilosa will write logs.
	LogPath string `toml:"log-path"`

	// Verbose toggles verbose logging which can be useful for debugging.
	Verbose bool `toml:"verbose"`

	// TLS
	TLS TLSConfig

	Cluster struct {
		// Disabled controls whether clustering functionality is enabled.
		Disabled      bool     `toml:"disabled"`
		Coordinator   bool     `toml:"coordinator"`
		ReplicaN      int      `toml:"replicas"`
		Hosts         []string `toml:"hosts"`
		LongQueryTime Duration `toml:"long-query-time"`
	} `toml:"cluster"`

	// Gossip config is based around memberlist.Config.
	Gossip struct {
		// Port indicates the port to which pilosa should bind for internal state sharing.
		Port  string   `toml:"port"`
		Seeds []string `toml:"seeds"`
		Key   string   `toml:"key"`
		// StreamTimeout is the timeout for establishing a stream connection with
		// a remote node for a full state sync, and for stream read and write
		// operations. Maps to memberlist TCPTimeout.
		StreamTimeout Duration `toml:"stream-timeout"`
		// SuspicionMult is the multiplier for determining the time an
		// inaccessible node is considered suspect before declaring it dead.
		// The actual timeout is calculated using the formula:
		//
		//   SuspicionTimeout = SuspicionMult * log(N+1) * ProbeInterval
		//
		// This allows the timeout to scale properly with expected propagation
		// delay with a larger cluster size. The higher the multiplier, the longer
		// an inaccessible node is considered part of the cluster before declaring
		// it dead, giving that suspect node more time to refute if it is indeed
		// still alive.
		SuspicionMult int `toml:"suspicion-mult"`
		// PushPullInterval is the interval between complete state syncs.
		// Complete state syncs are done with a single node over TCP and are
		// quite expensive relative to standard gossiped messages. Setting this
		// to zero will disable state push/pull syncs completely.
		//
		// Setting this interval lower (more frequent) will increase convergence
		// speeds across larger clusters at the expense of increased bandwidth
		// usage.
		PushPullInterval Duration `toml:"push-pull-interval"`
		// ProbeInterval and ProbeTimeout are used to configure probing behavior
		// for memberlist.
		//
		// ProbeInterval is the interval between random node probes. Setting
		// this lower (more frequent) will cause the memberlist cluster to detect
		// failed nodes more quickly at the expense of increased bandwidth usage.
		//
		// ProbeTimeout is the timeout to wait for an ack from a probed node
		// before assuming it is unhealthy. This should be set to 99-percentile
		// of RTT (round-trip time) on your network.
		ProbeInterval Duration `toml:"probe-interval"`
		ProbeTimeout  Duration `toml:"probe-timeout"`

		// Interval and Nodes are used to configure the gossip
		// behavior of memberlist.
		//
		// Interval is the interval between sending messages that need
		// to be gossiped that haven't been able to piggyback on probing messages.
		// If this is set to zero, non-piggyback gossip is disabled. By lowering
		// this value (more frequent) gossip messages are propagated across
		// the cluster more quickly at the expense of increased bandwidth.
		//
		// Nodes is the number of random nodes to send gossip messages to
		// per Interval. Increasing this number causes the gossip messages
		// to propagate across the cluster more quickly at the expense of
		// increased bandwidth.
		//
		// ToTheDeadTime is the interval after which a node has died that
		// we will still try to gossip to it. This gives it a chance to refute.
		Interval      Duration `toml:"interval"`
		Nodes         int      `toml:"nodes"`
		ToTheDeadTime Duration `toml:"to-the-dead-time"`
	} `toml:"gossip"`

	AntiEntropy struct {
		Interval Duration `toml:"interval"`
	} `toml:"anti-entropy"`

	Metric struct {
		// Service can be statsd, expvar, or none.
		Service string `toml:"service"`
		// Host tells the statsd client where to write.
		Host         string   `toml:"host"`
		PollInterval Duration `toml:"poll-interval"`
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
	c.Cluster.ReplicaN = DefaultReplicaN
	c.Cluster.Hosts = []string{}
	c.Cluster.LongQueryTime = Duration(time.Minute)

	// Gossip config.
	c.Gossip.Port = "14000"
	// c.Gossip.Seeds = []string{}
	// c.Gossip.Key = ""
	c.Gossip.StreamTimeout = Duration(10 * time.Second)
	c.Gossip.SuspicionMult = 4
	c.Gossip.PushPullInterval = Duration(30 * time.Second)
	c.Gossip.ProbeInterval = Duration(1 * time.Second)
	c.Gossip.ProbeTimeout = Duration(500 * time.Millisecond)
	c.Gossip.Interval = Duration(200 * time.Millisecond)
	c.Gossip.Nodes = 3
	c.Gossip.ToTheDeadTime = Duration(30 * time.Second)

	// AntiEntropy config.
	c.AntiEntropy.Interval = Duration(10 * time.Minute)

	// Metric config.
	c.Metric.Service = "none"
	// c.Metric.Host = ""
	c.Metric.PollInterval = Duration(0 * time.Minute)
	c.Metric.Diagnostics = true

	return c
}

// Validate that all configuration permutations are compatible with each other.
func (c *Config) Validate() error {
	if !c.Cluster.Disabled && len(c.Cluster.Hosts) > 0 {
		return ErrConfigClusterEnabledHosts
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
