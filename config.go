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

const (
	// DefaultDataDir is the default data directory.
	DefaultDataDir = "~/.pilosa"

	// DefaultHost is the default hostname to use.
	DefaultHost = "localhost"

	// DefaultPort is the default port to use with the hostname.
	DefaultPort = "10101"

	// DefaultClusterDisabled sets the node intercommunication method.
	DefaultClusterDisabled = false

	// DefaultMetrics sets the internal metrics to no-op.
	DefaultMetrics = "nop"

	// DefaultMaxWritesPerRequest is the default number of writes per request.
	DefaultMaxWritesPerRequest = 5000

	// Gossip config based on memberlist.Config.

	// Port indicates the port to which pilosa should bind for internal state sharing.
	DefaultGossipPort = "14000"

	// StreamTimeout is the timeout for establishing a stream connection with
	// a remote node for a full state sync, and for stream read and write
	// operations. Maps to memberlist TCPTimeout.
	DefaultGossipStreamTimeout = 10 * time.Second

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
	DefaultGossipSuspicionMult = 4

	// PushPullInterval is the interval between complete state syncs.
	// Complete state syncs are done with a single node over TCP and are
	// quite expensive relative to standard gossiped messages. Setting this
	// to zero will disable state push/pull syncs completely.
	//
	// Setting this interval lower (more frequent) will increase convergence
	// speeds across larger clusters at the expense of increased bandwidth
	// usage.
	DefaultGossipPushPullInterval = 30 * time.Second

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
	DefaultGossipProbeInterval = 1 * time.Second
	DefaultGossipProbeTimeout  = 500 * time.Millisecond

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
	DefaultGossipInterval      = 200 * time.Millisecond
	DefaultGossipNodes         = 3
	DefaultGossipToTheDeadTime = 30 * time.Second

	DefaultMetricPollInterval = 0 * time.Minute
)

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
	DataDir string `toml:"data-dir"`
	Bind    string `toml:"bind"`

	// Limits the number of mutating commands that can be in a single request to
	// the server. This includes SetBit, ClearBit, SetRowAttrs & SetColumnAttrs.
	MaxWritesPerRequest int `toml:"max-writes-per-request"`

	LogPath string `toml:"log-path"`
	Verbose bool   `toml:"verbose"`

	// TLS
	TLS TLSConfig

	Cluster struct {
		Disabled      bool     `toml:"disabled"`
		Coordinator   bool     `toml:"coordinator"`
		ReplicaN      int      `toml:"replicas"`
		Hosts         []string `toml:"hosts"`
		LongQueryTime Duration `toml:"long-query-time"`
	} `toml:"cluster"`

	Gossip struct {
		Port             string   `toml:"port"`
		Seeds            []string `toml:"seeds"`
		Key              string   `toml:"key"`
		StreamTimeout    Duration `toml:"stream-timeout"`
		SuspicionMult    int      `toml:"suspicion-mult"`
		PushPullInterval Duration `toml:"push-pull-interval"`
		ProbeTimeout     Duration `toml:"probe-timeout"`
		ProbeInterval    Duration `toml:"probe-interval"`
		Nodes            int      `toml:"nodes"`
		Interval         Duration `toml:"interval"`
		ToTheDeadTime    Duration `toml:"to-the-dead-time"`
	} `toml:"gossip"`

	AntiEntropy struct {
		Interval Duration `toml:"interval"`
	} `toml:"anti-entropy"`

	Metric struct {
		Service      string   `toml:"service"`
		Host         string   `toml:"host"`
		PollInterval Duration `toml:"poll-interval"`
		Diagnostics  bool     `toml:"diagnostics"`
	} `toml:"metric"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		DataDir:             DefaultDataDir,
		Bind:                ":" + DefaultPort,
		MaxWritesPerRequest: DefaultMaxWritesPerRequest,
		// LogPath: "",
		// Verbose: false,
		TLS: TLSConfig{},
	}

	// Cluster config.
	c.Cluster.Disabled = DefaultClusterDisabled
	// c.Cluster.Coordinator = false
	c.Cluster.ReplicaN = DefaultReplicaN
	c.Cluster.Hosts = []string{}
	c.Cluster.LongQueryTime = Duration(time.Minute)

	// Gossip config.
	// c.Gossip.Port = ""
	// c.Gossip.Seeds = []string{}
	// c.Gossip.Key = ""
	c.Gossip.StreamTimeout = Duration(DefaultGossipStreamTimeout)
	c.Gossip.SuspicionMult = DefaultGossipSuspicionMult
	c.Gossip.PushPullInterval = Duration(DefaultGossipPushPullInterval)
	c.Gossip.ProbeTimeout = Duration(DefaultGossipProbeTimeout)
	c.Gossip.ProbeInterval = Duration(DefaultGossipProbeInterval)
	c.Gossip.Nodes = DefaultGossipNodes
	c.Gossip.Interval = Duration(DefaultGossipInterval)
	c.Gossip.ToTheDeadTime = Duration(DefaultGossipToTheDeadTime)

	// AntiEntropy config.
	c.AntiEntropy.Interval = Duration(DefaultAntiEntropyInterval)

	// Metric config.
	c.Metric.Service = DefaultMetrics
	// c.Metric.Host = ""
	c.Metric.PollInterval = Duration(DefaultMetricPollInterval)
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
