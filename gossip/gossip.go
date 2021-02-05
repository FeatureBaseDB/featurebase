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

package gossip

import (
	"github.com/pilosa/pilosa/v2/toml"
)

// Config holds toml-friendly memberlist configuration.
type Config struct {
	// Port indicates the port to which pilosa should bind for internal state sharing.
	Port string `toml:"port"`

	// AdvertiseHost is the hostname or IP other nodes should use to connect to
	// this host. If left blank, the value for Host will be used. This is useful
	// in some proxy and NAT scenarios.
	AdvertiseHost string `toml:"advertise-host"`
	// AdvertisePort is the port other nodes will use to connect to this one.
	// Behaves like AdvertiseHost.
	AdvertisePort string `toml:"advertise-port"`

	Seeds []string `toml:"seeds"`
	Key   string   `toml:"key"`
	// StreamTimeout is the timeout for establishing a stream connection with
	// a remote node for a full state sync, and for stream read and write
	// operations. Maps to memberlist TCPTimeout.
	StreamTimeout toml.Duration `toml:"stream-timeout"`
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
	PushPullInterval toml.Duration `toml:"push-pull-interval"`
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
	ProbeInterval toml.Duration `toml:"probe-interval"`
	ProbeTimeout  toml.Duration `toml:"probe-timeout"`

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
	Interval      toml.Duration `toml:"interval"`
	Nodes         int           `toml:"nodes"`
	ToTheDeadTime toml.Duration `toml:"to-the-dead-time"`
}
