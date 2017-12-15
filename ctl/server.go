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

package ctl

import (
	"time"

	"github.com/pilosa/pilosa/server"
	"github.com/spf13/cobra"
)

// BuildServerFlags attaches a set of flags to the command for a server instance.
func BuildServerFlags(cmd *cobra.Command, srv *server.Command) {
	flags := cmd.Flags()
	flags.StringVarP(&srv.Config.DataDir, "data-dir", "d", "~/.pilosa", "Directory to store pilosa data files.")
	flags.StringVarP(&srv.Config.Bind, "bind", "b", ":10101", "Default URI on which pilosa should listen.")
	flags.StringVarP(&srv.Config.GossipPort, "gossip-port", "", "", "(DEPRECATED) Port to which pilosa should bind for internal state sharing.")
	flags.StringVarP(&srv.Config.GossipSeed, "gossip-seed", "", "", "(DEPRECATED) Host with which to seed the gossip membership.")
	// gossip
	flags.StringVarP(&srv.Config.Gossip.Port, "gossip.port", "", "", "Port to which pilosa should bind for internal state sharing.")
	flags.StringVarP(&srv.Config.Gossip.Seed, "gossip.seed", "", "", "Host with which to seed the gossip membership.")
	flags.StringVarP(&srv.Config.Gossip.Key, "gossip.key", "", "", "The path to file of the encryption key for gossip. The contents of the file should be either 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256.")

	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.StreamTimeout), "gossip.stream-timeout", "", 10*time.Second, "Timeout for establishing a stream connection with a remote node for a full state sync.")
	flags.IntVarP(&srv.Config.Gossip.SuspicionMult, "gossip.suspicion-mult", "", 4, "Multiplier for determining the time an inaccessible node is considered suspect before declaring it dead.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.PushPullInterval), "gossip.push-pull-interval", "", 30*time.Second, "Interval between complete state syncs.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.ProbeTimeout), "gossip.probe-timeout", "", 500*time.Millisecond, "Timeout to wait for an ack from a probed node before assuming it is unhealthy.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.ProbeInterval), "gossip.probe-interval", "", 1*time.Second, "Interval between random node probes.")
	flags.IntVarP(&srv.Config.Gossip.GossipNodes, "gossip.gossip-nodes", "", 3, "Number of random nodes to send gossip messages to per GossipInterval.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.GossipInterval), "gossip.gossip-interval", "", 200*time.Millisecond, "Interval between sending messages that need to be gossiped that haven't piggybacked on probing messages.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.GossipToTheDeadTime), "gossip.gossip-to-the-dead-time", "", 30*time.Second, "Interval after which a node has died that we will still try to gossip to it.")

	flags.StringVarP(&srv.Config.Cluster.Coordinator, "cluster.coordinator", "", "", "Host that will act as cluster coordinator during startup and resizing.")
	flags.IntVarP(&srv.Config.MaxWritesPerRequest, "max-writes-per-request", "", srv.Config.MaxWritesPerRequest, "Number of write commands per request.")
	flags.IntVarP(&srv.Config.Cluster.ReplicaN, "cluster.replicas", "", 1, "Number of hosts each piece of data should be stored on.")
	flags.StringSliceVarP(&srv.Config.Cluster.Hosts, "cluster.hosts", "", []string{}, "Comma separated list of hosts in cluster.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Cluster.PollInterval), "cluster.poll-interval", "", time.Minute, "Polling interval for cluster.") // TODO what actually is this?
	flags.DurationVarP((*time.Duration)(&srv.Config.Cluster.LongQueryTime), "cluster.long-query-time", "", time.Minute, "Duration that will trigger log and stat messages for slow queries.")
	flags.StringVar(&srv.Config.LogPath, "log-path", "", "Log path")
	flags.DurationVarP((*time.Duration)(&srv.Config.AntiEntropy.Interval), "anti-entropy.interval", "", time.Minute*10, "Interval at which to run anti-entropy routine.")
	flags.StringVarP(&srv.CPUProfile, "profile.cpu", "", "", "Where to store CPU profile.")
	flags.DurationVarP(&srv.CPUTime, "profile.cpu-time", "", 30*time.Second, "CPU profile duration.")
	flags.StringVarP(&srv.Config.Cluster.Type, "cluster.type", "", "gossip", "Determine how the cluster handles membership and state sharing. Choose from [static, gossip]")
	flags.StringVarP(&srv.Config.Metric.Service, "metric.service", "", "nop", "Default URI on which pilosa should listen.")
	flags.StringVarP(&srv.Config.Metric.Host, "metric.host", "", "", "Default URI to send metrics.")
	flags.BoolVarP((&srv.Config.Metric.Diagnostics), "metric.diagnostics", "", true, "Enabled diagnostics reporting.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Metric.PollInterval), "metric.poll-interval", "", time.Minute*0, "Polling interval metrics.")
	SetTLSConfig(flags, &srv.Config.TLS.CertificatePath, &srv.Config.TLS.CertificateKeyPath, &srv.Config.TLS.SkipVerify)
}
