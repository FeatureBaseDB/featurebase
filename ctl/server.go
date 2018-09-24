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
	flags.StringVarP(&srv.Config.DataDir, "data-dir", "d", srv.Config.DataDir, "Directory to store pilosa data files.")
	flags.StringVarP(&srv.Config.Bind, "bind", "b", srv.Config.Bind, "Default URI on which pilosa should listen.")
	flags.IntVarP(&srv.Config.MaxWritesPerRequest, "max-writes-per-request", "", srv.Config.MaxWritesPerRequest, "Number of write commands per request.")
	flags.StringVar(&srv.Config.LogPath, "log-path", srv.Config.LogPath, "Log path")
	flags.BoolVar(&srv.Config.Verbose, "verbose", srv.Config.Verbose, "Enable verbose logging")

	// TLS
	SetTLSConfig(flags, &srv.Config.TLS.CertificatePath, &srv.Config.TLS.CertificateKeyPath, &srv.Config.TLS.SkipVerify)

	// Handler
	flags.StringSliceVarP(&srv.Config.Handler.AllowedOrigins, "handler.allowed-origins", "", []string{}, "Comma separated list of allowed origin URIs (for CORS/WebUI).")

	// Cluster
	flags.BoolVarP(&srv.Config.Cluster.Disabled, "cluster.disabled", "", srv.Config.Cluster.Disabled, "Disabled multi-node cluster communication (used for testing)")
	flags.BoolVarP(&srv.Config.Cluster.Coordinator, "cluster.coordinator", "", srv.Config.Cluster.Coordinator, "Host that will act as cluster coordinator during startup and resizing.")
	flags.IntVarP(&srv.Config.Cluster.ReplicaN, "cluster.replicas", "", 1, "Number of hosts each piece of data should be stored on.")
	flags.StringSliceVarP(&srv.Config.Cluster.Hosts, "cluster.hosts", "", []string{}, "Comma separated list of hosts in cluster. Only used for testing.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Cluster.LongQueryTime), "cluster.long-query-time", "", time.Minute, "Duration that will trigger log and stat messages for slow queries.")

	// Translation
	flags.StringVarP(&srv.Config.Translation.PrimaryURL, "translation.primary-url", "", srv.Config.Translation.PrimaryURL, "DEPRECATED: URL for primary translation node for replication.")
	flags.IntVarP(&srv.Config.Translation.MapSize, "translation.map-size", "", srv.Config.Translation.MapSize, "Size in bytes of mmap to allocate for key translation.")

	// Gossip
	flags.StringVarP(&srv.Config.Gossip.Port, "gossip.port", "", srv.Config.Gossip.Port, "Port to which pilosa should bind for internal state sharing.")
	flags.StringSliceVarP(&srv.Config.Gossip.Seeds, "gossip.seeds", "", srv.Config.Gossip.Seeds, "Host with which to seed the gossip membership.")
	flags.StringVarP(&srv.Config.Gossip.Key, "gossip.key", "", srv.Config.Gossip.Key, "The path to file of the encryption key for gossip. The contents of the file should be either 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.StreamTimeout), "gossip.stream-timeout", "", (time.Duration)(srv.Config.Gossip.StreamTimeout), "Timeout for establishing a stream connection with a remote node for a full state sync.")
	flags.IntVarP(&srv.Config.Gossip.SuspicionMult, "gossip.suspicion-mult", "", srv.Config.Gossip.SuspicionMult, "Multiplier for determining the time an inaccessible node is considered suspect before declaring it dead.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.PushPullInterval), "gossip.push-pull-interval", "", (time.Duration)(srv.Config.Gossip.PushPullInterval), "Interval between complete state syncs.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.ProbeTimeout), "gossip.probe-timeout", "", (time.Duration)(srv.Config.Gossip.ProbeTimeout), "Timeout to wait for an ack from a probed node before assuming it is unhealthy.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.ProbeInterval), "gossip.probe-interval", "", (time.Duration)(srv.Config.Gossip.ProbeInterval), "Interval between random node probes.")
	flags.IntVarP(&srv.Config.Gossip.Nodes, "gossip.nodes", "", srv.Config.Gossip.Nodes, "Number of random nodes to send gossip messages to per GossipInterval.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.Interval), "gossip.interval", "", (time.Duration)(srv.Config.Gossip.Interval), "Interval between sending messages that need to be gossiped that haven't piggybacked on probing messages.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Gossip.ToTheDeadTime), "gossip.to-the-dead-time", "", (time.Duration)(srv.Config.Gossip.ToTheDeadTime), "Interval after which a node has died that we will still try to gossip to it.")

	// AntiEntropy
	flags.DurationVarP((*time.Duration)(&srv.Config.AntiEntropy.Interval), "anti-entropy.interval", "", (time.Duration)(srv.Config.AntiEntropy.Interval), "Interval at which to run anti-entropy routine.")

	// Metric
	flags.StringVarP(&srv.Config.Metric.Service, "metric.service", "", srv.Config.Metric.Service, "Default URI on which pilosa should listen.")
	flags.StringVarP(&srv.Config.Metric.Host, "metric.host", "", srv.Config.Metric.Host, "Default URI to send metrics.")
	flags.DurationVarP((*time.Duration)(&srv.Config.Metric.PollInterval), "metric.poll-interval", "", (time.Duration)(srv.Config.Metric.PollInterval), "Polling interval metrics.")
	flags.BoolVarP((&srv.Config.Metric.Diagnostics), "metric.diagnostics", "", srv.Config.Metric.Diagnostics, "Enabled diagnostics reporting.")
}
