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
	"fmt"
	"time"

	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/storage"
	"github.com/spf13/cobra"
)

// BuildServerFlags attaches a set of flags to the command for a server instance.
func BuildServerFlags(cmd *cobra.Command, srv *server.Command) {
	flags := cmd.Flags()
	flags.StringVar(&srv.Config.Name, "name", srv.Config.Name, "Name of the node in the cluster.")
	flags.StringVarP(&srv.Config.DataDir, "data-dir", "d", srv.Config.DataDir, "Directory to store pilosa data files.")
	flags.StringVarP(&srv.Config.Bind, "bind", "b", srv.Config.Bind, "Default URI on which pilosa should listen.")
	flags.StringVar(&srv.Config.BindGRPC, "bind-grpc", srv.Config.BindGRPC, "URI on which pilosa should listen for gRPC requests.")
	flags.StringVar(&srv.Config.Advertise, "advertise", srv.Config.Advertise, "Address to advertise externally.")
	flags.StringVar(&srv.Config.AdvertiseGRPC, "advertise-grpc", srv.Config.AdvertiseGRPC, "Address to advertise externally for gRPC.")
	flags.IntVar(&srv.Config.MaxWritesPerRequest, "max-writes-per-request", srv.Config.MaxWritesPerRequest, "Number of write commands per request.")
	flags.StringVar(&srv.Config.LogPath, "log-path", srv.Config.LogPath, "Log path")
	flags.BoolVar(&srv.Config.Verbose, "verbose", srv.Config.Verbose, "Enable verbose logging")
	flags.Uint64Var(&srv.Config.MaxMapCount, "max-map-count", srv.Config.MaxMapCount, "Limits the maximum number of active mmaps. Pilosa will fall back to reading files once this is exhausted. Set below your system's vm.max_map_count.")
	flags.Uint64Var(&srv.Config.MaxFileCount, "max-file-count", srv.Config.MaxFileCount, "Soft limit on the maximum number of fragment files Pilosa keeps open simultaneously.")
	flags.DurationVar((*time.Duration)(&srv.Config.LongQueryTime), "long-query-time", time.Duration(srv.Config.LongQueryTime), "Duration that will trigger log and stat messages for slow queries. Zero to disable.")
	flags.IntVar(&srv.Config.QueryHistoryLength, "query-history-length", srv.Config.QueryHistoryLength, "Number of queries to remember in history.")

	// TLS
	SetTLSConfig(flags, "", &srv.Config.TLS.CertificatePath, &srv.Config.TLS.CertificateKeyPath, &srv.Config.TLS.CACertPath, &srv.Config.TLS.SkipVerify, &srv.Config.TLS.EnableClientVerification)

	// Handler
	flags.StringSliceVar(&srv.Config.Handler.AllowedOrigins, "handler.allowed-origins", []string{}, "Comma separated list of allowed origin URIs (for CORS/Web UI).")

	// Cluster
	flags.IntVar(&srv.Config.Cluster.ReplicaN, "cluster.replicas", 1, "Number of hosts each piece of data should be stored on.")
	flags.DurationVar((*time.Duration)(&srv.Config.Cluster.LongQueryTime), "cluster.long-query-time", time.Duration(srv.Config.Cluster.LongQueryTime), "RENAMED TO 'long-query-time': Duration that will trigger log and stat messages for slow queries.") // negative duration indicates invalid value because 0 is meaningful
	flags.StringVar(&srv.Config.Cluster.Name, "cluster.name", srv.Config.Cluster.Name, "Human-readable name for the cluster.")

	// Translation
	flags.StringVar(&srv.Config.Translation.PrimaryURL, "translation.primary-url", srv.Config.Translation.PrimaryURL, "DEPRECATED: URL for primary translation node for replication.")
	flags.IntVar(&srv.Config.Translation.MapSize, "translation.map-size", srv.Config.Translation.MapSize, "Size in bytes of mmap to allocate for key translation.")

	// Gossip
	flags.StringVar(&srv.Config.Gossip.Port, "gossip.port", srv.Config.Gossip.Port, "Port to which pilosa should bind for internal state sharing.")
	flags.StringVar(&srv.Config.Gossip.AdvertiseHost, "gossip.advertise-host", srv.Config.Gossip.AdvertiseHost, "Host on which memberlist should advertise.")
	flags.StringVar(&srv.Config.Gossip.AdvertisePort, "gossip.advertise-port", srv.Config.Gossip.AdvertisePort, "Port on which memberlist should advertise.")

	flags.StringSliceVar(&srv.Config.Gossip.Seeds, "gossip.seeds", srv.Config.Gossip.Seeds, "Host with which to seed the gossip membership.")
	flags.StringVar(&srv.Config.Gossip.Key, "gossip.key", srv.Config.Gossip.Key, "The path to file of the encryption key for gossip. The contents of the file should be either 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256.")
	flags.DurationVar((*time.Duration)(&srv.Config.Gossip.StreamTimeout), "gossip.stream-timeout", (time.Duration)(srv.Config.Gossip.StreamTimeout), "Timeout for establishing a stream connection with a remote node for a full state sync.")
	flags.IntVar(&srv.Config.Gossip.SuspicionMult, "gossip.suspicion-mult", srv.Config.Gossip.SuspicionMult, "Multiplier for determining the time an inaccessible node is considered suspect before declaring it dead.")
	flags.DurationVar((*time.Duration)(&srv.Config.Gossip.PushPullInterval), "gossip.push-pull-interval", (time.Duration)(srv.Config.Gossip.PushPullInterval), "Interval between complete state syncs.")
	flags.DurationVar((*time.Duration)(&srv.Config.Gossip.ProbeTimeout), "gossip.probe-timeout", (time.Duration)(srv.Config.Gossip.ProbeTimeout), "Timeout to wait for an ack from a probed node before assuming it is unhealthy.")
	flags.DurationVar((*time.Duration)(&srv.Config.Gossip.ProbeInterval), "gossip.probe-interval", (time.Duration)(srv.Config.Gossip.ProbeInterval), "Interval between random node probes.")
	flags.IntVar(&srv.Config.Gossip.Nodes, "gossip.nodes", srv.Config.Gossip.Nodes, "Number of random nodes to send gossip messages to per GossipInterval.")
	flags.DurationVar((*time.Duration)(&srv.Config.Gossip.Interval), "gossip.interval", (time.Duration)(srv.Config.Gossip.Interval), "Interval between sending messages that need to be gossiped that haven't piggybacked on probing messages.")
	flags.DurationVar((*time.Duration)(&srv.Config.Gossip.ToTheDeadTime), "gossip.to-the-dead-time", (time.Duration)(srv.Config.Gossip.ToTheDeadTime), "Interval after which a node has died that we will still try to gossip to it.")

	// Etcd
	// Etcd.Name used Config.Name for it's value.
	// Etcd.Dir defaults to a directory under the pilosa data directory.
	flags.StringVar(&srv.Config.Etcd.LClientURL, "etcd.listen-client-address", srv.Config.Etcd.LClientURL, "Listen client address.")
	flags.StringVar(&srv.Config.Etcd.AClientURL, "etcd.advertise-client-address", srv.Config.Etcd.AClientURL, "Advertise client address. If not provided, uses the listen client address.")
	flags.StringVar(&srv.Config.Etcd.LPeerURL, "etcd.listen-peer-address", srv.Config.Etcd.LPeerURL, "Listen peer address.")
	flags.StringVar(&srv.Config.Etcd.APeerURL, "etcd.advertise-peer-address", srv.Config.Etcd.APeerURL, "Advertise peer address. If not provided, uses the listen peer address.")
	flags.StringVar(&srv.Config.Etcd.ClusterURL, "etcd.cluster-url", srv.Config.Etcd.ClusterURL, "Cluster URL to join.")
	// Etcd.ClusterName uses Cluster.Name for its value.
	flags.StringVar(&srv.Config.Etcd.InitCluster, "etcd.initial-cluster", srv.Config.Etcd.InitCluster, "Initial cluster name1=apurl1,name2=apurl2")

	// AntiEntropy
	flags.DurationVar((*time.Duration)(&srv.Config.AntiEntropy.Interval), "anti-entropy.interval", (time.Duration)(srv.Config.AntiEntropy.Interval), "Interval at which to run anti-entropy routine.")

	// Metric
	flags.StringVar(&srv.Config.Metric.Service, "metric.service", srv.Config.Metric.Service, "Where to send stats: can be expvar (in-memory served at /debug/vars), prometheus, statsd or none.")
	flags.StringVar(&srv.Config.Metric.Host, "metric.host", srv.Config.Metric.Host, "URI to send metrics when metric.service is statsd.")
	flags.DurationVar((*time.Duration)(&srv.Config.Metric.PollInterval), "metric.poll-interval", (time.Duration)(srv.Config.Metric.PollInterval), "Polling interval metrics.")
	flags.BoolVar((&srv.Config.Metric.Diagnostics), "metric.diagnostics", srv.Config.Metric.Diagnostics, "Enabled diagnostics reporting.")

	// Tracing
	flags.StringVar(&srv.Config.Tracing.AgentHostPort, "tracing.agent-host-port", srv.Config.Tracing.AgentHostPort, "Jaeger agent host:port.")
	flags.StringVar(&srv.Config.Tracing.SamplerType, "tracing.sampler-type", srv.Config.Tracing.SamplerType, "Jaeger sampler type (remote, const, probabilistic, ratelimiting) or 'off' to disable tracing completely.")
	flags.Float64Var(&srv.Config.Tracing.SamplerParam, "tracing.sampler-param", srv.Config.Tracing.SamplerParam, "Jaeger sampler parameter.")

	// Profiling
	flags.IntVar(&srv.Config.Profile.BlockRate, "profile.block-rate", srv.Config.Profile.BlockRate, "Sampling rate for goroutine blocking profiler. One sample per <rate> ns.")
	flags.IntVar(&srv.Config.Profile.MutexFraction, "profile.mutex-fraction", srv.Config.Profile.MutexFraction, "Sampling fraction for mutex contention profiling. Sample 1/<rate> of events.")

	// Storage
	// Note: the default for --storage.backend must be kept "" empty string.
	// Otherwise we cannot detect and honor the PILOSA_STORAGE_BACKEND env var
	// over-ride.
	// TODO: the comment above was carried over from the PILOSA_TXSRC flag, but
	// we should confirm that this still applies.
	flags.StringVar(&srv.Config.Storage.Backend, "storage.backend", storage.DefaultBackend, fmt.Sprintf("transaction/storage to use: one of roaring, rbf, bolt, or a blue-green setup: rbf_roaring, roaring_rbf, bolt_roaring, roaring_bolt, bolt_rbf, etc. The default is: %v. The env var PILOSA_STORAGE_BACKEND is over-ridden by --storage.backend option on the command line.", storage.DefaultBackend))
	flags.BoolVar(&srv.Config.Storage.FsyncEnabled, "storage.fsync", true, "enable fsync fully safe flush-to-disk")

	// RowcacheOn
	flags.BoolVar((&srv.Config.RowcacheOn), "rowcache-on", srv.Config.RowcacheOn, "turn on the rowcache for all backends (may speed some queries)")

	// RBF specific flags. See pilosa/rbf/cfg/cfg.go for definitions.
	srv.Config.RBFConfig.DefineFlags(flags)

	// Postgres endpoint
	flags.StringVar(&srv.Config.Postgres.Bind, "postgres.bind", srv.Config.Postgres.Bind, "Address to which to bind a postgres endpoint (leave blank to disable)")
	SetTLSConfig(flags, "postgres.", &srv.Config.Postgres.TLS.CertificatePath, &srv.Config.Postgres.TLS.CertificateKeyPath, &srv.Config.Postgres.TLS.CACertPath, &srv.Config.Postgres.TLS.SkipVerify, &srv.Config.Postgres.TLS.EnableClientVerification)
	flags.DurationVar((*time.Duration)(&srv.Config.Postgres.StartupTimeout), "postgres.startup-timeout", time.Duration(srv.Config.Postgres.StartupTimeout), "Timeout for postgres connection startup. (set 0 to disable)")
	flags.DurationVar((*time.Duration)(&srv.Config.Postgres.ReadTimeout), "postgres.read-timeout", time.Duration(srv.Config.Postgres.ReadTimeout), "Timeout for reads on a postgres connection. (set 0 to disable; does not include connection idling)")
	flags.DurationVar((*time.Duration)(&srv.Config.Postgres.WriteTimeout), "postgres.write-timeout", time.Duration(srv.Config.Postgres.WriteTimeout), "Timeout for writes on a postgres connection. (set 0 to disable)")
	flags.Uint32Var(&srv.Config.Postgres.MaxStartupSize, "postgres.max-startup-size", srv.Config.Postgres.MaxStartupSize, "Maximum acceptable size of a postgres startup packet, in bytes. (set 0 to disable)")
	flags.Uint16Var(&srv.Config.Postgres.ConnectionLimit, "postgres.connection-limit", srv.Config.Postgres.ConnectionLimit, "Maximum number of simultaneous postgres connections to allow. (set 0 to disable)")
}
