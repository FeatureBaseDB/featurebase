// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ctl

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/server"
	"github.com/featurebasedb/featurebase/v3/storage"
	"github.com/spf13/cobra"
)

// BuildServerFlags attaches a set of flags to the command for a server instance.
func BuildServerFlags(cmd *cobra.Command, srv *server.Command) {
	flags := cmd.Flags()
	flags.StringVar(&srv.Config.Name, "name", srv.Config.Name, "Name of the node in the cluster.")
	flags.StringVarP(&srv.Config.DataDir, "data-dir", "d", srv.Config.DataDir, "Directory to store FeatureBase data files.")
	flags.StringVarP(&srv.Config.Bind, "bind", "b", srv.Config.Bind, "Default URI on which FeatureBase should listen.")
	flags.StringVar(&srv.Config.BindGRPC, "bind-grpc", srv.Config.BindGRPC, "URI on which FeatureBase should listen for gRPC requests.")
	flags.StringVar(&srv.Config.Advertise, "advertise", srv.Config.Advertise, "Address to advertise externally.")
	flags.StringVar(&srv.Config.AdvertiseGRPC, "advertise-grpc", srv.Config.AdvertiseGRPC, "Address to advertise externally for gRPC.")
	flags.IntVar(&srv.Config.MaxWritesPerRequest, "max-writes-per-request", srv.Config.MaxWritesPerRequest, "Number of write commands per request.")
	flags.StringVar(&srv.Config.LogPath, "log-path", srv.Config.LogPath, "Log path")
	flags.BoolVar(&srv.Config.Verbose, "verbose", srv.Config.Verbose, "Enable verbose logging")
	flags.Uint64Var(&srv.Config.MaxMapCount, "max-map-count", srv.Config.MaxMapCount, "Limits the maximum number of active mmaps. FeatureBase will fall back to reading files once this is exhausted. Set below your system's vm.max_map_count.")
	flags.Uint64Var(&srv.Config.MaxFileCount, "max-file-count", srv.Config.MaxFileCount, "Soft limit on the maximum number of fragment files FeatureBase keeps open simultaneously.")
	flags.DurationVar((*time.Duration)(&srv.Config.LongQueryTime), "long-query-time", time.Duration(srv.Config.LongQueryTime), "Duration that will trigger log and stat messages for slow queries. Zero to disable.")
	flags.IntVar(&srv.Config.QueryHistoryLength, "query-history-length", srv.Config.QueryHistoryLength, "Number of queries to remember in history.")
	flags.Int64Var(&srv.Config.MaxQueryMemory, "max-query-memory", srv.Config.MaxQueryMemory, "Maximum memory allowed per Extract() or SELECT query.")

	// TLS
	SetTLSConfig(flags, "", &srv.Config.TLS.CertificatePath, &srv.Config.TLS.CertificateKeyPath, &srv.Config.TLS.CACertPath, &srv.Config.TLS.SkipVerify, &srv.Config.TLS.EnableClientVerification)

	// Handler
	flags.StringSliceVar(&srv.Config.Handler.AllowedOrigins, "handler.allowed-origins", []string{}, "Comma separated list of allowed origin URIs (for CORS/Web UI).")

	// Cluster
	flags.IntVar(&srv.Config.Cluster.ReplicaN, "cluster.replicas", 1, "Number of hosts each piece of data should be stored on.")
	flags.DurationVar((*time.Duration)(&srv.Config.Cluster.LongQueryTime), "cluster.long-query-time", time.Duration(srv.Config.Cluster.LongQueryTime), "RENAMED TO 'long-query-time': Duration that will trigger log and stat messages for slow queries.") // negative duration indicates invalid value because 0 is meaningful
	flags.StringVar(&srv.Config.Cluster.Name, "cluster.name", srv.Config.Cluster.Name, "Human-readable name for the cluster.")
	flags.StringVar(&srv.Config.Cluster.PartitionToNodeAssignment, "cluster.partition-to-node-assignment", srv.Config.Cluster.PartitionToNodeAssignment, "How to assign partitions to nodes. jmp-hash or modulus")

	// Translation
	flags.StringVar(&srv.Config.Translation.PrimaryURL, "translation.primary-url", srv.Config.Translation.PrimaryURL, "DEPRECATED: URL for primary translation node for replication.")
	flags.IntVar(&srv.Config.Translation.MapSize, "translation.map-size", srv.Config.Translation.MapSize, "Size in bytes of mmap to allocate for key translation.")

	// Etcd
	// Etcd.Name used Config.Name for its value.
	flags.StringVar(&srv.Config.Etcd.Dir, "etcd.dir", srv.Config.Etcd.Dir, "Directory to store etcd data files. If not provided, a directory will be created under the main data-dir directory.")
	// Etcd.ClusterName uses Cluster.Name for its value
	flags.StringVar(&srv.Config.Etcd.LClientURL, "etcd.listen-client-address", srv.Config.Etcd.LClientURL, "Listen client address.")
	flags.StringVar(&srv.Config.Etcd.AClientURL, "etcd.advertise-client-address", srv.Config.Etcd.AClientURL, "Advertise client address. If not provided, uses the listen client address.")
	flags.StringVar(&srv.Config.Etcd.LPeerURL, "etcd.listen-peer-address", srv.Config.Etcd.LPeerURL, "Listen peer address.")
	flags.StringVar(&srv.Config.Etcd.APeerURL, "etcd.advertise-peer-address", srv.Config.Etcd.APeerURL, "Advertise peer address. If not provided, uses the listen peer address.")
	flags.StringVar(&srv.Config.Etcd.ClusterURL, "etcd.cluster-url", srv.Config.Etcd.ClusterURL, "Cluster URL to join.")
	flags.StringVar(&srv.Config.Etcd.InitCluster, "etcd.initial-cluster", srv.Config.Etcd.InitCluster, "Initial cluster name1=apurl1,name2=apurl2")
	flags.Int64Var(&srv.Config.Etcd.HeartbeatTTL, "etcd.heartbeat-ttl", srv.Config.Etcd.HeartbeatTTL, "Timeout used to determine cluster status")

	flags.StringVar(&srv.Config.Etcd.Cluster, "etcd.static-cluster", srv.Config.Etcd.Cluster, "EXPERIMENTAL static featurebase cluster name1=apurl1,name2=apurl2")
	_ = flags.MarkHidden("etcd.static-cluster")
	flags.StringVar(&srv.Config.Etcd.EtcdHosts, "etcd.etcd-hosts", srv.Config.Etcd.EtcdHosts, "EXPERIMENTAL etcd server host:port comma separated list")
	_ = flags.MarkHidden("etcd.etcd-hosts") // TODO (twg) expose when ready for public consumption

	// External postgres database for ExternalLookup
	flags.StringVar(&srv.Config.LookupDBDSN, "lookup-db-dsn", "", "external (postgres) database DSN to use for ExternalLookup calls")

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

	flags.StringVar(&srv.Config.Storage.Backend, "storage.backend", storage.DefaultBackend, "Storage backend to use: 'rbf' is only supported value.")
	flags.BoolVar(&srv.Config.Storage.FsyncEnabled, "storage.fsync", true, "enable fsync fully safe flush-to-disk")

	// RBF specific flags. See pilosa/rbf/cfg/cfg.go for definitions.
	srv.Config.RBFConfig.DefineFlags(flags)

	flags.BoolVar(&srv.Config.SQL.EndpointEnabled, "sql.endpoint-enabled", srv.Config.SQL.EndpointEnabled, "Enable FeatureBase SQL /sql endpoint (default false)")

	// Future flags.
	flags.BoolVar(&srv.Config.Future.Rename, "future.rename", false, "Present application name as FeatureBase. Defaults to false, will default to true in an upcoming release.")

	// OAuth2.0 identity provider configuration
	flags.BoolVar(&srv.Config.Auth.Enable, "auth.enable", false, "Enable AuthN/AuthZ of featurebase, disabled by default.")
	flags.StringVar(&srv.Config.Auth.ClientId, "auth.client-id", srv.Config.Auth.ClientId, "Identity Provider's Application/Client ID.")
	flags.StringVar(&srv.Config.Auth.ClientSecret, "auth.client-secret", srv.Config.Auth.ClientSecret, "Identity Provider's Client Secret.")
	flags.StringVar(&srv.Config.Auth.AuthorizeURL, "auth.authorize-url", srv.Config.Auth.AuthorizeURL, "Identity Provider's Authorize URL.")
	flags.StringVar(&srv.Config.Auth.RedirectBaseURL, "auth.redirect-base-url", srv.Config.Auth.RedirectBaseURL, "Base URL of the featurebase instance used to redirect IDP.")
	flags.StringVar(&srv.Config.Auth.TokenURL, "auth.token-url", srv.Config.Auth.TokenURL, "Identity Provider's Token URL.")
	flags.StringVar(&srv.Config.Auth.GroupEndpointURL, "auth.group-endpoint-url", srv.Config.Auth.GroupEndpointURL, "Identity Provider's Group endpoint URL.")
	flags.StringVar(&srv.Config.Auth.LogoutURL, "auth.logout-url", srv.Config.Auth.LogoutURL, "Identity Provider's Logout URL.")
	flags.StringSliceVar(&srv.Config.Auth.Scopes, "auth.scopes", srv.Config.Auth.Scopes, "Comma separated list of scopes obtained from IdP")
	flags.StringVar(&srv.Config.Auth.SecretKey, "auth.secret-key", srv.Config.Auth.SecretKey, "Secret key used for auth.")
	flags.StringVar(&srv.Config.Auth.PermissionsFile, "auth.permissions", srv.Config.Auth.PermissionsFile, "Permissions' file with group authorization.")
	flags.StringVar(&srv.Config.Auth.QueryLogPath, "auth.query-log-path", srv.Config.Auth.QueryLogPath, "Path to log user queries")
	flags.StringSliceVar(&srv.Config.Auth.ConfiguredIPs, "auth.configured-ips", srv.Config.Auth.ConfiguredIPs, "List of configured IPs allowed for ingest")

	flags.BoolVar(&srv.Config.DataDog.Enable, "datadog.enable", false, "enable continuous profiling with DataDog cloud service, Note you must have DataDog agent installed")
	flags.StringVar(&srv.Config.DataDog.Service, "datadog.service", "default-service", "The Datadog service name, for example my-web-app")
	flags.StringVar(&srv.Config.DataDog.Env, "datadog.env", "default-env", "The Datadog environment name, for example, production")
	flags.StringVar(&srv.Config.DataDog.Version, "datadog.version", "default-version", "The version of your application")
	flags.StringVar(&srv.Config.DataDog.Tags, "datadog.tags", "molecula", "The tags to apply to an uploaded profile. Must be a list of in the format <KEY1>:<VALUE1>,<KEY2>:<VALUE2>")
	flags.BoolVar(&srv.Config.DataDog.CPUProfile, "datadog.cpu-profile", true, "golang pprof cpu profile ")
	flags.BoolVar(&srv.Config.DataDog.HeapProfile, "datadog.heap-profile", true, "golang pprof heap profile")
	flags.BoolVar(&srv.Config.DataDog.MutexProfile, "datadog.mutex-profile", false, "golang pprof mutex profile")
	flags.BoolVar(&srv.Config.DataDog.GoroutineProfile, "datadog.goroutine-profile", false, "golang pprof goroutine profile")
	flags.BoolVar(&srv.Config.DataDog.BlockProfile, "datadog.block-profile", false, "golang pprof goroutine ")
}
