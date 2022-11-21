// Copyright 2021 Molecula Corp. All rights reserved.
package ctl

import (
	"time"

	"github.com/molecula/featurebase/v3/server"
	"github.com/molecula/featurebase/v3/storage"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// serverFlagSet returns a pflag.FlagSet. All flags will be prefixed with the
// given prefix value, and default values come from the provided server.Config.
func serverFlagSet(srv *server.Config, prefix string) *pflag.FlagSet {
	// pre applies prefix to s when a prefix is provided.
	pre := func(s string) string {
		if prefix == "" {
			return s
		}
		return prefix + "." + s
	}

	// short will pass through the short flag as long as a prefix is not
	// specified.
	short := func(s string) string {
		if prefix == "" {
			return s
		}
		return ""
	}

	flags := pflag.NewFlagSet("featurebase", pflag.ExitOnError)
	flags.StringVar(&srv.Name, pre("name"), srv.Name, "Name of the node in the cluster.")
	flags.StringVar(&srv.MDSAddress, pre("mds-address"), srv.MDSAddress, "MDS service to register with.")
	flags.StringVar(&srv.WriteLogger, pre("write-logger"), srv.WriteLogger, "WriteLogger to read/write append logs.")
	flags.StringVar(&srv.Snapshotter, pre("snapshotter"), srv.Snapshotter, "Snapshotter to read/write snapshots.")
	flags.StringVarP(&srv.DataDir, pre("data-dir"), short("d"), srv.DataDir, "Directory to store FeatureBase data files.")
	flags.StringVarP(&srv.Bind, pre("bind"), short("b"), srv.Bind, "Default URI on which FeatureBase should listen.")
	flags.StringVar(&srv.BindGRPC, pre("bind-grpc"), srv.BindGRPC, "URI on which FeatureBase should listen for gRPC requests.")
	flags.StringVar(&srv.Advertise, pre("advertise"), srv.Advertise, "Address to advertise externally.")
	flags.StringVar(&srv.AdvertiseGRPC, pre("advertise-grpc"), srv.AdvertiseGRPC, "Address to advertise externally for gRPC.")
	flags.IntVar(&srv.MaxWritesPerRequest, pre("max-writes-per-request"), srv.MaxWritesPerRequest, "Number of write commands per request.")
	flags.StringVar(&srv.LogPath, pre("log-path"), srv.LogPath, "Log path")
	flags.BoolVar(&srv.Verbose, pre("verbose"), srv.Verbose, "Enable verbose logging")
	flags.Uint64Var(&srv.MaxMapCount, pre("max-map-count"), srv.MaxMapCount, "Limits the maximum number of active mmaps. FeatureBase will fall back to reading files once this is exhausted. Set below your system's vm.max_map_count.")
	flags.Uint64Var(&srv.MaxFileCount, pre("max-file-count"), srv.MaxFileCount, "Soft limit on the maximum number of fragment files FeatureBase keeps open simultaneously.")
	flags.DurationVar((*time.Duration)(&srv.LongQueryTime), pre("long-query-time"), time.Duration(srv.LongQueryTime), "Duration that will trigger log and stat messages for slow queries. Zero to disable.")
	flags.IntVar(&srv.QueryHistoryLength, pre("query-history-length"), srv.QueryHistoryLength, "Number of queries to remember in history.")
	flags.Int64Var(&srv.MaxQueryMemory, pre("max-query-memory"), srv.MaxQueryMemory, "Maximum memory allowed per Extract() or SELECT query.")

	// TLS
	SetTLSConfig(flags, pre(""), &srv.TLS.CertificatePath, &srv.TLS.CertificateKeyPath, &srv.TLS.CACertPath, &srv.TLS.SkipVerify, &srv.TLS.EnableClientVerification)

	// Handler
	flags.StringSliceVar(&srv.Handler.AllowedOrigins, pre("handler.allowed-origins"), []string{}, "Comma separated list of allowed origin URIs (for CORS/Web UI).")

	// Cluster
	flags.IntVar(&srv.Cluster.ReplicaN, pre("cluster.replicas"), 1, "Number of hosts each piece of data should be stored on.")
	flags.DurationVar((*time.Duration)(&srv.Cluster.LongQueryTime), pre("cluster.long-query-time"), time.Duration(srv.Cluster.LongQueryTime), "RENAMED TO 'long-query-time': Duration that will trigger log and stat messages for slow queries.") // negative duration indicates invalid value because 0 is meaningful
	flags.StringVar(&srv.Cluster.Name, pre("cluster.name"), srv.Cluster.Name, "Human-readable name for the cluster.")
	flags.StringVar(&srv.Cluster.PartitionToNodeAssignment, pre("cluster.partition-to-node-assignment"), srv.Cluster.PartitionToNodeAssignment, "How to assign partitions to nodes. jmp-hash or modulus")

	// Translation
	flags.StringVar(&srv.Translation.PrimaryURL, pre("translation.primary-url"), srv.Translation.PrimaryURL, "DEPRECATED: URL for primary translation node for replication.")
	flags.IntVar(&srv.Translation.MapSize, pre("translation.map-size"), srv.Translation.MapSize, "Size in bytes of mmap to allocate for key translation.")

	// Etcd
	// Etcd.Name used Config.Name for its value.
	flags.StringVar(&srv.Etcd.Dir, pre("etcd.dir"), srv.Etcd.Dir, "Directory to store etcd data files. If not provided, a directory will be created under the main data-dir directory.")
	// Etcd.ClusterName uses Cluster.Name for its value
	flags.StringVar(&srv.Etcd.LClientURL, pre("etcd.listen-client-address"), srv.Etcd.LClientURL, "Listen client address.")
	flags.StringVar(&srv.Etcd.AClientURL, pre("etcd.advertise-client-address"), srv.Etcd.AClientURL, "Advertise client address. If not provided, uses the listen client address.")
	flags.StringVar(&srv.Etcd.LPeerURL, pre("etcd.listen-peer-address"), srv.Etcd.LPeerURL, "Listen peer address.")
	flags.StringVar(&srv.Etcd.APeerURL, pre("etcd.advertise-peer-address"), srv.Etcd.APeerURL, "Advertise peer address. If not provided, uses the listen peer address.")
	flags.StringVar(&srv.Etcd.ClusterURL, pre("etcd.cluster-url"), srv.Etcd.ClusterURL, "Cluster URL to join.")
	flags.StringVar(&srv.Etcd.InitCluster, pre("etcd.initial-cluster"), srv.Etcd.InitCluster, "Initial cluster name1=apurl1,name2=apurl2")
	flags.Int64Var(&srv.Etcd.HeartbeatTTL, pre("etcd.heartbeat-ttl"), srv.Etcd.HeartbeatTTL, "Timeout used to determine cluster status")

	flags.StringVar(&srv.Etcd.Cluster, "etcd.static-cluster", srv.Etcd.Cluster, "EXPERIMENTAL static featurebase cluster name1=apurl1,name2=apurl2")
	flags.MarkHidden("etcd.static-cluster")
	flags.StringVar(&srv.Etcd.EtcdHosts, "etcd.etcd-hosts", srv.Etcd.EtcdHosts, "EXPERIMENTAL etcd server host:port comma separated list")
	flags.MarkHidden("etcd.etcd-hosts") // TODO (twg) expose when ready for public consumption

	// External postgres database for ExternalLookup
	flags.StringVar(&srv.LookupDBDSN, pre("lookup-db-dsn"), "", "external (postgres) database DSN to use for ExternalLookup calls")

	// AntiEntropy
	flags.DurationVar((*time.Duration)(&srv.AntiEntropy.Interval), pre("anti-entropy.interval"), (time.Duration)(srv.AntiEntropy.Interval), "Interval at which to run anti-entropy routine.")

	// Metric
	flags.StringVar(&srv.Metric.Service, pre("metric.service"), srv.Metric.Service, "Where to send stats: can be expvar (in-memory served at /debug/vars), prometheus, statsd or none.")
	flags.StringVar(&srv.Metric.Host, pre("metric.host"), srv.Metric.Host, "URI to send metrics when metric.service is statsd.")
	flags.DurationVar((*time.Duration)(&srv.Metric.PollInterval), pre("metric.poll-interval"), (time.Duration)(srv.Metric.PollInterval), "Polling interval metrics.")
	flags.BoolVar((&srv.Metric.Diagnostics), pre("metric.diagnostics"), srv.Metric.Diagnostics, "Enabled diagnostics reporting.")

	// Tracing
	flags.StringVar(&srv.Tracing.AgentHostPort, pre("tracing.agent-host-port"), srv.Tracing.AgentHostPort, "Jaeger agent host:port.")
	flags.StringVar(&srv.Tracing.SamplerType, pre("tracing.sampler-type"), srv.Tracing.SamplerType, "Jaeger sampler type (remote, const, probabilistic, ratelimiting) or 'off' to disable tracing completely.")
	flags.Float64Var(&srv.Tracing.SamplerParam, pre("tracing.sampler-param"), srv.Tracing.SamplerParam, "Jaeger sampler parameter.")

	// Profiling
	flags.IntVar(&srv.Profile.BlockRate, pre("profile.block-rate"), srv.Profile.BlockRate, "Sampling rate for goroutine blocking profiler. One sample per <rate> ns.")
	flags.IntVar(&srv.Profile.MutexFraction, pre("profile.mutex-fraction"), srv.Profile.MutexFraction, "Sampling fraction for mutex contention profiling. Sample 1/<rate> of events.")

	flags.StringVar(&srv.Storage.Backend, pre("storage.backend"), storage.DefaultBackend, "Storage backend to use: 'rbf' is only supported value.")
	flags.BoolVar(&srv.Storage.FsyncEnabled, pre("storage.fsync"), true, "enable fsync fully safe flush-to-disk")

	// RBF specific flags. See pilosa/rbf/cfg/cfg.go for definitions.
	srv.RBFConfig.DefineFlags(flags, prefix)

	flags.BoolVar(&srv.SQL.EndpointEnabled, pre("sql.endpoint-enabled"), srv.SQL.EndpointEnabled, "Enable FeatureBase SQL /sql endpoint (default false)")

	flags.DurationVar(&srv.CheckInInterval, pre("check-in-interval"), srv.CheckInInterval, "Interval between check-ins to MDS")

	// Future flags.
	flags.BoolVar(&srv.Future.Rename, pre("future.rename"), false, "Present application name as FeatureBase. Defaults to false, will default to true in an upcoming release.")

	// OAuth2.0 identity provider configuration
	flags.BoolVar(&srv.Auth.Enable, pre("auth.enable"), false, "Enable AuthN/AuthZ of featurebase, disabled by default.")
	flags.StringVar(&srv.Auth.ClientId, pre("auth.client-id"), srv.Auth.ClientId, "Identity Provider's Application/Client ID.")
	flags.StringVar(&srv.Auth.ClientSecret, pre("auth.client-secret"), srv.Auth.ClientSecret, "Identity Provider's Client Secret.")
	flags.StringVar(&srv.Auth.AuthorizeURL, pre("auth.authorize-url"), srv.Auth.AuthorizeURL, "Identity Provider's Authorize URL.")
	flags.StringVar(&srv.Auth.RedirectBaseURL, pre("auth.redirect-base-url"), srv.Auth.RedirectBaseURL, "Base URL of the featurebase instance used to redirect IDP.")
	flags.StringVar(&srv.Auth.TokenURL, pre("auth.token-url"), srv.Auth.TokenURL, "Identity Provider's Token URL.")
	flags.StringVar(&srv.Auth.GroupEndpointURL, pre("auth.group-endpoint-url"), srv.Auth.GroupEndpointURL, "Identity Provider's Group endpoint URL.")
	flags.StringVar(&srv.Auth.LogoutURL, pre("auth.logout-url"), srv.Auth.LogoutURL, "Identity Provider's Logout URL.")
	flags.StringSliceVar(&srv.Auth.Scopes, pre("auth.scopes"), srv.Auth.Scopes, "Comma separated list of scopes obtained from IdP")
	flags.StringVar(&srv.Auth.SecretKey, pre("auth.secret-key"), srv.Auth.SecretKey, "Secret key used for auth.")
	flags.StringVar(&srv.Auth.PermissionsFile, pre("auth.permissions"), srv.Auth.PermissionsFile, "Permissions' file with group authorization.")
	flags.StringVar(&srv.Auth.QueryLogPath, pre("auth.query-log-path"), srv.Auth.QueryLogPath, "Path to log user queries")
	flags.StringSliceVar(&srv.Auth.ConfiguredIPs, pre("auth.configured-ips"), srv.Auth.ConfiguredIPs, "List of configured IPs allowed for ingest")

	flags.BoolVar(&srv.DataDog.Enable, pre("datadog.enable"), false, "enable continuous profiling with DataDog cloud service, Note you must have DataDog agent installed")
	flags.StringVar(&srv.DataDog.Service, pre("datadog.service"), "default-service", "The Datadog service name, for example my-web-app")
	flags.StringVar(&srv.DataDog.Env, pre("datadog.env"), "default-env", "The Datadog environment name, for example, production")
	flags.StringVar(&srv.DataDog.Version, pre("datadog.version"), "default-version", "The version of your application")
	flags.StringVar(&srv.DataDog.Tags, pre("datadog.tags"), "molecula", "The tags to apply to an uploaded profile. Must be a list of in the format <KEY1>:<VALUE1>,<KEY2>:<VALUE2>")
	flags.BoolVar(&srv.DataDog.CPUProfile, pre("datadog.cpu-profile"), true, "golang pprof cpu profile ")
	flags.BoolVar(&srv.DataDog.HeapProfile, pre("datadog.heap-profile"), true, "golang pprof heap profile")
	flags.BoolVar(&srv.DataDog.MutexProfile, pre("datadog.mutex-profile"), false, "golang pprof mutex profile")
	flags.BoolVar(&srv.DataDog.GoroutineProfile, pre("datadog.goroutine-profile"), false, "golang pprof goroutine profile")
	flags.BoolVar(&srv.DataDog.BlockProfile, pre("datadog.block-profile"), false, "golang pprof goroutine ")

	flags.BoolVar(&srv.Dataframe.Enable, pre("dataframe.enable"), false, "EXPERIMENTAL enable support for Apply and Arrow")

	return flags
}

// BuildServerFlags attaches a set of flags to the command for a server instance.
func BuildServerFlags(cmd *cobra.Command, srv *server.Command) {
	flags := cmd.Flags()
	flags.AddFlagSet(serverFlagSet(srv.Config, ""))
}
