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
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	petcd "github.com/pilosa/pilosa/v2/etcd"
	rbfcfg "github.com/pilosa/pilosa/v2/rbf/cfg"
	"github.com/pilosa/pilosa/v2/storage"
	"github.com/pilosa/pilosa/v2/toml"
	"github.com/pkg/errors"
)

const (
	defaultBindPort            = "10101"
	defaultBindGRPCPort        = "20101"
	defaultDiagnosticsInterval = 1 * time.Hour

	namespacePilosa      = "pilosa"
	namespaceFeaturebase = "featurebase"
)

// TLSConfig contains TLS configuration
type TLSConfig struct {
	// CertificatePath contains the path to the certificate (.crt or .pem file)
	CertificatePath string `toml:"certificate"`
	// CertificateKeyPath contains the path to the certificate key (.key file)
	CertificateKeyPath string `toml:"key"`
	// CACertPath is the path to a CA certificate (.crt or .pem file)
	CACertPath string `toml:"ca-certificate"`
	// SkipVerify disables verification of server certificates when connecting to another Pilosa node
	SkipVerify bool `toml:"skip-verify"`
	// EnableClientVerification enables verification of client TLS certificates (Mutual TLS)
	EnableClientVerification bool `toml:"enable-client-verification"`
}

// Config represents the configuration for the command.
type Config struct {
	// Name a unique name for this node in the cluster.
	Name string `toml:"name"`

	// DataDir is the directory where Pilosa stores both indexed data and
	// running state such as cluster topology information.
	DataDir string `toml:"data-dir"`

	// Bind is the host:port on which Pilosa will listen.
	Bind string `toml:"bind"`

	// BindGRPC is the host:port on which Pilosa will bind for gRPC.
	BindGRPC string `toml:"bind-grpc"`

	// GRPCListener is an already-bound listener to use for gRPC.
	// This is for use by test infrastructure, where it's useful to
	// be able to dynamically generate the bindings by actually binding
	// to :0, and avoid "address already in use" errors.
	GRPCListener *net.TCPListener

	// Advertise is the address advertised by the server to other nodes
	// in the cluster. It should be reachable by all other nodes and should
	// route to an interface that Bind is listening on.
	Advertise string `toml:"advertise"`

	// AdvertiseGRPC is the address advertised by the server to other nodes
	// in the cluster. It should be reachable by all other nodes and should
	// route to an interface that BindGRPC is listening on.
	AdvertiseGRPC string `toml:"advertise-grpc"`

	// MaxWritesPerRequest limits the number of mutating commands that can be in
	// a single request to the server. This includes Set, Clear, ClearRow, Store, and SetBit.
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

	// MaxMapCount puts an in-process limit on the number of mmaps. After this
	// is exhausted, Pilosa will fall back to reading the file into memory
	// normally.
	MaxMapCount uint64 `toml:"max-map-count"`

	// MaxFileCount puts a soft, in-process limit on the number of open fragment
	// files. Once this limit is passed, Pilosa will only keep files open while
	// actively working with them, and will close them afterward. This has a
	// negative effect on performance for workloads which make small appends to
	// lots of fragments.
	MaxFileCount uint64 `toml:"max-file-count"`

	// TLS
	TLS TLSConfig `toml:"tls"`

	// WorkerPoolSize controls how many goroutines are created for
	// processing queries. Defaults to runtime.NumCPU(). It is
	// intentionally not defined as a flag... only exposed here so
	// that we can limit the size while running tests in CI so we
	// don't exhaust the goroutine limit.
	WorkerPoolSize int `toml:"-"`

	// ImportWorkerPoolSize controls how many goroutines are created for
	// processing importRoaring jobs. Defaults to runtime.NumCPU(). It is
	// intentionally not defined as a flag... only exposed here so
	// that we can limit the size while running tests in CI so we
	// don't exhaust the goroutine limit.
	ImportWorkerPoolSize int `toml:"-"`

	Cluster struct {
		ReplicaN int    `toml:"replicas"`
		Name     string `toml:"name"`
		// This LongQueryTime is deprecated but still exists for backward compatibility
		LongQueryTime toml.Duration `toml:"long-query-time"`
	} `toml:"cluster"`

	// Etcd config is based on embedded etcd.
	Etcd petcd.Options `toml:"etcd"`

	LongQueryTime toml.Duration `toml:"long-query-time"`

	Translation struct {
		MapSize int `toml:"map-size"`
		// DEPRECATED: Translation config supports translation store replication.
		PrimaryURL string `toml:"primary-url"`
	} `toml:"translation"`

	AntiEntropy struct {
		Interval toml.Duration `toml:"interval"`
	} `toml:"anti-entropy"`

	Metric struct {
		// Service can be statsd, prometheus, expvar, or none.
		Service string `toml:"service"`
		// Host tells the statsd client where to write.
		Host         string        `toml:"host"`
		PollInterval toml.Duration `toml:"poll-interval"`
		// Diagnostics toggles sending some limited diagnostic information to
		// Pilosa's developers.
		Diagnostics bool `toml:"diagnostics"`
	} `toml:"metric"`

	Tracing struct {
		// SamplerType is the type of sampler to use.
		SamplerType string `toml:"sampler-type"`
		// SamplerParam is the parameter passed to the tracing sampler.
		// Its meaning is dependent on the type of sampler.
		SamplerParam float64 `toml:"sampler-param"`
		// AgentHostPort is the host:port of the local agent.
		AgentHostPort string `toml:"agent-host-port"`
	} `toml:"tracing"`

	Profile struct {
		// BlockRate is passed directly to runtime.SetBlockProfileRate
		BlockRate int `toml:"block-rate"`
		// MutexFraction is passed directly to runtime.SetMutexProfileFraction
		MutexFraction int `toml:"mutex-fraction"`
	} `toml:"profile"`

	Postgres struct {
		// Bind is the address to which to bind a postgres endpoint.
		// If this is empty, no endpoint will be created.
		Bind string `toml:"bind"`
		// TLS configuration for postgres connections.
		TLS TLSConfig `toml:"tls"`

		StartupTimeout toml.Duration `toml:"startup-timeout"`
		ReadTimeout    toml.Duration `toml:"read-timeout"`
		WriteTimeout   toml.Duration `toml:"write-timout"`

		MaxStartupSize uint32 `toml:"max-startup-size"`

		// ConnectionLimit is the maximum number of postgres connections to allow simultaneously.
		// Setting this to 0 disables the limit.
		// This mostly exists because other DBs seem to have it.
		ConnectionLimit uint16 `toml:"max-connections"`
	} `toml:"postgres"`

	// Storage.Backend determines which Tx implementation the holder/Index will
	// use; one of the available transactional-storage engines. Choices are
	// listed in the string constants below. Should be one of "roaring","bolt",
	// "rbf", "bolt_roaring", "roaring_bolt", "rbf_roaring", "roaring_rbf",
	// "bolt_rbf", "rbf_bolt", or any later addition. The engines with _
	// underscore indicate use of a blueGreenTx with a comparison of values back
	// from each Tx method, and a panic if they differ. This is an effective
	// test for consistency. If "rbf_roaring" is specified, then the roaring
	// values are the ones actually returned from the blueGreenTx. If
	// "roaring_rbf" is chosen, then the RBF values are the ones actually
	// returned from the blueGreenTx.
	Storage *storage.Config `toml:"storage"`

	// RowcacheOn, if true, turns on the row cache for all storage backends.
	// The default is now off because it makes rbf queries faster and uses
	// much less memory.
	RowcacheOn bool `toml:"rowcache-on"`

	// RBFConfig defines all externally configurable RBF flags.
	RBFConfig *rbfcfg.Config `toml:"rbf"`

	// QueryHistoryLength sets the maximum number of queries that are maintained
	// for the /query-history endpoint. This parameter is per-node, and the
	// result combines the history from all nodes.
	QueryHistoryLength int `toml:"query-history-length"`

	// LookupDBDSN is an external database to connect to for `ExternalLookup` queries.
	LookupDBDSN string `toml:"lookup-db-dsn"`

	// The percentage of time spent recalculating the disk and memory usage cache.
	UsageDutyCycle float64 `toml:"usage-duty-cycle"`

	// Future flags are used to represent features or functionality which is not
	// yet the default behavior, but will be in a future release.
	Future struct {
		// Rename, if true, will outwardly present the name of the application
		// as FeatureBase instead of Pilosa.
		Rename bool `toml:"rename"`
	} `toml:"future"`
}

// Namespace returns the namespace to use based on the Future flag.
func (c *Config) Namespace() string {
	if c.Future.Rename {
		return namespaceFeaturebase
	}
	return namespacePilosa
}

// MustValidate checks that all ports in a Config are unique and not zero.
// We disallow zero because the tests need to be using from the pre-allocated
// block of ports maintained by the pilosa/test/port port-mapper.
func (c *Config) MustValidate() {
	err := c.validate()
	if err != nil {
		panic(err)
	}
}

func (c *Config) validate() error {
	hostPort := []string{
		"Bind", c.Bind, // :10101
		"BindGRPC", c.BindGRPC, // :20101
		"Advertise", c.Advertise, //  on hp = 'http://localhost:63002'
		"AdvertiseGRPC", c.AdvertiseGRPC, //  on hp = 'http://localhost:63003'
		"Etcd.LClientURL", c.Etcd.LClientURL, //  on hp = ':14000'
		"Etcd.AClientURL", c.Etcd.AClientURL, // ""
		"Etcd.LPeerURL", c.Etcd.LPeerURL, // ":"
		"Etcd.APeerURL", c.Etcd.APeerURL, // ""
		"Etcd.ClusterURL", c.Etcd.ClusterURL,
		"Postgres.Bind", c.Postgres.Bind,
	}
	ports := make(map[int]bool)
	n := len(hostPort)
	for i := 0; i < n; i += 2 {
		name := hostPort[i]
		hp := hostPort[i+1]
		if hp == "" {
			continue
		}
		if name == "Advertise" && (hp == "" || hp == ":") {
			continue
		}
		if name == "AdvertiseGRPC" && (hp == "" || hp == ":") {
			continue
		}

		hp = strings.TrimPrefix(hp, "http://")
		hp = strings.TrimPrefix(hp, "https://")
		splt := strings.Split(hp, ":")
		if len(splt) != 2 {
			return fmt.Errorf("'%v' host:port '%v' did not have a colon; all='%#v'", name, hp, hostPort)
		}
		portstring := splt[1]
		port, err := strconv.Atoi(portstring)
		if err != nil {
			return fmt.Errorf("on '%v', could not convert '%v' to int in '%v': '%v'", name, portstring, hp, err)
		}
		if port == 0 {
			return fmt.Errorf("name '%v': zero port found, not allowed. '%v'. all ='%#v'", name, hp, hostPort)
		}
		if ports[port] {
			return fmt.Errorf("name '%v': duplicate port found, not allowed. '%v' with port %v. all ='%#v'", name, hp, port, hostPort)
		}
		ports[port] = true
	}
	return nil
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		Name:                "pilosa0",
		DataDir:             "~/.pilosa",
		Bind:                ":" + defaultBindPort,
		BindGRPC:            ":" + defaultBindGRPCPort,
		MaxWritesPerRequest: 5000,

		// We default these Max File/Map counts very high. This is basically a
		// backwards compatibility thing where we don't want to cause different
		// behavior for those who had previously set their system limits high,
		// and weren't experiencing any bad behavior. Ideally you want these set
		// a bit below your system limits.
		MaxMapCount:  1000000,
		MaxFileCount: 1000000,

		TLS: TLSConfig{},

		WorkerPoolSize:       runtime.NumCPU(),
		ImportWorkerPoolSize: runtime.NumCPU(),

		Storage:   storage.NewDefaultConfig(),
		RBFConfig: rbfcfg.NewDefaultConfig(),

		QueryHistoryLength: 100,

		LongQueryTime: toml.Duration(-time.Minute),
	}

	// Cluster config.
	c.Cluster.Name = "cluster0"
	c.Cluster.ReplicaN = 1
	c.Cluster.LongQueryTime = toml.Duration(-time.Minute) //TODO remove this once cluster.longQueryTime is fully deprecated

	// AntiEntropy config.
	c.AntiEntropy.Interval = toml.Duration(0)

	// Metric config.
	c.Metric.Service = "none"
	c.Metric.PollInterval = toml.Duration(0 * time.Minute)
	c.Metric.Diagnostics = false

	// Tracing config.
	c.Tracing.SamplerType = "off"
	c.Tracing.SamplerParam = 0.001

	c.Profile.BlockRate = 10000000 // 1 sample per 10 ms
	c.Profile.MutexFraction = 100  // 1% sampling

	// Postgres config (off by default).
	c.Postgres.MaxStartupSize = 8 * 1024 * 1024
	c.Postgres.StartupTimeout = toml.Duration(5 * time.Second)
	c.Postgres.ReadTimeout = toml.Duration(10 * time.Second)
	c.Postgres.WriteTimeout = toml.Duration(10 * time.Second)
	// we don't really need a connection limit

	c.Etcd.AClientURL = ""
	c.Etcd.LClientURL = "http://localhost:10301"
	c.Etcd.APeerURL = ""
	c.Etcd.LPeerURL = "http://localhost:10401"
	c.Etcd.Dir = ""
	c.Etcd.Name = ""
	c.Etcd.ClusterName = ""
	c.Etcd.InitCluster = c.Name + "=" + c.Etcd.LPeerURL
	c.Etcd.HeartbeatTTL = 5

	c.Etcd.TrustedCAFile = ""
	c.Etcd.ClientCertFile = ""
	c.Etcd.ClientKeyFile = ""
	c.Etcd.PeerCertFile = ""
	c.Etcd.PeerKeyFile = ""

	// Disk and Memory Usage
	c.UsageDutyCycle = 20.0

	// Future flags.
	c.Future.Rename = false

	return c
}

// validateAddrs controls the address fields in the Config object
// and fills in any blanks.
// The addresses fields must be guaranteed by the caller to either be
// completely empty, or have both a host part and a port part
// separated by a colon. In the latter case either can be empty to
// indicate it's left unspecified.
func (c *Config) validateAddrs(ctx context.Context) error {
	// Validate the advertise address.
	advScheme, advHost, advPort, err := validateAdvertiseAddr(ctx, c.Advertise, c.Bind, defaultBindPort)
	if err != nil {
		return errors.Wrapf(err, "validating advertise address")
	}
	c.Advertise = schemeHostPortString(advScheme, advHost, advPort)

	// Validate the listen address.
	listenScheme, listenHost, listenPort, err := validateListenAddr(ctx, c.Bind, defaultBindPort)
	if err != nil {
		return errors.Wrap(err, "validating listen address")
	}
	c.Bind = schemeHostPortString(listenScheme, listenHost, listenPort)

	// Validate the gRPC advertise address.
	_, grpcAdvHost, grpcAdvPort, err := validateAdvertiseAddr(ctx, c.AdvertiseGRPC, c.BindGRPC, defaultBindGRPCPort)
	if err != nil {
		return errors.Wrapf(err, "validating grpc advertise address")
	}
	c.AdvertiseGRPC = schemeHostPortString("grpc", grpcAdvHost, grpcAdvPort)

	// Validate the gRPC listen address.
	_, grpcListenHost, grpcListenPort, err := validateListenAddr(ctx, c.BindGRPC, defaultBindGRPCPort)
	if err != nil {
		return errors.Wrap(err, "validating grpc listen address")
	}
	c.BindGRPC = schemeHostPortString("grpc", grpcListenHost, grpcListenPort)

	return nil
}

// validateAdvertiseAddr validates and normalizes an address accessible
// Ensures that if the "host" part is empty, it gets filled in with
// the configured listen address if any, otherwise it makes a best
// guess at the outbound IP address.
// Returns scheme, host, port as strings.
func validateAdvertiseAddr(ctx context.Context, advAddr, listenAddr, defaultPort string) (string, string, string, error) {
	listenScheme, listenHost, listenPort, err := splitAddr(listenAddr, defaultPort)
	if err != nil {
		return "", "", "", errors.Wrap(err, "getting listen address")
	}

	advScheme, advHostPort := splitScheme(advAddr)
	advHost, advPort := "", ""
	if advHostPort != "" {
		var err error
		advHost, advPort, err = net.SplitHostPort(advHostPort)
		if err != nil {
			return "", "", "", errors.Wrapf(err, "splitting host port: %s", advHostPort)
		}
	}
	// If no advertise scheme was specified, use the one from
	// the listen address.
	if advScheme == "" {
		advScheme = listenScheme
	}
	// If there was no port number, reuse the one from the listen
	// address.
	if advPort == "" || advPort == "0" {
		advPort = listenPort
	}
	// Resolve non-numeric to numeric.
	portNumber, err := net.DefaultResolver.LookupPort(ctx, "tcp", advPort)
	if err != nil {
		return "", "", "", errors.Wrapf(err, "looking up non-numeric port: %v", advPort)
	}
	advPort = strconv.Itoa(portNumber)

	// If the advertise host is empty, then we have two cases.
	if advHost == "" {
		if listenHost == "0.0.0.0" {
			advHost = outboundIP().String()
		} else {
			advHost = listenHost
		}
	}
	return advScheme, advHost, advPort, nil
}

// outboundIP gets the preferred outbound ip of this machine.
func outboundIP() net.IP {
	// This is not actually making a connection to 8.8.8.8.
	// net.Dial() selects the IP address that would be used
	// if an actual connection to 8.8.8.8 were made, so this
	// choice of address is just meant to ensure that an
	// external address is returned (as opposed to a local
	// address like 127.0.0.1).
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

// validateListenAddr validates and normalizes an address suitable for
// use with net.Listen(). This accepts an empty "host" part to signify
// the default (localhost) should be used. Rresolves host names to IP
// addresses.
// Returns scheme, host, port as strings.
func validateListenAddr(ctx context.Context, addr, defaultPort string) (string, string, string, error) {
	scheme, host, port, err := splitAddr(addr, defaultPort)
	if err != nil {
		return "", "", "", errors.Wrap(err, "getting listen address")
	}
	rHost, rPort, err := resolveAddr(ctx, host, port)
	if err != nil {
		return "", "", "", errors.Wrap(err, "resolving address")
	}
	return scheme, rHost, rPort, nil
}

// splitScheme returns two strings: the scheme and the hostPort.
func splitScheme(addr string) (string, string) {
	parts := strings.SplitN(addr, "://", 2)
	if len(parts) == 1 {
		return "", addr
	}
	return parts[0], parts[1]
}

func schemeHostPortString(scheme, host, port string) string {
	var s string
	if scheme != "" {
		s += fmt.Sprintf("%s://", scheme)
	}
	return s + net.JoinHostPort(host, port)
}

// splitAddr returns scheme, host, port as strings.
func splitAddr(addr string, defaultPort string) (string, string, string, error) {
	scheme, hostPort := splitScheme(addr)
	host, port := "", ""
	if hostPort != "" {
		var err error
		host, port, err = net.SplitHostPort(hostPort)
		if err != nil {
			return "", "", "", errors.Wrapf(err, "splitting host port: %s", hostPort)
		}
	}
	// It's not ideal to have a default here, but the alterative
	// results in a port of 0, which causes Pilosa to listen on
	// a random port.
	if port == "" {
		port = defaultPort
	}
	return scheme, host, port, nil
}

// resolveAddr resolves non-numeric addresses to numeric (IP, port) addresses.
func resolveAddr(ctx context.Context, host, port string) (string, string, error) {
	resolver := net.DefaultResolver

	// Resolve the port number. This may translate service names
	// e.g. "postgresql" to a numeric value.
	portNumber, err := resolver.LookupPort(ctx, "tcp", port)
	if err != nil {
		return "", "", errors.Wrapf(err, "resolving up port: %v", port)
	}
	port = strconv.Itoa(portNumber)

	// Resolve the address.
	if host == "" || host == "localhost" {
		return host, port, nil
	}

	addr, err := lookupAddr(ctx, resolver, host)
	if err != nil {
		return "", "", errors.Wrap(err, "looking up address")
	}
	return addr, port, nil
}

// lookupAddr resolves the given address/host to an IP address. If
// multiple addresses are resolved, it returns the first IPv4 address
// available if there is one, otherwise the first address.
func lookupAddr(ctx context.Context, resolver *net.Resolver, host string) (string, error) {
	// Resolve the IP address or hostname to an IP address.
	addrs, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", errors.Wrap(err, "looking up IP addresses")
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("cannot resolve %q to an address", host)
	}

	// LookupIPAddr() can return a mix of IPv6 and IPv4
	// addresses. Return the first IPv4 address if possible.
	for _, addr := range addrs {
		if ip := addr.IP.To4(); ip != nil {
			return ip.String(), nil
		}
	}

	// No IPv4 address, return the first resolved address instead.
	return addrs[0].String(), nil
}
