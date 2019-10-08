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

	"github.com/pilosa/pilosa/v2/gossip"
	"github.com/pilosa/pilosa/v2/toml"
	"github.com/pkg/errors"
	jaeger "github.com/uber/jaeger-client-go"
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
	// DataDir is the directory where Pilosa stores both indexed data and
	// running state such as cluster topology information.
	DataDir string `toml:"data-dir"`

	// Bind is the host:port on which Pilosa will listen.
	Bind string `toml:"bind"`

	// Advertise is the address advertised by the server to other nodes
	// in the cluster. It should be reachable by all other nodes and should
	// route to an interface that Bind is listening on.
	Advertise string `toml:"advertise"`

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
	WorkerPoolSize int

	// ImportWorkerPoolSize controls how many goroutines are created for
	// processing importRoaring jobs. Defaults to runtime.NumCPU(). It is
	// intentionally not defined as a flag... only exposed here so
	// that we can limit the size while running tests in CI so we
	// don't exhaust the goroutine limit.
	ImportWorkerPoolSize int

	Cluster struct {
		// Disabled controls whether clustering functionality is enabled.
		Disabled    bool     `toml:"disabled"`
		Coordinator bool     `toml:"coordinator"`
		ReplicaN    int      `toml:"replicas"`
		Hosts       []string `toml:"hosts"`
		// TODO(2.0) move this out of cluster. (why is it here??)
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
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		DataDir:             "~/.pilosa",
		Bind:                ":10101",
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
	}

	// Cluster config.
	c.Cluster.Disabled = false
	c.Cluster.ReplicaN = 1
	c.Cluster.Hosts = []string{}
	c.Cluster.LongQueryTime = toml.Duration(time.Minute)

	// Gossip config.
	c.Gossip.Port = "14000"
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
	c.Metric.PollInterval = toml.Duration(0 * time.Minute)
	c.Metric.Diagnostics = true

	// Tracing config.
	c.Tracing.SamplerType = jaeger.SamplerTypeRemote
	c.Tracing.SamplerParam = 0.001

	c.Profile.BlockRate = 10000000 // 1 sample per 10 ms
	c.Profile.MutexFraction = 100  // 1% sampling

	return c
}

// validateAddrs controls the address fields in the Config object
// and fills in any blanks.
// The addresses fields must be guaranteed by the caller to either be
// completely empty, or have both a host part and a port part
// separated by a colon. In the latter case either can be empty to
// indicate it's left unspecified.
func (cfg *Config) validateAddrs(ctx context.Context) error {
	// Validate the advertise address.
	advScheme, advHost, advPort, err := validateAdvertiseAddr(ctx, cfg.Advertise, cfg.Bind)
	if err != nil {
		return errors.Wrapf(err, "validating advertise address")
	}
	cfg.Advertise = schemeHostPortString(advScheme, advHost, advPort)

	// Validate the listen address.
	listenScheme, listenHost, listenPort, err := validateListenAddr(ctx, cfg.Bind)
	if err != nil {
		return errors.Wrap(err, "validating listen address")
	}
	cfg.Bind = schemeHostPortString(listenScheme, listenHost, listenPort)

	return nil
}

// validateAdvertiseAddr validates and normalizes an address accessible
// Ensures that if the "host" part is empty, it gets filled in with
// the configured listen address if any, otherwise it makes a best
// guess at the outbound IP address.
// Returns scheme, host, port as strings.
func validateAdvertiseAddr(ctx context.Context, advAddr, listenAddr string) (string, string, string, error) {
	listenScheme, listenHost, listenPort, err := splitAddr(listenAddr)
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
func validateListenAddr(ctx context.Context, addr string) (string, string, string, error) {
	scheme, host, port, err := splitAddr(addr)
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
func splitAddr(addr string) (string, string, string, error) {
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
		port = "10101"
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
