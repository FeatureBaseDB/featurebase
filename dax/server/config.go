// Copyright 2021 Molecula Corp. All rights reserved.
package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/dax/queryer"
	"github.com/featurebasedb/featurebase/v3/errors"
	fbserver "github.com/featurebasedb/featurebase/v3/server"
)

const (
	defaultBindPort      = "8080"
	defaultStorageMethod = "boltdb"
)

// Config represents the configuration for the command.
type Config struct {
	// Bind is the host:port on which Pilosa will listen.
	Bind string `toml:"bind"`

	// Advertise is the address advertised by the server to other nodes
	// in the cluster. It should be reachable by all other nodes and should
	// route to an interface that Bind is listening on.
	Advertise string `toml:"advertise"`

	// Seed is used to seed the default rand.Source. If Seed is 0 (i.e. not set)
	// the default rand.Source will be seeded using the current time. Note: this
	// is not very useful at the moment because Table.CreateID() uses package
	// crypto/rand which doesn't honor this seed.
	Seed int64 `toml:"seed"`

	// Verbose toggles verbose logging which can be useful for debugging.
	Verbose bool `toml:"verbose"`

	// LogPath configures where Pilosa will write logs.
	LogPath string `toml:"log-path"`

	Controller ControllerOptions `toml:"controller"`
	Queryer    QueryerOptions    `toml:"queryer"`
	Computer   ComputerOptions   `toml:"computer"`
}

type ControllerOptions struct {
	Run    bool              `toml:"run"`
	Config controller.Config `toml:"config"`
}

type QueryerOptions struct {
	Run    bool           `toml:"run"`
	Config queryer.Config `toml:"config"`
}

type ComputerOptions struct {
	Run    bool            `toml:"run"`
	N      int             `toml:"n"`
	Config fbserver.Config `toml:"config"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		Controller: ControllerOptions{
			Config: controller.Config{
				RegistrationBatchTimeout: time.Second * 3,
				StorageMethod:            defaultStorageMethod,
				StorageEnv:               "development",
				StorageConfigFile:        "",
				SnappingTurtleTimeout:    time.Minute * 3,
			},
		},
		Bind: ":" + defaultBindPort,
		Computer: ComputerOptions{
			Config: *fbserver.NewConfig(),
		},
	}
	return c
}

// MustValidate is carried over from server/config.go; it's just a stubbed out
// no-op for now.
func (c *Config) MustValidate() {
	err := c.validate()
	if err != nil {
		panic(err)
	}
}

func (c *Config) validate() error {
	return nil
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

// splitScheme returns two strings: the scheme and the hostPort.
func splitScheme(addr string) (string, string) {
	parts := strings.SplitN(addr, "://", 2)
	if len(parts) == 1 {
		return "", addr
	}
	return parts[0], parts[1]
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
