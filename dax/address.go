package dax

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// Address is a string of the form [scheme]://[host]:[port]/[path]
type Address string

// String returns the Address as a string type.
func (a Address) String() string {
	return string(a)
}

// Scheme returns the [scheme] portion of the Address. This may be an empty
// string if Address does not contain a scheme.
func (a Address) Scheme() string {
	return parse(a).scheme
}

// HostPort returns the [host]:[port] portion of the Address; in other words,
// the Address stripped of any scheme and path.
func (a Address) HostPort() string {
	return parse(a).hostPort()
}

// Host returns the [host] portion of the Address.
func (a Address) Host() string {
	return parse(a).host
}

// Port returns the [port] portion of the Address. If the port values is invalid
// or does not exist, the returned value will default to 0.
func (a Address) Port() uint16 {
	return parse(a).port
}

// Path returns the [path] portion of the Address.
func (a Address) Path() string {
	return parse(a).path
}

// OverrideScheme overrides Address's current scheme with the one provided. If
// an empty scheme is provided, OverrideScheme will return just the
// host:port/path.
func (a Address) OverrideScheme(scheme string) string {
	addr := parse(a)
	if scheme == "" {
		return addr.hostPortPath()
	}
	return scheme + "://" + addr.hostPortPath()
}

// WithScheme ensures that the string returned contains the scheme portion of a
// URL. Because an Address may not have a scheme (for example, it could be just
// "host:80"), this method can be applied to an address when it needs to be used
// as a URL. If the address's existing scheme is blank, the default scheme
// provided will be used. If address is blank, the default scheme will not be
// added; i.e. address will remain blank.
func (a Address) WithScheme(dflt string) string {
	// If address is empty, don't add a scheme to it.
	if a == "" {
		return ""
	}

	addr := parse(a)
	if addr.scheme != "" {
		return a.String()
	}
	return dflt + "://" + addr.hostPortPath()
}

type addr struct {
	scheme string
	host   string
	port   uint16
	path   string
}

// parse breaks the address up into scheme://host:port. It currently assumes
// that very rigid structure; in other words, if an address does not follow that
// format, return values may be unexpected.
func parse(a Address) addr {
	var scheme string
	var host string
	var port uint16
	var path string

	aStr := string(a)

	var hostPortPath string
	if parts := strings.Split(aStr, "://"); len(parts) > 1 {
		scheme = parts[0]
		hostPortPath = parts[1]
	} else {
		hostPortPath = aStr
	}

	var hostPort string
	if parts := strings.SplitN(hostPortPath, "/", 2); len(parts) == 2 {
		hostPort = parts[0]
		path = parts[1]
	} else {
		hostPort = parts[0]
	}

	if parts := strings.Split(hostPort, ":"); len(parts) == 2 {
		host = parts[0]
		portStr := parts[1]
		port64, err := strconv.ParseInt(portStr, 10, 32)
		if err == nil {
			port = uint16(port64)
		}
	} else {
		host = hostPort
	}

	return addr{
		scheme: scheme,
		host:   host,
		port:   port,
		path:   path,
	}
}

func (a addr) hostPort() string {
	if a.port == 0 {
		return a.host
	}
	return fmt.Sprintf("%s:%d", a.host, a.port)
}

func (a addr) hostPortPath() string {
	ret := ""
	if a.port == 0 {
		ret = a.host
	} else {
		ret = fmt.Sprintf("%s:%d", a.host, a.port)
	}

	if a.path != "" {
		ret += "/" + a.path
	}

	return ret
}

// Addresses is a sortable slice of Address.
type Addresses []Address

func (a Addresses) Len() int           { return len(a) }
func (a Addresses) Less(i, j int) bool { return a[i] < a[j] }
func (a Addresses) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// AddressManager is an interface for any service which needs to maintain a list
// of addresses, and receive add/remove address requests from other services.
type AddressManager interface {
	AddAddresses(context.Context, ...Address) error
	RemoveAddresses(context.Context, ...Address) error
}

// Ensure type implements interface.
var _ AddressManager = &NopAddressManager{}

// NopAddressManager is a no-op implementation of the AddressManager interface.
type NopAddressManager struct{}

func NewNopAddressManager() *NopAddressManager {
	return &NopAddressManager{}
}

func (a *NopAddressManager) AddAddresses(ctx context.Context, addrs ...Address) error { return nil }

func (a *NopAddressManager) RemoveAddresses(ctx context.Context, addrs ...Address) error { return nil }
