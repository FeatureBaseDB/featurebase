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

package pilosa

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var addressRegexp = regexp.MustCompile("^(([+a-z]+):\\/\\/)?([0-9a-z.-]+)?(:([0-9]+))?$")

// URI represents a Pilosa URI.
// A Pilosa URI consists of three parts:
// 1) Scheme: Protocol of the URI. Default: http.
// 2) Host: Hostname or IP URI. Default: localhost.
// 3) Port: Port of the URI. Default: 10101.
//
// All parts of the URI are optional. The following are equivalent:
// 	http://localhost:10101
// 	http://localhost
// 	http://:10101
// 	localhost:10101
// 	localhost
// 	:10101
type URI struct {
	scheme string
	host   string
	port   uint16
}

// DefaultURI creates and returns the default URI.
func DefaultURI() *URI {
	return &URI{
		scheme: "http",
		host:   "localhost",
		port:   10101,
	}
}

// NewURIFromHostPort returns a URI with specified host and port.
func NewURIFromHostPort(host string, port uint16) (*URI, error) {
	// TODO: validate host
	return &URI{
		scheme: "http",
		host:   host,
		port:   port,
	}, nil
}

// NewURIFromAddress parses the passed address and returns a URI.
func NewURIFromAddress(address string) (*URI, error) {
	return parseAddress(address)
}

// Scheme returns the scheme of this URI.
func (u *URI) Scheme() string {
	return u.scheme
}

// Host returns the host of this URI.
func (u *URI) Host() string {
	return u.host
}

// Port returns the port of this URI.
func (u *URI) Port() uint16 {
	return u.port
}

// SetPort updates the port
func (u *URI) SetPort(port uint16) {
	u.port = port
}

// Normalize returns the address in a form usable by a HTTP client.
func (u *URI) Normalize() string {
	scheme := u.scheme
	index := strings.Index(scheme, "+")
	if index >= 0 {
		scheme = scheme[:index]
	}
	return fmt.Sprintf("%s://%s:%d", scheme, u.host, u.port)
}

// HostPort returns the address suitable for passing to `net.Listener.Listen`
func (u *URI) HostPort() string {
	return fmt.Sprintf("%s:%d", u.host, u.port)
}

// Equals returns true if the checked URI is equivalent to this URI.
func (u URI) Equals(other *URI) bool {
	if other == nil {
		return false
	}
	return u.scheme == other.scheme &&
		u.host == other.host &&
		u.port == other.port
}

func parseAddress(address string) (uri *URI, err error) {
	m := addressRegexp.FindStringSubmatch(address)
	if m == nil {
		return nil, errors.New("invalid address")
	}
	scheme := "http"
	if m[2] != "" {
		scheme = m[2]
	}
	host := "localhost"
	if m[3] != "" {
		host = m[3]
	}
	var port = 10101
	if m[5] != "" {
		port, err = strconv.Atoi(m[5])
		if err != nil {
			return nil, errors.New("error converting port string to int")
		}
	}
	uri = &URI{
		scheme: scheme,
		host:   host,
		port:   uint16(port),
	}
	return uri, nil
}
