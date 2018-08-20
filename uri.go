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
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var schemeRegexp = regexp.MustCompile("^[+a-z]+$")
var hostRegexp = regexp.MustCompile(`^[0-9a-z.-]+$|^\[[:0-9a-fA-F]+\]$`)
var addressRegexp = regexp.MustCompile(`^(([+a-z]+):\/\/)?([0-9a-z.-]+|\[[:0-9a-fA-F]+\])?(:([0-9]+))?$`)

// URI represents a Pilosa URI.
// A Pilosa URI consists of three parts:
// 1) Scheme: Protocol of the URI. Default: http.
// 2) Host: Hostname or IP URI. Default: localhost. IPv6 addresses should be written in brackets, e.g., `[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]`.
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
	Scheme string `json:"scheme"`
	Host   string `json:"host"`
	Port   uint16 `json:"port"`
}

// defaultURI creates and returns the default URI.
func defaultURI() *URI {
	return &URI{
		Scheme: "http",
		Host:   "localhost",
		Port:   10101,
	}
}

type URIs []URI

func (u URIs) HostPortStrings() []string {
	s := make([]string, len(u))
	for i, a := range u {
		s[i] = a.HostPort()
	}
	return s
}

// NewURIFromHostPort returns a URI with specified host and port.
func NewURIFromHostPort(host string, port uint16) (*URI, error) {
	uri := defaultURI()
	err := uri.setHost(host)
	if err != nil {
		return nil, errors.Wrap(err, "setting uri host")
	}
	uri.SetPort(port)
	return uri, nil
}

// NewURIFromAddress parses the passed address and returns a URI.
func NewURIFromAddress(address string) (*URI, error) {
	return parseAddress(address)
}

// setScheme sets the scheme of this URI.
func (u *URI) setScheme(scheme string) error {
	m := schemeRegexp.FindStringSubmatch(scheme)
	if m == nil {
		return errors.New("invalid scheme")
	}
	u.Scheme = scheme
	return nil
}

// setHost sets the host of this URI.
func (u *URI) setHost(host string) error {
	m := hostRegexp.FindStringSubmatch(host)
	if m == nil {
		return errors.New("invalid host")
	}
	u.Host = host
	return nil
}

// SetPort sets the port of this URI.
func (u *URI) SetPort(port uint16) {
	u.Port = port
}

// HostPort returns `Host:Port`
func (u *URI) HostPort() string {
	// XXX: The following is just to make TestHandler_Status; remove it
	if u == nil {
		return ""
	}
	s := fmt.Sprintf("%s:%d", u.Host, u.Port)
	return s
}

// normalize returns the address in a form usable by a HTTP client.
func (u *URI) normalize() string {
	scheme := u.Scheme
	index := strings.Index(scheme, "+")
	if index >= 0 {
		scheme = scheme[:index]
	}
	return fmt.Sprintf("%s://%s:%d", scheme, u.Host, u.Port)
}

// String returns the address as a string.
func (u URI) String() string {
	return fmt.Sprintf("%s://%s:%d", u.Scheme, u.Host, u.Port)
}

// Path returns URI with path
func (u *URI) Path(path string) string {
	return fmt.Sprintf("%s%s", u.normalize(), path)
}

// The following methods are required to implement pflag Value interface.

// Set sets the uri value.
func (u *URI) Set(value string) error {
	uri, err := NewURIFromAddress(value)
	if err != nil {
		return err
	}
	*u = *uri
	return nil
}

// Type returns the type of a uri.
func (u URI) Type() string {
	return "URI"
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
			return nil, errors.New("converting port string to int")
		}
		if port > 65535 {
			return nil, errors.New("port must be in range 0 - 65535")
		}
	}
	uri = &URI{
		Scheme: scheme,
		Host:   host,
		Port:   uint16(port),
	}
	return uri, nil
}

// MarshalJSON marshals URI into a JSON-encoded byte slice.
func (u *URI) MarshalJSON() ([]byte, error) {
	var output struct {
		Scheme string `json:"scheme,omitempty"`
		Host   string `json:"host,omitempty"`
		Port   uint16 `json:"port,omitempty"`
	}
	output.Scheme = u.Scheme
	output.Host = u.Host
	output.Port = u.Port

	return json.Marshal(output)
}

func (u *URI) UnmarshalJSON(b []byte) error {
	var input struct {
		Scheme string `json:"scheme,omitempty"`
		Host   string `json:"host,omitempty"`
		Port   uint16 `json:"port,omitempty"`
	}
	if err := json.Unmarshal(b, &input); err != nil {
		return err
	}
	u.Scheme = input.Scheme
	u.Host = input.Host
	u.Port = input.Port
	return nil
}
