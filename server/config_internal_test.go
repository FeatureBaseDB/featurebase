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
	"net"
	"os"
	"strings"
	"testing"
)

type addrs struct{ bind, advertise string }

func TestConfig_validateAddrs(t *testing.T) {

	// Prepare some reference strings that will be checked in the
	// test below.
	outboundAddr := outboundIP().String()
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	hostAddr, err := lookupAddr(context.Background(), net.DefaultResolver, hostname)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(hostAddr, ":") {
		hostAddr = "[" + hostAddr + "]"
	}

	tests := []struct {
		expErr string
		in     addrs
		exp    addrs
	}{
		// Default values; addresses set empty.
		{"",
			addrs{"", ""},
			addrs{":10101", ":10101"}},
		{"",
			addrs{":", ""},
			addrs{":10101", ":10101"}},
		{"",
			addrs{"", ":"},
			addrs{":10101", ":10101"}},
		{"",
			addrs{":", ":"},
			addrs{":10101", ":10101"}},
		// Listener :port.
		{"",
			addrs{":1234", ""},
			addrs{":1234", ":1234"}},
		// Listener with host:port.
		{"",
			addrs{hostAddr + ":10101", ""},
			addrs{hostAddr + ":10101", hostAddr + ":10101"}},
		// Listener with host:.
		{"",
			addrs{hostAddr + ":", ""},
			addrs{hostAddr + ":10101", hostAddr + ":10101"}},
		// Listener with scheme:.
		{"",
			addrs{"http://" + hostAddr + ":", ""},
			addrs{"http://" + hostAddr + ":10101", "http://" + hostAddr + ":10101"}},
		// Listener with localhost:port.
		{"",
			addrs{"localhost:1234", ""},
			addrs{"localhost:1234", "localhost:1234"}},
		// Listener with localhost:.
		{"",
			addrs{"localhost:", ""},
			addrs{"localhost:10101", "localhost:10101"}},
		// Listener and advertise addresses.
		{"",
			addrs{hostAddr + ":1234", hostAddr + ":"},
			addrs{hostAddr + ":1234", hostAddr + ":1234"}},
		// Explicit port number in advertise addr.
		{"",
			addrs{hostAddr + ":1234", hostAddr + ":7890"},
			addrs{hostAddr + ":1234", hostAddr + ":7890"}},
		// Use a non-numeric port number.
		{"",
			addrs{":postgresql", ""},
			addrs{":5432", ":5432"}},
		// Advertise port 0 means reuse listen port.
		{"",
			addrs{":1234", ":0"},
			addrs{":1234", ":1234"}},
		// Listen on all interfaces. Determine advertise address.
		{"",
			addrs{"0.0.0.0:1234", ""},
			addrs{"0.0.0.0:1234", outboundAddr + ":1234"}},
		// Expected errors.

		// Missing port number.
		{"missing port in address",
			addrs{"localhost", ""},
			addrs{}},
		{"missing port in address",
			addrs{":1234", "localhost"},
			addrs{}},
		// Invalid port number.
		{"invalid port",
			addrs{"localhost:-1234", ""},
			addrs{}},
		{"validating advertise address",
			addrs{"localhost:foo", ""},
			addrs{}},
		{"no such host",
			addrs{"333.333.333.333:1234", ""},
			addrs{}},
	}

	for i, test := range tests {
		c := NewConfig()

		c.Bind = test.in.bind
		c.Advertise = test.in.advertise

		err := c.validateAddrs(context.Background())

		if err != nil && test.expErr == "" {
			t.Fatal(err)
		} else if err == nil && test.expErr != "" {
			t.Fatalf("test %d: expected error string to contain %s, but got no error", i, test.expErr)
		} else if err != nil && test.expErr != "" {
			if strings.Contains(err.Error(), test.expErr) {
				continue
			} else {
				t.Fatalf("test %d: expected error string to contain %s, but got %s", i, test.expErr, err.Error())
			}
		}

		if c.Bind != test.exp.bind {
			t.Fatalf("test %d: bind address: expected %s, but got %s", i, test.exp.bind, c.Bind)
		} else if c.Advertise != test.exp.advertise {
			t.Fatalf("test %d: advertise address: expected %s, but got %s", i, test.exp.advertise, c.Advertise)
		}
	}
}
