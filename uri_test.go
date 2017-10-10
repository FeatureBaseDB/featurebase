// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package pilosa

import "testing"

func TestDefaultURI(t *testing.T) {
	uri := DefaultURI()
	compare(t, uri, "http", "localhost", 10101)
}

func TestURIWithHostPort(t *testing.T) {
	uri, err := NewURIFromHostPort("index1.pilosa.com", 3333)
	if err != nil {
		t.Fatal(err)
	}
	compare(t, uri, "http", "index1.pilosa.com", 3333)
}

func TestURIWithInvalidHostPort(t *testing.T) {
	_, err := NewURIFromHostPort("index?.pilosa.com", 3333)
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func TestNewURIFromAddress(t *testing.T) {
	for _, item := range validFixture() {
		uri, err := NewURIFromAddress(item.address)
		if err != nil {
			t.Fatalf("Can't parse address: %s, %s", item.address, err)
		}
		if uri.Error() != nil {
			t.Fatalf("Valid addresses shouldn't have attached errors")
		}
		if !uri.Valid() {
			t.Fatalf("Valid() should return true for valid addresses")
		}
		compare(t, uri, item.scheme, item.host, item.port)
	}
}

func TestURIFromAddress(t *testing.T) {
	for _, item := range validFixture() {
		uri := URIFromAddress(item.address)
		if uri.Error() != nil {
			t.Fatalf("Can't parse address: %s, %s", item.address, uri.Error())
		}
		if !uri.Valid() {
			t.Fatalf("Valid() should return true for valid addresses")
		}
		compare(t, uri, item.scheme, item.host, item.port)
	}
}

func TestNewURIFromAddressInvalidAddress(t *testing.T) {
	for _, addr := range invalidFixture() {
		uri, err := NewURIFromAddress(addr)
		if err == nil {
			t.Fatalf("Invalid address should return an error: %s", addr)
		}
		if uri.Error() == nil {
			t.Fatalf("Invalid addreseses should have attached errors")
		}
		if uri.Valid() {
			t.Fatalf("Valid() should return false for invalid addresses")
		}
	}
}

func TestURIFromAddressInvalidAddress(t *testing.T) {
	for _, addr := range invalidFixture() {
		uri := URIFromAddress(addr)
		if uri.Error() == nil {
			t.Fatalf("Invalid address should return an error: %s", addr)
		}
		if uri.Valid() {
			t.Fatalf("Valid() should return false for invalid addresses")
		}
	}
}

func TestNormalizedAddress(t *testing.T) {
	uri, err := NewURIFromAddress("http+protobuf://big-data.pilosa.com:6888")
	if err != nil {
		t.Fatalf("Can't parse address")
	}
	if uri.Normalize() != "http://big-data.pilosa.com:6888" {
		t.Fatalf("Normalized address is not normal")
	}
}

func TestEquals(t *testing.T) {
	uri1 := DefaultURI()
	if uri1.Equals(nil) {
		t.Fatalf("URI should not be equal to nil")
	}
	if !uri1.Equals(DefaultURI()) {
		t.Fatalf("URI should be equal to another URI with the same scheme, host and port")
	}
}

func TestSetScheme(t *testing.T) {
	uri := DefaultURI()
	target := "fun"
	err := uri.SetScheme(target)
	if err != nil {
		t.Fatal(err)
	}
	if uri.Scheme() != target {
		t.Fatalf("%s != %s", uri.Scheme(), target)
	}
}

func TestSetHost(t *testing.T) {
	uri := DefaultURI()
	target := "10.20.30.40"
	err := uri.SetHost(target)
	if err != nil {
		t.Fatal(err)
	}
	if uri.Host() != target {
		t.Fatalf("%s != %s", uri.host, target)
	}
}

func TestSetPort(t *testing.T) {
	uri := DefaultURI()
	target := uint16(9999)
	uri.SetPort(target)
	if uri.Port() != target {
		t.Fatalf("%d != %d", uri.port, target)
	}
}

func TestSetInvalidScheme(t *testing.T) {
	uri := DefaultURI()
	err := uri.SetScheme("?invalid")
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestSetInvalidHost(t *testing.T) {
	uri := DefaultURI()
	err := uri.SetHost("index?.pilosa.com")
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestHostPort(t *testing.T) {
	uri, err := NewURIFromHostPort("i.pilosa.com", 15001)
	if err != nil {
		t.Fatal(err)
	}
	target := "i.pilosa.com:15001"
	if uri.HostPort() != target {
		t.Fatalf("%s != %s", uri.HostPort(), target)
	}
}

func compare(t *testing.T, uri *URI, scheme string, host string, port uint16) {
	if uri.Scheme() != scheme {
		t.Fatalf("Scheme does not match: %s != %s", uri.scheme, scheme)
	}
	if uri.Host() != host {
		t.Fatalf("Host does not match: %s != %s", uri.host, host)
	}
	if uri.Port() != port {
		t.Fatalf("Port does not match: %d != %d", uri.port, port)
	}
}

type uriItem struct {
	address string
	scheme  string
	host    string
	port    uint16
}

func validFixture() []uriItem {
	var test = []uriItem{
		{"http+protobuf://index1.pilosa.com:3333", "http+protobuf", "index1.pilosa.com", 3333},
		{"index1.pilosa.com:3333", "http", "index1.pilosa.com", 3333},
		{"https://index1.pilosa.com", "https", "index1.pilosa.com", 10101},
		{"index1.pilosa.com", "http", "index1.pilosa.com", 10101},
		{"https://:3333", "https", "localhost", 3333},
		{":3333", "http", "localhost", 3333},
		{"[::1]", "http", "[::1]", 10101},
		{"[::1]:3333", "http", "[::1]", 3333},
		{"[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]:3333", "http", "[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]", 3333},
		{"https://[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]:3333", "https", "[fd42:4201:f86b:7e09:216:3eff:fefa:ed80]", 3333},
	}
	return test
}

func invalidFixture() []string {
	return []string{"foo:bar", "http://foo:", "foo:", ":bar", "http://pilosa.com:129999999999999999999999993", "fd42:4201:f86b:7e09:216:3eff:fefa:ed80"}
}
