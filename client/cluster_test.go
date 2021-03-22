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

package client

import (
	"testing"

	pnet "github.com/pilosa/pilosa/v2/net"
)

func TestNewClusterWithHost(t *testing.T) {
	c := NewClusterWithHost(pnet.DefaultURI())
	hosts := c.Hosts()
	if len(hosts) != 1 || !hosts[0].Equals(pnet.DefaultURI()) {
		t.Fail()
	}
}

func TestAddHost(t *testing.T) {
	const addr = "http://localhost:3000"
	c := DefaultCluster()
	if c.Hosts() == nil {
		t.Fatalf("Hosts should not be nil")
	}
	uri, err := pnet.NewURIFromAddress(addr)
	if err != nil {
		t.Fatalf("Cannot parse address")
	}
	target, err := pnet.NewURIFromAddress(addr)
	if err != nil {
		t.Fatalf("Cannot parse address")
	}
	c.AddHost(uri)
	hosts := c.Hosts()
	if len(hosts) != 1 || !hosts[0].Equals(target) {
		t.Fail()
	}
}

func TestHosts(t *testing.T) {
	c := DefaultCluster()
	if c.Host() != nil {
		t.Fatalf("Hosts with empty cluster should return nil")
	}
	c = NewClusterWithHost(pnet.DefaultURI())
	if !c.Host().Equals(pnet.DefaultURI()) {
		t.Fatalf("Host should return a value if there are hosts in the cluster")
	}
}

func TestRemoveHost(t *testing.T) {
	uri, err := pnet.NewURIFromAddress("index1.pilosa.com:9999")
	if err != nil {
		t.Fatal(err)
	}
	c := NewClusterWithHost(uri)
	if len(c.hosts) != 1 {
		t.Fatalf("The cluster should contain the host")
	}
	uri, err = pnet.NewURIFromAddress("index1.pilosa.com:9999")
	if err != nil {
		t.Fatal(err)
	}
	c.RemoveHost(uri)
	if len(c.Hosts()) != 0 {
		t.Fatalf("The cluster should not contain the host")
	}
}
