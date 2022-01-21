// Copyright 2021 Molecula Corp. All rights reserved.
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	"testing"

	pnet "github.com/molecula/featurebase/v3/net"
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
