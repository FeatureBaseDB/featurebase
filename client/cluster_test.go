// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	"testing"

	pnet "github.com/featurebasedb/featurebase/v3/net"
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
