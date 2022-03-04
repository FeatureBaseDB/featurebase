// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"fmt"
	"testing"
	"time"

	pnet "github.com/molecula/featurebase/v3/net"
	"github.com/molecula/featurebase/v3/testhook"
	"github.com/molecula/featurebase/v3/topology"
)

// utilities used by tests

// NewTestCluster returns a cluster with n nodes and uses a mod-based hasher.
func NewTestCluster(tb testing.TB, n int) *cluster {
	path, err := testhook.TempDir(tb, "pilosa-cluster-")
	if err != nil {
		panic(err)
	}

	availableShardFileFlushDuration.Set(100 * time.Millisecond)
	c := newCluster()
	c.ReplicaN = 1
	c.Hasher = NewTestModHasher()
	c.Path = path

	for i := 0; i < n; i++ {
		c.noder.AppendNode(&topology.Node{
			ID:  fmt.Sprintf("node%d", i),
			URI: NewTestURI("http", fmt.Sprintf("host%d", i), uint16(0)),
		})
	}

	cNodes := c.noder.Nodes()

	c.Node = cNodes[0]
	return c
}

// NewTestURI is a test URI creator that intentionally swallows errors.
func NewTestURI(scheme, host string, port uint16) pnet.URI {
	uri := pnet.DefaultURI()
	_ = uri.SetScheme(scheme)
	_ = uri.SetHost(host)
	uri.SetPort(port)
	return *uri
}

func NewTestURIFromHostPort(host string, port uint16) pnet.URI {
	uri := pnet.DefaultURI()
	_ = uri.SetHost(host)
	uri.SetPort(port)
	return *uri
}

// ModHasher represents a simple, mod-based hashing.
type TestModHasher struct{}

// NewTestModHasher returns a new instance of ModHasher with n buckets.
func NewTestModHasher() *TestModHasher { return &TestModHasher{} }

func (*TestModHasher) Hash(key uint64, n int) int { return int(key) % n }

func (*TestModHasher) Name() string { return "mod" }

var _ = NewTestClusterWithReplication // happy linter

func NewTestClusterWithReplication(tb testing.TB, nNodes, nReplicas, partitionN int) (c *cluster, cleaner func()) {
	path, err := testhook.TempDir(tb, "pilosa-cluster-")
	if err != nil {
		panic(err)
	}

	// holder
	h := NewHolder(path, mustHolderConfig())

	// cluster
	availableShardFileFlushDuration.Set(100 * time.Millisecond)
	c = newCluster()
	c.holder = h
	c.ReplicaN = nReplicas
	c.Hasher = &topology.Jmphasher{}
	c.Path = path
	c.partitionN = partitionN

	for i := 0; i < nNodes; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		c.noder.AppendNode(&topology.Node{
			ID:  nodeID,
			URI: NewTestURI("http", fmt.Sprintf("host%d", i), uint16(0)),
		})
	}

	cNodes := c.noder.Nodes()

	c.Node = cNodes[0]

	if err := c.holder.Open(); err != nil {
		panic(err)
	}

	return c, func() {
		c.holder.Close()
		c.close()
	}
}
