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
	"bytes"
	"fmt"
	"testing"
	"time"

	pnet "github.com/molecula/featurebase/v2/net"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/testhook"
	"github.com/molecula/featurebase/v2/topology"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
)

// utilities used by tests

// mustAddR is a helper for calling roaring.Container.Add() in tests to
// keep the linter happy that we are checking the error.
func mustAddR(changed bool, err error) {
	PanicOn(err)
}

// mustRemove is a helper for calling Tx.Remove() in tests to
// keep the linter happy that we are checking the error.
func mustRemove(changeCount int, err error) {
	PanicOn(err)
}

func getTestBitmapAsRawRoaring(bitsToSet ...uint64) []byte {
	b := roaring.NewBitmap()
	changed := b.DirectAddN(bitsToSet...)
	n := len(bitsToSet)
	if changed != n {
		panic(fmt.Sprintf("changed=%v but bitsToSet len = %v", changed, n))
	}
	buf := bytes.NewBuffer(make([]byte, 0, 100000))
	_, err := b.WriteTo(buf)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

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
	h := NewHolder(path, nil)

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
