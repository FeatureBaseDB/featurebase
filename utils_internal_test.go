// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"fmt"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/etcd"
	pnet "github.com/molecula/featurebase/v3/net"
	"github.com/molecula/featurebase/v3/testhook"
)

// utilities used by tests

// NewTestCluster returns a cluster with n nodes and uses a mod-based hasher.
func NewTestCluster(tb testing.TB, n int) *cluster {
	if n > 1 && etcd.AllowCluster() {
		tb.Skipf("cluster size %d not supported in unclustered mode", n)
	}
	path, err := testhook.TempDir(tb, "pilosa-cluster-")
	if err != nil {
		panic(err)
	}

	availableShardFileFlushDuration.Set(100 * time.Millisecond)
	c := newCluster()
	c.ReplicaN = 1
	c.Hasher = NewTestModHasher()
	c.Path = path

	nodes := make([]*disco.Node, 0, n)

	for i := 0; i < n; i++ {
		nodes = append(nodes, &disco.Node{
			ID:  fmt.Sprintf("node%d", i),
			URI: NewTestURI("http", fmt.Sprintf("host%d", i), uint16(0)),
		})
	}
	c.noder = disco.NewLocalNoder(nodes)

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

func TestReplaceFirstFromBack(t *testing.T) {
	for name, test := range map[string]struct {
		input       string
		exp         string
		toReplace   string
		replacement string
	}{
		"url": {
			input:       "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/authorize",
			exp:         "https://login.microsoftonline.com/4a137d66-d161-4ae4-b1e6-07e9920874b8/oauth2/v2.0/devicecode",
			toReplace:   "authorize",
			replacement: "devicecode",
		},
		"unicode": {
			input:       "那不是兽人号角",
			exp:         "那是一只兽人号角",
			toReplace:   "不是",
			replacement: "是一只",
		},
		"multiple": {
			input:       "cowscowscowscowscows",
			exp:         "cowscowscowscowscats",
			toReplace:   "cows",
			replacement: "cats",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := ReplaceFirstFromBack(test.input, test.toReplace, test.replacement); got != test.exp {
				t.Fatalf("expected %v, got %v", test.exp, got)
			}
		})
	}
}
