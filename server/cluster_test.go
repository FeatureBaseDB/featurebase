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

package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/test"
	"github.com/pilosa/pilosa/v2/test/port"
	"golang.org/x/sync/errgroup"
)

// Ensure program can send/receive broadcast messages.
func TestMain_SendReceiveMessage(t *testing.T) {
	ms := test.MustRunCluster(t, 2)
	m0, m1 := ms.GetNode(0), ms.GetNode(1)
	defer ms.Close()

	// Expected indexes and Fields
	expected := map[string][]string{
		"i": {"f"},
	}

	// Create a client for each node.
	client0 := m0.Client()
	client1 := m1.Client()

	// Create indexes and fields on one node.
	if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
		t.Fatal(err)
	}

	// Make sure node0 knows about the index and field created.
	schema0, err := client0.Schema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	received0 := map[string][]string{}
	for _, idx := range schema0 {
		received0[idx.Name] = []string{}
		for _, field := range idx.Fields {
			received0[idx.Name] = append(received0[idx.Name], field.Name)
		}
	}
	if !reflect.DeepEqual(received0, expected) {
		t.Fatalf("unexpected schema on node0: %s", received0)
	}

	// Make sure node1 knows about the index and field created.
	schema1, err := client1.Schema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	received1 := map[string][]string{}
	for _, idx := range schema1 {
		received1[idx.Name] = []string{}
		for _, field := range idx.Fields {
			received1[idx.Name] = append(received1[idx.Name], field.Name)
		}
	}
	if !reflect.DeepEqual(received1, expected) {
		t.Fatalf("unexpected schema on node1: %s", received1)
	}

	// Write data on first node.
	if _, err := m0.Query(t, "i", "", fmt.Sprintf(`
            Set(1, f=1)
            Set(%d, f=1)
        `, 2*pilosa.ShardWidth+1)); err != nil {
		t.Fatal(err)
	}

	// We have to wait for the broadcast message to be sent before checking state.
	time.Sleep(1 * time.Second)

	// Make sure node0 knows about the latest MaxShard.
	maxShards0, err := client0.MaxShardByIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if maxShards0["i"] != 2 {
		t.Fatalf("unexpected maxShard on node0: %d", maxShards0["i"])
	}

	// Make sure node1 knows about the latest MaxShard.
	maxShards1, err := client1.MaxShardByIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if maxShards1["i"] != 2 {
		t.Fatalf("unexpected maxShard on node1: %d", maxShards1["i"])
	}
}

// Ensure that an empty node comes up in a NORMAL state.
func TestClusterResize_EmptyNode(t *testing.T) {
	m0 := test.RunCommand(t)
	defer m0.Close()

	state0, err := m0.API.State()
	if err != nil || state0 != string(pilosa.ClusterStateNormal) {
		t.Fatalf("unexpected cluster state: %s, error: %v", state0, err)
	}
}

// Ensure that a cluster of empty nodes comes up in a NORMAL state.
func TestClusterResize_EmptyNodes(t *testing.T) {
	clus := test.MustRunCluster(t, 2)
	defer clus.Close()

	state0, err0 := clus.GetNode(0).API.State()
	state1, err1 := clus.GetNode(1).API.State()
	if err0 != nil || state0 != string(pilosa.ClusterStateNormal) {
		t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
	} else if err1 != nil || state1 != string(pilosa.ClusterStateNormal) {
		t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
	}
}

// Ensure that adding a node correctly resizes the cluster.
func TestClusterResize_AddNode(t *testing.T) {
	// Why are we skipping this test under blue-green with Roaring?
	//
	// We see red test: during resize during importRoaringBits
	// PILOSA_STORAGE_BACKEND=rbf_roaring go test -v  -tags=' shardwidth20'  "-gcflags=all=-d=checkptr=0" -run TestClusterResize_AddNode/"ContinuousShards"
	// green:
	// PILOSA_STORAGE_BACKEND=roaring_rbf go test -v  -tags=' shardwidth20'  "-gcflags=all=-d=checkptr=0" -run TestClusterResize_AddNode/"ContinuousShards"
	//
	// but rbf_badger and badger_rbf are both green (use the same data values for containers).
	//
	// Conclude: roaring reads a different size of data []byte in (due to ops log) bits vs others (RBF, badger), so
	// we can't do blue-green with roaring on this test.
	skipTestUnderBlueGreenWithRoaring(t)

	t.Run("NoData", func(t *testing.T) {
		clus := test.MustRunCluster(t, 2)
		defer clus.Close()

		state0, err0 := clus.GetNode(0).API.State()
		state1, err1 := clus.GetNode(1).API.State()
		if err0 != nil || !test.CheckClusterState(clus.GetNode(0), string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
		} else if err1 != nil || !test.CheckClusterState(clus.GetNode(1), string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
		}
	})
	t.Run("WithIndex", func(t *testing.T) {
		// Configure node0
		m0 := test.MustRunCluster(t, 1).GetNode(0)
		defer m0.Close()

		seed := m0.GossipAddress()

		// Create a client for each node.
		client0 := m0.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
			t.Fatal(err)
		}

		// Configure node1
		m1 := test.NewCommandNode(t)

		m1.Config.Gossip.Seeds = []string{seed}

		if err := port.GetListeners(func(lsns []*net.TCPListener) error {
			portsCfg := test.GenPortsConfig(test.NewPorts(lsns))

			m1.Config.Gossip.Port = portsCfg[0].Gossip.Port
			m1.Config.Etcd = portsCfg[0].Etcd
			m1.Config.Name = portsCfg[0].Name
			m1.Config.Cluster.Name = portsCfg[0].Cluster.Name
			m1.Config.BindGRPC = portsCfg[0].BindGRPC

			return m1.Start()
		}, 4, 10); err != nil {
			t.Fatalf("starting second main: %v", err)
		}
		defer m1.Close()

		state0, err0 := m0.API.State()
		state1, err1 := m1.API.State()
		if err0 != nil || !test.CheckClusterState(m0, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
		} else if err1 != nil || !test.CheckClusterState(m1, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node1 cluster state: %s, error; %v", state1, err1)
		}
	})
	t.Run("ContinuousShards", func(t *testing.T) {

		// Configure node0
		m0 := test.MustRunCluster(t, 1).GetNode(0)
		defer m0.Close()

		seed := m0.GossipAddress()

		// Create a client for each node.
		client0 := m0.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
			t.Fatal(err)
		}

		col := pilosa.ShardWidth + 20

		// Write data on first node.
		if _, err := m0.Queryf(t, "i", "", `
				Set(1, f=1)
				Set(%d, f=1)
			`, col); err != nil {
			t.Fatal(err)
		}
		// exp is the expected result for the Row queries that follow.
		exp := fmt.Sprintf(`{"results":[{"attrs":{},"columns":[1,%d]}]}`, col)

		// Verify the data exists on the single node.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)

		// Configure node1
		m1 := test.NewCommandNode(t)

		m1.Config.Gossip.Seeds = []string{seed}

		if err := port.GetListeners(func(lsns []*net.TCPListener) error {
			portsCfg := test.GenPortsConfig(test.NewPorts(lsns))

			m1.Config.Gossip.Port = portsCfg[0].Gossip.Port
			m1.Config.Etcd = portsCfg[0].Etcd
			m1.Config.Name = portsCfg[0].Name
			m1.Config.Cluster.Name = portsCfg[0].Cluster.Name
			m1.Config.BindGRPC = portsCfg[0].BindGRPC

			return m1.Start()
		}, 4, 10); err != nil {
			t.Fatalf("starting second main: %v", err)
		}
		defer m1.Close()

		state0, err0 := m0.API.State()
		state1, err1 := m1.API.State()
		if err0 != nil || !test.CheckClusterState(m0, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
		} else if err1 != nil || !test.CheckClusterState(m1, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
		}

		// Verify the data exists on both nodes.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)
		m1.QueryExpect(t, "i", "", `Row(f=1)`, exp)
	})
	t.Run("OneShard", func(t *testing.T) {
		// Configure node0
		m0 := test.MustRunCluster(t, 1).GetNode(0)
		defer m0.Close()

		seed := m0.GossipAddress()

		// Create a client for each node.
		client0 := m0.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
			t.Fatal(err)
		}

		// Write data on first node.
		if _, err := m0.Query(t, "i", "", `
				Set(1, f=1)
			`); err != nil {
			t.Fatal(err)
		}
		// exp is the expected result for the Row queries that follow.
		exp := `{"results":[{"attrs":{},"columns":[1]}]}`

		// Verify the data exists on the single node.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)

		// Configure node1
		m1 := test.NewCommandNode(t)
		m1.Config.Gossip.Seeds = []string{seed}

		if err := port.GetListeners(func(lsns []*net.TCPListener) error {
			portsCfg := test.GenPortsConfig(test.NewPorts(lsns))

			m1.Config.Gossip.Port = portsCfg[0].Gossip.Port
			m1.Config.Etcd = portsCfg[0].Etcd
			m1.Config.Name = portsCfg[0].Name
			m1.Config.Cluster.Name = portsCfg[0].Cluster.Name
			m1.Config.BindGRPC = portsCfg[0].BindGRPC

			return m1.Start()
		}, 4, 10); err != nil {
			t.Fatalf("starting second main: %v", err)
		}
		defer m1.Close()

		state0, err0 := m0.API.State()
		state1, err1 := m1.API.State()
		if err0 != nil || !test.CheckClusterState(m0, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
		} else if err1 != nil || !test.CheckClusterState(m1, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
		}

		// Verify the data exists on both nodes.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)
		m1.QueryExpect(t, "i", "", `Row(f=1)`, exp)
	})
	t.Run("SkippedShard", func(t *testing.T) {
		// same reason as the ContinuousShards test above.

		// Configure node0
		m0 := test.MustRunCluster(t, 1).GetNode(0)
		defer m0.Close()

		seed := m0.GossipAddress()

		// Create a client for each node.
		client0 := m0.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
			t.Fatal(err)
		}

		col := pilosa.ShardWidth*2 + 20

		// Write data on first node. Note that no data is placed on shard 1.
		if _, err := m0.Queryf(t, "i", "", `
				Set(1, f=1)
				Set(%d, f=1)
			`, col); err != nil {
			t.Fatal(err)
		}

		// exp is the expected result for the Row queries that follow.
		exp := fmt.Sprintf(`{"results":[{"attrs":{},"columns":[1,%d]}]}`, col)

		// Verify the data exists on the single node.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)

		// Configure node1
		m1 := test.NewCommandNode(t)
		m1.Config.Gossip.Seeds = []string{seed}

		if err := port.GetListeners(func(lsns []*net.TCPListener) error {
			portsCfg := test.GenPortsConfig(test.NewPorts(lsns))

			m1.Config.Gossip.Port = portsCfg[0].Gossip.Port
			m1.Config.Etcd = portsCfg[0].Etcd
			m1.Config.Name = portsCfg[0].Name
			m1.Config.Cluster.Name = portsCfg[0].Cluster.Name
			m1.Config.BindGRPC = portsCfg[0].BindGRPC

			return m1.Start()
		}, 4, 10); err != nil {
			t.Fatalf("starting second main: %v", err)
		}

		defer m1.Close()

		state0, err0 := m0.API.State()
		state1, err1 := m1.API.State()
		if err0 != nil || !test.CheckClusterState(m0, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
		} else if err1 != nil || !test.CheckClusterState(m1, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
		}

		// Verify the data exists on both nodes.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)
		m1.QueryExpect(t, "i", "", `Row(f=1)`, exp)
	})
}

// Ensure that adding a node correctly resizes the cluster.
func TestClusterResize_AddNodeConcurrentIndex(t *testing.T) {
	skipTestUnderBlueGreenWithRoaring(t)

	t.Run("WithIndex", func(t *testing.T) {
		// Configure node0
		m0 := test.MustRunCluster(t, 1).GetNode(0)
		defer m0.Close()

		seed := m0.GossipAddress()

		// Create a client for each node.
		client0 := m0.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
			t.Fatal(err)
		}

		errc := make(chan error)
		go func() {
			_, err := m0.API.CreateIndex(context.Background(), "blah", pilosa.IndexOptions{})
			errc <- err
		}()

		// Configure node1
		m1 := test.NewCommandNode(t)
		m1.Config.Gossip.Seeds = []string{seed}
		if err := port.GetListeners(func(lsns []*net.TCPListener) error {
			portsCfg := test.GenPortsConfig(test.NewPorts(lsns))

			m1.Config.Gossip.Port = portsCfg[0].Gossip.Port
			m1.Config.Etcd = portsCfg[0].Etcd
			m1.Config.Name = portsCfg[0].Name
			m1.Config.Cluster.Name = portsCfg[0].Cluster.Name
			m1.Config.BindGRPC = portsCfg[0].BindGRPC
			return m1.Start()
		}, 4, 10); err != nil {
			t.Fatalf("starting second main: %v", err)
		}
		defer m1.Close()

		state0, err0 := m0.API.State()
		state1, err1 := m1.API.State()
		if err0 != nil || !test.CheckClusterState(m0, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
		} else if err1 != nil || !test.CheckClusterState(m1, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
		}

		if err := <-errc; err != nil {
			t.Fatalf("error from index creation: %v", err)
		}
	})
	t.Run("ContinuousShards", func(t *testing.T) {
		// Configure node0
		m0 := test.MustRunCluster(t, 1).GetNode(0)
		defer m0.Close()

		seed := m0.GossipAddress()

		// Create a client for each node.
		client0 := m0.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
			t.Fatal(err)
		}

		col := pilosa.ShardWidth + 20

		// Write data on first node.
		if _, err := m0.Queryf(t, "i", "", `
				Set(1, f=1)
				Set(%d, f=1)
			`, col); err != nil {
			t.Fatal(err)
		}

		// exp is the expected result for the Row queries that follow.
		exp := fmt.Sprintf(`{"results":[{"attrs":{},"columns":[1,%d]}]}`, col)

		// Verify the data exists on the single node.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)

		// Configure node1
		m1 := test.NewCommandNode(t)
		m1.Config.Gossip.Seeds = []string{seed}
		if err := port.GetListeners(func(lsns []*net.TCPListener) error {
			portsCfg := test.GenPortsConfig(test.NewPorts(lsns))

			m1.Config.Gossip.Port = portsCfg[0].Gossip.Port
			m1.Config.Etcd = portsCfg[0].Etcd
			m1.Config.Name = portsCfg[0].Name
			m1.Config.Cluster.Name = portsCfg[0].Cluster.Name
			m1.Config.BindGRPC = portsCfg[0].BindGRPC
			return m1.Start()
		}, 4, 10); err != nil {
			t.Fatalf("starting second main: %v", err)
		}
		errc := make(chan error, 1)
		go func() {
			_, err := m0.API.CreateIndex(context.Background(), "blah", pilosa.IndexOptions{})
			errc <- err
		}()
		defer m1.Close()

		state0, err0 := m0.API.State()
		state1, err1 := m1.API.State()
		if err0 != nil || !test.CheckClusterState(m0, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
		} else if err1 != nil || !test.CheckClusterState(m1, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
		}

		// Verify the data exists on both nodes.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)
		m1.QueryExpect(t, "i", "", `Row(f=1)`, exp)
	})
	t.Run("SkippedShard", func(t *testing.T) {

		// Configure node0
		m0 := test.MustRunCluster(t, 1).GetNode(0)
		defer m0.Close()

		seed := m0.GossipAddress()

		// Create a client for each node.
		client0 := m0.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
			t.Fatal(err)
		}

		col := pilosa.ShardWidth*2 + 20

		// Write data on first node. Note that no data is placed on shard 1.
		if _, err := m0.Queryf(t, "i", "", `
				Set(1, f=1)
				Set(%d, f=1)
			`, col); err != nil {
			t.Fatal(err)
		}

		// exp is the expected result for the Row queries that follow.
		exp := fmt.Sprintf(`{"results":[{"attrs":{},"columns":[1,%d]}]}`, col)

		// Verify the data exists on the single node.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)

		// Configure node1
		m1 := test.NewCommandNode(t)
		m1.Config.Gossip.Seeds = []string{seed}
		if err := port.GetListeners(func(lsns []*net.TCPListener) error {
			portsCfg := test.GenPortsConfig(test.NewPorts(lsns))

			m1.Config.Gossip.Port = portsCfg[0].Gossip.Port
			m1.Config.Etcd = portsCfg[0].Etcd
			m1.Config.Name = portsCfg[0].Name
			m1.Config.Cluster.Name = portsCfg[0].Cluster.Name
			m1.Config.BindGRPC = portsCfg[0].BindGRPC

			errc := make(chan error, 1)
			go func() {
				_, err := m0.API.CreateIndex(context.Background(), "blah", pilosa.IndexOptions{})
				errc <- err
			}()
			return m1.Start()
		}, 4, 10); err != nil {
			t.Fatalf("starting second main: %v", err)
		}
		defer m1.Close()

		state0, err0 := m0.API.State()
		state1, err1 := m1.API.State()
		if err0 != nil || !test.CheckClusterState(m0, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
		} else if err1 != nil || !test.CheckClusterState(m1, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
		}

		// Verify the data exists on both nodes.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)
		m1.QueryExpect(t, "i", "", `Row(f=1)`, exp)
	})
	t.Run("WithIndexKeys", func(t *testing.T) {
		// Configure node0
		m0 := test.MustRunCluster(t, 1).GetNode(0)
		defer m0.Close()

		seed := m0.GossipAddress()

		// Create a client for each node.
		client0 := m0.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{Keys: true}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
			t.Fatal(err)
		}

		// Write data on first node.
		if _, err := m0.Query(t, "i", "", `
				Set('col1', f=1)
				Set('col2', f=1)
			`); err != nil {
			t.Fatal(err)
		}

		// exp is the expected result for the Row queries that follow.
		exp := `{"results":[{"attrs":{},"columns":[],"keys":["col2","col1"]}]}`

		// Verify the data exists on the single node.
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)

		// Configure node1
		m1 := test.NewCommandNode(t)
		m1.Config.Gossip.Seeds = []string{seed}
		if err := port.GetListeners(func(lsns []*net.TCPListener) error {
			portsCfg := test.GenPortsConfig(test.NewPorts(lsns))

			m1.Config.Gossip.Port = portsCfg[0].Gossip.Port
			m1.Config.Etcd = portsCfg[0].Etcd
			m1.Config.Name = portsCfg[0].Name
			m1.Config.Cluster.Name = portsCfg[0].Cluster.Name
			m1.Config.BindGRPC = portsCfg[0].BindGRPC

			errc := make(chan error, 1)
			go func() {
				_, err := m0.API.CreateIndex(context.Background(), "blah", pilosa.IndexOptions{})
				errc <- err
			}()
			return m1.Start()
		}, 4, 10); err != nil {
			t.Fatalf("starting second main: %v", err)
		}

		state0, err0 := m0.API.State()
		state1, err1 := m1.API.State()
		if err0 != nil || !test.CheckClusterState(m0, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
		} else if err1 != nil || !test.CheckClusterState(m1, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
		}
		m0.QueryExpect(t, "i", "", `Row(f=1)`, exp)
		m1.QueryExpect(t, "i", "", `Row(f=1)`, exp)
	})
}

// Ensure that redundant gossip seeds are used
func TestCluster_GossipMembership(t *testing.T) {
	t.Skip("skipping gossip test")
	t.Run("Node0Down", func(t *testing.T) {
		// Configure node0
		m0 := test.MustRunCluster(t, 1).GetNode(0)
		defer m0.Close()

		seed := m0.GossipAddress()

		var eg errgroup.Group

		// Configure node1
		m1 := test.NewCommandNode(t)
		defer m1.Close()
		eg.Go(func() error {
			// Pass invalid seed as first in list
			m1.Config.Gossip.Seeds = []string{"http://localhost:8765", seed}
			if err := port.GetPort(func(p int) error {
				m1.Config.Gossip.Port = fmt.Sprintf("%d", p)
				return m1.Start()
			}, 10); err != nil {
				t.Fatalf("starting second main: %v", err)
			}

			return nil
		})

		// Configure node1
		m2 := test.NewCommandNode(t)
		defer m2.Close()
		eg.Go(func() error {
			// Pass invalid seed as first in list
			m2.Config.Gossip.Seeds = []string{seed, "http://localhost:8765"}
			err := port.GetPort(func(p int) error {
				m2.Config.Gossip.Port = fmt.Sprintf("%d", p)
				return m2.Start()
			}, 10)

			if err != nil {
				t.Fatalf("starting second main: %v", err)
			}
			defer m2.Close()
			return nil
		})

		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		state0, err0 := m0.API.State()
		state1, err1 := m1.API.State()
		state2, err2 := m2.API.State()
		if err0 != nil || !test.CheckClusterState(m0, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
		} else if err1 != nil || !test.CheckClusterState(m1, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
		} else if err2 != nil || !test.CheckClusterState(m2, string(pilosa.ClusterStateNormal), 1000) {
			t.Fatalf("unexpected node2 cluster state: %s, error: %v", state2, err2)
		}

		numNodes := len(m0.API.Hosts(context.Background()))
		if numNodes != 3 {
			t.Fatalf("Expected 3 nodes, got %d", numNodes)
		}
	})
}

func TestClusterResize_RemoveNode(t *testing.T) {
	cluster := test.MustRunCluster(t, 3)
	defer cluster.Close()
	coord := cluster.GetCoordinator()
	other := cluster.GetNonCoordinator()

	mustNodeID := func(baseURL string) string {
		body := test.Do(t, "GET", fmt.Sprintf("%s/status", baseURL), "").Body
		var resp map[string]interface{}
		err := json.Unmarshal([]byte(body), &resp)
		if err != nil {
			panic(err)
		}
		if localID, ok := resp["localID"].(string); ok {
			return localID
		}
		panic("localID should be a string")
	}

	t.Run("ErrorRemoveInvalidNode", func(t *testing.T) {
		resp := test.Do(t, "POST", coord.URL()+"/cluster/resize/remove-node", `{"id": "invalid-node-id"}`)
		expBody := "removing node: finding node to remove: node with provided ID does not exist"
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected StatusCode %d but got %d", http.StatusNotFound, resp.StatusCode)
		} else if strings.TrimSpace(resp.Body) != expBody {
			t.Fatalf("expected Body '%s' but got '%s'", expBody, strings.TrimSpace(resp.Body))
		}
	})

	t.Run("ErrorRemoveCoordinator", func(t *testing.T) {
		nodeID := mustNodeID(coord.URL())
		resp := test.Do(t, "POST", coord.URL()+"/cluster/resize/remove-node", fmt.Sprintf(`{"id": "%s"}`, nodeID))

		expBody := "removing node: calling node leave: coordinator cannot be removed; first, make a different node the new coordinator"
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected StatusCode %d but got %d", http.StatusInternalServerError, resp.StatusCode)
		} else if strings.TrimSpace(resp.Body) != expBody {
			t.Fatalf("expected Body '%s' but got '%s'", expBody, strings.TrimSpace(resp.Body))
		}
	})

	t.Run("ErrorRemoveOnNonCoordinator", func(t *testing.T) {
		coordinatorNodeID := mustNodeID(coord.URL())
		nodeID := mustNodeID(other.URL())
		resp := test.Do(t, "POST", other.URL()+"/cluster/resize/remove-node", fmt.Sprintf(`{"id": "%s"}`, nodeID))

		expBody := fmt.Sprintf("removing node: calling node leave: node removal requests are only valid on the coordinator node: %s", coordinatorNodeID)
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected StatusCode %d but got %d", http.StatusInternalServerError, resp.StatusCode)
		} else if strings.TrimSpace(resp.Body) != expBody {
			t.Fatalf("expected Body '%s' but got '%s'", expBody, strings.TrimSpace(resp.Body))
		}
	})

	t.Run("ErrorRemoveWithoutReplicas", func(t *testing.T) {
		client0 := coord.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
			t.Fatal(err)
		}

		// This is an attempt to ensure there is data on both nodes, but is not guaranteed.
		// TODO: Deterministic node IDs would ensure consistent results
		setColumns := ""
		for i := 0; i < 20; i++ {
			setColumns += fmt.Sprintf("Set(%d, f=1) ", i*pilosa.ShardWidth)
		}

		if _, err := coord.Query(t, "i", "", setColumns); err != nil {
			t.Fatal(err)
		}

		nodeID := mustNodeID(other.URL())
		resp := test.Do(t, "POST", coord.URL()+"/cluster/resize/remove-node", fmt.Sprintf(`{"id": "%s"}`, nodeID))
		expBody := "not enough data to perform resize"
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected StatusCode %d but got %d", http.StatusInternalServerError, resp.StatusCode)
		} else if !strings.Contains(resp.Body, expBody) {
			t.Fatalf("expected to contain '%s' but got '%s'", expBody, strings.TrimSpace(resp.Body))
		}
	})
}

func TestClusterMutualTLS(t *testing.T) {
	commandOpts := make([][]server.CommandOption, 3)
	configs := make([]*server.Config, 3)
	for i := range configs {
		conf := server.NewConfig()
		configs[i] = conf
		conf.Bind = "https://localhost:0"
		conf.TLS.CertificatePath = "./testdata/certs/localhost.crt"
		conf.TLS.CertificateKeyPath = "./testdata/certs/localhost.key"
		conf.TLS.CACertPath = "./testdata/certs/pilosa-ca.crt"
		conf.TLS.EnableClientVerification = true
		conf.TLS.SkipVerify = false
		commandOpts[i] = append(commandOpts[i], server.OptCommandConfig(conf))
	}

	cluster := test.MustRunCluster(t, 3, commandOpts...)
	defer cluster.Close()
	m0 := cluster.GetNode(0)

	client0 := m0.Client()
	if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client0.CreateField(context.Background(), "i", "f"); err != nil {
		t.Fatal(err)
	}
}

func skipTestUnderBlueGreenWithRoaring(t *testing.T) {
	src := pilosa.CurrentBackend()
	if strings.Contains(src, "_") {
		if strings.Contains(src, "roaring") {
			t.Skip("skip for roaring blue-green")
		}
	}
}
