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
	"reflect"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/gossip"
	"github.com/pilosa/pilosa/test"
)

// Ensure program can send/receive broadcast messages.
func TestMain_SendReceiveMessage(t *testing.T) {

	m0 := test.MustRunMain()
	defer m0.Close()

	m1 := test.MustRunMain()
	defer m1.Close()

	// Update cluster config
	m0.Server.Cluster.Nodes = []*pilosa.Node{
		{URI: m0.Server.URI},
		{URI: m1.Server.URI},
	}
	m1.Server.Cluster.Nodes = m0.Server.Cluster.Nodes

	// Configure node0

	// get the host portion of addr to use for binding
	m0.Config.Gossip.Port = "0"
	m0.Config.Gossip.Seed = ""

	m0.Server.Cluster.Coordinator = m0.Server.URI
	m0.Server.Cluster.Topology = &pilosa.Topology{NodeSet: []pilosa.URI{m0.Server.URI, m1.Server.URI}}
	m0.Server.Cluster.EventReceiver = gossip.NewGossipEventReceiver()
	gossipMemberSet0, err := gossip.NewGossipMemberSet(m0.Server.URI.HostPort(), m0.Config, m0.Server)
	if err != nil {
		t.Fatal(err)
	}
	m0.Server.Cluster.MemberSet = gossipMemberSet0
	m0.Server.Broadcaster = m0.Server
	m0.Server.Gossiper = gossipMemberSet0
	m0.Server.Handler.Broadcaster = m0.Server.Broadcaster
	m0.Server.Holder.Broadcaster = m0.Server.Broadcaster
	m0.Server.BroadcastReceiver = gossipMemberSet0

	if err := m0.Server.BroadcastReceiver.Start(m0.Server); err != nil {
		t.Fatal(err)
	}
	// Open Cluster management.
	if err := m0.Server.Cluster.Open(); err != nil {
		t.Fatal(err)
	}

	// Configure node1

	// get the host portion of addr to use for binding
	m1.Config.Gossip.Port = "0"
	m1.Config.Gossip.Seed = gossipMemberSet0.Seed()

	m1.Server.Cluster.Coordinator = m0.Server.URI
	m1.Server.Cluster.EventReceiver = gossip.NewGossipEventReceiver()
	gossipMemberSet1, err := gossip.NewGossipMemberSet(m1.Server.URI.HostPort(), m1.Config, m1.Server)
	if err != nil {
		t.Fatal(err)
	}
	m1.Server.Cluster.MemberSet = gossipMemberSet1
	m1.Server.Broadcaster = m1.Server
	m1.Server.Gossiper = gossipMemberSet1
	m1.Server.Handler.Broadcaster = m1.Server.Broadcaster
	m1.Server.Holder.Broadcaster = m1.Server.Broadcaster
	m1.Server.BroadcastReceiver = gossipMemberSet1

	if err := m1.Server.BroadcastReceiver.Start(m1.Server); err != nil {
		t.Fatal(err)
	}
	// Open Cluster management.
	if err := m1.Server.Cluster.Open(); err != nil {
		t.Fatal(err)
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// Expected indexes and Frames
	expected := map[string][]string{
		"i": []string{"f"},
	}

	// Create a client for each node.
	client0 := m0.Client()
	client1 := m1.Client()

	// Create indexes and frames on one node.
	if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client0.CreateFrame(context.Background(), "i", "f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Make sure node0 knows about the index and frame created.
	schema0, err := client0.Schema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	received0 := map[string][]string{}
	for _, idx := range schema0 {
		received0[idx.Name] = []string{}
		for _, frame := range idx.Frames {
			received0[idx.Name] = append(received0[idx.Name], frame.Name)
		}
	}
	if !reflect.DeepEqual(received0, expected) {
		t.Fatalf("unexpected schema on node0: %s", received0)
	}

	// Make sure node1 knows about the index and frame created.
	schema1, err := client1.Schema(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	received1 := map[string][]string{}
	for _, idx := range schema1 {
		received1[idx.Name] = []string{}
		for _, frame := range idx.Frames {
			received1[idx.Name] = append(received1[idx.Name], frame.Name)
		}
	}
	if !reflect.DeepEqual(received1, expected) {
		t.Fatalf("unexpected schema on node1: %s", received1)
	}

	// Write data on first node.
	if _, err := m0.Query("i", "", `
            SetBit(rowID=1, frame="f", columnID=1)
            SetBit(rowID=1, frame="f", columnID=2400000)
        `); err != nil {
		t.Fatal(err)
	}

	// We have to wait for the broadcast message to be sent before checking state.
	time.Sleep(1 * time.Second)

	// Make sure node0 knows about the latest MaxSlice.
	maxSlices0, err := client0.MaxSliceByIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if maxSlices0["i"] != 2 {
		t.Fatalf("unexpected maxSlice on node0: %d", maxSlices0["i"])
	}

	// Make sure node1 knows about the latest MaxSlice.
	maxSlices1, err := client1.MaxSliceByIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if maxSlices1["i"] != 2 {
		t.Fatalf("unexpected maxSlice on node1: %d", maxSlices1["i"])
	}

	// Write input definition to the first node.
	if _, err := m0.CreateDefinition("i", "test", `{
            "frames": [{"name": "event-time",
                        "options": {
                            "cacheType": "ranked",
                            "timeQuantum": "YMD"
                        }}],
            "fields": [{"name": "columnID",
                        "primaryKey": true
                        }]}
        `); err != nil {
		t.Fatal(err)
	}

	// We have to wait for the broadcast message to be sent before checking state.
	time.Sleep(1 * time.Second)

	frame0 := m0.Server.Holder.Frame("i", "event-time")
	if frame0 == nil {
		t.Fatal("frame not found")
	}
	frame1 := m1.Server.Holder.Frame("i", "event-time")
	if frame1 == nil {
		t.Fatal("frame not found")
	}
}

// Ensure that an empty node comes up in a NORMAL state.
func TestClusterResize_EmptyNode(t *testing.T) {
	m0 := test.MustRunMain()
	defer m0.Close()

	if m0.Server.Cluster.State != pilosa.ClusterStateNormal {
		t.Fatalf("unexpected cluster state: %s", m0.Server.Cluster.State)
	}
}

// Ensure that a cluster of empty nodes comes up in a NORMAL state.
func TestClusterResize_EmptyNodes(t *testing.T) {
	// Configure node0
	m0 := test.NewMain()
	defer m0.Close()

	gossipHost := "localhost"
	gossipPort := 0
	seed, coord, err := m0.RunWithTransport(gossipHost, gossipPort, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Configure node1
	m1 := test.NewMain()
	defer m1.Close()

	seed, coord, err = m1.RunWithTransport(gossipHost, gossipPort, seed, &coord)
	if err != nil {
		t.Fatal(err)
	}

	if m0.Server.Cluster.State != pilosa.ClusterStateNormal {
		t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State)
	} else if m1.Server.Cluster.State != pilosa.ClusterStateNormal {
		t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State)
	}
}

// Ensure that adding a node correctly resizes the cluster.
func TestClusterResize_AddNode(t *testing.T) {
	t.Run("NoData", func(t *testing.T) {
		// Configure node0
		m0 := test.NewMain()
		defer m0.Close()

		seed, coord, err := m0.RunWithTransport("localhost", 0, "", nil)
		if err != nil {
			t.Fatal(err)
		}

		// Configure node1
		m1 := test.NewMain()
		defer m1.Close()

		var eg errgroup.Group
		eg.Go(func() error {
			_, _, err = m1.RunWithTransport("localhost", 0, seed, &coord)
			if err != nil {
				return err
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		time.Sleep(1 * time.Second)

		if m0.Server.Cluster.State != pilosa.ClusterStateNormal {
			t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State)
		} else if m1.Server.Cluster.State != pilosa.ClusterStateNormal {
			t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State)
		}
	})
	t.Run("WithIndex", func(t *testing.T) {
		// Configure node0
		m0 := test.NewMain()
		defer m0.Close()

		seed, coord, err := m0.RunWithTransport("localhost", 0, "", nil)
		if err != nil {
			t.Fatal(err)
		}

		// Create a client for each node.
		client0 := m0.Client()

		// Create indexes and frames on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateFrame(context.Background(), "i", "f", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		}

		// Configure node1
		m1 := test.NewMain()
		defer m1.Close()

		var eg errgroup.Group
		eg.Go(func() error {
			_, _, err = m1.RunWithTransport("localhost", 0, seed, &coord)
			if err != nil {
				return err
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		// Give the cluster time to settle.
		time.Sleep(1 * time.Second)

		if m0.Server.Cluster.State != pilosa.ClusterStateNormal {
			t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State)
		} else if m1.Server.Cluster.State != pilosa.ClusterStateNormal {
			t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State)
		}
	})
	t.Run("ContinuousSlices", func(t *testing.T) {

		// Configure node0
		m0 := test.NewMain()
		defer m0.Close()

		seed, coord, err := m0.RunWithTransport("localhost", 0, "", nil)
		if err != nil {
			t.Fatal(err)
		}

		// Create a client for each node.
		client0 := m0.Client()
		//client1 := m1.Client()

		// Create indexes and frames on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateFrame(context.Background(), "i", "f", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		}

		// Write data on first node.
		if _, err := m0.Query("i", "", `
				SetBit(rowID=1, frame="f", columnID=1)
				SetBit(rowID=1, frame="f", columnID=1300000)
			`); err != nil {
			t.Fatal(err)
		}

		// Configure node1
		m1 := test.NewMain()
		defer m1.Close()

		var eg errgroup.Group
		eg.Go(func() error {
			_, _, err = m1.RunWithTransport("localhost", 0, seed, &coord)
			if err != nil {
				return err
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		// Give the cluster time to settle.
		time.Sleep(1 * time.Second)

		if m0.Server.Cluster.State != pilosa.ClusterStateNormal {
			t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State)
		} else if m1.Server.Cluster.State != pilosa.ClusterStateNormal {
			t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State)
		}
	})
	t.Run("SkippedSlice", func(t *testing.T) {

		// Configure node0
		m0 := test.NewMain()
		defer m0.Close()

		seed, coord, err := m0.RunWithTransport("localhost", 0, "", nil)
		if err != nil {
			t.Fatal(err)
		}

		// Create a client for each node.
		client0 := m0.Client()
		//client1 := m1.Client()

		// Create indexes and frames on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateFrame(context.Background(), "i", "f", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		}

		// Write data on first node. Note that no data is placed on slice 1.
		if _, err := m0.Query("i", "", `
				SetBit(rowID=1, frame="f", columnID=1)
				SetBit(rowID=1, frame="f", columnID=2400000)
			`); err != nil {
			t.Fatal(err)
		}

		// Configure node1
		m1 := test.NewMain()
		defer m1.Close()

		var eg errgroup.Group
		eg.Go(func() error {
			_, _, err = m1.RunWithTransport("localhost", 0, seed, &coord)
			if err != nil {
				return err
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		// Give the cluster time to settle.
		time.Sleep(1 * time.Second)

		if m0.Server.Cluster.State != pilosa.ClusterStateNormal {
			t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State)
		} else if m1.Server.Cluster.State != pilosa.ClusterStateNormal {
			t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State)
		}
	})
}
