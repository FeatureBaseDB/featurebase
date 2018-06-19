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
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

// Ensure program can send/receive broadcast messages.
func TestMain_SendReceiveMessage(t *testing.T) {
	ms := test.MustRunMainWithCluster(t, 2)
	m0, m1 := ms[0], ms[1]
	defer m0.Close()
	defer m1.Close()

	m0.Server.Cluster.SetState(pilosa.ClusterStateNormal)
	m1.Server.Cluster.SetState(pilosa.ClusterStateNormal)

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// Expected indexes and Fields
	expected := map[string][]string{
		"i": []string{"f"},
	}

	// Create a client for each node.
	client0 := m0.Client()
	client1 := m1.Client()

	// Create indexes and fields on one node.
	if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
		t.Fatal(err)
	} else if err := client0.CreateField(context.Background(), "i", "f", pilosa.FieldOptions{}); err != nil {
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
	if _, err := m0.Query("i", "", `
            SetBit(row=1, field="f", col=1)
            SetBit(row=1, field="f", col=2400000)
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
}

// Ensure that an empty node comes up in a NORMAL state.
func TestClusterResize_EmptyNode(t *testing.T) {
	m0 := test.MustRunMain()
	defer m0.Close()

	if m0.Server.Cluster.State() != pilosa.ClusterStateNormal {
		t.Fatalf("unexpected cluster state: %s", m0.Server.Cluster.State())
	}
}

// Ensure that a cluster of empty nodes comes up in a NORMAL state.
func TestClusterResize_EmptyNodes(t *testing.T) {
	// Configure node0
	m0 := test.NewMainWithCluster(true)
	defer m0.Close()

	gossipHost := "localhost"
	gossipPort := 0
	seed, err := m0.RunWithTransport(gossipHost, gossipPort, []string{})
	if err != nil {
		t.Fatal(err)
	}

	// Configure node1
	m1 := test.NewMainWithCluster(false)
	defer m1.Close()

	seed, err = m1.RunWithTransport(gossipHost, gossipPort, []string{seed})
	if err != nil {
		t.Fatal(err)
	}

	if m0.Server.Cluster.State() != pilosa.ClusterStateNormal {
		t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State())
	} else if m1.Server.Cluster.State() != pilosa.ClusterStateNormal {
		t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State())
	}
}

// Ensure that adding a node correctly resizes the cluster.
func TestClusterResize_AddNode(t *testing.T) {
	t.Run("NoData", func(t *testing.T) {
		// Configure node0
		m0 := test.NewMainWithCluster(true)
		defer m0.Close()

		seed, err := m0.RunWithTransport("localhost", 0, []string{})
		if err != nil {
			t.Fatal(err)
		}

		// Configure node1
		m1 := test.NewMainWithCluster(false)
		defer m1.Close()

		var eg errgroup.Group
		eg.Go(func() error {
			_, err = m1.RunWithTransport("localhost", 0, []string{seed})
			if err != nil {
				return err
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		if !checkClusterState(m0.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State())
		} else if !checkClusterState(m1.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State())
		}
	})
	t.Run("WithIndex", func(t *testing.T) {
		// Configure node0
		m0 := test.NewMainWithCluster(true)
		defer m0.Close()

		seed, err := m0.RunWithTransport("localhost", 0, []string{})
		if err != nil {
			t.Fatal(err)
		}

		// Create a client for each node.
		client0 := m0.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f", pilosa.FieldOptions{}); err != nil {
			t.Fatal(err)
		}

		// Configure node1
		m1 := test.NewMainWithCluster(false)
		defer m1.Close()

		var eg errgroup.Group
		eg.Go(func() error {
			_, err = m1.RunWithTransport("localhost", 0, []string{seed})
			if err != nil {
				return err
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		if !checkClusterState(m0.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State())
		} else if !checkClusterState(m1.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State())
		}
	})
	t.Run("ContinuousSlices", func(t *testing.T) {

		// Configure node0
		m0 := test.NewMainWithCluster(true)
		defer m0.Close()

		seed, err := m0.RunWithTransport("localhost", 0, []string{})
		if err != nil {
			t.Fatal(err)
		}

		// Create a client for each node.
		client0 := m0.Client()
		//client1 := m1.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f", pilosa.FieldOptions{}); err != nil {
			t.Fatal(err)
		}

		// Write data on first node.
		if _, err := m0.Query("i", "", `
				SetBit(row=1, field="f", col=1)
				SetBit(row=1, field="f", col=1300000)
			`); err != nil {
			t.Fatal(err)
		}

		// Configure node1
		m1 := test.NewMainWithCluster(false)
		defer m1.Close()

		var eg errgroup.Group
		eg.Go(func() error {
			_, err = m1.RunWithTransport("localhost", 0, []string{seed})
			if err != nil {
				return err
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		if !checkClusterState(m0.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State())
		} else if !checkClusterState(m1.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State())
		}
	})
	t.Run("SkippedSlice", func(t *testing.T) {

		// Configure node0
		m0 := test.NewMainWithCluster(true)
		defer m0.Close()

		seed, err := m0.RunWithTransport("localhost", 0, []string{})
		if err != nil {
			t.Fatal(err)
		}

		// Create a client for each node.
		client0 := m0.Client()
		//client1 := m1.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f", pilosa.FieldOptions{}); err != nil {
			t.Fatal(err)
		}

		// Write data on first node. Note that no data is placed on slice 1.
		if _, err := m0.Query("i", "", `
				SetBit(row=1, field="f", col=1)
				SetBit(row=1, field="f", col=2400000)
			`); err != nil {
			t.Fatal(err)
		}

		// Configure node1
		m1 := test.NewMainWithCluster(false)
		defer m1.Close()

		var eg errgroup.Group
		eg.Go(func() error {
			_, err = m1.RunWithTransport("localhost", 0, []string{seed})
			if err != nil {
				return err
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		if !checkClusterState(m0.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State())
		} else if !checkClusterState(m1.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State())
		}
	})
}

// Ensure that redundant gossip seeds are used
func TestCluster_GossipMembership(t *testing.T) {
	t.Run("Node0Down", func(t *testing.T) {
		// Configure node0
		m0 := test.NewMainWithCluster(true)
		defer m0.Close()

		seed, err := m0.RunWithTransport("localhost", 0, []string{})
		if err != nil {
			t.Fatal(err)
		}

		// Configure node1
		m1 := test.NewMainWithCluster(false)
		defer m1.Close()

		var eg errgroup.Group
		eg.Go(func() error {
			// Pass invalid seed as first in list
			_, err := m1.RunWithTransport("localhost", 0, []string{"http://localhost:8765", seed})
			if err != nil {
				return err
			}
			return nil
		})

		// Configure node2
		m2 := test.NewMainWithCluster(false)
		defer m2.Close()

		eg.Go(func() error {
			// Pass invalid seed as last in list
			_, err := m2.RunWithTransport("localhost", 0, []string{seed, "http://localhost:8765"})
			if err != nil {
				return err
			}
			return nil
		})

		if err := eg.Wait(); err != nil {
			t.Fatal(err)
		}

		if !checkClusterState(m0.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node0 cluster state: %s", m0.Server.Cluster.State())
		} else if !checkClusterState(m1.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node1 cluster state: %s", m1.Server.Cluster.State())
		} else if !checkClusterState(m2.Server.Cluster, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node2 cluster state: %s", m2.Server.Cluster.State())
		}

		numNodes := len(m0.Server.Cluster.Status().Nodes)
		if numNodes != 3 {
			t.Fatalf("Expected 3 nodes, got %d", numNodes)
		}
	})
}

func TestClusterResize_RemoveNode(t *testing.T) {
	cluster := test.MustRunMainWithCluster(t, 3)
	m0 := cluster[0]
	m1 := cluster[1]

	mustNodeID := func(baseURL string) string {
		body := test.MustDo("GET", fmt.Sprintf("%s/status", baseURL), "").Body
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
		resp := test.MustDo("POST", m0.URL()+fmt.Sprintf("/cluster/resize/remove-node"), `{"id": "invalid-node-id"}`)
		expBody := "removing node: finding node to remove: node with provided ID does not exist"
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected StatusCode %d but got %d", http.StatusNotFound, resp.StatusCode)
		} else if strings.TrimSpace(resp.Body) != expBody {
			t.Fatalf("expected Body '%s' but got '%s'", expBody, strings.TrimSpace(resp.Body))
		}
	})

	t.Run("ErrorRemoveCoordinator", func(t *testing.T) {
		nodeID := mustNodeID(m0.URL())
		resp := test.MustDo("POST", m0.URL()+fmt.Sprintf("/cluster/resize/remove-node"), fmt.Sprintf(`{"id": "%s"}`, nodeID))

		expBody := "removing node: calling node leave: coordinator cannot be removed; first, make a different node the new coordinator."
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected StatusCode %d but got %d", http.StatusInternalServerError, resp.StatusCode)
		} else if strings.TrimSpace(resp.Body) != expBody {
			t.Fatalf("expected Body '%s' but got '%s'", expBody, strings.TrimSpace(resp.Body))
		}
	})

	t.Run("ErrorRemoveOnNonCoordinator", func(t *testing.T) {
		coordinatorNodeID := mustNodeID(m0.URL())
		nodeID := mustNodeID(m1.URL())
		resp := test.MustDo("POST", m1.URL()+fmt.Sprintf("/cluster/resize/remove-node"), fmt.Sprintf(`{"id": "%s"}`, nodeID))

		expBody := fmt.Sprintf("removing node: calling node leave: node removal requests are only valid on the coordinator node: %s", coordinatorNodeID)
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected StatusCode %d but got %d", http.StatusInternalServerError, resp.StatusCode)
		} else if strings.TrimSpace(resp.Body) != expBody {
			t.Fatalf("expected Body '%s' but got '%s'", expBody, strings.TrimSpace(resp.Body))
		}
	})

	t.Run("ErrorRemoveWithoutReplicas", func(t *testing.T) {
		client0 := m0.Client()

		// Create indexes and fields on one node.
		if err := client0.CreateIndex(context.Background(), "i", pilosa.IndexOptions{}); err != nil && err != pilosa.ErrIndexExists {
			t.Fatal(err)
		} else if err := client0.CreateField(context.Background(), "i", "f", pilosa.FieldOptions{}); err != nil {
			t.Fatal(err)
		}

		// This is an attempt to ensure there is data on both nodes, but is not guaranteed.
		// TODO: Deterministic node IDs would ensure consistent results
		setColumns := ""
		for i := 0; i < 20; i++ {
			setColumns += fmt.Sprintf("SetBit(row=1, field=\"f\", col=%d) ", i*pilosa.SliceWidth)
		}

		if _, err := m0.Query("i", "", setColumns); err != nil {
			t.Fatal(err)
		}

		nodeID := mustNodeID(m1.URL())
		resp := test.MustDo("POST", m0.URL()+fmt.Sprintf("/cluster/resize/remove-node"), fmt.Sprintf(`{"id": "%s"}`, nodeID))
		expBody := "not enough data to perform resize"
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected StatusCode %d but got %d", http.StatusInternalServerError, resp.StatusCode)
		} else if !strings.Contains(resp.Body, expBody) {
			t.Fatalf("expected to contain '%s' but got '%s'", expBody, strings.TrimSpace(resp.Body))
		}
	})
}

// checkClusterState polls a given cluster for its state until it
// receives a matching state. It polls up to n times before returning.
func checkClusterState(c *pilosa.Cluster, state string, n int) bool {
	for i := 0; i < n; i++ {
		if c.State() == state {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
