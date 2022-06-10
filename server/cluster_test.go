// Copyright 2021 Molecula Corp. All rights reserved.
package server_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/server"
	"github.com/molecula/featurebase/v3/test"
)

// Ensure program can send/receive broadcast messages.
func TestMain_SendReceiveMessage(t *testing.T) {
	ms := test.MustRunCluster(t, 3)
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
func TestCluster_EmptyNode(t *testing.T) {
	m0 := test.RunCommand(t)
	defer m0.Close()

	state0, err := m0.API.State()
	if err != nil || state0 != disco.ClusterStateNormal {
		t.Fatalf("unexpected cluster state: %s, error: %v", state0, err)
	}
}

// Ensure that a cluster of empty nodes comes up in a NORMAL state.
// Do not combine this with TestCluster_EmptyNode; for non-clustering
// builds, this test gets skipped but we still want that test.
func TestCluster_EmptyNodes(t *testing.T) {
	clus := test.MustRunCluster(t, 3)
	defer clus.Close()

	state0, err0 := clus.GetNode(0).API.State()
	state1, err1 := clus.GetNode(1).API.State()
	if err0 != nil || state0 != disco.ClusterStateNormal {
		t.Fatalf("unexpected node0 cluster state: %s, error: %v", state0, err0)
	} else if err1 != nil || state1 != disco.ClusterStateNormal {
		t.Fatalf("unexpected node1 cluster state: %s, error: %v", state1, err1)
	}
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
