// Copyright 2021 Molecula Corp. All rights reserved.
package etcd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/testhook"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	"go.etcd.io/etcd/pkg/types"
)

const initVal = "test"
const newVal = "newValue"

func TestClusterKv(t *testing.T) {
	if !AllowCluster() {
		t.Skip("only testing clusters when clustering is allowed")
	}
	c := NewFakeCluster(t, 3, 2)
	err := c.Start()
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	c.MustAwaitClusterState(disco.ClusterStateDown)
	err = c.BringUp()
	if err != nil {
		t.Fatalf("bringing up cluster: %v", err)
	}
	c.MustAwaitClusterState(disco.ClusterStateNormal)
	_, err = c.Elect()
	if err != nil {
		t.Fatalf("trying to cause election: %v", err)
	}
	ctx := context.TODO()
	c.nodes[0].SetState(ctx, disco.NodeStateStarting)
	c.nodes[1].SetState(ctx, disco.NodeStateStarting)
	c.MustAwaitClusterState(disco.ClusterStateStarting)
	c.nodes[0].SetState(ctx, disco.NodeStateStarted)
	c.nodes[1].SetState(ctx, disco.NodeStateStarted)
	// Two of three nodes are up, one is down, we have 2 replicas, so
	// we should be able to handle reads but not writes, so we're in
	// a Degraded state.
	c.MustAwaitClusterState(disco.ClusterStateDegraded)
	err = c.Stop()
	if err != nil {
		t.Fatalf("stopping cluster: %v", err)
	}
}

func TestLeasedKv(t *testing.T) {
	cfg := embed.NewConfig()

	clientURL := unixSocket(t)
	peerURL := unixSocket(t)
	cfg.LPUrls = types.MustNewURLs([]string{peerURL})
	cfg.APUrls = types.MustNewURLs([]string{peerURL})
	cfg.LCUrls = types.MustNewURLs([]string{clientURL})
	cfg.ACUrls = types.MustNewURLs([]string{clientURL})
	cfg.InitialCluster = cfg.Name + "=" + peerURL

	dir, err := testhook.TempDir(t, "leasedkv-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg.Dir = dir
	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cli := v3client.New(etcd.Server)
	defer func() {
		etcd.Close()
		<-etcd.Server.StopNotify()
		cli.Close()
	}()
	wrapper := &Etcd{e: etcd, cli: cli, logger: logger.NewLogfLogger(t)}

	lkv := newLeasedKV(wrapper, context.TODO(), "/test", 1)

	ctx := context.Background()

	err = lkv.Start(initVal)
	if err != nil {
		t.Fatal(err)
	}

	v, err := lkv.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if v != initVal {
		t.Fatal("obtained value is not the same as expected. Obtained:", v, "Expected:", initVal)
	}

	err = lkv.Set(ctx, "otherValue")
	if err != nil {
		t.Fatal(err)
	}
	err = lkv.Set(ctx, "3")
	if err != nil {
		t.Fatal(err)
	}
	err = lkv.Set(ctx, newVal)
	if err != nil {
		t.Fatal(err)
	}

	v, err = lkv.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if v != newVal {
		t.Fatal("obtained value is not the same as expected. Obtained:", v, "Expected:", newVal)
	}

	time.Sleep(5 * time.Second)

	lkv.Stop()

	// we need to wait to force the lease expiration
	time.Sleep(5 * time.Second)

	_, err = lkv.Get(ctx)
	if err == nil || !errors.Is(err, disco.ErrNoResults) {
		t.Fatal("expected error:", disco.ErrNoResults, "obtained:", err)
	}
}
