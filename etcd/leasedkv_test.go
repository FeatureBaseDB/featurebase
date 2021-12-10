// Copyright 2021 Molecula Corp. All rights reserved.
package etcd

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/testhook"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver/api/v3client"
)

const initVal = "test"
const newVal = "newValue"

func TestLeasedKv(t *testing.T) {
	cfg := embed.NewConfig()

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

	lkv := newLeasedKV(wrapper, "/test", 1)

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
