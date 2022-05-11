// Copyright 2021 Molecula Corp. All rights reserved.
package etcd

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
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

func TestLeasedKv(t *testing.T) {
	cfg := embed.NewConfig()

	clientURL := pilosa.EtcdUnixSocket(t)
	peerURL := pilosa.EtcdUnixSocket(t)
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

// listenerWithURL builds a TCP listener and corresponding http://localhost:%d
// URL, and returns those.
func listenerWithURL() (listener *net.TCPListener, url string, err error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return listener, url, err
	}
	listener = l.(*net.TCPListener)
	port := listener.Addr().(*net.TCPAddr).Port
	url = fmt.Sprintf("http://localhost:%d", port)
	return listener, url, err
}
