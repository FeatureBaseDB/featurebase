// Copyright 2021 Molecula Corp. All rights reserved.
package etcd

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/testhook"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver/api/v3client"
	"go.etcd.io/etcd/pkg/types"
)

const initVal = "test"
const newVal = "newValue"

// listenerWithURL builds a TCP listener and corresponding http://localhost:%d
// URL, and returns those. Identical to the copy in /test, except we can't
// import that because it imports us.
func listenerWithURL() (listener *net.TCPListener, url string, err error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return listener, url, err
	}
	listener = l.(*net.TCPListener)
	port := listener.Addr().(*net.TCPAddr).Port
	url = fmt.Sprintf("http://localhost:%d", port)
	return listener, url, err
}

func TestLeasedKv(t *testing.T) {
	cfg := embed.NewConfig()

	clientListener, clientURL, err := listenerWithURL()
	if err != nil {
		t.Fatal(errors.Wrap(err, "creating client listener"))
	}
	peerListener, peerURL, err := listenerWithURL()
	if err != nil {
		t.Fatal(errors.Wrap(err, "creating peer listener"))
	}
	cfg.LPUrls = types.MustNewURLs([]string{peerURL})
	cfg.LPeerSocket = []*net.TCPListener{peerListener}
	cfg.APUrls = types.MustNewURLs([]string{peerURL})
	cfg.LCUrls = types.MustNewURLs([]string{clientURL})
	cfg.LClientSocket = []*net.TCPListener{clientListener}
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
