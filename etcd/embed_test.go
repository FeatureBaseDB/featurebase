package etcd

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/logger"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestRestartEtcd(t *testing.T) {
	cfg := embed.NewConfig()
	cfg.Dir = "default.etcd"
	cfg.EnableGRPCGateway = false
	curl, _ := url.Parse(unixSocket(t))
	// append-in-place to replace any existing URLs with our new URL
	cfg.LPUrls = append(cfg.LPUrls[:0], *curl)
	cfg.APUrls = cfg.LPUrls
	cfg.InitialCluster = "default=" + cfg.LPUrls[0].String()
	curl, _ = url.Parse(unixSocket(t))
	cfg.LCUrls = append(cfg.LCUrls[:0], *curl)
	cfg.ACUrls = cfg.LCUrls
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.Server.ReadyNotify():
		t.Logf("Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		t.Logf("Server took too long to start!")
	}
	e.Server.Stop()
	e.Close()

	e, err = embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.Server.ReadyNotify():
		t.Logf("Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		t.Logf("Server took too long to start!")
	}
	e.Close()
}

func TestParseOptions(t *testing.T) {
	e := &Etcd{options: Options{ClusterURL: "http://foo"}, logger: logger.NewLogfLogger(t)}
	curl, _ := url.Parse(unixSocket(t))
	e.options.LClientURL = curl.String()

	e.options.LPeerURL = "i'm a teapot"
	_, err := e.parseOptions()
	if err == nil {
		t.Fatalf("invalid peer URL should be rejected")
	}
	curl, _ = url.Parse(unixSocket(t))
	e.options.LPeerURL = curl.String()

	e.options.ClusterURL = "http://foo"
	_, err = e.parseOptions()
	if err == nil {
		t.Fatalf("cluster URL should be rejected")
	}
	e.options.ClusterURL = ""
	e.options.InitCluster = "a,b"
	_, err = e.parseOptions()
	if AllowCluster() {
		if err != nil {
			t.Fatalf("expect options parsing to succeed")
		}
	} else {
		t.Logf("no-allow: %v", err)
		if err == nil {
			t.Fatalf("should have failed to parse a multi-node cluster in non-clustered build")
		}
	}

	// verify failure on start with invalid options
	state, err := e.Start(context.Background())
	if err == nil {
		t.Fatalf("should have gotten error starting etcd with invalid options")
	}
	if state != disco.InitialClusterStateNew {
		t.Fatalf("expected cluster state of %q, got %q", disco.InitialClusterStateNew, state)
	}
}
