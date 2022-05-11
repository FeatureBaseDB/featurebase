package etcd

import (
	"net/url"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestRestartEtcd(t *testing.T) {
	cfg := embed.NewConfig()
	cfg.Dir = "default.etcd"
	curl, _ := url.Parse(pilosa.EtcdUnixSocket(t))
	cfg.LPUrls = append(cfg.LPUrls, *curl)
	curl, _ = url.Parse(pilosa.EtcdUnixSocket(t))
	cfg.LCUrls = append(cfg.LCUrls, *curl)
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
