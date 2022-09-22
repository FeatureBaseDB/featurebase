package etcd

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
)

// Helper utilities to allow us to create etcd cluster configs
// that can clean up after themselves.

// DirCleaner represents the subset of the testing.TB interface
// we care about, allowing us to take objects which behave like
// that without importing all of testing to get them.
type DirCleaner interface {
	Cleanup(func())
	Fatalf(string, ...interface{})
	Logf(string, ...interface{})
	Name() string
	TempDir() string
}

var unixSocketCounter uint64

// unixSocket returns a url for use in test etcd clusters, referring
// to a file which will be cleaned up after a test completes.
func unixSocket(dc DirCleaner) string {
	count := atomic.AddUint64(&unixSocketCounter, 1)
	addr := fmt.Sprintf("fake:%d", count)
	dc.Cleanup(func() {
		err := os.Remove(addr)

		if err != nil && !os.IsNotExist(err) { // not an error if the socket is not present
			dc.Logf("could not remove '%s', %v", addr, err)
		}
	})
	return fmt.Sprintf("unix://%s", addr)
}

// GenEtcdConfigs generates a set of etcd configs, and an initial cluster
// URL. It also makes temporary etcd dirs. Everything it creates
// is cleaned up by `dc.Cleanup` after the test/benchmark completes. On
// error, it calls `dc.Fatalf`, presumably terminating the test.
func GenEtcdConfigs(dc DirCleaner, n int) (clusterName string, cfgs []Options) {
	allPeerURLs := make([]string, n)
	cfgs = make([]Options, n)
	clusterName = fmt.Sprintf("cluster-%s", dc.Name())
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("server%d", i)
		discoDir := dc.TempDir()
		d, err := os.Open(discoDir)
		if err != nil {
			dc.Fatalf("creating temp directory: %v", err)
		}
		defer d.Close()
		err = d.Chmod(0o700)
		if err != nil {
			dc.Fatalf("fixing temp directory mode: %v", err)
		}
		clientURL := unixSocket(dc)
		peerURL := unixSocket(dc)
		cfgs[i] = Options{
			Name:          name,
			Dir:           discoDir,
			LClientURL:    clientURL,
			AClientURL:    clientURL,
			LPeerURL:      peerURL,
			APeerURL:      peerURL,
			HeartbeatTTL:  60,
			UnsafeNoFsync: true,
		}
		allPeerURLs[i] = fmt.Sprintf("%s=%s", name, peerURL)
	}
	peerURLs := strings.Join(allPeerURLs, ",")
	for i := range cfgs {
		cfgs[i].InitCluster = peerURLs
	}
	return clusterName, cfgs
}
