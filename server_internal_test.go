// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/storage"
	"github.com/molecula/featurebase/v3/testhook"
)

func TestMonitorAntiEntropyZero(t *testing.T) {

	td, err := testhook.TempDirInDir(t, *TempDir, "")
	if err != nil {
		t.Fatalf("getting temp dir: %v", err)
	}
	cfg := &storage.Config{FsyncEnabled: false, Backend: storage.DefaultBackend}
	s, err := NewServer(OptServerDataDir(td),
		OptServerAntiEntropyInterval(0), OptServerStorageConfig(cfg))
	if err != nil {
		t.Fatalf("making new server: %v", err)
	}
	defer s.Close()

	ch := make(chan struct{})
	go func() {
		s.monitorAntiEntropy()
		close(ch)
	}()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("monitorAntiEntropy should have returned immediately with duration 0")
	}
}
