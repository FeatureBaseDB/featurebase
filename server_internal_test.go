// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/storage"
	"github.com/featurebasedb/featurebase/v3/testhook"
)

func TestMonitorAntiEntropyZero(t *testing.T) {

	td, err := testhook.TempDir(t, "")
	if err != nil {
		t.Fatalf("getting temp dir: %v", err)
	}
	cfg := &storage.Config{FsyncEnabled: false, Backend: storage.DefaultBackend}
	s, err := NewServer(OptServerDataDir(td), OptServerStorageConfig(cfg))
	if err != nil {
		t.Fatalf("making new server: %v", err)
	}
	defer s.Close()

	ch := make(chan struct{})
	go func() {
		close(ch)
	}()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("monitorAntiEntropy should have returned immediately with duration 0")
	}
}

func TestAddToWaitGroup(t *testing.T) {
	// if this test times out / panics we have a problem, otherwise we're fine
	td := t.TempDir()
	cfg := &storage.Config{FsyncEnabled: false, Backend: storage.DefaultBackend}
	s, err := NewServer(OptServerDataDir(td), OptServerStorageConfig(cfg))
	if err != nil {
		t.Fatalf("making new server: %v", err)
	}

	oks := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			oks <- s.addToWaitGroup(1)
			time.Sleep(10 * time.Millisecond)
			defer s.wg.Done()
		}()
	}

	for i := 0; i < 10; i++ {
		ok := <-oks
		if !ok {
			t.Fatalf("unexpected close during WaitGroup add")
		}
	}

	s.Close()
	if ok := s.addToWaitGroup(1); ok {
		t.Fatalf("shouldn't be able to add while server is closing")
	}
}
