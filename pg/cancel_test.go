// Copyright 2021 Molecula Corp. All rights reserved.
package pg

import (
	"crypto/rand"
	"testing"
)

func TestCancel(t *testing.T) {
	mgr := NewLocalCancellationManager(rand.Reader)
	notify, cancel, token, err := mgr.Token()
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-notify:
		t.Fatal("unexpected cancellation")
	default:
	}

	err = mgr.Cancel(token)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-notify:
	default:
		t.Fatal("cancellation not propogated")
	}

	select {
	case <-notify:
		t.Fatal("unexpected cancellation")
	default:
	}

	err = mgr.Cancel(CancellationToken{PID: -1, Key: -1})
	if err == nil {
		t.Fatal("invalid cancellation completed")
	}

	select {
	case <-notify:
		t.Fatal("unexpected cancellation")
	default:
	}

	cancel()

	select {
	case _, ok := <-notify:
		if ok {
			t.Fatal("unexpected cancellation")
		}
	default:
		t.Fatal("expected cancellation channel to be closed")
	}

	err = mgr.Cancel(token)
	if err == nil {
		t.Fatal("invalid cancellation completed")
	}
}
