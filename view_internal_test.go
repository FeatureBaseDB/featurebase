// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

// mustOpenView returns a new instance of View with a temporary path.
func mustOpenView(tb testing.TB) *view {
	_, _, _, v := newTestView(tb)
	if err := v.openEmpty(); err != nil {
		tb.Fatalf("opening empty test view: %v", err)
	}
	return v
}

// Ensure that simultaneous attempts to grab a new fragment don't clash even
// if the broadcast operation takes a bit of time.
func TestView_CreateFragmentRace(t *testing.T) {
	var creates errgroup.Group
	v := mustOpenView(t)

	// Use a broadcaster which intentionally fails.
	v.broadcaster = delayBroadcaster{delay: 10 * time.Millisecond}

	shard := uint64(0)

	creates.Go(func() error {
		_, err := v.CreateFragmentIfNotExists(shard)
		return err
	})
	creates.Go(func() error {
		_, err := v.CreateFragmentIfNotExists(shard)
		return err
	})
	err := creates.Wait()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// delayBroadcaster is a nopBroadcaster with a configurable delay.
type delayBroadcaster struct {
	nopBroadcaster
	delay time.Duration
}

// SendSync is an implementation of Broadcaster SendSync which delays for a
// specified interval before succeeding.
func (d delayBroadcaster) SendSync(Message) error {
	time.Sleep(d.delay)
	return nil
}
