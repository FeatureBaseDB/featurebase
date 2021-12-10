// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/testhook"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
	"golang.org/x/sync/errgroup"
)

// mustOpenView returns a new instance of View with a temporary path.
func mustOpenView(tb testing.TB, index, field, name string) *view {
	path, err := testhook.TempDirInDir(tb, *TempDir, "pilosa-view-")
	if err != nil {
		PanicOn(err)
	}

	fo := FieldOptions{
		CacheType: DefaultCacheType,
		CacheSize: DefaultCacheSize,
	}

	h := NewHolder(path, mustHolderConfig())
	// h needs an *Index so we can call h.Index() and get Index.Txf, in TestView_DeleteFragment

	cim := &CreateIndexMessage{
		Index:     index,
		CreatedAt: 0,
		Meta:      IndexOptions{},
	}

	idx, err := h.createIndex(cim, false)
	testhook.Cleanup(tb, func() {
		h.Close()
	})
	PanicOn(err)

	v := newView(h, path, index, field, name, fo)
	v.idx = idx
	if err := v.openEmpty(); err != nil {
		PanicOn(err)
	}
	return v
}

// Ensure view can open and retrieve a fragment.
func TestView_DeleteFragment(t *testing.T) {
	v := mustOpenView(t, "i", "f", "v")
	defer v.close()

	shard := uint64(9)

	// Create fragment.
	fragment, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		t.Fatal(err)
	} else if fragment == nil {
		t.Fatal("expected fragment")
	}

	err = v.deleteFragment(shard)
	if err != nil {
		t.Fatal(err)
	}

	if v.Fragment(shard) != nil {
		t.Fatal("fragment still exists in view")
	}

	// Recreate fragment with same shard, verify that the old fragment was not reused.
	fragment2, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		t.Fatal(err)
	} else if fragment == fragment2 {
		t.Fatal("failed to create new fragment")
	}
}

// Ensure that simultaneous attempts to grab a new fragment don't clash even
// if the broadcast operation takes a bit of time.
func TestView_CreateFragmentRace(t *testing.T) {
	var creates errgroup.Group
	v := mustOpenView(t, "i", "f", "v")
	defer v.close()

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
