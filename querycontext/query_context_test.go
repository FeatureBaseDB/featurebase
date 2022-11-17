// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package querycontext

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	rbfcfg "github.com/featurebasedb/featurebase/v3/rbf/cfg"
	"golang.org/x/sync/errgroup"
)

var rbfTestConfig = func() *rbfcfg.Config {
	cfg := rbfcfg.NewDefaultConfig()
	cfg.FsyncEnabled = false
	cfg.MaxSize = (1 << 28)
	cfg.MaxWALSize = (1 << 28)
	return cfg
}()

// testTxStore creates a testTxStoreWrapper with the provided path (relative to a TempDir)
// and KeySplitter, or fails the test. It also registers a cleanup function
// which closes the TxStore, and fails the test if that close doesn't succeed,
// for instance if the test leaves a QueryContext open. Neat, huh!
func testTxStore(tb testing.TB, path string, ks KeySplitter) *testTxStoreWrapper {
	dir := filepath.Join(tb.TempDir(), path)
	txs, err := NewRBFTxStore(dir, rbfTestConfig, ks)
	if err != nil {
		tb.Fatalf("opening TxStore: %v", err)
	}
	tb.Cleanup(func() {
		err := txs.Close()
		if err != nil {
			tb.Errorf("closing TxStore: %v", err)
		}
	})
	return newTestTxStoreWrapper(tb, txs)
}

func TestTxStoreClose(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "foo")
	ctx := context.Background()
	txs, err := NewRBFTxStore(dir, rbfTestConfig, nil)
	if err != nil {
		t.Fatalf("opening TxStore: %v", err)
	}
	qcx, err := txs.NewQueryContext(ctx)
	if err != nil {
		t.Fatalf("creating initial query context: %v", err)
	}
	_, err = qcx.NewWrite("i", "f", "v", 0)
	if err == nil {
		t.Fatalf("should get error requesting a write from a read qcx")
	}
	err = txs.Close() // should fail because of the qcx...
	if err == nil {
		t.Fatalf("should have failed to close TxStore because of open query")
	}
	qcx.Release()
	err = txs.Close()
	if err != nil {
		t.Fatalf("should have closed TxStore successfully")
	}
	qcx, err = txs.NewQueryContext(ctx)
	if err == nil {
		qcx.Release()
		t.Fatalf("should have failed to open query context on closed TxStore")
	}
	qcx, err = txs.NewWriteQueryContext(ctx, txs.Scope())
	if err == nil {
		qcx.Release()
		t.Fatalf("should have failed to open query context on closed TxStore")
	}
	err = txs.Close()
	if err == nil {
		t.Fatalf("should get error on double-close")
	}
}

func TestShardList(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	prev := &shardList{}
	prevSeen := map[ShardID]struct{}{}
	for i := 0; i < 20; i++ {
		sl := &shardList{}
		seen := make(map[ShardID]struct{})
		for j := 0; j < 20; j++ {
			add := ShardID(rng.Intn(20))
			sl.Add(add)
			seen[add] = struct{}{}
		}
		// verify that we have the same set of things in the list directly
		if len(sl.any) != len(seen) {
			t.Fatalf("expected %d entries, got %d: %v vs %d", len(seen), len(sl.any), seen, sl.any)
		}
		for _, v := range sl.any {
			if _, ok := seen[v]; !ok {
				t.Fatalf("found %d in shard list, not seen", v)
			}
		}
		// verify that we neither allow things not included, nor disallow things included
		for j := ShardID(0); j < 20; j++ {
			_, ok1 := seen[j]
			ok2 := sl.Allowed(j)
			if ok1 != ok2 {
				t.Fatalf("seen %t vs allowed %t mismatch for %d", ok1, ok2, j)
			}
		}
		// verify that overlap between our two lists matches overlap between the current "seen"
		// and previous "seen"
		overlap := prev.Overlap(*sl)
		var computedOverlap bool
		for i := range seen {
			if _, ok := prevSeen[i]; ok {
				computedOverlap = true
				break
			}
		}
		if overlap != computedOverlap {
			t.Fatalf("shardlists %#v and %#v: Overlap() %t, computed overlap %t", prev, sl, overlap, computedOverlap)
		}
		// and stash these for next time
		prev = sl
		prevSeen = seen
	}
}

func TestTxStore(t *testing.T) {
	ctx := context.Background()
	txs := testTxStore(t, "foo", nil)
	q, _ := txs.NewQueryContext(ctx)
	_, _ = q.NewRead("i", "f", "v", 0)
	txs.expect(1)
	err := txs.Close()
	if err == nil {
		t.Fatalf("shouldn't close a database while still open")
	}
	q.Release()
	q.expect(1)
	err = q.Commit()
	if err == nil {
		t.Fatalf("shouldn't be able to commit after releasing")
	}
	q, _ = txs.NewWriteQueryContext(ctx, txs.Scope())
	q.Error("oops")
	q.expect(1)
	err = q.Commit()
	if err == nil {
		t.Fatalf("shouldn't have been able to commit with error)")
	}
	nctx, cancel := context.WithCancel(ctx)
	q, _ = txs.NewWriteQueryContext(nctx, txs.Scope())
	cancel()
	q.expect(1)
	err = q.Commit()
	if err == nil {
		t.Fatalf("shouldn't have been able to commit after context cancelled")
	}
}

// writeReq represents a possible write request. some of them will
// actually just read, and don't need to be within write scope.
type writeReq struct {
	index    IndexName
	field    FieldName
	shard    ShardID
	justRead bool
}

// satisfyWriteRequest tries to process the writes in a list of
// writeReqs, by requesting them in order, and returns an error
// if it encounters any errors.
func satisfyWriteRequest(t *testing.T, txs *testTxStoreWrapper, requests []writeReq) error {
	if len(requests) == 0 {
		return nil
	}
	scope := txs.Scope()
	prevIndex := requests[0].index
	prevField := requests[0].field
	shards := []ShardID{requests[0].shard}
	add := func(index IndexName, field FieldName, shards ...ShardID) {
		if field == "" {
			if len(shards) == 1 && shards[0] == 0 {
				scope.AddIndex(index)
			} else {
				scope.AddIndexShards(index, shards...)
			}
		} else {
			if len(shards) == 1 && shards[0] == 0 {
				scope.AddField(index, field)
			} else {
				scope.AddFieldShards(index, field, shards...)
			}
		}
	}
	// batch shards together. this is mostly irrelevant, but if we happened to get
	// shard lists in an unsorted order, and didn't handle that correctly, this would
	// catch that.
	for _, req := range requests[1:] {
		// don't add scope for a request that isn't a write
		if req.justRead {
			continue
		}
		if req.index == prevIndex && req.field == prevField {
			shards = append(shards, req.shard)
		} else {
			add(prevIndex, prevField, shards...)
			shards = append(shards[:0], req.shard)
			prevIndex = req.index
			prevField = req.field
		}
	}
	add(prevIndex, prevField, shards...)
	qcx, err := txs.NewWriteQueryContext(context.Background(), scope)
	if err != nil {
		return err
	}
	defer qcx.Release()
	for _, i := range rand.Perm(len(requests)) {
		req := requests[i]
		if req.justRead {
			qr, err := qcx.NewRead(req.index, req.field, "v", req.shard)
			if err != nil {
				return err
			}
			_, _ = qr.Contains(1)
			if !scope.Allowed(req.index, req.field, "v", req.shard) {
				qcx.expect(1)
				_, err := qcx.NewWrite(req.index, req.field, "v", req.shard)
				if err == nil {
					qcx.Error("write was allowed when it should have been out of scope")
				}
			}
		} else {
			qw, err := qcx.NewWrite(req.index, req.field, "v", req.shard)
			if err != nil {
				return err
			}
			_, _ = qw.Add(1)
		}
	}
	if t.Failed() {
		qcx.Error("an error occurred during read or write ops")
	}
	// When testing this, we're spawning a number of satisfyWriteRequest tasks
	// at once. We want to be checking the overlap case more than we care about
	// the no-overlap case, but we don't want to actually spend *time* sleeping.
	// Gosched() hints that we should let one of the other goroutines start
	// trying to do things, improving the chances of an overlap.
	runtime.Gosched()
	return qcx.Commit()
}

// delayWriteRequest is like satisfyWriteRequest, but doesn't commit
// the write request until the provided channel is closed, allowing us
// to verify that new requests can still happen before this one is closed.
func delayWriteRequest(t *testing.T, txs *testTxStoreWrapper, writeList []writeReq, ch chan struct{}) (func() error, error) {
	writes := txs.Scope()
	for _, write := range writeList {
		writes.AddFieldShards(write.index, write.field, write.shard)
	}
	qcx, err := txs.NewWriteQueryContext(context.Background(), writes)
	if err != nil {
		return nil, err
	}
	for _, i := range rand.Perm(len(writeList)) {
		write := writeList[i]
		qw, err := qcx.NewWrite(write.index, write.field, "v", write.shard)
		if err != nil {
			qcx.Release()
			return nil, err
		}
		qw.Add(1)
	}
	return func() error {
		<-ch
		return qcx.Commit()
	}, nil
}

func testSomeWriteRequests(t *testing.T, writeRequests [][]writeReq) {
	var splitter KeySplitter
	// try some of these with a splitIndexes splitter, some with an indexShard splitter,
	// so they both get tested
	if len(writeRequests)%3 == 1 {
		splitter = &flexibleKeySplitter{splitIndexes: map[IndexName]struct{}{"a": {}}}
	} else {
		splitter = &indexShardKeySplitter{}
	}
	txs := testTxStore(t, "foo", splitter)
	eg := errgroup.Group{}
	for i := range writeRequests {
		req := writeRequests[i]
		eg.Go(func() error {
			return satisfyWriteRequest(t, txs, req)
		})
	}
	err := eg.Wait()
	if err != nil {
		t.Logf("write requests (%d):", len(writeRequests))
		for _, req := range writeRequests {
			t.Logf("> %v", req)
		}
		t.Fatalf("running reqs: %v", err)
	}
}

// overlappingWriteReqs represents a test case for allowing write requests to
// coexist.
type overlappingWriteReqs struct {
	splitter    KeySplitter
	reqs        [][]writeReq
	shouldError bool
}

// testOverlappingWriteRequests takes multiple batches of writeReqs, creates
// a QueryScope from each batch, and tries to open queries with those writes
// simultaneously. If the writeReq batches overlap, this will time out.
//
// the timeout is returned as an error, other failures cause the test to fail.
func testOverlappingWriteRequests(t *testing.T, write overlappingWriteReqs) error {
	txs := testTxStore(t, "foo", write.splitter)
	// we spawn goroutines to open each of these, but wait until doneCh
	// closes before closing any of them, so they're all open at once.
	doneCh := make(chan struct{})
	eg := errgroup.Group{}
	closeFuncs := make(chan func() error, len(write.reqs))
	for i := range write.reqs {
		i := i
		req := write.reqs[i]
		eg.Go(func() (err error) {
			fn, err := delayWriteRequest(t, txs, req, doneCh)
			if err == nil {
				closeFuncs <- fn
			}
			return err
		})
	}
	errCh := make(chan error)
	go func() {
		errCh <- eg.Wait()
		close(errCh)
	}()
	defer func() {
		close(doneCh)
		// we have an obligation to close every QueryContext before we close
		// the TxStore. but we couldn't open them all! So. We check whether
		// we actually got a response from errCh. If that times out, some of
		// the delayWriteRequest calls aren't done yet, which means they haven't
		// updated their entry in closeFuncs. So, we close all the existing
		// QueryContexts, and try again. At least something ought to succeed,
		// but it's possible not everything will, so we keep trying until we
		// actually got them all done, call the remaining closeFuncs, and
		// exit.
		pending := true
		expected := len(write.reqs)
		for pending {
			// wait up to 50ms for errCh to provide a result; if it doesn't,
			// we need to try again
			select {
			case <-errCh:
				pending = false
			case <-time.After(50 * time.Millisecond):
				pending = true
			}
			// run any pending close funcs. If errCh is closed, we should
			// have all of our closeFuncs. If it isn't, we might only have
			// some, because others haven't been created yet. So we grab
			// everything currently available.
			gotClose := true
			for gotClose {
				select {
				case fn := <-closeFuncs:
					if fn != nil {
						if err := fn(); err != nil {
							t.Errorf("closing transaction: %v", err)
						}
					}
					expected--
					if expected == 0 {
						pending = false
					}
				default:
					gotClose = false
				}
			}
		}
	}()
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("write requests (%d):", len(write.reqs))
			for _, req := range write.reqs {
				t.Logf("> %v", req)
			}
			t.Fatalf("running reqs: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		if !write.shouldError {
			// Only log this if we didn't expect an error.
			t.Logf("write requests (%d):", len(write.reqs))
			for _, req := range write.reqs {
				t.Logf("> %v", req)
			}
		}
		return errors.New("timed out trying to create write requests")
	}
	// all the writes should still be pending
	txs.expect(1)
	err := txs.Close()
	if err == nil {
		t.Fatalf("close completed while transactions were pending")
	}

	return nil
}

// This test is here to make sure that the overlap-checking isn't
// insanely aggressive; in theory, these three requests can all coexist at once.
func TestOverlappingWriteRequests(t *testing.T) {
	overlapping := []overlappingWriteReqs{
		{
			splitter: nil,
			reqs: [][]writeReq{
				{{"a", "f", 0, false}},
				{{"b", "f", 0, false}},
				{{"a", "f", 1, false}},
			},
		},
		{
			// we should be able to do two fields in i, but we wouldn't in j.
			splitter: NewFlexibleKeySplitter("i"),
			reqs: [][]writeReq{
				{{"i", "f", 0, false}},
				{{"i", "g", 0, false}},
				{{"j", "f", 0, false}},
			},
		},
		{
			// this should fail
			splitter: NewFlexibleKeySplitter("i"),
			reqs: [][]writeReq{
				{{"i", "f", 0, false}},
				{{"i", "g", 0, false}},
				{{"j", "f", 0, false}},
				{{"j", "g", 0, false}},
			},
			shouldError: true,
		},
		{
			// this should fail more than once
			splitter: NewFlexibleKeySplitter("i"),
			reqs: [][]writeReq{
				{{"i", "f", 0, false}},
				{{"i", "g", 0, false}},
				{{"j", "f", 0, false}},
				{{"j", "g", 0, false}},
				{{"i", "f", 0, false}},
				{{"i", "g", 0, false}},
				{{"i", "f", 0, false}},
			},
			shouldError: true,
		},
	}
	for i, testCase := range overlapping {
		// run these in separate test cases, because testTxStore is closing in
		// t.Cleanup, and we want them wrapped up as we go.
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			err := testOverlappingWriteRequests(t, testCase)
			if testCase.shouldError {
				if err == nil {
					t.Fatalf("expected error for case %d, but didn't get it", i)
				}
			} else {
				if (err != nil) != testCase.shouldError {
					t.Fatalf("case %d: unexpected error %v", i, err)
				}
			}
		})
	}
}

func buildRequestsFromBytes(data []byte) [][]writeReq {
	// To fuzz these, we'll use []byte. each pair of bytes is an index/shard pair, and
	// we break the list into batches of up to 50 such that the sum of their values caps
	// at 256.
	var out [][]writeReq
	var sub []writeReq
	total := 0
	for len(data) >= 2 {
		idx := data[0] % 5
		index := IndexName(rune(idx) + 'a')
		total += int(idx)
		fld := (data[0] / 5) % 3
		field := FieldName(rune(fld) + 'a')
		// generate some index-only requests
		if fld == 0 {
			field = ""
		}
		shard := ShardID(data[1] % 8)
		total += int(shard)
		// some requests are just reads. reads won't be added to our scope, so if there's
		// no corresponding write, they'll be outside of scope, so we're verifying that we
		// can read outside of scope.
		justRead := ((data[1] >> 4) % 4) != 0
		sub = append(sub, writeReq{index, field, shard, justRead})
		if total > 10 {
			out = append(out, sub)
			sub = []writeReq{}
			total = 0
		}
		data = data[2:]
	}
	if len(sub) > 0 {
		out = append(out, sub)
	}
	return out
}

func FuzzWriteRequests(f *testing.F) {
	rng := rand.New(rand.NewSource(3))
	// Add a bit of random data so that we get more coverage even when not fuzzing
	for i := 0; i < 20; i++ {
		seed := make([]byte, rand.Intn(20)+20)
		_, _ = rng.Read(seed)
		f.Add(seed)
	}
	for _, seed := range [][]byte{{0, 4, 0, 5, 1, 4, 1, 5}, {4, 0, 3, 1, 3, 2, 3, 3, 2, 6, 3, 8, 3, 9}, {2, 4, 2, 5, 2, 6, 2, 4, 2, 5}, {0, 2, 0, 3, 0, 4, 0, 5, 0, 1}, {0, 1, 2, 3, 4, 5}, {0, 0, 0, 1, 0, 2, 0, 3, 0, 4}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		writeRequests := buildRequestsFromBytes(data)
		testSomeWriteRequests(t, writeRequests)
	})
}
