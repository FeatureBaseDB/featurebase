// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package querycontext

import (
	"context"
	"errors"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

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
	txs, err := newRbfTxStore("foo", nil)
	if err != nil {
		t.Fatalf("creating Tx store: %v", err)
	}
	q, err := txs.NewQueryContext(ctx)
	if err != nil {
		t.Fatalf("creating read context: %v", err)
	}
	err = txs.Close()
	if err == nil {
		t.Fatalf("shouldn't close a database with unused QueryContext live")
	}
	_, err = q.NewRead("i", "f", "v", 0)
	if err != nil {
		t.Fatalf("creating read query: %v", err)
	}
	err = txs.Close()
	if err == nil {
		t.Fatalf("shouldn't close a database while still open")
	}
	q.Release()
	err = txs.Close()
	if err != nil {
		t.Fatalf("closing database: %v", err)
	}
}

// writeReq represents a possible write request.
type writeReq struct {
	index IndexName
	field FieldName
	shard ShardID
}

// satisfyWriteRequest tries to process the writes in a list of
// writeReqs, by requesting them in order, and returns an error
// if it encounters any errors.
func satisfyWriteRequest(t *testing.T, txs TxStore, writeList []writeReq) error {
	if len(writeList) == 0 {
		return nil
	}
	writes := txs.Scope()
	prevIndex := writeList[0].index
	prevField := writeList[0].field
	shards := []ShardID{writeList[0].shard}
	add := func(index IndexName, field FieldName, shards ...ShardID) {
		if field == "" {
			if len(shards) == 1 && shards[0] == 0 {
				writes.AddIndex(index)
			} else {
				writes.AddIndexShards(index, shards...)
			}
		} else {
			if len(shards) == 1 && shards[0] == 0 {
				writes.AddField(index, field)
			} else {
				writes.AddFieldShards(index, field, shards...)
			}
		}
	}
	// batch shards together. this is mostly irrelevant, but if we happened to get
	// shard lists in an unsorted order, and didn't handle that correctly, this would
	// catch that.
	for _, write := range writeList[1:] {
		if write.index == prevIndex && write.field == prevField {
			shards = append(shards, write.shard)
		} else {
			add(prevIndex, prevField, shards...)
			shards = append(shards[:0], write.shard)
			prevIndex = write.index
			prevField = write.field
		}
	}
	add(prevIndex, prevField, shards...)
	qcx, err := txs.NewWriteQueryContext(context.Background(), writes)
	defer qcx.Release()
	if err != nil {
		return err
	}
	for _, i := range rand.Perm(len(writeList)) {
		write := writeList[i]
		_, err := qcx.NewWrite(write.index, write.field, "v", write.shard)
		if err != nil {
			return err
		}
		// try reading something that could be outside our scope, because that
		// should still be allowed
		_, err = qcx.NewRead(write.index, write.field, "v", write.shard+5)
		if err != nil {
			return err
		}
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
func delayWriteRequest(t *testing.T, txs TxStore, writeList []writeReq, ch chan struct{}) (func() error, error) {
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
		_, err := qcx.NewWrite(write.index, write.field, "v", write.shard)
		if err != nil {
			qcx.Release()
			return nil, err
		}
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
	txs, err := newRbfTxStore("foo", splitter)
	if err != nil {
		t.Fatalf("creating Tx store: %v", err)
	}
	eg := errgroup.Group{}
	for i := range writeRequests {
		req := writeRequests[i]
		eg.Go(func() error {
			return satisfyWriteRequest(t, txs, req)
		})
	}
	err = eg.Wait()
	if err != nil {
		t.Logf("write requests (%d):", len(writeRequests))
		for _, req := range writeRequests {
			t.Logf("> %v", req)
		}
		t.Fatalf("running reqs: %v", err)
	}
	err = txs.Close()
	if err != nil {
		t.Fatalf("closing tx store: %v", err)
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
	txs, err := newRbfTxStore("foo", write.splitter)
	if err != nil {
		t.Fatalf("creating Tx store: %v", err)
	}
	// we spawn goroutines to open each of these, but wait until doneCh
	// closes before closing any of them, so they're all open at once.
	doneCh := make(chan struct{})
	eg := errgroup.Group{}
	closeFuncs := make([]func() error, len(write.reqs))
	for i := range write.reqs {
		i := i
		req := write.reqs[i]
		eg.Go(func() (err error) {
			closeFuncs[i], err = delayWriteRequest(t, txs, req, doneCh)
			return err
		})
	}
	errCh := make(chan error)
	go func() {
		errCh <- eg.Wait()
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
		t.Logf("write requests (%d):", len(write.reqs))
		for _, req := range write.reqs {
			t.Logf("> %v", req)
		}
		return errors.New("timed out trying to create write requests")
	}
	// all the writes should still be pending
	err = txs.Close()
	if err == nil {
		t.Fatalf("close completed while transactions were pending")
	}
	close(doneCh)
	for _, fn := range closeFuncs {
		if err := fn(); err != nil {
			t.Fatalf("closing transaction: %v", err)
		}
	}
	err = txs.Close()
	if err != nil {
		t.Fatalf("closing tx store: %v", err)
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
				{{"a", "f", 0}},
				{{"b", "f", 0}},
				{{"a", "f", 1}},
			},
		},
		{
			// we should be able to do two fields in i, but we wouldn't in j.
			splitter: NewFlexibleKeySplitter("i"),
			reqs: [][]writeReq{
				{{"i", "f", 0}},
				{{"i", "g", 0}},
				{{"j", "f", 0}},
			},
		},
		{
			// this should fail
			splitter: NewFlexibleKeySplitter("i"),
			reqs: [][]writeReq{
				{{"i", "f", 0}},
				{{"i", "g", 0}},
				{{"j", "f", 0}},
				{{"j", "g", 0}},
			},
			shouldError: true,
		},
	}
	for i, testCase := range overlapping {
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
	}
}

func writeRequestsFromBytes(data []byte) [][]writeReq {
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
		sub = append(sub, writeReq{index, field, shard})
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
		writeRequests := writeRequestsFromBytes(data)
		testSomeWriteRequests(t, writeRequests)
	})
}
