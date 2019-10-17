// Copyright 2019 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pilosa/pilosa/v2/logger"
	"github.com/pkg/errors"
)

// snapshotQueue is a thing which can handle enqueuing snapshots. A snapshot
// queue distinguishes between high-priority requests, which get satisfied
// by the next available worker, and regular requests, which get enqueued
// if there's space in the queue, and otherwise dropped. There's also a
// separate background task to scan a holder for fragments which may need
// snapshots, but which is processed only when the queue is empty, and only
// slowly. "Await" awaits an existing snapshot if one is already enqueued.
// "Immediate" tries to do one right away. (If one's already enqueued, this
// can leave it in the queue, which will ignore anything that shows up with
// the request flag cleared.)
//
// Await, Enqueue, and Immediate should be called only with the fragment lock
// held.
type snapshotQueue interface {
	Immediate(*fragment) error
	Enqueue(*fragment)
	Await(*fragment) error
	ScanHolder(*Holder)
	Stop()
}

// queuelessSnapshotQueue isn't a snapshot queue, but it satisfies the
// interface and can be used as an interface.
type queuelessSnapshotQueue struct{}

func (q *queuelessSnapshotQueue) Enqueue(f *fragment) {
	_ = f.snapshot()
}

func (q *queuelessSnapshotQueue) Await(f *fragment) error {
	return nil
}

func (q *queuelessSnapshotQueue) Immediate(f *fragment) error {
	return f.snapshot()
}

func (q *queuelessSnapshotQueue) ScanHolder(h *Holder) {
}

func (q *queuelessSnapshotQueue) Stop() {
}

// defaultSnapshotQueue is the fallback to use if none is available,
// and currently uses queueless -- it runs all snapshots immediately.
var defaultSnapshotQueue *queuelessSnapshotQueue

// newSnapshotQueue makes a new snapshot queue, of depth N, with
// w worker threads.
func newSnapshotQueue(n int, w int, l logger.Logger) snapshotQueue {
	sq := prioritySnapshotQueue{normal: make(chan snapshotRequest, n), urgent: make(chan snapshotRequest), background: make(chan snapshotRequest), done: make(chan struct{}), logger: l}
	if sq.logger == nil {
		sq.logger = logger.NewStandardLogger(os.Stderr)
	}
	for i := 0; i < w; i++ {
		go sq.worker()
	}
	return &sq
}

type snapshotRequest struct {
	frag *fragment
	when time.Time
}

// prioritySnapshotQueue gives preference to "immediate" requests, and
// dispreference to "background" requests from ScanHolder. It timestamps
// requests, so it can discard a request if the most recent snapshot is
// newer than the request. The snapshotPending flag in the fragment is
// used to track that a given fragment thinks it has been successfully
// enqueued. Background requests are not considered enqueued, since
// they'll never get processed if there's anything else. In normal workloads,
// immediate/urgent snapshots should be rare, but we'll happily drop
// most requests on the floor; the scanner should pick them up once things
// are quiet.
type prioritySnapshotQueue struct {
	logger     logger.Logger
	urgent     chan snapshotRequest
	normal     chan snapshotRequest
	background chan snapshotRequest
	done       chan struct{}
	mu         sync.RWMutex
	stats      struct {
		enqueued int64
		skipped  int64
	}
}

func (sq *prioritySnapshotQueue) worker() {
	// We don't want a race condition on these. If they're non-nil when
	// we get them, they should get closed at some point. If done is
	// already nil, we shouldn't do anything.
	sq.mu.RLock()
	urgent := sq.urgent
	normal := sq.normal
	background := sq.background
	done := sq.done
	sq.mu.RUnlock()
	if done == nil {
		sq.logger.Printf("prioritySnapshotQueue worker: no done channel, already done?")
		return
	}
	ok := true
	var req snapshotRequest
	for ok {
		req.frag = nil

		select {
		case req, ok = <-urgent:
		default:
			select {
			case req, ok = <-urgent:
			case req, ok = <-normal:
			default:
				select {
				case req, ok = <-urgent:
				case req, ok = <-normal:
				case req, ok = <-background:
				case _, ok = <-done:
				}
			}
		}
		sq.process(req)
		if req.frag != nil {
			sq.process(req)
		}
	}
}

// process actually runs a fragment. it will do this if either the fragment
// has a pending snapshot, or the force flag is set.
func (sq *prioritySnapshotQueue) process(req snapshotRequest) {
	if req.frag == nil {
		return
	}
	f := req.frag
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapshotStamp.Before(req.when) {
		f.snapshotErr = f.snapshot()
		if f.snapshotErr != nil {
			fmt.Printf("snapshot error: %v\n", f.snapshotErr)
			sq.logger.Printf("snapshot error: %v", f.snapshotErr)
		}
		f.snapshotPending = false
		f.snapshotCond.Broadcast()
	}
}

func (sq *prioritySnapshotQueue) Stop() {
	close(sq.done)
	sq.mu.Lock()
	defer sq.mu.Unlock()
	sq.done = nil
	close(sq.normal)
	sq.normal = nil
	close(sq.urgent)
	sq.urgent = nil
	close(sq.background)
	sq.background = nil
	sq.logger.Printf("snapshot queue: enqueued %d, skipped %d\n", sq.stats.enqueued, sq.stats.skipped)
}

func (sq *prioritySnapshotQueue) Enqueue(f *fragment) {
	if f.snapshotPending {
		return
	}
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.normal == nil {
		sq.logger.Printf("requested snapshot after snapshot queue was closed")
		return
	}
	// we have to set this before enqueing, because it's
	// otherwise possible that we're at the head of the queue,
	// and the recipient gets the fragment before we execute the
	// line after the send.
	f.snapshotPending = true
	// try to enqueue snapshot
	select {
	case sq.normal <- snapshotRequest{frag: f, when: time.Now()}:
		sq.stats.enqueued++
		return
	default:
		sq.stats.skipped++
		f.snapshotPending = false
		return
	}
}

func (sq *prioritySnapshotQueue) Await(f *fragment) (err error) {
	for f.snapshotPending {
		f.snapshotCond.Wait()
	}
	err, f.snapshotErr = f.snapshotErr, nil
	return err
}

func (sq *prioritySnapshotQueue) Immediate(f *fragment) error {
	sq.mu.RLock()
	// no deferred unlock, because we want to unlock this before calling Await.
	f.snapshotPending = true
	if sq.urgent == nil {
		sq.mu.RUnlock()
		sq.logger.Printf("requested immediate snapshot after snapshot queue was closed")
		return errors.New("requested immediate snapshot after snapshot queue was closed")
	}
	sq.urgent <- snapshotRequest{frag: f, when: time.Now()}
	sq.mu.RUnlock()
	return sq.Await(f)
}

func (sq *prioritySnapshotQueue) considerFragment(f *fragment) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snapshotPending {
		return false
	}
	if f.opN > f.MaxOpN {
		return true
	}
	return false
}

// ScanHolder iterates through the holder's indexes/fields/views/fragments,
// looking for fragments which have OpN high enough to justify a snapshot
// but don't seem to have one pending. It then dumps these in that low
// priority background queue.
func (sq *prioritySnapshotQueue) ScanHolder(h *Holder) {
	sq.mu.RLock()
	background := sq.background
	done := sq.done
	sq.mu.RUnlock()
	// to avoid abusing things, cap activity rate; every time we finish
	// the holder, or every couple hundred fragments considered, we
	// pause for a bit.
	counter := 0
	for {
		hits := 0
		for _, index := range h.indexes {
			for _, field := range index.fields {
				for _, view := range field.viewMap {
					for _, frag := range view.fragments {
						if sq.considerFragment(frag) {
							hits++
							select {
							case background <- snapshotRequest{frag: frag, when: time.Now()}:
								sq.logger.Printf("found fragment needing snapshot: %s\n", frag.path)
							case <-done:
								return
							}
						} else {
							// Count fragments examined *without* finding anything that
							// needed a snapshot. When we find things that need snapshots,
							// the time it takes the workers to respond to us is enough
							// of a delay to keep us from eating every CPU. So, if a lot
							// of things need snapshots, and the workers aren't doing
							// anything else, ScanHolder will mostly keep them saturated.
							// If they're busy, we'll block forever in the write to the
							// background queue. If there's nothing that needs snapshots,
							// we pause frequently for a second or so at a time.
							counter++
							if counter%100 == 0 {
								time.Sleep(1 * time.Second)
							}
						}
					}
				}
			}
		}
		if hits > 0 {
			sq.logger.Printf("background scan: %d fragments needed snapshots\n", hits)
			hits = 0
		} else {
			// no reason to be active if we're not finding anything
			time.Sleep(60 * time.Second)
		}
	}
}
