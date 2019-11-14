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
//
// ScanHolder spawns a new goroutine. You don't need to use `go` on it.
type snapshotQueue interface {
	Immediate(*fragment) error
	Enqueue(*fragment)
	Await(*fragment) error
	ScanHolder(*Holder)
	Stop()
}

// queuelessSnapshotQueue isn't a snapshot queue, but it satisfies the
// interface.
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
	sq.spawnWorkers(w)
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
	logger           logger.Logger
	urgent           chan snapshotRequest
	normal           chan snapshotRequest
	background       chan snapshotRequest
	done             chan struct{}
	mu               sync.RWMutex
	scanWG, workerWG sync.WaitGroup
	stats            struct {
		enqueued int64
		skipped  int64
	}
}

func (sq *prioritySnapshotQueue) spawnWorkers(w int) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.done == nil {
		sq.logger.Printf("prioritySnapshotQueue worker: no done channel, already done?")
		return
	}
	sq.workerWG.Add(w)
	for i := 0; i < w; i++ {
		go sq.worker(sq.urgent, sq.normal, sq.background, sq.done)
	}
}

func (sq *prioritySnapshotQueue) worker(urgent, normal, background chan snapshotRequest, done chan struct{}) {
	// We don't want a race condition on these. If they're non-nil when
	// we get them, they should get closed at some point. If done is
	// already nil, we shouldn't do anything.
	defer sq.workerWG.Done()
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
		if req.frag != nil {
			sq.process(req)
		}
	}
}

// process actually runs a fragment. it will do this if either the fragment
// has a pending snapshot, or the force flag is set.
func (sq *prioritySnapshotQueue) process(req snapshotRequest) {
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

// Stop shuts down the snapshot queue. It first marks it as done, causing
// the background scanner(s), if any, to shut down, then waits for them, then
// closes and nils the queues. The background scanner has to get stopped
// because otherwise it might try to write to those closed queues.
func (sq *prioritySnapshotQueue) Stop() {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	close(sq.done)
	// scanners need to be done before we close the other channels.
	sq.scanWG.Wait()
	sq.done = nil
	close(sq.normal)
	sq.normal = nil
	close(sq.urgent)
	sq.urgent = nil
	close(sq.background)
	sq.background = nil
	sq.logger.Printf("snapshot queue: enqueued %d, skipped %d\n", sq.stats.enqueued, sq.stats.skipped)
}

// Enqueue tries to add a fragment to the queue, if the fragment is not already
// enqueued. You should hold a lock on the fragment when calling this.
func (sq *prioritySnapshotQueue) Enqueue(f *fragment) {
	if f.snapshotPending {
		return
	}
	sq.mu.RLock()
	defer sq.mu.RUnlock()
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

// Await returns when f is not pending a snapshot. Call with the fragment lock
// held. Await waits on a condition variable inside f, associated with the
// fragment's lock, so this does not conflict with the lock being used for
// snapshots.
func (sq *prioritySnapshotQueue) Await(f *fragment) (err error) {
	for f.snapshotPending {
		f.snapshotCond.Wait()
	}
	err, f.snapshotErr = f.snapshotErr, nil
	return err
}

// Immediate forces an immediate snapshot of the given fragment. Call with
// the fragment locked. If the queue is already closing, the fragment does
// not get snapshotted.
func (sq *prioritySnapshotQueue) Immediate(f *fragment) error {
	sq.mu.RLock()
	// no deferred unlock, because we want to unlock this before calling Await.
	// Not because that needs this lock, but because once we're that far, we
	// *don't* need this lock anymore so someone else should have it.
	if sq.urgent == nil {
		sq.mu.RUnlock()
		sq.logger.Printf("requested immediate snapshot after snapshot queue was closed")
		return errors.New("requested immediate snapshot after snapshot queue was closed")
	}
	f.snapshotPending = true
	req := snapshotRequest{frag: f, when: time.Now()}
	// if the fragment was already in the work queue, it's *possible*
	// that the only available worker just picked it off the queue, and
	// is now waiting on getting the fragment's lock, so it can run
	// a snapshot. So we let go of the lock on the fragment, send the
	// request, then request the fragment lock again, because Await will
	// be sleeping on the condition variable associated with the lock,
	// which means it needs to hold the lock so it can let it go during
	// the wait... No, really, this made sense.
	f.mu.Unlock()
	sq.urgent <- req
	sq.mu.RUnlock()
	f.mu.Lock()
	return sq.Await(f)
}

// needsSnapshot determines whether a fragment probably wants snapshotting.
// Specifically, it looks for fragments not already marked to receive
// snapshots, but which have a high enough opN to justify a snapshot. This
// is only used from the background scan.
func (sq *prioritySnapshotQueue) needsSnapshot(f *fragment) bool {
	if f == nil {
		return false
	}
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

// ScanHolder spawns a goroutine which iterates through the holder's
// indexes/fields/views/fragments, looking for fragments which have OpN
// high enough to justify a snapshot but don't seem to have one pending.
// It then dumps these in the low priority background queue.
func (sq *prioritySnapshotQueue) ScanHolder(h *Holder) {
	sq.mu.Lock()
	sq.scanWG.Add(1)
	go sq.scanHolderWorker(h, sq.background, sq.done)
	sq.mu.Unlock()
}

// scanHolderWorker is a background task that scans a holder looking for
// fragments which need snapshots taken. It's the cleanup task for snapshots
// that would have been requested by Enqueue, but the queue was full.
func (sq *prioritySnapshotQueue) scanHolderWorker(h *Holder, background chan snapshotRequest, done chan struct{}) {
	defer sq.scanWG.Done()
	var indexNames, fieldNames, viewNames []string
	var fragNums []uint64
	for {
		// To avoid abusing things, cap activity rate; every time we finish
		// the holder, or every couple hundred fragments considered, we
		// pause for a bit.
		counter := 0
		hits := 0
		h.mu.Lock()
		indexNames = indexNames[:0]
		for indexName := range h.indexes {
			indexNames = append(indexNames, indexName)
		}
		h.mu.Unlock()
		for _, indexName := range indexNames {
			h.mu.Lock()
			index := h.indexes[indexName]
			h.mu.Unlock()
			if index == nil {
				continue
			}
			fieldNames = fieldNames[:0]
			index.mu.Lock()
			for fieldName := range index.fields {
				fieldNames = append(fieldNames, fieldName)
			}
			index.mu.Unlock()
			for _, fieldName := range fieldNames {
				index.mu.Lock()
				field := index.fields[fieldName]
				index.mu.Unlock()
				if field == nil {
					continue
				}
				viewNames = viewNames[:0]
				field.mu.Lock()
				for viewName := range field.viewMap {
					viewNames = append(viewNames, viewName)
				}
				field.mu.Unlock()
				for _, viewName := range viewNames {
					field.mu.Lock()
					view := field.viewMap[viewName]
					field.mu.Unlock()
					if view == nil {
						continue
					}
					fragNums := fragNums[:0]
					view.mu.Lock()
					for fragNum := range view.fragments {
						fragNums = append(fragNums, fragNum)
					}
					view.mu.Unlock()
					for _, fragNum := range fragNums {
						view.mu.Lock()
						frag := view.fragments[fragNum]
						view.mu.Unlock()
						if sq.needsSnapshot(frag) {
							hits++
							select {
							case background <- snapshotRequest{frag: frag, when: time.Now()}:
								sq.logger.Debugf("found fragment needing snapshot: %s\n", frag.path)
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
							if counter == 100 {
								select {
								case <-time.After(1 * time.Second):
								case <-done:
									return
								}
								counter = 0
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
			sq.logger.Printf("background scan: no fragments needed snapshots, waiting\n")
			// No reason to be active if we're not finding anything.
			select {
			case <-time.After(60 * time.Second):
			case <-done:
				return
			}
		}
	}
}
