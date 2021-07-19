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
	"context"
	"fmt"
	"io"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/testhook"
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
// If you create a queue, it should get stopped at some point. The
// atomicSnapshotQueue implementation used as defaultSnapshotQueue has
// a Start function which will tell you whether it actually started a
// queue. This logic exists because in a normal server case, you probably
// want the queue to be shut down as part of server shutdown, but if you're
// running cluster tests, you probably want to start and shop the queue as
// part of the test, not stop it when any server terminates.
//
// It's less likely to be desireable to start/stop individual queues,
// because fragments use the defaultSnapshotQueue anyway. This design
// needs revisiting.
type SnapshotQueue interface {
	Immediate(*fragment) error
	Enqueue(*fragment)
	Await(*fragment) error
	ScanHolder(*Holder, chan struct{})
	Stop()
}

// queuelessSnapshotQueue isn't a snapshot queue, but it satisfies the
// interface.
type queuelessSnapshotQueue struct{}

func (q *queuelessSnapshotQueue) Enqueue(f *fragment) {
	// We don't actually try to enqueue the snapshot; it breaks things
	// if a snapshot gets caused during a transaction.
}

func (q *queuelessSnapshotQueue) Await(f *fragment) error {
	return nil
}

func (q *queuelessSnapshotQueue) Immediate(f *fragment) error {
	return f.snapshot()
}

func (q *queuelessSnapshotQueue) ScanHolder(h *Holder, done chan struct{}) {
}

func (q *queuelessSnapshotQueue) Stop() {
}

var defaultSnapshotQueue = &queuelessSnapshotQueue{}

// newSnapshotQueue makes a new snapshot queue, of depth N, with
// w worker threads.
func newSnapshotQueue(n int, w int, l logger.Logger) SnapshotQueue {
	ctx, cancel := context.WithCancel(context.Background())
	sq := &prioritySnapshotQueue{
		normal:     make(chan snapshotRequest, n),
		urgent:     make(chan snapshotRequest),
		background: make(chan snapshotRequest),
		ctx:        ctx,
		cancel:     cancel,
		maxOpN:     10000,
		logger:     l,
	}
	if sq.logger == nil {
		sq.logger = logger.NewStandardLogger(os.Stderr)
	}
	_ = testhook.Opened(NewAuditor(), sq, nil)
	sq.spawnWorkers(w)
	return sq
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
	ctx              context.Context
	cancel           context.CancelFunc
	mu               sync.RWMutex
	scanWG, workerWG sync.WaitGroup
	maxOpN           int
	observedOpN      [16]uint32
	stats            struct {
		enqueued uint32
		skipped  uint32
	}
	stopped bool
}

func (sq *prioritySnapshotQueue) spawnWorkers(w int) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	if sq.ctx.Err() != nil {
		sq.logger.Infof("prioritySnapshotQueue worker: already done")
		return
	}
	sq.workerWG.Add(w)
	for i := 0; i < w; i++ {
		go sq.worker(sq.ctx, sq.urgent, sq.normal, sq.background)
	}
}

func (sq *prioritySnapshotQueue) worker(ctx context.Context, urgent, normal, background chan snapshotRequest) {
	defer sq.workerWG.Done()
	done := ctx.Done()
	ok := true
	var req snapshotRequest
	for ok {
		req.frag = nil
		select {
		case _, ok = <-done:
		case req, ok = <-urgent:
		default:
			select {
			case _, ok = <-done:
			case req, ok = <-urgent:
			case req, ok = <-normal:
			default:
				select {
				case _, ok = <-done:
				case req, ok = <-urgent:
				case req, ok = <-normal:
				case req, ok = <-background:
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
			fmt.Printf("ERROR: snapshot error: %v\n", f.snapshotErr)
			sq.logger.Errorf("snapshot error: %v", f.snapshotErr)
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
	if sq.stopped {
		return
	}
	sq.stopped = true
	sq.cancel()
	// scanners need to be done before we close the other channels.
	sq.scanWG.Wait()
	close(sq.normal)
	sq.normal = nil
	close(sq.urgent)
	sq.urgent = nil
	close(sq.background)
	sq.background = nil
	_ = testhook.Closed(NewAuditor(), sq, nil)
	enqueued := atomic.LoadUint32(&sq.stats.enqueued)
	skipped := atomic.LoadUint32(&sq.stats.skipped)
	if skipped > 0 || enqueued > 1 {
		sq.logger.Infof("snapshot queue: enqueued %d, skipped %d\n", sq.stats.enqueued, sq.stats.skipped)
	}
}

// Enqueue tries to add a fragment to the queue, if the fragment is not already
// enqueued. You should hold a lock on the fragment when calling this.
func (sq *prioritySnapshotQueue) Enqueue(f *fragment) {
	if f.snapshotPending {
		return
	}
	sq.observeOpN(uint32(f.opN))
	sq.mu.RLock()
	defer sq.mu.RUnlock()
	if sq.normal == nil {
		sq.logger.Infof("requested snapshot after snapshot queue was closed")
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
		atomic.AddUint32(&sq.stats.enqueued, 1)
		return
	default:
		atomic.AddUint32(&sq.stats.skipped, 1)
		f.snapshotPending = false
		return
	}
}

// Await returns when f is not pending a snapshot. Call with the fragment lock
// held. Await waits on a condition variable inside f, associated with the
// fragment's lock, so this does not conflict with the lock being used for
// snapshots.
//
// Note that workers don't stop just because the queue's been stopped; only
// the background scanner is stopped. So an Await shouldn't block forever
// even if the queue gets shut down. If you're reading this, possibly that
// analysis is incorrect.
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
		sq.logger.Errorf("requested immediate snapshot after snapshot queue was closed")
		return errors.New("requested immediate snapshot after snapshot queue was closed")
	}
	f.snapshotPending = true
	sq.observeOpN(uint32(f.opN))
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

// ScanHolder spawns a goroutine which iterates through the holder's
// indexes/fields/views/fragments, looking for fragments which have OpN
// high enough to justify a snapshot but don't seem to have one pending.
// It then dumps these in the low priority background queue.
func (sq *prioritySnapshotQueue) ScanHolder(h *Holder, done chan struct{}) {
	sq.mu.Lock()
	sq.scanWG.Add(1)
	go sq.scanHolderWorker(h, sq.background, done)
	sq.mu.Unlock()
}

// observeOpN reports that a given value of opN was "observed", meaning,
// we encountered a fragment which had that value. This happens for every
// enqueue/immediate, including enqueue attempts which fail to actually
// enter the queue, and it also happens for fragments noticed by the background
// scan but which don't have high enough opN to trigger a snapshot.
func (sq *prioritySnapshotQueue) observeOpN(n uint32) {
	// aka "log2(n) + 1", or 0 for n==0
	pow2 := 32 - bits.LeadingZeros32(n)
	// 15 == 16384. Our usual fragment maxOpN is 10k, so most fragments
	// should end up in the 8k-16k bucket, rather than the 16k+ bucket,
	// unless we've got a lot of ingests with large batches going on,
	// in which case the 16k bucket will win.
	if pow2 > 15 {
		pow2 = 15
	}
	// store in inverse order so the lowest slot in the array is the
	// highest cardinality
	atomic.AddUint32(&sq.observedOpN[15-pow2], 1)
}

// computeMaxOpN tries to pick a reasonable new maxOpN for the background
// scan to use. On a quiet system, we want to gradually lower opN, picking
// the fragments with the highest opN values first, because those offer the
// largest benefit. So, whenever we check a fragment in the background, if we
// *don't* snapshot it, we'll "observe" its OpN value, and then we pick a
// value which picks up at least 1/4 of them.
//
// If there's ingest activity, the Immediate and Enqueue operations will
// "observe" the OpN of fragments submitted to them. This can drive OpN back
// up, if those fragments frequently have very high opN values, which reflects
// the fact that we have enough of that activity that we don't need the
// background scanner adding more.
//
// If we have enough ingest activity that the background scanner never actually
// gets to submit work, we'll rarely get here, because the background scanner
// will block until there's no snapshots pending for the normal workload.
// When we do, we'll probably pick a MaxOpN which is dominated by the ingest
// workload's opN values. So for instance, if everything coming in from the
// ingest workload has 10k or more items, because that's the default fragment
// maxOpN, that will probably set the background snapshot queue value to 8k.
func (sq *prioritySnapshotQueue) computeMaxOpN() {
	sq.logger.Debugf("observedOpN by power of 2: %d\n", sq.observedOpN[:])
	total := uint32(0)
	for i := range sq.observedOpN {
		total += atomic.LoadUint32(&sq.observedOpN[i])
	}
	target := (total / 4) + 1
	subTotal := uint32(0)
	for i := range sq.observedOpN {
		v := atomic.LoadUint32(&sq.observedOpN[i])
		subTotal += v
		if subTotal >= target {
			prevMaxOpN := sq.maxOpN
			sq.maxOpN = (1 << (15 - uint(i))) / 2
			if sq.maxOpN > 0 {
				sq.maxOpN--
			}
			if prevMaxOpN != sq.maxOpN {
				sq.logger.Infof("background scan: %d/%d fragments considered have opN %d or higher\n",
					subTotal, total, sq.maxOpN)
			}
			break
		}
	}
	// It's conceptually possible that we'll miss a couple of observations
	// here but that's not really important. This is all pretty approximate.
	for i := range sq.observedOpN {
		atomic.StoreUint32(&sq.observedOpN[i], 0)
	}
}

// prioritySnapshotQueueScanner is the data type that implements HolderOperator
// and represents a single scan of a holder, with a given maxOpN.
type prioritySnapshotQueueScanner struct {
	HolderFilterAll
	HolderProcessNone
	sq                  *prioritySnapshotQueue
	holder              *Holder
	queue               chan snapshotRequest
	ctx                 context.Context
	maxOpN              int
	seen, hits, counter int
}

func (s *prioritySnapshotQueueScanner) ProcessFragment(f *fragment) error {
	if f == nil {
		return nil
	}
	s.seen++
	// we can't defer this reasonably, because otherwise we'll keep
	// the fragment locked forever if we end up trying to send it
	// to the queue, but the workers are busy on other fragments.
	f.mu.Lock()
	open := f.open
	snapshotPending, opN := f.snapshotPending, f.opN
	f.mu.Unlock()

	// a pending snapshot is one that is either in the normal or
	// immediate queue, or is trying to get into the normal queue
	// and about to fail, but either way, it already got observed
	// there, so we don't need to observe it here. A closed fragment
	// doesn't matter to us -- it should be a transient state that
	// happens during a shutdown, or shouldn't happen, but we don't
	// care about it.
	if snapshotPending || !open {
		return nil
	}
	if opN <= s.maxOpN {
		// observe the value but don't do a snapshot
		s.sq.observeOpN(uint32(opN))
		s.counter++
		if s.counter == 1000 {
			select {
			case <-time.After(1 * time.Second):
			case <-s.ctx.Done():
				return io.EOF
			}
			s.counter = 0
		}
		return nil
	}
	// we don't observe values when we decide to trigger a snapshot,
	// because those values will be changing anyway. we could also
	// observe them as zero, but that's also sort of wrong.
	s.hits++
	select {
	case s.queue <- snapshotRequest{frag: f, when: time.Now()}:
		s.sq.logger.Debugf("found fragment needing snapshot: %s\n", f.path())
	case <-s.ctx.Done():
		return io.EOF
	}
	return nil

}

func contextMergedWithStructChan(ctx context.Context, ch chan struct{}) (context.Context, context.CancelFunc) {
	canCancel, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-ch:
			cancel()
		case <-canCancel.Done():
			// don't need to cancel, but do need to exit this
			// function
		}
	}()
	return canCancel, cancel
}

// scanHolderWorker is a background task that scans a holder looking for
// fragments which need snapshots taken. It's the cleanup task for snapshots
// that would have been requested by Enqueue, but the queue was full.
func (sq *prioritySnapshotQueue) scanHolderWorker(h *Holder, background chan snapshotRequest, done chan struct{}) {
	defer sq.scanWG.Done()
	ctx, cancel := contextMergedWithStructChan(sq.ctx, done)
	defer cancel()
	scanner := &prioritySnapshotQueueScanner{
		sq:     sq,
		holder: h,
		queue:  background,
		ctx:    sq.ctx,
		maxOpN: sq.maxOpN,
	}
	for {
		err := h.Process(ctx, scanner)
		if err != nil {
			return
		}

		if scanner.hits > 0 {
			sq.logger.Infof("background scan: %d/%d fragments needed snapshots\n", scanner.hits, scanner.seen)
			scanner.hits = 0
		} else {
			sq.logger.Debugf("background scan: no fragments needed snapshots, waiting\n")
			// No reason to be active if we're not finding anything.
			select {
			case <-time.After(60 * time.Second):
			case <-ctx.Done():
				return
			}
		}
		scanner.seen = 0
		sq.computeMaxOpN()
		scanner.maxOpN = sq.maxOpN
	}
}
