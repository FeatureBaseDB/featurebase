// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0

package task

import (
	"sync"
	"sync/atomic"
)

// Pool represents a worker-pool type thing, which will call a given
// function in parallel aiming for a given level of concurrency.
// To use a pool, you create it, passing in a worker function; it
// then spawns goroutines to run that function in a loop. If the Pool's
// Block method is called, this marks one instance of the worker goroutine
// as blocked; the Unblock method marks it as unblocked. When there are
// insufficient unblocked goroutines, more are spawned. When there are
// excess goroutines, they exit.
//
// The pool can be shut down by calling Close(), setting its target number
// of workers to 0.
type Pool struct {
	mu        sync.Mutex // locker used for cond
	cond      *sync.Cond // notify of exiting workers
	step      func()
	targetN   int32 // desired number
	unblocked int32 // currently active and unblocked
	live      int32 // currently active including blocked
	stats     PoolStats
}

type PoolStats interface {
	PoolSize(int) // reports current pool size
}

// NewPool creates a pool that attempts to keep targetN goroutines
// active, executing step() repeatedly. It updates poolSize with the
// current size of the pool when that changes.
func NewPool(targetN int, step func(), stats PoolStats) *Pool {
	p := &Pool{targetN: int32(targetN), step: step, stats: stats}
	p.cond = sync.NewCond(&p.mu)
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 0; i < targetN; i++ {
		p.addWorker()
	}
	return p
}

// Block marks a worker as blocked, indicating that we may need a new worker
// spawned because the caller is about to be blocked for an indeterminate
// period of time. If a new worker is needed, it's spawned immediately before
// Block returns.
func (p *Pool) Block() {
	p.mu.Lock()
	defer p.mu.Unlock()
	unblocked := atomic.AddInt32(&p.unblocked, -1)
	target := atomic.LoadInt32(&p.targetN)
	if unblocked < target {
		p.addWorker()
	}
}

// Unblock marks a worker as unblocked, potentially allowing the pool to
// retire a worker thread at some point in the future.
func (p *Pool) Unblock() {
	atomic.AddInt32(&p.unblocked, 1)
}

// Shutdown tells a pool to terminate by setting its desired pool size
// to zero, but does not wait for the jobs in it to stop. It is safe to
// call this before calling Close.
func (p *Pool) Shutdown() {
	atomic.StoreInt32(&p.targetN, 0)
}

// Stats reports on the pool's current state -- total live workers it
// has, how many it thinks are unblocked, and what its target is.
// These numbers are sampled individually, and there's no locking, so they
// are not guaranteed to be consistent. This is useful for approximate
// monitoring.
func (p *Pool) Stats() (live, unblocked, target int) {
	return int(atomic.LoadInt32(&p.live)), int(atomic.LoadInt32(&p.unblocked)), int(atomic.LoadInt32(&p.targetN))
}

// Close is a Shutdown followed by waiting for all jobs to exit.
func (p *Pool) Close() {
	// important to note: p.cond.Wait() is actually releasing this lock,
	// then reacquiring it when the wait succeeds. This means that
	// nothing which uses the lock can trigger between our read of
	// live, and our wait on the condition variable...
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Shutdown()
	live := atomic.LoadInt32(&p.live)
	for live > 0 {
		p.cond.Wait()
		// This line occurs while we hold p.mu. addWorker can't be called
		// except from inside something that would also hold the lock.
		// So, if the value can't be stale and increasing, and it can't
		// increase anyway once targetN is 0.
		live = atomic.LoadInt32(&p.live)
	}
}

// addWorker increments the number of unblocked things, and starts a worker.
// The unblocked count is technically wrong until the worker gets running, but
// it's right "soon". The live count maintenance is done inside the worker.
func (p *Pool) addWorker() {
	// update worker count. we don't notify the condition variable because
	// increasing workers can't make us more-closed.
	live := atomic.AddInt32(&p.live, 1)
	if p.stats != nil {
		p.stats.PoolSize(int(live))
	}
	atomic.AddInt32(&p.unblocked, 1)
	go p.work()
}

// work runs the provided work function in a loop as long as there's not
// too many unblocked goroutines, otherwise it exits.
func (p *Pool) work() {
	defer func() {
		// The lock prevents our modification of p.live from
		// happening between the read of p.live and the wait on
		// the condition variable in p.Close. Otherwise, it's
		// possible for these to interleave as:
		//
		// p.Close        this function
		// -------        -------------
		// read p.live
		//                modify p.live
		//                broadcast to p.cond
		// p.Cond.Wait
		//
		// and the wait never terminates because the broadcast
		// happened before that.
		p.mu.Lock()
		defer p.mu.Unlock()
		live := atomic.AddInt32(&p.live, -1)
		if p.stats != nil {
			p.stats.PoolSize(int(live))
		}
		// notify any waiters that we're done
		if live == 0 {
			p.cond.Broadcast()
		}
	}()
	for {
		unblocked := atomic.LoadInt32(&p.unblocked)
		target := atomic.LoadInt32(&p.targetN)
		for unblocked > target {
			// Might have too many!
			swapped := atomic.CompareAndSwapInt32(&p.unblocked, unblocked, unblocked-1)
			if swapped {
				// we've successfully removed ourselves from the unblocked count.
				// now return, letting the deferred add above remove us from the live
				// count as well.
				return
			}
			// If the swap failed, unblocked increased or decreased. We
			// re-extract it, and try the loop again. If it's no longer higher
			// than the target, this loop ends and we continue running.
			// If it's higher than the target, we'll try again with this new
			// value.
			// We also reload target because someone could have told us to
			// terminate.
			unblocked = atomic.LoadInt32(&p.unblocked)
			target = atomic.LoadInt32(&p.targetN)
		}
		p.step()
	}
}
