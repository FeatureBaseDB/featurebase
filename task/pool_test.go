// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0

package task

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

// db represents a thing which can be locked, and which can
// perform read and write operations, which are modeled as channels which
// a workload can wait on writes to, and which embeds a lockable RWMutex.
// The RWMutex actually makes this slightly stricter than the semantics
// of RBF, which usually allows writes and reads to coexist, but fairly
// accurately represents the specific issue that RBF can't *finish* a write
// while an older read is active. Not the same, but has similar impact.
type db struct {
	read, write chan struct{}
	sync.Mutex
}

// server represents a set of dbs, which jobs can be run against. They're
// [26] because they're denoted by lowercase/uppercase letters.
type server struct {
	dbs     [26]db
	mu      sync.Mutex     // mutex to govern access to readers
	readers [26][]workload // a list of readers associated with each db
	waiters [26]struct {
		mu   sync.Mutex
		cond *sync.Cond
	}
	pool *Pool
	jobs chan *job
	tb   testing.TB
}

// a job represents a single operation on a server, and a receiver
// waiting to hear back when it's done. It also has a reference to the
// bitmasks of read/write locks so that the parent operation can clean
// them all up when it's done. This is roughly parallel to the Qcx/Tx
// locking behavior in featurebase.
type job struct {
	descr  workload // the workload that generated this job, used to identify them
	id     int
	write  bool
	locked *uint32 // bitmask of read-locked jobs
	ch     chan<- struct{}
}

// workload represents a series of jobs as letters;
// lowercase letters read from the read channel of a component, uppercase
// letters read from the write channel of the corresponding lowercase
// component. each operation locks components as it reaches them for
// the first time, then unlocks all of them at the end of the string.
type workload string

// runJob grabs a single job from the server's job queue, does it, and
// notifies the waiter. To "do" a job is to acquire the appropriate
// lock (for read or write), mark the appropriate bit in a bitmap of
// active locks, and then read from either a read or write channel, which
// then corresponds to values being passed to Satisfy.
func (s *server) runJob() {
	j, ok := <-s.jobs
	if !ok {
		return
	}
	if j.write {
		s.pool.Block()
		s.dbs[j.id].Lock()
		s.pool.Unblock()
		s.mu.Lock()
		// obtain list of existing readers
		waiting := make([]workload, len(s.readers[j.id]))
		copy(waiting, s.readers[j.id])
		s.mu.Unlock()
		<-s.dbs[j.id].write
		// In RBF, the write lock can't be released until the last outstanding
		// reader predating this write terminates, but that's asynchronous
		// from the actual request processing. So, similarly, we launch a thing
		// that will unlock this slot in the database, once it's done waiting
		// for any readers. We do that without the pool marked as blocked.
		go func() {
			// but the write can't actually complete until any pending readers
			// that were already in play complete
			if len(waiting) > 0 {
				// We might need to wait for things. We need to be sure,
				// though, that the server's list of readers for this isn't
				// changing while we're checking it. So, we grab the specific
				// lock, then check the reader list, and if we think we need
				// to wait, we wait on a condition variable which then
				// releases that lock so something else can update the reader
				// list and notify us.
				func() {
					s.waiters[j.id].mu.Lock()
					defer s.waiters[j.id].mu.Unlock()
					// we have to check this with the specific lock held, so if
					// anything were to change the list, it'd have to wait
					// until we're done or waiting on the cond.
					stillWaiting := s.stillWaiting(j.id, waiting)
					for stillWaiting {
						s.waiters[j.id].cond.Wait()
						stillWaiting = s.stillWaiting(j.id, waiting)
					}
				}()
			}
			s.dbs[j.id].Unlock()
		}()
	} else {
		s.pool.Block()
		// attach us to the list of known readers, which must exit before
		// any writers starting after them can exit
		s.mu.Lock()
		s.readers[j.id] = append(s.readers[j.id], j.descr)
		s.mu.Unlock()
		s.pool.Unblock()
		cur := atomic.LoadUint32(j.locked)
		// mask this bit in
		for (cur>>j.id)&1 == 0 {
			added := cur | (1 << j.id)
			atomic.CompareAndSwapUint32(j.locked, cur, added)
			cur = atomic.LoadUint32(j.locked)
		}
		<-s.dbs[j.id].read
	}
	j.ch <- struct{}{}
}

// stillWaiting determines whether we're still waiting on anything in
// a given list terminating.
func (s *server) stillWaiting(id int, waitingOn []workload) bool {
	s.mu.Lock()
	readers := s.readers[id]
	s.mu.Unlock()
	for _, waiter := range waitingOn {
		for _, reader := range readers {
			if waiter == reader {
				return true
			}
		}
	}
	return false
}

// runWorkload runs the tasks within a workload, passing them to the worker
// queue, and then waiting for them all to complete. When it's done waiting
// for them, it releases any locks they obtained.
func (s *server) runWorkload(w workload) {
	var locked uint32
	defer func() {
		// unlock everything marked as locked
		read := atomic.LoadUint32(&locked)
		for i := 0; i < 32; i++ {
			if (read>>i)&1 != 0 {
				s.waiters[i].mu.Lock()
				s.mu.Lock()
				// remove us from readers list
				for j := range s.readers[i] {
					if s.readers[i][j] == w {
						copy(s.readers[i][j:], s.readers[i][j+1:])
						s.readers[i] = s.readers[i][:len(s.readers[i])-1]
						break
					}
				}
				s.mu.Unlock()
				s.waiters[i].mu.Unlock()
				// and wake up anything that was waiting for this.
				s.waiters[i].cond.Broadcast()
			}
		}
	}()
	ch := make(chan struct{})
	eg := &errgroup.Group{}
	j := job{ch: ch, locked: &locked, descr: w}
	for _, c := range w {
		switch {
		case c >= 'a' && c <= 'z':
			j.id = int(c - 'a')
			j.write = false
		case c >= 'A' && c <= 'Z':
			j.id = int(c - 'A')
			j.write = true
		default:
			s.tb.Logf("unhandled character '%c'", c)
			continue
		}
		j := j
		eg.Go(func() error {
			s.jobs <- &j
			<-ch
			return nil
		})
	}
	_ = eg.Wait()
}

// newServer creates a server associated with the given testing.TB,
// allowing us to log things.
func newServer(tb testing.TB) *server {
	s := &server{tb: tb, jobs: make(chan *job)}
	for i := range s.dbs {
		s.dbs[i].read = make(chan struct{})
		s.dbs[i].write = make(chan struct{})
		s.waiters[i].cond = sync.NewCond(&s.waiters[i].mu)
	}
	return s
}

// close shuts the server down by closing all of its channels, and may
// not really be necessary.
func (s *server) close() {
	for i := range s.dbs {
		db := &s.dbs[i]
		db.Lock()
		close(db.read)
		close(db.write)
		db.Unlock()
	}
	close(s.jobs)
}

// Satisfy satisfies the given read or write operations asynchronously,
// but waits for all of them in this batch to complete before returning.
func (s *server) satisfy(w workload) {
	var eg errgroup.Group
	for _, c := range w {
		var id int
		var write bool
		switch {
		case c >= 'a' && c <= 'z':
			id = int(c - 'a')
			write = false
		case c >= 'A' && c <= 'Z':
			id = int(c - 'A')
			write = true
		default:
			s.tb.Logf("unhandled character '%c'", c)
			continue
		}
		eg.Go(func() error {
			if write {
				s.dbs[id].write <- struct{}{}
			} else {
				s.dbs[id].read <- struct{}{}
			}
			return nil
		})
	}
	_ = eg.Wait()
}

// makeWorkload generates a sequence of letters, some of which may be
// capitalized, in order
func makeWorkload() workload {
	var letters [26]byte
	var n int
	write := rand.Intn(8) == 0
	for i := 0; i < 26; i++ {
		if rand.Intn(4) == 0 {
			if write {
				letters[n] = 'A' + byte(i)
			} else {
				letters[n] = 'a' + byte(i)
			}
			n++
		}
	}
	return workload(letters[:n])
}

// testRandomWorkload makes up an arbitrary workload and tries to run
// the server against it.
func testRandomWorkload(t *testing.T) {
	s := newServer(t)
	eg := &errgroup.Group{}
	p := NewPool(2, s.runJob, nil)
	s.pool = p
	defer p.Close()
	defer s.close()
	var workloads []workload // the requests we make
	var quick []workload     // the requests that get satisfied soon
	var slow []workload      // the requests that don't get satisfied until later
	for i := 0; i < 10; i++ {
		w := makeWorkload()
		if len(w) == 0 {
			continue
		}
		workloads = append(workloads, w)
		partial := rand.Intn(26)
		// possibly truncate and postpone some
		if partial < len(w) {
			quick = append(quick, w[:partial])
			slow = append(slow, w[partial:])
		} else {
			quick = append(quick, w)
		}
	}
	for _, w := range workloads {
		w := w
		eg.Go(func() error {
			s.runWorkload(w)
			return nil
		})
	}
	for _, w := range quick {
		w := w
		eg.Go(func() error {
			s.satisfy(w)
			return nil
		})
	}
	l, u, target := p.Stats()
	// Only one worker at a time can be invoking the mark-as-blocked logic,
	// so you can run after it marks that, but before the new worker is spawned,
	// but the next worker can't invoke the blocked logic until that completes.
	//
	// Live count always decreases after unblocked count on the exit path, and
	// increases before unblocked count on the startup path. So even if the
	// samples are interrupted, I think it should be impossible for live
	// to be less than unblocked.
	if u < target-1 || l < u {
		t.Fatalf("inconsistent pool stats: %d live, %d unblocked, %d target", l, u, target)
	}
	for _, w := range slow {
		w := w
		eg.Go(func() error {
			s.satisfy(w)
			return nil
		})
	}
	_ = eg.Wait()
}

// TestRandomWorkloads makes up some arbitrary workloads, then tries to
// satisfy them out of order.
// In theory, this should work for any sequence of operations as long as
// no operation has the same letter for both read and write ops, and
// ops always occur in order.
func TestRandomWorkloads(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			testRandomWorkload(t)
		})
	}
}

func TestServer(t *testing.T) {
	s := newServer(t)
	eg := &errgroup.Group{}
	p := NewPool(3, s.runJob, nil)
	s.pool = p
	defer p.Close()
	defer s.close()
	request := func(w workload) {
		eg.Go(func() error {
			s.runWorkload(w)
			return nil
		})
	}
	// requesting "abcd" means that the reader "abcd" will still be active on
	// a until all the other letters show up.
	request("abcd")
	// satisfy won't complete until at least two of the jobs have happened,
	// so there's a decent chance that we've marked ourselves as a reader on
	// a.
	s.satisfy("abc")
	// so we request a write on A. we get the write lock, but we can't
	// relinquish it until "d" shows up.
	request("A")
	// satisfy that request immediately, but to no avail.
	s.satisfy("A")
	// three more requests come in. if they get pool slots, they definitely
	// block; that request on A can't have finished yet. so they could fully
	// block our work pool.
	request("A")
	request("A")
	request("A")
	// spawn something to provide "efg"
	go s.satisfy("efg")
	// runWorkload means we actually block waiting for it. if all the worker
	// pool is blocked waiting on A, we can't do that.
	s.runWorkload("efg")
	// now we provide the missing d, which should allow the first request to
	// finally complete, and then the next three A, which should finish
	// the rest.
	s.satisfy("dAAA")
	// If we spawned new jobs, this should complete. Otherwise it should hang
	// because the requests can't be satisfied because the queue is full
	// of blocked operations.
	_ = eg.Wait()
}

func TestPoolStartup(t *testing.T) {
	var counter int32
	started := make(chan struct{})
	done := make(chan struct{})
	addAndWait := func() {
		<-started
		atomic.AddInt32(&counter, 1)
		<-done
	}
	// we expect this to spawn three counters
	p := NewPool(3, addAndWait, nil)
	time.Sleep(50 * time.Millisecond)
	v := atomic.LoadInt32(&counter)
	if v != 0 {
		t.Fatalf("expected no adds yet, got %d", v)
	}
	close(started)
	time.Sleep(50 * time.Millisecond)
	v = atomic.LoadInt32(&counter)
	if v != 3 {
		t.Fatalf("expected 3 adds, got %d", v)
	}
	// Tell the pool to stop processing jobs
	p.Shutdown()
	// Allow the jobs to complete. Since this happens after the
	// shutdown has set desired pool size to zero, they should now all exit.
	close(done)
	p.Close()
	time.Sleep(50 * time.Millisecond)
	v = atomic.LoadInt32(&counter)
	if v != 3 {
		t.Fatalf("expected no more adds, got %d including previous 3", v)
	}
}

// There was a race condition in Pool.Close(), where it was
// possible to have a worker thread broadcast to the condition
// variable *after* the Close had checked the current value of
// p.live, but *before* it had gotten to waiting. This is a very
// narrow window. If you're chasing this down, consider adding
// a short time.Sleep before the `p.Cond.Wait` call in `Pool.Close`,
// with which this test would typically deadlock within the first
// few iterations. The 1M repetitions produced about 65% failures
// on my laptop, with successes taking under two seconds to run.
//
// The key interaction is that the individual worker pool `work`
// calls are exiting almost immediately after `p.targetN` gets set
// to zero; if they take any time to exit, the Close will
// be waiting on the condition variable before they get there.
func TestPoolShutdown(t *testing.T) {
	for i := 0; i < 1000000; i++ {
		ch := make(chan struct{})
		doSomething := func() {
			<-ch
		}
		p := NewPool(3, doSomething, nil)
		close(ch)
		p.Close()
	}
}
