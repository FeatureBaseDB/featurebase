// home https://github.com/glycerine/lmdb-go
// Copyright (c) 2020, the lmdb-go authors
// Copyright (c) 2015, Bryan Matsuo
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

//     Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.

//     Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.

//     Neither the name of the author nor the names of its contributors may be
//     used to endorse or promote products derived from this software without specific
//     prior written permission.

// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package pilosa

import (
	"github.com/glycerine/idem"
)

// Barrier allows us to temporarily halt all readers, so that
// a writer can commit alone and thus compact the db.
// The Barrier starts unblocked, alllowing passage to any
// caller of WaitAtGate().
type Barrier struct {
	wait       chan *appointment // send upon entering the waiting room.
	halt       *idem.Halter
	blockReqCh chan *blockReq
	unblockCh  chan *unblock
}

type blockReq struct {
	count int
	done  chan struct{}
}

func newBlockReq(count int) *blockReq {
	return &blockReq{
		count: count,
		done:  make(chan struct{}),
	}
}

type appointment struct {
	id   int
	done chan struct{}
}

func newAppointment(id int) *appointment {
	return &appointment{
		id:   id,
		done: make(chan struct{}),
	}
}

// NewBarrier is either open, allowing immediate passage,
// or blocked, halting all callers at WaitAtGate()
// until the barrier is opened. By default it is open.
//
// Barrier.Close() must be called when the barrier
// is no longer needed to avoid a goroutine leak.
func NewBarrier() (b *Barrier) {
	b = &Barrier{
		wait:       make(chan *appointment), // waiters indicate they are waiting for the gate by sending here.
		halt:       idem.NewHalter(),
		blockReqCh: make(chan *blockReq),
		unblockCh:  make(chan *unblock),
	}
	go func() {
		defer b.halt.Done.Close()

		var waitlist []*appointment
		var curBlockReq *blockReq

		for {
			select {
			case br := <-b.blockReqCh:
				if br.count == 0 {
					close(br.done)
					continue
				}
				if curBlockReq == nil {
					// good, changing state from open to closed barrier.
				} else {
					panic("got 2nd block request atop of first")
				}
				curBlockReq = br
				//vv("barrier: request to block for %v waiters", br.count)
				if len(waitlist) != 0 {
					panic("had waiters when we were open, internal/client bug")
				}
			case appt := <-b.wait:
				//vv("barrier.wait sees appt = '%#v' and  curBlockReq = '%#v'", appt, curBlockReq)
				if curBlockReq == nil {
					close(appt.done)
					continue
				}
				waitlist = append(waitlist, appt)
				n := len(waitlist)
				th := curBlockReq.count
				if th < 0 {
					// infinite waiters. we block everybody until we
					// see an unblock request.
					continue
				}
				if n >= th {
					close(curBlockReq.done)
					curBlockReq = nil
				}
			case ub := <-b.unblockCh:
				for _, appt := range waitlist {
					close(appt.done)
				}
				waitlist = nil
				curBlockReq = nil
				close(ub.done)
			case <-b.halt.ReqStop.Chan:
				return
			}
		}
	}()
	return
}

// WaitAtGate will return immediately
// if the barrier is unblocked. Otherwise
// it will not return until another
// goroutine unblocks the barrier.
func (b *Barrier) WaitAtGate(id int) {
	appt := newAppointment(id)
	select {
	case b.wait <- appt:
		select {
		case <-appt.done:
		case <-b.halt.ReqStop.Chan:
		}
	case <-b.halt.ReqStop.Chan:
	}
}

// Close should be called to stop the
// barrier's background goroutine when
// you are done using the barrier.
func (b *Barrier) Close() {
	b.halt.ReqStop.Close()
	<-b.halt.Done.Chan
}

type unblock struct {
	done chan struct{}
}

func newUnblock() *unblock {
	return &unblock{
		done: make(chan struct{}),
	}
}

// Unblock lets all waiting goroutines resume execution.
func (b *Barrier) UnblockReaders() {
	ub := newUnblock()
	select {
	case b.unblockCh <- ub:
		select {
		case <-ub.done:
		case <-b.halt.ReqStop.Chan:
		}
	case <-b.halt.ReqStop.Chan:
	}
}

// BlockUntil is called with a count, the
// number of waiters required to be present and waiting
// at the gate before call returns.
// A count of < 0 will return immediately and raise
// the barrier to any number of arriving readers.
// A count of 0 is a no-op.
//
// Otherwise we raise the barrier
// and wait until we have seen count other goroutines waiting
// on it.
//
// We return without releasing the waiters. Call
// Open when you want them to resume.
func (b *Barrier) BlockUntil(count int) {
	if count == 0 {
		return
	}
	req := newBlockReq(count)
	b.blockReqCh <- req
	if count > 0 {
		<-req.done
	}
}

// BlockAllReadersNoWait raises the barrier to
// an infinite number of waiters and returns immediately
// to the caller.
func (b *Barrier) BlockAllReadersNoWait() {
	req := newBlockReq(-1) // -1 means block any number of readers.
	b.blockReqCh <- req
	// don't wait. <-req.done
}
