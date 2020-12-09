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
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestBarrierHolds(t *testing.T) {
	b := NewBarrier()
	defer b.Close()

	released := int64(0)

	waiter := func(i int) {
		b.WaitAtGate(i)
		//vv("goro %v is released", i)
		atomic.AddInt64(&released, 1)
	}

	for i := 0; i < 3; i++ {
		go waiter(i)
	}
	time.Sleep(time.Second)
	r := atomic.SwapInt64(&released, 0)
	if r != 3 {
		panic("open barrier held back goro")
	}
	//vv("good: barrier started open")

	//seenAll := make(chan bool)
	b.BlockAllReadersNoWait()
	for i := 0; i < 3; i++ {
		go waiter(i)
	}

	time.Sleep(time.Second)
	r = atomic.SwapInt64(&released, 0)
	if r != 0 {
		panic("bad: barrier did not hold back goro")
	}
	//vv("good: barrier of 4 did not release on 3")
	go waiter(4)

	time.Sleep(time.Second)
	r = atomic.SwapInt64(&released, 0)
	if r != 0 {
		panic(fmt.Sprintf("bad: barrier did not hold back goro, should wait for unblock. r = %v", r))
	}

	b.UnblockReaders()

	time.Sleep(time.Second)
	r = atomic.SwapInt64(&released, 0)
	if r != 4 {
		panic("bad: unblock should have released 4 goro")
	}

}
