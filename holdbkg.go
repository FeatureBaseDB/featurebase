// Copyright 2017 Pilosa Corp.
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
	"github.com/glycerine/idem"
)

// holderBackgroundGoro avoids the deadlock versus race dilmena
// when creating a new index. The troubles were in having
// the field/view code try to inform the Holder of the indexes
// from a platoon of worker goroutines and tests; see index.go:273 inside
// Index.openFields(). There calling i.holder.addIndex(i) instead
// of locking cleans things up alot. There's no need anymore
// to track which goroutines are holding the Holder's
// mu lock.
type holderBackgroundGoro struct {
	h          *Holder
	halt       *idem.Halter
	reqIndexCh chan *indexReq
	setIdxCh   chan *Index
	getAllCh   chan *indexReq
	delIdxCh   chan string
}

// indexReq is used to ask the holderBackgrounGoro
// for index information.
type indexReq struct {
	// basic request to map an index name to *Index
	index string
	idx   *Index

	// double duty as a getAll indexes request
	getAll bool
	all    []*Index

	done chan struct{}
}

func newIndexReq(index string) *indexReq {
	return &indexReq{
		index: index,
		done:  make(chan struct{}),
	}
}

func (b *holderBackgroundGoro) index(index string) *Index {
	if b == nil {
		// utils_internal_test.go:448 from
		// TestCluster_ResizeStates/Multiple_nodes,_with_data
		// wants to pass a nil b/nil Holder. sigh. don't panic.
		return nil
	}
	req := newIndexReq(index)
	b.reqIndexCh <- req
	<-req.done
	return req.idx
}

func (b *holderBackgroundGoro) isClosed() bool {
	select {
	case <-b.halt.Done.Chan:
		return true
	default:
		return false
	}
}

func (b *holderBackgroundGoro) start() {
	go func() {
		defer b.halt.Done.Close()
		for {
			select {
			case <-b.halt.ReqStop.Chan:
				return

			case r := <-b.reqIndexCh:
				r.idx = b.h.indexesOwnedByBkgr[r.index]
				close(r.done)

			case idx := <-b.setIdxCh:
				b.h.indexesOwnedByBkgr[idx.Name()] = idx
				// atomic operation, client doesn't need to wait on done,
				// so don't take the time to do another channel, just leave done open.

			case target := <-b.delIdxCh:
				delete(b.h.indexesOwnedByBkgr, target)
				// atomic operation, client doesn't need to wait on done,
				// so don't take the time to do another channel, just leave done open.

			case r := <-b.getAllCh:
				r.all = make([]*Index, 0, len(b.h.indexesOwnedByBkgr))
				for _, index := range b.h.indexesOwnedByBkgr {
					r.all = append(r.all, index)
				}
				close(r.done)
			}
		}
	}()
}

func (h *Holder) stopBkgr() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.bkgr != nil {
		h.bkgr.halt.ReqStop.Close()
		<-h.bkgr.halt.Done.Chan
	}
}

func (h *Holder) newHolderBackgroundGoro() (b *holderBackgroundGoro) {

	b = &holderBackgroundGoro{
		h:          h,
		halt:       idem.NewHalter(),
		reqIndexCh: make(chan *indexReq),
		setIdxCh:   make(chan *Index),
		getAllCh:   make(chan *indexReq),
		delIdxCh:   make(chan string),
	}
	b.start()
	return b
}
