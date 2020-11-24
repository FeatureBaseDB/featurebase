// Copyright 2020 Pilosa Corp.
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
	"sort"
	"sync"
	"time"
)

type ActiveQueryStatus struct {
	Query string        `json:"query"`
	Node  string        `json:"node"`
	Index string        `json:"index"`
	Age   time.Duration `json:"age"`
}

type PastQueryStatus struct {
	Query   string        `json:"query"`
	Node    string        `json:"nodeID"`
	Index   string        `json:"index"`
	Start   time.Time     `json:"start"`
	Runtime time.Duration `json:"runtime"`
}

type activeQuery struct {
	query   string
	node    string
	index   string
	started time.Time
}

type pastQuery struct {
	query   string
	node    string
	index   string
	started time.Time
	runtime time.Duration
}

type queryStatusUpdate struct {
	q       *activeQuery
	end     bool
	endTime time.Time
}

type queryTracker struct {
	updates chan<- queryStatusUpdate
	checks  chan<- chan<- []*activeQuery
	history *ringBuffer

	wg   sync.WaitGroup
	stop chan struct{}
}

type ringBuffer struct {
	queries []pastQuery
	start   int
	count   int
	mu      sync.Mutex
}

// newRingBuffer initializes an empty RingBuffer of specified capacity.
func newRingBuffer(n int) *ringBuffer {
	return &ringBuffer{
		queries: make([]pastQuery, n),
		start:   0,
		count:   0,
	}
}

// add adds a new element to the queue, overwriting the oldest if it is already full.
func (b *ringBuffer) add(q pastQuery) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// len(b.queries) is used here as the *capacity* of the ringBuffer
	b.queries[(b.start+b.count)%len(b.queries)] = q
	if b.count == len(b.queries) {
		b.start = (b.start + 1) % len(b.queries)
	} else {
		b.count++
	}
}

// slice returns the contents of the RingBuffer, in insertion order.
func (b *ringBuffer) slice() []pastQuery {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append(b.queries[b.start:b.count], b.queries[0:b.start]...)
}

func newQueryTracker(historyLength int) *queryTracker {
	done := make(chan struct{})
	updates := make(chan queryStatusUpdate, 128)
	checks := make(chan chan<- []*activeQuery)
	history := newRingBuffer(historyLength)
	tracker := &queryTracker{
		updates: updates,
		checks:  checks,
		history: history,
		stop:    done,
	}
	tracker.wg.Add(1)
	go func() {
		defer tracker.wg.Done()

		activeQueries := make(map[*activeQuery]struct{})

		for {
			select {
			case update := <-updates:
				if update.end {
					pq := pastQuery{update.q.query, update.q.node, update.q.index, update.q.started, update.endTime.Sub(update.q.started)}
					tracker.history.add(pq)
					delete(activeQueries, update.q)
				} else {
					activeQueries[update.q] = struct{}{}
				}
			case check := <-checks:
				out := make([]*activeQuery, len(activeQueries))
				i := 0
				for q := range activeQueries {
					out[i] = q
					i++
				}
				check <- out
				close(check)
			case <-done:
				return
			}
		}
	}()
	return tracker
}

func (t *queryTracker) Start(query, nodeID, index string, start time.Time) *activeQuery {
	q := &activeQuery{query, nodeID, index, start}
	t.updates <- queryStatusUpdate{q, false, time.Time{}}
	return q
}

func (t *queryTracker) Finish(q *activeQuery) {
	t.updates <- queryStatusUpdate{q, true, time.Now()}
}

func (t *queryTracker) ActiveQueries() []ActiveQueryStatus {
	ch := make(chan []*activeQuery, 1)
	t.checks <- ch
	queries := <-ch
	sort.Slice(queries, func(i, j int) bool {
		switch {
		case queries[i].started.Before(queries[j].started):
			return true
		case queries[i].started.After(queries[j].started):
			return false
		case queries[i].query < queries[j].query:
			return true
		case queries[i].query > queries[j].query:
			return false
		default:
			return false
		}
	})
	now := time.Now()
	out := make([]ActiveQueryStatus, len(queries))
	for i, v := range queries {
		out[i] = ActiveQueryStatus{v.query, v.node, v.index, now.Sub(v.started)}
	}
	return out
}

func (t *queryTracker) PastQueries() []PastQueryStatus {
	queries := t.history.slice()
	out := make([]PastQueryStatus, len(queries))
	for i, v := range queries {
		out[i] = PastQueryStatus{v.query, v.node, v.index, v.started, v.runtime}
	}
	return out

}

func (t *queryTracker) Stop() {
	close(t.stop)
	t.wg.Wait()
}
