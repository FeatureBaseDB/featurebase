// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"sort"
	"sync"
	"time"
)

type ActiveQueryStatus struct {
	PQL   string        `json:"PQL"`
	SQL   string        `json:"SQL,omitempty"`
	Node  string        `json:"node"`
	Index string        `json:"index"`
	Age   time.Duration `json:"age"`
}

type PastQueryStatus struct {
	PQL       string        `json:"PQL"`
	SQL       string        `json:"SQL,omitempty"`
	Node      string        `json:"nodeID"`
	Index     string        `json:"index"`
	Start     time.Time     `json:"start"`
	Runtime   time.Duration `json:"runtime"` // deprecated
	RuntimeNs time.Duration `json:"runtimeNanoseconds"`
}

type activeQuery struct {
	PQL     string
	SQL     string
	node    string
	index   string
	started time.Time
}

type pastQuery struct {
	PQL     string
	SQL     string
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
					pq := pastQuery{update.q.PQL, update.q.SQL, update.q.node, update.q.index, update.q.started, update.endTime.Sub(update.q.started)}
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

func (t *queryTracker) Start(pql, sql, nodeID, index string, start time.Time) *activeQuery {
	q := &activeQuery{pql, sql, nodeID, index, start}
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
		case queries[i].PQL < queries[j].PQL:
			return true
		case queries[i].PQL > queries[j].PQL:
			return false
		default:
			return false
		}
	})
	now := time.Now()
	out := make([]ActiveQueryStatus, len(queries))
	for i, v := range queries {
		out[i] = ActiveQueryStatus{v.PQL, v.SQL, v.node, v.index, now.Sub(v.started)}
	}
	return out
}

func (t *queryTracker) PastQueries() []PastQueryStatus {
	queries := t.history.slice()
	out := make([]PastQueryStatus, len(queries))
	for i, v := range queries {
		out[i] = PastQueryStatus{v.PQL, v.SQL, v.node, v.index, v.started, v.runtime, v.runtime}
	}
	return out

}

func (t *queryTracker) Stop() {
	close(t.stop)
	t.wg.Wait()
}
