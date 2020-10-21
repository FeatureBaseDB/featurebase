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
	Age   time.Duration `json:"age"`
}

type PastQueryStatus struct {
	Query   string        `json:"query"`
	Node    string        `json:"node"`
	Age     time.Duration `json:"age"`
	Runtime time.Duration `json:"runtime"`
}

type activeQuery struct {
	query   string
	node    string
	started time.Time
}

type pastQuery struct {
	query   string
	node    string
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
	history map[pastQuery]struct{} // TODO not in memory
	wg      sync.WaitGroup
	stop    chan struct{}
}

func newQueryTracker() *queryTracker {
	done := make(chan struct{})
	updates := make(chan queryStatusUpdate, 128)
	checks := make(chan chan<- []*activeQuery)
	history := make(map[pastQuery]struct{})
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
					delete(activeQueries, update.q)
				} else {
					activeQueries[update.q] = struct{}{}
					pq := pastQuery{update.q.query, update.q.node, update.q.started, update.endTime.Sub(update.q.started)} // TODO move end time to api.go
					tracker.history[pq] = struct{}{}
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

func (t *queryTracker) Start(query, nodeID string) *activeQuery {
	now := time.Now()
	q := &activeQuery{query, nodeID, now}
	t.updates <- queryStatusUpdate{q, false, time.Time{}}
	return q
}

func (t *queryTracker) Finish(q *activeQuery, endTime time.Time) {
	t.updates <- queryStatusUpdate{q, true, endTime}
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
		out[i] = ActiveQueryStatus{v.query, v.node, now.Sub(v.started)}
	}
	return out
}

func (t *queryTracker) PastQueries() []PastQueryStatus {
	queries := make([]pastQuery, 0, len(t.history))
	for pq := range t.history {
		queries = append(queries, pq)
	}
	// TODO use sort.Sort
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
	out := make([]PastQueryStatus, len(queries))
	for i, v := range queries {
		out[i] = PastQueryStatus{v.query, v.node, now.Sub(v.started), v.runtime}
	}
	return out

}

func (t *queryTracker) Stop() {
	close(t.stop)
	t.wg.Wait()
}
