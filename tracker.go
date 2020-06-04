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
	Age   time.Duration `json:"age"`
}

type activeQuery struct {
	query   string
	started time.Time
}

type queryStatusUpdate struct {
	q   *activeQuery
	end bool
}

type queryTracker struct {
	updates chan<- queryStatusUpdate
	checks  chan<- chan<- []*activeQuery
	wg      sync.WaitGroup
	stop    chan struct{}
}

func newQueryTracker() *queryTracker {
	done := make(chan struct{})
	updates := make(chan queryStatusUpdate, 128)
	checks := make(chan chan<- []*activeQuery)
	tracker := &queryTracker{
		updates: updates,
		checks:  checks,
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

func (t *queryTracker) Start(query string) *activeQuery {
	now := time.Now()
	q := &activeQuery{query, now}
	t.updates <- queryStatusUpdate{q, false}
	return q
}

func (t *queryTracker) Finish(q *activeQuery) {
	t.updates <- queryStatusUpdate{q, true}
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
		out[i] = ActiveQueryStatus{v.query, now.Sub(v.started)}
	}
	return out
}

func (t *queryTracker) Stop() {
	close(t.stop)
	t.wg.Wait()
}
