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
	"testing"
	"time"
)

func TestQueryTracker(t *testing.T) {
	tracker := newQueryTracker()
	defer tracker.Stop()

	if queries := tracker.ActiveQueries(); len(queries) > 0 {
		t.Fatalf("expected no active queries; found %v", queries)
	}

	qs := tracker.Start("test query", "node0")

	var queries []ActiveQueryStatus
	for len(queries) < 1 {
		queries = tracker.ActiveQueries()
	}
	if len(queries) > 1 || queries[0].Query != "test query" {
		t.Fatalf("unexpected queries: %v", queries)
	}

	tracker.Finish(qs, time.Now())

	for len(queries) > 0 {
		queries = tracker.ActiveQueries()
	}
}
