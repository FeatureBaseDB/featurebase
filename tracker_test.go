package pilosa

import (
	"fmt"
	"testing"
	"time"
)

func TestRingBuffer(t *testing.T) {
	tests := []struct {
		start   int
		count   int
		queries []string
	}{
		{start: 0, count: 0, queries: []string{}},
		{start: 0, count: 1, queries: []string{"0"}},
		{start: 0, count: 2, queries: []string{"0", "1"}},
		{start: 0, count: 3, queries: []string{"0", "1", "2"}},
		{start: 0, count: 4, queries: []string{"0", "1", "2", "3"}},
		{start: 0, count: 5, queries: []string{"0", "1", "2", "3", "4"}},
		{start: 1, count: 5, queries: []string{"1", "2", "3", "4", "5"}},
		{start: 2, count: 5, queries: []string{"2", "3", "4", "5", "6"}},
		{start: 3, count: 5, queries: []string{"3", "4", "5", "6", "7"}},
		{start: 4, count: 5, queries: []string{"4", "5", "6", "7", "8"}},
		{start: 0, count: 5, queries: []string{"5", "6", "7", "8", "9"}},
		{start: 1, count: 5, queries: []string{"6", "7", "8", "9", "10"}},
		{start: 2, count: 5, queries: []string{"7", "8", "9", "10", "11"}},
	}
	buffer := newRingBuffer(5)
	for k := 0; k < len(tests); k++ {
		if !(buffer.start == tests[k].start && buffer.count == tests[k].count) {
			t.Fatalf("expected %d %d, found %d %d", tests[k].start, tests[k].count, buffer.start, buffer.count)
		}

		for n, q := range buffer.slice() {
			if q.PQL != tests[k].queries[n] {
				t.Fatalf("test[%d], buffer[%d] expected querystring '%s', found '%s'", k, n, tests[k].queries[n], q.PQL)
			}
		}
		buffer.add(pastQuery{PQL: fmt.Sprintf("%d", k)})
	}
}

func TestQueryTracker(t *testing.T) {
	tracker := newQueryTracker(5)
	defer tracker.Stop()

	if queries := tracker.ActiveQueries(); len(queries) > 0 {
		t.Fatalf("expected no active queries; found %v", queries)
	}

	qs := tracker.Start("test query", "test SQL", "node0", "i", time.Now())

	var queries []ActiveQueryStatus
	for len(queries) < 1 {
		queries = tracker.ActiveQueries()
	}
	if len(queries) > 1 || queries[0].PQL != "test query" {
		t.Fatalf("unexpected queries: %v", queries)
	}

	tracker.Finish(qs)

	for len(queries) > 0 {
		queries = tracker.ActiveQueries()
	}
}
