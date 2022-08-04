// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

// util_test.go has unit tests for utility functions from util.go

import (
	"testing"
	"time"
)

func TestGetLoopProgress(t *testing.T) {
	// TODO: try to find more sneaky cases
	cases := []struct {
		name  string
		start time.Time
		now   time.Time
		i     uint
		total uint
	}{
		{
			"one minute, half done",
			time.Date(1969, time.June, 9, 4, 20, 0, 0, time.UTC),
			time.Date(1969, time.June, 9, 4, 21, 0, 0, time.UTC),
			10,
			20,
		},
		{
			"four seconds, half done",
			time.Date(1969, time.June, 9, 4, 20, 0, 0, time.UTC),
			time.Date(1969, time.June, 9, 4, 20, 4, 0, time.UTC),
			10,
			20,
		},
		{
			"one minute, one second, 5 μs, half done",
			time.Date(1969, time.June, 9, 4, 20, 0, 0, time.UTC),
			time.Date(1969, time.June, 9, 4, 21, 1, 5, time.UTC),
			10,
			20,
		},
		{
			"one minute, one done",
			time.Date(1969, time.June, 9, 4, 20, 0, 0, time.UTC),
			time.Date(1969, time.June, 9, 4, 21, 0, 0, time.UTC),
			1,
			20,
		},
		{
			"one minute, 1/5 done",
			time.Date(1969, time.June, 9, 4, 20, 0, 0, time.UTC),
			time.Date(1969, time.June, 9, 4, 21, 0, 0, time.UTC),
			10,
			50,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// we expect that it will be the avg time per message times
			// the number of remaining messages
			expected := time.Duration((float64(c.now.Sub(c.start)) / float64(c.i+1)) * float64(c.total-(c.i+1)))
			expectedPct := 100 * (float64(c.i+1) / float64(c.total))

			timeLeft, pctDone := GetLoopProgress(c.start, c.now, c.i, c.total)
			if timeLeft != expected {
				t.Errorf("Time left was incorrect, expected: %d, but got: %d", expected, timeLeft)
			}
			if pctDone != expectedPct {
				t.Errorf("Percentage done was incorrect, expected: %f, but got: %f", expectedPct, pctDone)
			}
		})
	}
}

func TestGetMemoryUsage(t *testing.T) {
	if _, err := GetMemoryUsage(); err != nil {
		t.Fatalf("unexpected error getting memory usage: %v", err)
	}
}

func TestGetDiskUsage(t *testing.T) {
	tdir := t.TempDir()
	if _, err := GetDiskUsage(tdir); err != nil {
		t.Fatalf("unexpected error getting disk usage: %v", err)
	}

	if _, err := GetDiskUsage(""); err == nil {
		t.Fatal("expected error getting disk usage but got nil")
	}

}
