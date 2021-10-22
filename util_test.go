package pilosa

// util_test.go has unit tests for utility functions from util.go

import (
	"testing"
	"time"
)

func TestEstTimeLeft(t *testing.T) {
	cases := []struct {
		start time.Time
		now   time.Time
		i     uint
		total uint
	}{
		{
			time.Date(1969, time.June, 9, 4, 20, 0, 0, time.UTC),
			time.Date(1969, time.June, 9, 4, 21, 0, 0, time.UTC),
			10,
			20,
		},
		{
			time.Date(1969, time.June, 9, 4, 20, 0, 0, time.UTC),
			time.Date(1969, time.June, 9, 4, 20, 4, 0, time.UTC),
			10,
			20,
		},
		{
			time.Date(1969, time.June, 9, 4, 20, 0, 0, time.UTC),
			time.Date(1969, time.June, 9, 4, 21, 1, 5, time.UTC),
			10,
			20,
		},
		{
			time.Date(1969, time.June, 9, 4, 20, 0, 0, time.UTC),
			time.Date(1969, time.June, 9, 4, 21, 0, 0, time.UTC),
			1,
			20,
		},
		{
			time.Date(1969, time.June, 9, 4, 20, 0, 0, time.UTC),
			time.Date(1969, time.June, 9, 4, 21, 0, 0, time.UTC),
			10,
			50,
		},
	}

	for _, c := range cases {
		// we expect that it will be the avg time per message times
		// the number of remaining messages
		expected := time.Duration((float64(c.now.Sub(c.start)) / float64(c.i+1)) * float64(c.total-(c.i+1)))

		timeLeft := EstTimeLeft(c.start, c.now, c.i, c.total)
		if timeLeft != expected {
			t.Errorf("Time left was incorrect, expected: %d, but got: %d", expected, timeLeft)
		}
	}
}
