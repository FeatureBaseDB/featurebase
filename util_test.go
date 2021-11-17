// Copyright 2021 Pilosa Corp.
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

// util_test.go has unit tests for utility functions from util.go
//

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

func TestFormatTimestampNano(t *testing.T) {
	if FormatTimestampNano(0, 69, "s") != "1970-01-01T00:01:09Z" {
		t.Fatal("Timestamp not formatted properly")
	}
	if FormatTimestampNano(0, 420, "ms") != "1970-01-01T00:00:00.42Z" {
		t.Fatal("Timestamp not formatted properly")
	}
	if FormatTimestampNano(420, 0, "μs") != "1970-01-01T00:00:00.00000042Z" {
		t.Fatal("Timestamp not formatted properly")
	}
	if FormatTimestampNano(420, 69, "us") != "1970-01-01T00:00:00.000489Z" {
		t.Fatal("Timestamp not formatted properly")
	}
	if FormatTimestampNano(69, 420, "ns") != "1970-01-01T00:00:00.000000489Z" {
		t.Fatal("Timestamp not formatted properly")
	}
}
