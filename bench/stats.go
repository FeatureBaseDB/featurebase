package bench

import (
	"math"
	"time"
)

// Stats object helps track timing stats.
type Stats struct {
	Min            time.Duration
	Max            time.Duration
	Mean           time.Duration
	sumSquareDelta float64
	Total          time.Duration
	Num            int64
	All            []time.Duration
	SaveAll        bool
}

// NewStats gets a Stats object.
func NewStats() *Stats {
	return &Stats{
		Min: 1<<63 - 1,
		All: make([]time.Duration, 0),
	}
}

// Add adds a new time to the stats object.
func (s *Stats) Add(td time.Duration) {
	if s.SaveAll {
		s.All = append(s.All, td)
	}
	s.Num += 1
	s.Total += td
	if td < s.Min {
		s.Min = td
	}
	if td > s.Max {
		s.Max = td
	}

	// online variance calculation
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
	delta := td - s.Mean
	s.Mean += delta / time.Duration(s.Num)
	s.sumSquareDelta += float64(delta * (td - s.Mean))
}

// Avg returns the average of all durations Added to the Stats object.
func (s *Stats) Avg() time.Duration {
	return s.Total / time.Duration(s.Num)
}

// AddToResults serializes the summary of Stats and adds them to the results
// map.
func AddToResults(s *Stats, results map[string]interface{}) {
	results["min"] = s.Min
	results["max"] = s.Max
	results["avg"] = s.Mean
	variance := s.sumSquareDelta / float64(s.Num)
	results["std"] = time.Duration(math.Sqrt(variance))
	if s.SaveAll {
		results["all"] = s.All
	}
}
