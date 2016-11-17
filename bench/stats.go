package bench

import (
	"math"
	"time"
)

type Stats struct {
	Min            time.Duration
	Max            time.Duration
	Mean           time.Duration
	SumSquareDelta float64
	Variance       float64
	StdDev         time.Duration
	Total          time.Duration
	Num            int64
}

func NewStats() *Stats {
	return &Stats{
		Min: 1<<63 - 1,
	}
}

func (s *Stats) Add(td time.Duration) {
	s.Num += 1
	s.Total += td
	if td < s.Min {
		s.Min = td
	}
	if td > s.Max {
		s.Max = td
	}

	// these three comprise an online variance calculation
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
	delta := td - s.Mean
	s.Mean += delta / time.Duration(s.Num)
	s.SumSquareDelta += float64(delta * (td - s.Mean))

	// these are the useful results, but don't need to be updated every iteration
	s.Variance = s.SumSquareDelta / float64(s.Num)
	s.StdDev = time.Duration(math.Sqrt(s.Variance))
}

func (s *Stats) Avg() time.Duration {
	return s.Total / time.Duration(s.Num)
}

func AddToResults(s *Stats, results map[string]interface{}) {
	results["min"] = s.Min
	results["max"] = s.Max
	results["avg"] = s.Avg()
}
