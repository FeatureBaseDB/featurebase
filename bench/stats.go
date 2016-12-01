package bench

import (
	"time"
)

type Stats struct {
	Min     time.Duration
	Max     time.Duration
	Total   time.Duration
	Num     int64
	All     []time.Duration
	SaveAll bool
}

func NewStats() *Stats {
	return &Stats{
		Min: 1<<63 - 1,
		All: make([]time.Duration, 0),
	}
}

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
}

func (s *Stats) Avg() time.Duration {
	return s.Total / time.Duration(s.Num)
}

func AddToResults(s *Stats, results map[string]interface{}) {
	results["min"] = s.Min
	results["max"] = s.Max
	results["avg"] = s.Avg()
	if s.SaveAll {
		results["all"] = s.All
	}
}
