package pilosa

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidTimeQuantum is returned when parsing a time quantum.
var ErrInvalidTimeQuantum = errors.New("invalid time quantum")

// TimeQuantum represents a time granularity for time-based bitmaps.
type TimeQuantum string

// HasYear returns true if the quantum contains a 'Y' unit.
func (q TimeQuantum) HasYear() bool { return strings.ContainsRune(string(q), 'Y') }

// HasMonth returns true if the quantum contains a 'M' unit.
func (q TimeQuantum) HasMonth() bool { return strings.ContainsRune(string(q), 'M') }

// HasDay returns true if the quantum contains a 'D' unit.
func (q TimeQuantum) HasDay() bool { return strings.ContainsRune(string(q), 'D') }

// HasHour returns true if the quantum contains a 'H' unit.
func (q TimeQuantum) HasHour() bool { return strings.ContainsRune(string(q), 'H') }

// Valid returns true if q is a valid time quantum value.
func (q TimeQuantum) Valid() bool {
	switch q {
	case "Y", "YM", "YMD", "YMDH",
		"M", "MD", "MDH",
		"D", "DH",
		"H",
		"":
		return true
	default:
		return false
	}
}

// ParseTimeQuantum parses v into a time quantum.
func ParseTimeQuantum(v string) (TimeQuantum, error) {
	q := TimeQuantum(strings.ToUpper(v))
	if !q.Valid() {
		return "", ErrInvalidTimeQuantum
	}
	return q, nil
}

// FrameByTimeUnit returns the frame name for time with a given quantum unit.
func FrameByTimeUnit(name string, t time.Time, unit rune) string {
	switch unit {
	case 'Y':
		return fmt.Sprintf("%s_%s", name, t.Format("2006"))
	case 'M':
		return fmt.Sprintf("%s_%s", name, t.Format("200601"))
	case 'D':
		return fmt.Sprintf("%s_%s", name, t.Format("20060102"))
	case 'H':
		return fmt.Sprintf("%s_%s", name, t.Format("2006010215"))
	default:
		return ""
	}
}

// FramesByTime returns a list of frames for a given timestamp.
func FramesByTime(name string, t time.Time, q TimeQuantum) []string {
	a := make([]string, 0, len(q))
	for _, unit := range q {
		frame := FrameByTimeUnit(name, t, unit)
		if frame == "" {
			continue
		}
		a = append(a, frame)
	}
	return a
}

// FramesByTimeRange returns a list of frames to traverse to query a time range.
func FramesByTimeRange(name string, start, end time.Time, q TimeQuantum) []string {
	t := start

	// Save flags for performance.
	hasYear := q.HasYear()
	hasMonth := q.HasMonth()
	hasDay := q.HasDay()
	hasHour := q.HasHour()

	var results []string

	// Walk up from smallest units to largest units.
	if hasHour || hasDay || hasMonth {
		for t.Before(end) {
			if hasHour {
				if !nextDayGTE(t, end) {
					break
				} else if t.Hour() != 0 {
					results = append(results, FrameByTimeUnit(name, t, 'H'))
					t = t.Add(time.Hour)
					continue
				}

			}

			if hasDay {
				if !nextMonthGTE(t, end) {
					break
				} else if t.Day() != 1 {
					results = append(results, FrameByTimeUnit(name, t, 'D'))
					t = t.AddDate(0, 0, 1)
					continue
				}
			}

			if hasMonth {
				if !nextYearGTE(t, end) {
					break
				} else if t.Month() != 1 {
					results = append(results, FrameByTimeUnit(name, t, 'M'))
					t = t.AddDate(0, 1, 0)
					continue
				}
			}

			// If a unit exists but isn't set and there are no larger units
			// available then we need to exit the loop because we are no longer
			// making progress.
			break
		}
	}

	// Walk back down from largest units to smallest units.
	for t.Before(end) {
		if hasYear && nextYearGTE(t, end) {
			results = append(results, FrameByTimeUnit(name, t, 'Y'))
			t = t.AddDate(1, 0, 0)
		} else if hasMonth && nextMonthGTE(t, end) {
			results = append(results, FrameByTimeUnit(name, t, 'M'))
			t = t.AddDate(0, 1, 0)
		} else if hasDay && nextDayGTE(t, end) {
			results = append(results, FrameByTimeUnit(name, t, 'D'))
			t = t.AddDate(0, 0, 1)
		} else if hasHour {
			results = append(results, FrameByTimeUnit(name, t, 'H'))
			t = t.Add(time.Hour)
		} else {
			break
		}
	}

	return results
}

func nextYearGTE(t time.Time, end time.Time) bool {
	next := t.AddDate(1, 0, 0)
	if next.Year() == end.Year() {
		return true
	}
	return end.After(next)
}

func nextMonthGTE(t time.Time, end time.Time) bool {
	next := t.AddDate(0, 1, 0)
	y1, m1, _ := next.Date()
	y2, m2, _ := end.Date()
	if (y1 == y2) && (m1 == m2) {
		return true
	}
	return end.After(next)
}

func nextDayGTE(t time.Time, end time.Time) bool {
	next := t.AddDate(0, 0, 1)
	y1, m1, d1 := next.Date()
	y2, m2, d2 := end.Date()
	if (y1 == y2) && (m1 == m2) && (d1 == d2) {
		return true
	}
	return end.After(next)
}
