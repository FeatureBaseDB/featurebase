package pilosa

import (
	"errors"
	"strings"
	"time"
)

// TimeQuantum represents a time granularity for time-based bitmap ids.
type TimeQuantum uint

// ParseTimeQuantum parses s into a quantum.
func ParseTimeQuantum(s string) (TimeQuantum, error) {
	switch strings.ToUpper(s) {
	case "Y":
		return Y, nil
	case "M":
		return YM, nil
	case "D":
		return YMD, nil
	case "H":
		return YMDH, nil
	default:
		return 0, errors.New("invalid quantum")
	}
}

// String returns the string representation of the quantum.
func (q TimeQuantum) String() string {
	switch q {
	case Y:
		return "Y"
	case YM:
		return "M"
	case YMD:
		return "D"
	case YMDH:
		return "H"
	default:
		return "Y"
	}
}

const (
	Y    TimeQuantum = 3
	YM   TimeQuantum = 2
	YMD  TimeQuantum = 1
	YMDH TimeQuantum = 0
)

// TimeID returns a packed time identifier from a year/month/day/hour & tile.
func TimeID(q TimeQuantum, year uint, month uint, day uint, hour uint, tileID uint64) uint64 {
	v := uint64((uint(q) << 30) | ((year - 1970) << 23) | (month << 19) | (day << 14) | (hour << 9))
	return (v << 32) | tileID
}

// TimeIDsFromRange returns timestamp bitmap ids within a time range.
func TimeIDsFromRange(start, end time.Time, tileID uint64) []uint64 {
	t := start

	var results []uint64
	for t.Before(end) {
		if !nextDay(t, end) {
			break
		}
		if t.Hour() == 0 {
			if !nextMonth(t, end) {
				break
			}

			if t.Day() == 1 {
				if !nextYear(t, end) {
					break
				}

				if t.Month() == 1 {
					break
				}

				results = append(results, TimeID(YM, uint(t.Year()), uint(t.Month()), 0, 0, tileID))
				t = t.AddDate(0, 1, 0)
			} else {
				results = append(results, TimeID(YMD, uint(t.Year()), uint(t.Month()), uint(t.Day()), 0, tileID))
				t = t.AddDate(0, 0, 1)
			}
		} else {
			results = append(results, TimeID(YMDH, uint(t.Year()), uint(t.Month()), uint(t.Day()), uint(t.Hour()), tileID))
			t = t.Add(time.Hour)
		}
	}

	for t.Before(end) {
		if nextYear(t, end) {
			results = append(results, TimeID(Y, uint(t.Year()), 0, 0, 0, tileID))
			t = t.AddDate(1, 0, 0)
		} else if nextMonth(t, end) {
			results = append(results, TimeID(YM, uint(t.Year()), uint(t.Month()), 0, 0, tileID))
			t = t.AddDate(0, 1, 0)
		} else if nextDay(t, end) {
			results = append(results, TimeID(YMD, uint(t.Year()), uint(t.Month()), uint(t.Day()), 0, tileID))
			t = t.AddDate(0, 0, 1)
		} else {
			results = append(results, TimeID(YMDH, uint(t.Year()), uint(t.Month()), uint(t.Day()), uint(t.Hour()), tileID))
			t = t.Add(time.Hour)
		}
	}

	return results
}

func nextYear(start time.Time, end time.Time) bool {
	next := start.AddDate(1, 0, 0)
	if next.Year() == end.Year() {
		return true
	}
	return end.After(next)
}

func nextMonth(start time.Time, end time.Time) bool {
	next := start.AddDate(0, 1, 0)
	y1, m1, _ := next.Date()
	y2, m2, _ := end.Date()
	if (y1 == y2) && (m1 == m2) {
		return true
	}
	return end.After(next)
}

func nextDay(start time.Time, end time.Time) bool {
	next := start.AddDate(0, 0, 1)
	y1, m1, d1 := next.Date()
	y2, m2, d2 := end.Date()
	if (y1 == y2) && (m1 == m2) && (d1 == d2) {
		return true
	}
	return end.After(next)
}

// TimeIDsFromQuantum returns a list of time identifiers for a single quantum.
func TimeIDsFromQuantum(q TimeQuantum, t time.Time, tileID uint64) []uint64 {
	y, m, d, h := uint(t.Year()), uint(t.Month()), uint(t.Day()), uint(t.Hour())

	v := make([]uint64, 0, 4)
	if q <= Y {
		v = append(v, TimeID(Y, y, 0, 0, 0, tileID))
	}
	if q <= YM {
		v = append(v, TimeID(YM, y, m, 0, 0, tileID))
	}
	if q <= YMD {
		v = append(v, TimeID(YMD, y, m, d, 0, tileID))
	}
	if q <= YMDH {
		v = append(v, TimeID(YMDH, y, m, d, h, tileID))
	}
	return v
}
