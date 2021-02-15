// Copyright 2017 Pilosa Corp.
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
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
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

// The following methods are required to implement pflag Value interface.

// Set sets the time quantum value.
func (q *TimeQuantum) Set(value string) error {
	*q = TimeQuantum(value)
	return nil
}

func (q TimeQuantum) String() string {
	return string(q)
}

// Type returns the type of a time quantum value.
func (q TimeQuantum) Type() string {
	return "TimeQuantum"
}

// viewByTimeUnit returns the view name for time with a given quantum unit.
func viewByTimeUnit(name string, t time.Time, unit rune) string {
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

// viewsByTime returns a list of views for a given timestamp.
func viewsByTime(name string, t time.Time, q TimeQuantum) []string { // nolint: unparam
	a := make([]string, 0, len(q))
	for _, unit := range q {
		view := viewByTimeUnit(name, t, unit)
		if view == "" {
			continue
		}
		a = append(a, view)
	}
	return a
}

// viewsByTimeRange returns a list of views to traverse to query a time range.
func viewsByTimeRange(name string, start, end time.Time, q TimeQuantum) []string { // nolint: unparam
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
					results = append(results, viewByTimeUnit(name, t, 'H'))
					t = t.Add(time.Hour)
					continue
				}

			}

			if hasDay {
				if !nextMonthGTE(t, end) {
					break
				} else if t.Day() != 1 {
					results = append(results, viewByTimeUnit(name, t, 'D'))
					t = t.AddDate(0, 0, 1)
					continue
				}
			}

			if hasMonth {
				if !nextYearGTE(t, end) {
					break
				} else if t.Month() != 1 {
					results = append(results, viewByTimeUnit(name, t, 'M'))
					t = addMonth(t)
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
			results = append(results, viewByTimeUnit(name, t, 'Y'))
			t = t.AddDate(1, 0, 0)
		} else if hasMonth && nextMonthGTE(t, end) {
			results = append(results, viewByTimeUnit(name, t, 'M'))
			t = addMonth(t)
		} else if hasDay && nextDayGTE(t, end) {
			results = append(results, viewByTimeUnit(name, t, 'D'))
			t = t.AddDate(0, 0, 1)
		} else if hasHour {
			results = append(results, viewByTimeUnit(name, t, 'H'))
			t = t.Add(time.Hour)
		} else {
			break
		}
	}

	return results
}

// addMonth adds a month similar to time.AddDate(0, 1, 0), but
// in certain edge cases it doesn't normalize for days late in the month.
// In the "YM" case where t.Day is greater than 28, there are
// edge cases where using time.AddDate() to add a month will result
// in two "months" being added (Jan 31 + 1mo = March 2).
func addMonth(t time.Time) time.Time {
	if t.Day() > 28 {
		t = time.Date(t.Year(), t.Month(), 1, t.Hour(), 0, 0, 0, t.Location())
	}
	t = t.AddDate(0, 1, 0)
	return t
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

// parseTime parses a string or int64 into a time.Time value.
func parseTime(t interface{}) (time.Time, error) {
	var err error
	var calcTime time.Time
	switch v := t.(type) {
	case string:
		if calcTime, err = time.Parse(TimeFormat, v); err != nil {
			// if the default parsing fails, check if user tried to
			// supply partial time eg year and month
			calcTime, err := parsePartialTime(v)
			return calcTime, err
		}
	case int64:
		calcTime = time.Unix(v, 0).UTC()
	default:
		return time.Time{}, errors.New("arg must be a timestamp")
	}
	return calcTime, nil
}

// parsePartialTime parses strings where the time provided is only partial
// eg given 2006-02, it extracts the year and month and the rest of the
// components are set to the default values. The time must have the format
// used in parseTime. The year must be present. The rest of the components are
// optional but if a component is present in the input, this implies that all
// the preceding components are also specified. For example, if the hour is provided
// then the day, month and year must be present. This function could and should be
// simplified
func parsePartialTime(t string) (time.Time, error) {
	// helper parseCustomHourMinute parses strings of the form HH:MM to
	// hour and minute component
	parseHourMinute := func(t string) (hour, minute int, err error) {
		// time should have the format HH:MM
		subStrings := strings.Split(t, ":")
		switch len(subStrings) {
		case 2:
			// has minutes
			minute, err = strconv.Atoi(subStrings[1])
			if err != nil {
				return -1, -1, errors.New("Invalid Time")
			}
			fallthrough
		case 1:
			hour, err = strconv.Atoi(subStrings[0])
			if err != nil {
				return -1, -1, errors.New("Invalid Time")
			}
		default:
			return -1, -1, errors.New("Invalid Time")
		}

		return
	}
	// helper trim function
	trim := func(subMatches []string) (filtered []string, err error) {
		restAreEmpty := func(ss []string) bool {
			for _, s := range ss {
				if s != "" {
					return false
				}
			}
			return true
		}
		if len(subMatches) <= 1 {
			return nil, errors.New("Invalid time")
		}
		// ignore full match which is at index 0
		subMatches = subMatches[1:] // ignore full match which is at index 0
		for i, s := range subMatches {
			if s != "" {
				if i > 0 {
					s = s[1:] // remove preceding hyphen or T
				}
				filtered = append(filtered, s)
			} else {
				// rest must be empty for date-time to be valid
				if !restAreEmpty(subMatches[i:]) {
					return nil, errors.New("Invalid date-time")
				}
				break
			}
		}
		return filtered, nil
	}

	var errInvalidTime error = errors.New("cannot parse string time")
	var regex = regexp.MustCompile(`^(\d{4})(-\d{2})?(-\d{2})?(T.+)?$`)
	subMatches := regex.FindStringSubmatch(t)
	subMatches, err := trim(subMatches)
	if err != nil {
		return time.Time{}, errInvalidTime
	}
	// defaults
	var (
		yr    int
		month = time.January
		day   = 1
		hour  = 0
		min   = 0
	)
	// year must be set, the rest are optional
	switch len(subMatches) {
	case 4:
		// time
		hour, min, err = parseHourMinute(subMatches[3])
		if err != nil {
			return time.Time{}, errInvalidTime
		}
		fallthrough
	case 3:
		// day
		day, err = strconv.Atoi(subMatches[2])
		if err != nil {
			return time.Time{}, errInvalidTime
		}
		fallthrough
	case 2:
		// month
		monthNum, err := strconv.Atoi(subMatches[1])
		month = time.Month(monthNum)
		if err != nil {
			return time.Time{}, errInvalidTime
		}
		fallthrough
	case 1:
		// year
		yr, err = strconv.Atoi(subMatches[0])
		if err != nil {
			return time.Time{}, errInvalidTime
		}
	default:
		return time.Time{}, errInvalidTime
	}
	return time.Date(yr, month, day, hour, min, 0, 0, time.UTC), nil
}

// minMaxViews returns the min and max view from a list of views
// with a time quantum taken into consideration. It assumes that
// all views represent the same base view name (the logic depends
// on the views sorting correctly in alphabetical order).
func minMaxViews(views []string, q TimeQuantum) (min string, max string) {
	// Sort the list of views.
	sort.Strings(views)

	// Determine the least significant quantum and set that as the
	// number of string characters to compare against.
	var chars int
	if q.HasYear() {
		chars = 4
	} else if q.HasMonth() {
		chars = 6
	} else if q.HasDay() {
		chars = 8
	} else if q.HasHour() {
		chars = 10
	}

	// min: get the first view with the matching number of time chars.
	for _, v := range views {
		if len(viewTimePart(v)) == chars {
			min = v
			break
		}
	}

	// max: get the first view (from the end) with the matching number of time chars.
	for i := len(views) - 1; i >= 0; i-- {
		if len(viewTimePart(views[i])) == chars {
			max = views[i]
			break
		}
	}

	return min, max
}

// timeOfView returns a valid time.Time based on the view string.
// For upper bound use, the result can be adjusted by one by setting
// the `adj` argument to `true`.
func timeOfView(v string, adj bool) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}

	layout := "2006010203"
	timePart := viewTimePart(v)

	switch len(timePart) {
	case 4: // year
		t, err := time.Parse(layout[:4], timePart)
		if err != nil {
			return time.Time{}, err
		}
		if adj {
			t = t.AddDate(1, 0, 0)
		}
		return t, nil
	case 6: // month
		t, err := time.Parse(layout[:6], timePart)
		if err != nil {
			return time.Time{}, err
		}
		if adj {
			t = addMonth(t)
		}
		return t, nil
	case 8: // day
		t, err := time.Parse(layout[:8], timePart)
		if err != nil {
			return time.Time{}, err
		}
		if adj {
			t = t.AddDate(0, 0, 1)
		}
		return t, nil
	case 10: // hour
		t, err := time.Parse(layout[:10], timePart)
		if err != nil {
			return time.Time{}, err
		}
		if adj {
			t = t.Add(time.Hour)
		}
		return t, nil
	}

	return time.Time{}, fmt.Errorf("invalid time format on view: %s", v)
}

// viewTimePart returns the time portion of a string view name.
// e.g. the view "string_201901" would return "201901".
func viewTimePart(v string) string {
	parts := strings.Split(v, "_")
	return parts[len(parts)-1]
}
