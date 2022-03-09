package dax

import (
	"strings"
)

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

// IsEmpty returns true if the quantum is empty.
func (q TimeQuantum) IsEmpty() bool { return string(q) == "" }

func (q TimeQuantum) Granularity() rune {
	var g rune
	for _, g = range q {
	}
	return g
}

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

// String returns the TimeQuantum as a string type.
func (q TimeQuantum) String() string {
	return string(q)
}

// Type returns the type of a time quantum value.
func (q TimeQuantum) Type() string {
	return "TimeQuantum"
}
