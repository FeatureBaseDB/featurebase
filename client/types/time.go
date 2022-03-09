// Copyright 2021 Molecula Corp. All rights reserved.
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package types

import (
	"time"
)

// TimeQuantum type represents valid time quantum values time fields.
type TimeQuantum string

// TimeQuantum constants
const (
	TimeQuantumNone             TimeQuantum = ""
	TimeQuantumYear             TimeQuantum = "Y"
	TimeQuantumMonth            TimeQuantum = "M"
	TimeQuantumDay              TimeQuantum = "D"
	TimeQuantumHour             TimeQuantum = "H"
	TimeQuantumYearMonth        TimeQuantum = "YM"
	TimeQuantumMonthDay         TimeQuantum = "MD"
	TimeQuantumDayHour          TimeQuantum = "DH"
	TimeQuantumYearMonthDay     TimeQuantum = "YMD"
	TimeQuantumMonthDayHour     TimeQuantum = "MDH"
	TimeQuantumYearMonthDayHour TimeQuantum = "YMDH"
)

// List of time units.
const (
	TimeUnitSeconds      = "s"
	TimeUnitMilliseconds = "ms"
	TimeUnitMicroseconds = "µs"
	TimeUnitNanoseconds  = "ns"
)

// TimeUnitNano returns the number of nanoseconds in unit.
func TimeUnitNano(unit string) int64 {
	switch unit {
	case TimeUnitSeconds:
		return int64(time.Second)
	case TimeUnitMilliseconds:
		return int64(time.Millisecond)
	case TimeUnitMicroseconds:
		return int64(time.Microsecond)
	default:
		return int64(time.Nanosecond)
	}
}
