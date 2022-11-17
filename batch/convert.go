package batch

import (
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/errors"
)

var (
	MinTimestampNano = time.Unix(-1<<32, 0).UTC()       // 1833-11-24T17:31:44Z
	MaxTimestampNano = time.Unix(1<<32, 0).UTC()        // 2106-02-07T06:28:16Z
	MinTimestamp     = time.Unix(-62135596799, 0).UTC() // 0001-01-01T00:00:01Z
	MaxTimestamp     = time.Unix(253402300799, 0).UTC() // 9999-12-31T23:59:59Z

	ErrTimestampOutOfRange = errors.New("", "value provided for timestamp field is out of range")
)

type TimeUnit string

const (
	TimeUnitSeconds      = TimeUnit(featurebase.TimeUnitSeconds)
	TimeUnitMilliseconds = TimeUnit(featurebase.TimeUnitMilliseconds)
	TimeUnitMicroseconds = TimeUnit(featurebase.TimeUnitMicroseconds)
	TimeUnitUSeconds     = TimeUnit(featurebase.TimeUnitUSeconds)
	TimeUnitNanoseconds  = TimeUnit(featurebase.TimeUnitNanoseconds)
)

// TimestampToInt64 converts the provided timestamp to an int64 as the number of
// units past the epoch.
func TimestampToInt64(unit TimeUnit, epoch time.Time, ts time.Time) (int64, error) {
	var err error

	unit, err = validateTimeUnit(unit)
	if err != nil {
		return 0, errors.Wrap(err, "validating time unit")
	}

	epoch, err = validateEpoch(epoch)
	if err != nil {
		return 0, errors.Wrap(err, "validating epoch")
	}

	// Check if the epoch alone is out-of-range. If so, ingest should halt,
	// regardless of state of the timestamp out-of-range CLI option.
	if err := validateTimestamp(unit, epoch); err != nil {
		return 0, errors.Wrap(err, "validating epoch")
	}

	epochAsInt64 := timestampToInt(unit, epoch)

	// Check if the timestamp is out-of-range.
	if err := validateTimestamp(unit, ts); err != nil {
		return 0, errors.Wrapf(ErrTimestampOutOfRange, "validating timestamp: %s", ts)
	}

	tsAsInt64 := timestampToInt(unit, ts)

	return tsAsInt64 - epochAsInt64, nil
}

// validateTimeUnit checks if the time unit is supported. If the provided unit
// is blank, validateTimeUnit returns the default TimeUnit.
func validateTimeUnit(unit TimeUnit) (TimeUnit, error) {
	switch unit {
	case "":
		return TimeUnitSeconds, nil

	case TimeUnitSeconds,
		TimeUnitMilliseconds,
		TimeUnitMicroseconds,
		TimeUnitUSeconds,
		TimeUnitNanoseconds:
		return unit, nil
	}

	return "", errors.Errorf("unsupported time unit: %s", unit)
}

// validateEpoch checks if the epoch is supported. If the provided epoch
// is "zero", validateEpoch returns the default epoch value.
func validateEpoch(epoch time.Time) (time.Time, error) {
	if epoch.IsZero() {
		return time.Unix(0, 0), nil
	}
	return epoch, nil
}

// validateTimestamp checks if the timestamp is within the range of what FB accepts.
func validateTimestamp(unit TimeUnit, ts time.Time) error {
	// Min and Max timestamps that Featurebase accepts
	var minStamp, maxStamp time.Time
	switch unit {
	case TimeUnitNanoseconds:
		minStamp = MinTimestampNano
		maxStamp = MaxTimestampNano
	default:
		minStamp = MinTimestamp
		maxStamp = MaxTimestamp
	}

	if ts.Before(minStamp) || ts.After(maxStamp) {
		return errors.Errorf("timestamp value (%v) must be within min: %v and max: %v", ts, minStamp, maxStamp)
	}
	return nil
}

// timestampToInt takes a time unit and a time.Time and converts it to an
// integer value.
func timestampToInt(unit TimeUnit, ts time.Time) int64 {
	switch unit {
	case TimeUnitSeconds:
		return ts.Unix()
	case TimeUnitMilliseconds:
		return ts.UnixMilli()
	case TimeUnitMicroseconds, TimeUnitUSeconds:
		return ts.UnixMicro()
	case TimeUnitNanoseconds:
		return ts.UnixNano()
	}
	return 0
}

// intToTimestamp takes a timeunit and an integer value and converts it to
// time.Time.
func intToTimestamp(unit TimeUnit, val int64) (time.Time, error) {
	switch unit {
	case TimeUnitSeconds:
		return time.Unix(val, 0).UTC(), nil
	case TimeUnitMilliseconds:
		return time.UnixMilli(val).UTC(), nil
	case TimeUnitMicroseconds, TimeUnitUSeconds:
		return time.UnixMicro(val).UTC(), nil
	case TimeUnitNanoseconds:
		return time.Unix(0, val).UTC(), nil
	default:
		return time.Time{}, errors.Errorf("Unknown time unit: '%v'", unit)
	}
}

// Int64ToTimestamp converts the provided int64 to a timestamp based on the time unit
// and epoch.
func Int64ToTimestamp(unit TimeUnit, epoch time.Time, val int64) (time.Time, error) {
	return intToTimestamp(unit, timestampToInt(unit, epoch)+val)
}
