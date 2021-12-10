// Copyright 2021 Molecula Corp. All rights reserved.
package pql

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// pow10 is a map used to avoid the float64 required by math.Pow10()
var pow10 = map[int64]int64{
	0:  1,
	1:  10,
	2:  100,
	3:  1000,
	4:  10000,
	5:  100000,
	6:  1000000,
	7:  10000000,
	8:  100000000,
	9:  1000000000,
	10: 10000000000,
	11: 100000000000,
	12: 1000000000000,
	13: 10000000000000,
	14: 100000000000000,
	15: 1000000000000000,
	16: 10000000000000000,
	17: 100000000000000000,
	18: 1000000000000000000,
	//19: 10000000000000000000,
}

// Pow10 is a function which can be used in place of math.Pow10()
// to avoid the float64 logic. Note that only powers 0-18 are
// currently supported; anything else will return 0, which is
// probably going to result in incorrect values.
func Pow10(p int64) int64 {
	return pow10[p]
}

// Decimal represents a decimal value; the intention
// is to avoid relying on float64, and the primary
// purpose is to have a predictable way to encode such
// values used in query strings.
// Scale is the number of digits to the right of the
// decimal point.
// Precision is currently not considered; precision, for
// our purposes is implied to be the complete, known value.
type Decimal struct {
	Value int64
	Scale int64
}

func (d Decimal) Clone() (r *Decimal) {
	r = &Decimal{
		Value: d.Value,
		Scale: d.Scale,
	}
	return
}

// NewDecimal returns a Decimal based on the provided arguments.
func NewDecimal(value, scale int64) Decimal {
	return Decimal{
		Value: value,
		Scale: scale,
	}
}

// MinMax returns the minimum and maximum values
// supported by the provided scale.
func MinMax(scale int64) (Decimal, Decimal) {
	min := NewDecimal(math.MinInt64, scale)
	max := NewDecimal(math.MaxInt64, scale)
	return min, max
}

// LessThan returns true if d < d2.
func (d Decimal) LessThan(d2 Decimal) bool {
	return d.lessThan(d2, false)
}

// LessThanOrEqualTo returns true if d <= d2.
func (d Decimal) LessThanOrEqualTo(d2 Decimal) bool {
	return d.lessThan(d2, true)
}

// GreaterThan returns true if d > d2.
func (d Decimal) GreaterThan(d2 Decimal) bool {
	return d.greaterThan(d2, false)
}

// GreaterThanOrEqualTo returns true if d >= d2.
func (d Decimal) GreaterThanOrEqualTo(d2 Decimal) bool {
	return d.greaterThan(d2, true)
}

// EqualTo returns true if d == d2.
func (d Decimal) EqualTo(d2 Decimal) bool {
	if d.Scale == d2.Scale {
		return d.Value == d2.Value
	}

	quotientD := quotient(d)
	quotientD2 := quotient(d2)
	if quotientD != quotientD2 {
		return false
	}
	remainderD, remainderD2 := remainder(d), remainder(d2)
	if d.Scale < d2.Scale {
		scaleDiff := d2.Scale - d.Scale
		return (remainderD * pow10[scaleDiff]) == remainderD2
	}
	scaleDiff := d.Scale - d2.Scale
	return remainderD == (remainderD2 * pow10[scaleDiff])
}

func (d Decimal) lessThan(d2 Decimal, eq bool) bool {
	if d.Scale == d2.Scale {
		if eq {
			return d.Value <= d2.Value
		}
		return d.Value < d2.Value
	}

	quotientD, quotientD2 := quotient(d), quotient(d2)
	if quotientD < quotientD2 {
		return true
	} else if quotientD == quotientD2 {
		remainderD, remainderD2 := remainder(d), remainder(d2)
		if d.Scale < d2.Scale {
			scaleDiff := d2.Scale - d.Scale
			if eq {
				return (remainderD * pow10[scaleDiff]) <= remainderD2
			}
			return (remainderD * pow10[scaleDiff]) < remainderD2
		}
		scaleDiff := d.Scale - d2.Scale
		if eq {
			return remainderD <= (remainderD2 * pow10[scaleDiff])
		}
		return remainderD < (remainderD2 * pow10[scaleDiff])
	}

	return false
}

func (d Decimal) greaterThan(d2 Decimal, eq bool) bool {
	if d.Scale == d2.Scale {
		if eq {
			return d.Value >= d2.Value
		}
		return d.Value > d2.Value
	}

	quotientD, quotientD2 := quotient(d), quotient(d2)
	if quotientD > quotientD2 {
		return true
	} else if quotientD == quotientD2 {
		remainderD, remainderD2 := remainder(d), remainder(d2)
		if d.Scale < d2.Scale {
			scaleDiff := d2.Scale - d.Scale
			if eq {
				return (remainderD * pow10[scaleDiff]) >= remainderD2
			}
			return (remainderD * pow10[scaleDiff]) > remainderD2
		}
		scaleDiff := d.Scale - d2.Scale
		if eq {
			return remainderD >= (remainderD2 * pow10[scaleDiff])
		}
		return remainderD > (remainderD2 * pow10[scaleDiff])
	}

	return false
}

// SupportedByScale returns true if d can be represented
// by a decimal based on scale.
// For example:
// scale = 2:
// min: -92233720368547758.08
// max:  92233720368547758.07
// would not support: NewDecimal(9223372036854775807, 0)
func (d Decimal) SupportedByScale(scale int64) bool {
	min, max := MinMax(scale)

	if d.GreaterThanOrEqualTo(min) && d.LessThanOrEqualTo(max) {
		return true
	}
	return false
}

// IsValid returns true if the decimal does not break
// any assumption or resrictions on input.
func (d Decimal) IsValid() bool {
	if d.Scale < -18 || d.Scale > 19 {
		return false
	}
	return true
}

// ToInt64 returns d as an int64 adjusted to the
// provided scale.
func (d Decimal) ToInt64(scale int64) int64 {
	var ret int64
	scaleDiff := scale - d.Scale
	if scaleDiff == 0 {
		ret = d.Value
	} else if scaleDiff < 0 {
		ret = d.Value / Pow10(-1*scaleDiff)
	} else {
		ret = d.Value * Pow10(scaleDiff)
	}
	return ret
}

// Float64 returns d as a float64.
// TODO: this could potentially lose precision; we should audit
// its use and protect against unexpected results.
func (d Decimal) Float64() float64 {
	var ret float64
	if d.Scale == 0 {
		ret = float64(d.Value)
	} else {
		ret = float64(d.Value) / math.Pow10(int(d.Scale))
	}
	return ret
}

// String returns the string representation of the decimal.
func (d Decimal) String() string {
	var s string

	var neg bool
	sval := fmt.Sprintf("%d", d.Value)

	// Strip the negative sign off for now, and
	// re-apply it at the end.
	if sval[0] == '-' {
		neg = true
		sval = sval[1:]
	}

	if d.Scale == 0 {
		s = sval
	} else if d.Scale < 0 {
		s = sval + strings.Repeat("0", int(-1*d.Scale))
	} else {
		var bufLen int
		if int(d.Scale) < len(sval) {
			bufLen = len(sval) + 1
		} else {
			bufLen = int(d.Scale) + 2
		}

		buf := make([]byte, bufLen)
		j := 0
		for i := range buf {
			z := len(buf) - 1 - i // index into buf from the end
			if i == int(d.Scale) {
				buf[z] = '.'
				continue
			}
			if len(sval) > j {
				buf[z] = sval[len(sval)-1-j]
				j++
			} else {
				buf[z] = '0'
			}
		}
		s = string(buf)
	}

	if neg {
		return "-" + s
	}
	return s
}

const (
	stateSign         = "sign"
	stateLeadingZeros = "zeros"
	stateMantissa     = "mantissa"
)

// ParseDecimal parses a string into a Decimal.
func ParseDecimal(s string) (Decimal, error) {
	var sign bool
	var value int64
	var scale int64
	var err error

	// General steps:
	// - Get the sign value
	// - Trim leading zeros
	// - Push characters into a buffer
	// - Track position of decimal point
	// - Trim trailing zeros of buffer
	// - value = buffer -> int
	// - scale = len(buffer) - tracked position

	var decimalPos int = -1
	var pos int
	mantissa := make([]byte, len(s))

	state := stateSign
	var foundLeadingZero bool
	for i := 0; i < len(s); i++ {
		switch state {
		case stateSign:
			switch s[i] {
			case '-':
				sign = true
				fallthrough
			case '+':
				state = stateLeadingZeros
				// Resume loop and look at next character
				continue
			}
			state = stateLeadingZeros
			fallthrough
		case stateLeadingZeros:
			if s[i] == '0' {
				foundLeadingZero = true
				continue
			}
			state = stateMantissa
			fallthrough
		case stateMantissa:
			switch s[i] {
			case '.':
				if decimalPos == -1 {
					decimalPos = pos
				} else {
					return Decimal{}, errors.Errorf("invalid decimal string: %s", s)
				}
				continue
			default:
				mantissa[pos] = s[i]
				pos++
			}
		}
	}

	// If we've gotten here and state is still in stateSign or
	// it's in stateLeadingZeros without finding any zeros,
	// it means no value was provided.
	if state == stateSign || (state == stateLeadingZeros && !foundLeadingZero) {
		return Decimal{}, errors.New("decimal string is empty")
	}

	// Trim trailing zeros from mantissa. If we ended up with no
	// characters at all in string, thus, pos == 0, the loop doesn't
	// happen and we pick [:0], which is correct, probably.
	trimZeroCnt := 0
	for i := pos - 1; i >= 0 && mantissa[i] == '0'; i-- {
		trimZeroCnt++
	}
	mantissa = mantissa[:pos-trimZeroCnt]

	// Based on where (or if) the decimal was found,
	// calculate scale.
	if decimalPos == -1 {
		scale = -1 * int64(trimZeroCnt)
	} else {
		scale = int64(len(mantissa) - decimalPos)
	}

	// If mantissa is empty, treat it as "0".
	if len(mantissa) == 0 {
		mantissa = []byte{'0'}
		sign = false
		scale = 0
	}

	// If the mantissa can't be represented by an int64, but it contains
	// enough decimal places such that we can sacrifice precision, then
	// we do that. This is an attempt to be compatible with the way
	// `strconv.ParseFloat` works.
	if m, s, ok := reducePrecision(sign, mantissa, scale); ok {
		mantissa = m
		scale = s
	} else {
		return Decimal{}, errors.Errorf("value out of range: %s", mantissa)
	}

	// We have to use ParseUint here (as opposed to ParseInt) because
	// math.MinInt64 is a valid value, but its absolute value is not.
	// So this allows us to handle that one value without overflow, and
	// then we check for the uint bounds in the next step.
	uvalue, err := strconv.ParseUint(string(mantissa), 10, 64)
	if err != nil {
		return Decimal{}, errors.Wrap(err, "converting mantissa string to uint64")
	}

	if (sign && uvalue > -1*math.MinInt64) || (!sign && uvalue > math.MaxInt64) {
		return Decimal{}, errors.New("value out of range")
	}
	value = int64(uvalue)

	if sign {
		value *= -1
	}

	return Decimal{
		Value: value,
		Scale: scale,
	}, nil
}

// reducePrecision takes a []byte mantissa and scale, and if possible
// will adjust the mantissa (by reducing precision) until it can be
// represented by an int64. The returned bool indicates whether the
// reduction was successful.
func reducePrecision(sign bool, mantissa []byte, scale int64) ([]byte, int64, bool) {
	// Trim leading zeros before considering length.
	var zeroIdx int
	for i := range mantissa {
		if mantissa[i] == '0' {
			zeroIdx++
		} else {
			break
		}
	}
	mantissa = mantissa[zeroIdx:]

	// If we zero out the mantissa to an empty
	// string, that means it's value should be 0.
	if len(mantissa) == 0 {
		mantissa = []byte{'0'}
		return mantissa, scale, true
	}

	lenMantissa := len(mantissa)
	maxStr := "9223372036854775807"
	if sign {
		maxStr = "9223372036854775808"
	}

	if lenMantissa <= 18 || (lenMantissa == 19 && string(mantissa) <= maxStr) {
		return mantissa, scale, true
	}

	// If we don't have any decimal places to sacrifice,
	// we can't change anything.
	if scale <= 0 {
		return mantissa, scale, false
	}

	return reducePrecision(sign, mantissa[:len(mantissa)-1], scale-1)
}

func quotient(d Decimal) int64 {
	if d.Scale == 0 {
		return d.Value
	} else if d.Scale > 0 && d.Scale < 19 {
		return d.Value / pow10[d.Scale]
	} else if d.Scale < 0 && d.Scale > -19 {
		return d.Value * pow10[-1*d.Scale]
	}
	return 0
}

func remainder(d Decimal) int64 {
	if d.Scale >= 0 && d.Scale < 19 {
		return d.Value % pow10[d.Scale]
	}
	return 0
}

// UnmarshalJSON is a custom unmarshaller for the Decimal
// type. The intention is to avoid the use of float64
// anywhere, so this unmarhaller parses the decimal out
// of the byte string.
func (d *Decimal) UnmarshalJSON(data []byte) error {
	o, err := ParseDecimal(string(data))
	if err != nil {
		return errors.Wrapf(err, "parsing decimal: %s", string(data))
	}
	d.Value = o.Value
	d.Scale = o.Scale

	return nil
}

// MarshalJSON is a custom marshaller for the Decimal type.
func (d Decimal) MarshalJSON() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalYAML is a custom unmarshaller for the Decimal
// type.
func (d *Decimal) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var data string
	if err := unmarshal(&data); err != nil {
		return err
	}

	o, err := ParseDecimal(data)
	if err != nil {
		return errors.Wrapf(err, "parsing decimal: %s", data)
	}
	d.Value = o.Value
	d.Scale = o.Scale

	return nil
}

// MarshalYAML is a custom marshaller for the Decimal type.
func (d Decimal) MarshalYAML() (interface{}, error) {
	// TODO: I don't love that this results in a quoted string
	// in the yaml document:
	//
	// min: "-100.05"
	//
	// It would be nice if we could get that to result in:
	//
	// min: -100.05
	//
	// Note that we _can_ do that by casting the output as
	// float64 (for certain cases), but the whole point of
	// Decimal is to avoid using float64.
	return d.String(), nil
}
