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

package pql

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

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

// ToInt64 returns d as an int64 adjusted to the
// provided scale.
func (d Decimal) ToInt64(scale int64) int64 {
	var ret int64
	scaleDiff := scale - d.Scale
	if scaleDiff == 0 {
		ret = d.Value
	} else {
		ret = int64(float64(d.Value) * math.Pow10(int(scaleDiff)))
	}
	return ret
}

// Float64 returns d as a float64.
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
	// - Trim leading whitespace/zeros
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
			case ' ':
				continue
			case '-':
				sign = true
				fallthrough
			case '+':
				state = stateLeadingZeros
			default:
				state = stateLeadingZeros
				i--
			}
		case stateLeadingZeros:
			switch s[i] {
			case '0':
				foundLeadingZero = true
				continue
			default:
				state = stateMantissa
				i--
			}
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

	// Trim trailing zeros/spaces of mantissa
	// for any portion that would have been to the
	// right of the decimal.
	trimZeroCnt := 0
	trimSpaceCnt := 0
	for i := len(mantissa) - 1; i >= 0; i-- {
		switch mantissa[i] {
		case uint8(0), uint8(32): // nil/space
			trimSpaceCnt++
			continue
		case uint8(48): // zero
			trimZeroCnt++
			continue
		}
		break
	}
	mantissa = mantissa[:len(mantissa)-trimSpaceCnt-trimZeroCnt]

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

	value, err = strconv.ParseInt(string(mantissa), 10, 64)
	if err != nil {
		return Decimal{}, errors.Wrap(err, "converting mantissa to uint32")
	}
	// Because we pulled the sign off at the beginning, if value is
	// negative here, it likely means the string had two "-"" characters.
	if value < 0 {
		return Decimal{}, errors.New("invalid negative value")
	}

	if sign {
		value *= -1
	}

	return Decimal{
		Value: value,
		Scale: scale,
	}, nil
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
