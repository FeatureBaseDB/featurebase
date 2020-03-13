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
// is to avoid relying on float64, and they primary
// purpose is to have a predictable way to encode such
// values used in query strings.
// Sign = true represents a negative value.
// Scale is the number of digits to the right of the
// decimal point.
// Precision is currently not considered; precision, for
// our purposes is implied to be the complete, known value.
type Decimal struct {
	Sign  bool
	Value uint32
	Scale int64
}

// ToInt64 returns d as an int64 adjusted to the
// provided scale.
func (d Decimal) ToInt64(scale int64) int64 {
	var ret int64
	scaleDiff := scale - d.Scale
	if scaleDiff == 0 {
		ret = int64(d.Value)
	} else {
		ret = int64(float64(d.Value) * math.Pow10(int(scaleDiff)))
	}
	if d.Sign {
		ret *= -1
	}
	return ret
}

// String returns the string representation of the decimal.
func (d Decimal) String() string {
	var s string

	sval := fmt.Sprintf("%d", d.Value)
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

	if d.Sign {
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
	if s == "" {
		return Decimal{}, nil
	}
	var sign bool
	var value uint64
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

	value, err = strconv.ParseUint(string(mantissa), 10, 32)
	if err != nil {
		return Decimal{}, errors.Wrap(err, "converting mantissa to uint32")
	}

	return Decimal{
		Sign:  sign,
		Value: uint32(value),
		Scale: scale,
	}, nil
}
