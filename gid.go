// Copyright (c) 2014 The Go Authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
// 	* Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
// * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package pilosa

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
)

// Sourced https://github.com/bradfitz/http2/blob/dc0c5c000ec33e263612939744d51a3b68b9cece/gotrack.go
var goroutineSpace = []byte("goroutine ")
var littleBuf = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 64)
		return &buf
	},
}

var _ = curGID // happy linter

func curGID() uint64 {
	if true {
		return 0 // avoid doing too much work during production profiling.
	}

	bp := littleBuf.Get().(*[]byte)
	defer littleBuf.Put(bp)
	b := *bp
	b = b[:runtime.Stack(b, false)]
	// Parse the 4707 out of "goroutine 4707 ["
	b = bytes.TrimPrefix(b, goroutineSpace)
	i := bytes.IndexByte(b, ' ')
	if i < 0 {
		panic(fmt.Sprintf("No space found in %q", b))
	}
	b = b[:i]
	n, err := parseUintBytes(b, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse goroutine ID out of %q: %v", b, err))
	}
	return n
}

// parseUintBytes is like strconv.ParseUint, but using a []byte.
func parseUintBytes(s []byte, base int, bitSize int) (n uint64, err error) {
	var cutoff, maxVal uint64

	if bitSize == 0 {
		bitSize = int(strconv.IntSize)
	}

	s0 := s
	switch {
	case len(s) < 1:
		err = strconv.ErrSyntax
		return n, &strconv.NumError{Func: "ParseUint", Num: string(s0), Err: err}

	case 2 <= base && base <= 36:
		// valid base; nothing to do

	case base == 0:
		// Look for octal, hex prefix.
		switch {
		case s[0] == '0' && len(s) > 1 && (s[1] == 'x' || s[1] == 'X'):
			base = 16
			s = s[2:]
			if len(s) < 1 {
				err = strconv.ErrSyntax
				return n, &strconv.NumError{Func: "ParseUint", Num: string(s0), Err: err}
			}
		case s[0] == '0':
			base = 8
		default:
			base = 10
		}

	default:
		err = errors.New("invalid base " + strconv.Itoa(base))
		return n, &strconv.NumError{Func: "ParseUint", Num: string(s0), Err: err}
	}

	n = 0
	cutoff = cutoff64(base)
	maxVal = 1<<uint(bitSize) - 1

	for i := 0; i < len(s); i++ {
		var v byte
		d := s[i]
		switch {
		case '0' <= d && d <= '9':
			v = d - '0'
		case 'a' <= d && d <= 'z':
			v = d - 'a' + 10
		case 'A' <= d && d <= 'Z':
			v = d - 'A' + 10
		default:
			n = 0
			err = strconv.ErrSyntax
			return n, &strconv.NumError{Func: "ParseUint", Num: string(s0), Err: err}
		}
		if int(v) >= base {
			n = 0
			err = strconv.ErrSyntax
			return n, &strconv.NumError{Func: "ParseUint", Num: string(s0), Err: err}
		}

		if n >= cutoff {
			// n*base overflows
			n = 1<<64 - 1
			err = strconv.ErrRange
			return n, &strconv.NumError{Func: "ParseUint", Num: string(s0), Err: err}
		}
		n *= uint64(base)

		n1 := n + uint64(v)
		if n1 < n || n1 > maxVal {
			// n+v overflows
			n = 1<<64 - 1
			err = strconv.ErrRange
			return n, &strconv.NumError{Func: "ParseUint", Num: string(s0), Err: err}
		}
		n = n1
	}

	return n, nil
}

// Return the first number n such that n*base >= 1<<64.
func cutoff64(base int) uint64 {
	if base < 2 {
		return 0
	}
	return (1<<64-1)/uint64(base) + 1
}
