// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pql

import (
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pkg/errors"
)

// error strings in the parser
const duplicateArgErrorMessage = "duplicate argument provided"
const intOutOfRangeError = "integer is not in signed 64-bit range"
const invalidTimestampError = "string is not a valid timestamp"

// parser represents a parser for the PQL language.
type parser struct {
	r io.Reader
	//scanner *bufScanner
	PQL
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *parser {
	return &parser{
		r: r,
		//		scanner: newBufScanner(r),
	}
}

// ParseString parses s into a query.
func ParseString(s string) (*Query, error) {
	return NewParser(strings.NewReader(s)).Parse()
}

// Parse parses the next node in the query.
func (p *parser) Parse() (*Query, error) {
	buf, err := ioutil.ReadAll(p.r)
	if err != nil {
		return nil, errors.Wrap(err, "reading buffer to parse")
	}
	p.PQL = PQL{
		Buffer: string(buf),
	}
	err = p.Init()
	if err != nil {
		return nil, errors.Wrap(err, "creating parser")
	}
	err = p.PQL.Parse()
	if err != nil {
		return nil, errors.Wrap(err, "parsing")
	}

	// Handle specific panics from the parser and return them as errors.
	var v interface{}
	func() {
		defer func() { v = recover() }()
		p.Execute()
	}()
	if v != nil {
		errorMessage, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected parser error of type %T: %[1]v", v)
		}
		if strings.HasPrefix(errorMessage, duplicateArgErrorMessage) || strings.HasPrefix(errorMessage, intOutOfRangeError) || strings.HasPrefix(errorMessage, invalidTimestampError) {
			return nil, fmt.Errorf("%s", v)
		} else {
			panic(v)
		}
	}
	for _, call := range p.Query.Calls {
		if call == nil {
			return nil, fmt.Errorf("unexpected nil Call in query's call list")
		}
		if err := call.CheckCallInfo(); err != nil {
			return nil, err
		}
	}

	return &p.Query, nil
}

// Unquote interprets s as a single-quoted, double-quoted, or
// backquoted Go string literal, returning the string value that s
// quotes. It is a copy of stdlib's strconv.Unquote, but modified so
// that if s is single-quoted, it can still be a string rather than
// only character literal. This version of Unquote also accepts
// unquoted strings and passes them back unchanged.
func Unquote(s string) (string, error) {
	n := len(s)
	if n < 2 {
		return s, nil
	}
	quote := s[0]
	if quote != '"' && quote != '\'' && quote != '`' {
		return s, nil
	}
	if quote != s[n-1] {
		return "", strconv.ErrSyntax
	}
	s = s[1 : n-1]

	if quote == '`' {
		if contains(s, '`') {
			return "", strconv.ErrSyntax
		}
		if contains(s, '\r') {
			// -1 because we know there is at least one \r to remove.
			buf := make([]byte, 0, len(s)-1)
			for i := 0; i < len(s); i++ {
				if s[i] != '\r' {
					buf = append(buf, s[i])
				}
			}
			return string(buf), nil
		}
		return s, nil
	}
	if quote != '"' && quote != '\'' {
		return "", strconv.ErrSyntax
	}
	if contains(s, '\n') {
		return "", strconv.ErrSyntax
	}

	// Is it trivial? Avoid allocation.
	if !contains(s, '\\') && !contains(s, quote) {
		switch quote {
		case '"', '\'':
			if utf8.ValidString(s) {
				return s, nil
			}
		}
	}

	var runeTmp [utf8.UTFMax]byte
	buf := make([]byte, 0, 3*len(s)/2) // Try to avoid more allocations.
	for len(s) > 0 {
		c, multibyte, ss, err := strconv.UnquoteChar(s, quote)
		if err != nil {
			return "", err
		}
		s = ss
		if c < utf8.RuneSelf || !multibyte {
			buf = append(buf, byte(c))
		} else {
			n := utf8.EncodeRune(runeTmp[:], c)
			buf = append(buf, runeTmp[:n]...)
		}
	}
	return string(buf), nil
}

// contains reports whether the string contains the byte c.
func contains(s string, c byte) bool {
	return strings.ContainsRune(s, rune(c))
}
