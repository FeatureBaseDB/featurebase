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
	"io"
	"strconv"
	"strings"
)

// TimeFormat is the go-style time format used to parse string dates.
const TimeFormat = "2006-01-02T15:04"

// Parser represents a parser for the PQL language.
type Parser struct {
	scanner *bufScanner
}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	return &Parser{
		scanner: newBufScanner(r),
	}
}

// ParseString parses s into a query.
func ParseString(s string) (*Query, error) {
	return NewParser(strings.NewReader(s)).Parse()
}

// Parse parses the next node in the query.
func (p *Parser) Parse() (*Query, error) {
	q := &Query{}
	for {
		call, err := p.parseCall()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		q.Calls = append(q.Calls, call)
	}

	// Require at least one call.
	if len(q.Calls) == 0 {
		return nil, io.ErrUnexpectedEOF
	}

	return q, nil
}

// parseCall parses the next function call.
func (p *Parser) parseCall() (*Call, error) {
	var c Call

	// Read call name.
	tok, pos, lit := p.scanIgnoreWhitespace()
	if tok == EOF {
		return nil, io.EOF
	} else if tok != IDENT {
		return nil, &ParseError{Message: fmt.Sprintf("expected identifier, found: %s", lit), Pos: pos}
	}
	c.Name = lit

	// Scan opening parenthesis.
	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}

	// Parse children first.
	children, err := p.parseChildren()
	if err != nil {
		return nil, err
	}
	c.Children = children

	// If next token is a closing paren then exit.
	if tok, pos, lit := p.scanIgnoreWhitespace(); tok == RPAREN {
		return &c, nil
	} else if tok == IDENT {
		p.unscan(1)
	} else if tok != COMMA {
		return nil, parseErrorf(pos, "expected comma, right paren, or identifier, found %q", lit)
	}

	// Parse key/value arguments.
	args, err := p.parseArgs()
	if err != nil {
		return nil, err
	}
	c.Args = args

	// Scan closing parenthesis.
	if err := p.expect(RPAREN); err != nil {
		return nil, err
	}

	return &c, nil
}

// parseChildren parses call children.
func (p *Parser) parseChildren() ([]*Call, error) {
	var offset int
	var children []*Call
	for {
		// Ensure next two tokens are IDENT+LPAREN.
		if tok, _, _ := p.scanIgnoreWhitespace(); tok != IDENT {
			p.unscanIgnoreWhitespace(1 + offset)
			return children, nil
		}
		if tok, _, _ := p.scan(); tok != LPAREN {
			p.unscanIgnoreWhitespace(2 + offset)
			return children, nil
		}

		// Push tokens back on scanner and parse as a call.
		p.unscan(2)
		child, err := p.parseCall()
		if err != nil {
			return nil, err
		}
		children = append(children, child)

		// Exit if closing paren.
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok == RPAREN {
			p.unscan(1)
			return children, nil
		} else if tok != COMMA {
			return nil, parseErrorf(pos, "expected comma or right paren, found %q", lit)
		}

		// Make sure comma is unscanned.
		offset = 1
	}
}

// parseArgs parses key/value arguments.
func (p *Parser) parseArgs() (map[string]interface{}, error) {
	args := make(map[string]interface{})
	for {
		// Parse key.
		tok, pos, lit := p.scanIgnoreWhitespace()
		if tok == RPAREN {
			p.unscan(1)
			return args, nil
		} else if tok != IDENT {
			return nil, parseErrorf(pos, "expected argument key, found %q", lit)
		}
		key := lit

		// Expect '=' or a comparison next.
		var op Token
		switch tok, pos, lit := p.scanIgnoreWhitespace(); tok {
		case ASSIGN:
		case EQ, NEQ, LT, LTE, GT, GTE, BETWEEN:
			op = tok
		default:
			return nil, parseErrorf(pos, "expected equals sign or comparison operator, found %q", lit)
		}

		// Parse value.
		var value interface{}
		tok, pos, lit = p.scanIgnoreWhitespace()
		switch tok {
		case IDENT:
			if lit == "true" {
				value = true
			} else if lit == "false" {
				value = false
			} else if lit == "null" {
				value = nil
			} else {
				value = lit
			}
		case STRING:
			value = lit
		case INTEGER:
			v, err := strconv.ParseInt(lit, 10, 64)
			if err != nil {
				return nil, err
			}
			value = v
		case FLOAT:
			v, err := strconv.ParseFloat(lit, 64)
			if err != nil {
				return nil, err
			}
			value = v
		case LBRACK:
			v, err := p.parseList()
			if err != nil {
				return nil, err
			}
			value = v
		default:
			return nil, parseErrorf(pos, "invalid argument value: %q", lit)
		}

		// Ensure key doesn't already exist.
		if _, ok := args[key]; ok {
			return nil, parseErrorf(pos, "argument key already used: %s", key)
		}

		// If op is specified then create a condition.
		if op != 0 {
			value = &Condition{Op: op, Value: value}
		}

		// Add key/value pair to arguments.
		args[key] = value

		// Exit if closing paren.
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok == RPAREN {
			p.unscan(1)
			return args, nil
		} else if tok != COMMA {
			return nil, parseErrorf(pos, "expected comma or right paren, found %q", lit)
		}
	}
}

// parseList parses a list of primitives. This is used by the TopN() filters.
func (p *Parser) parseList() ([]interface{}, error) {
	var values []interface{}
	for {
		// Read next value.
		tok, pos, lit := p.scanIgnoreWhitespace()
		switch tok {
		case IDENT:
			if lit == "true" {
				values = append(values, true)
			} else if lit == "false" {
				values = append(values, false)
			} else {
				values = append(values, lit)
			}
		case STRING:
			values = append(values, lit)
		case INTEGER:
			v, err := strconv.ParseInt(lit, 10, 64)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		default:
			return nil, parseErrorf(pos, "invalid list value: %q", lit)
		}

		// Expect a comma or closing bracket next.
		if tok, pos, lit := p.scanIgnoreWhitespace(); tok == RBRACK {
			break
		} else if tok != COMMA {
			return nil, parseErrorf(pos, "expected comma, found %q", lit)
		}
	}
	return values, nil
}

// scan returns the next token from the scanner.
func (p *Parser) scan() (tok Token, pos Pos, lit string) { return p.scanner.Scan() }

// scanIgnoreWhitespace returns the next non-whitespace token from the scanner.
func (p *Parser) scanIgnoreWhitespace() (tok Token, pos Pos, lit string) {
	tok, pos, lit = p.scan()
	if tok == WS {
		tok, pos, lit = p.scan()
	}
	return
}

// unscan returns the last n tokens back to the scanner.
func (p *Parser) unscan(n int) {
	for i := 0; i < n; i++ {
		p.scanner.unscan()
	}
}

// unscanIgnoreWhitespace returns the last n non-WS tokens back to the scanner.
func (p *Parser) unscanIgnoreWhitespace(n int) {
	for i := 0; i < n; {
		p.scanner.unscan()
		if tok, _, _ := p.scanner.curr(); tok != WS {
			i++
		}
	}
}

// expect returns an error if the next token is not exp.
func (p *Parser) expect(exp Token) error {
	if tok, pos, lit := p.scan(); tok != exp {
		return parseErrorf(pos, "expected %s, found %q", exp.String(), lit)
	}
	return nil
}

// pos returns the current position.
func (p *Parser) pos() Pos { return p.scanner.pos() }

// ParseError represents an error that occurred while parsing a PQL query.
type ParseError struct {
	Message string
	Pos     Pos
}

// Error returns a string representation of e.
func (e *ParseError) Error() string {
	return fmt.Sprintf("%s occurred at line %d, char %d", e.Message, e.Pos.Line+1, e.Pos.Char+1)
}

// parseErrorf returns a formatted parse error.
func parseErrorf(pos Pos, format string, args ...interface{}) *ParseError {
	return &ParseError{
		Message: fmt.Sprintf(format, args...),
		Pos:     pos,
	}
}
