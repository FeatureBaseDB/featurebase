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
	"bufio"
	"bytes"
	"io"
	"unicode"
)

// Scanner represents a PQL lexical scanner.
type Scanner struct {
	r   io.RuneScanner
	pos Pos
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReader(r)}
}

// Scan returns the next token and position from the underlying reader.
func (s *Scanner) Scan() (tok Token, pos Pos, lit string) {
	pos = s.pos

	// Read next code point.
	ch := s.read()

	// If we see whitespace then consume all contiguous whitespace.
	// If we see a letter, or certain acceptable special characters, then consume
	// as an ident or reserved word. If we see quotes, then scan as string.
	if isWhitespace(ch) {
		s.unread()
		return s.scanWhitespace()
	} else if isIdentFirstChar(ch) {
		s.unread()
		return s.scanIdent()
	} else if isDigit(ch) || ch == '-' {
		s.unread()
		return s.scanNumber()
	} else if ch == '"' || ch == '\'' {
		s.unread()
		return s.scanString()
	}

	// Otherwise parse individual characters.
	switch ch {
	case eof:
		return EOF, pos, ""
	case '=':
		if next := s.read(); next == '=' {
			return EQ, pos, "=="
		}
		s.unread()
		return ASSIGN, pos, string(ch)
	case '!':
		if next := s.read(); next == '=' {
			return NEQ, pos, "!="
		}
		s.unread()
		return ASSIGN, pos, string(ch)
	case '<':
		if next := s.read(); next == '=' {
			return LTE, pos, "<="
		}
		s.unread()
		return LT, pos, string(ch)
	case '>':
		next := s.read()
		if next == '=' {
			return GTE, pos, ">="
		} else if next == '<' {
			return BETWEEN, pos, "><"
		}
		s.unread()
		return GT, pos, string(ch)
	case ',':
		return COMMA, pos, string(ch)
	case '(':
		return LPAREN, pos, string(ch)
	case ')':
		return RPAREN, pos, string(ch)
	case '[':
		return LBRACK, pos, string(ch)
	case ']':
		return RBRACK, pos, string(ch)
	default:
		return ILLEGAL, pos, string(ch)
	}
}

// read returns the next code point from the underlying reader and updates the pos.
func (s *Scanner) read() rune {
	// Read next rune from underlying reader.
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return eof
	}

	// Update position information.
	if ch == '\n' {
		s.pos.Line++
		s.pos.Char = 0
	} else {
		s.pos.Char++
	}

	return ch
}

// unread pushes the previously read rune back onto the reader.
func (s *Scanner) unread() {
	if s.pos.Char == 0 {
		s.pos.Line--
	} else {
		s.pos.Char--
	}

	s.r.UnreadRune()
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() (tok Token, pos Pos, lit string) {
	pos = s.pos

	var buf bytes.Buffer
	for {
		ch := s.read()
		if ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.unread()
			break
		}
		buf.WriteRune(ch)
	}

	return WS, pos, buf.String()
}

func (s *Scanner) scanIdent() (tok Token, pos Pos, lit string) {
	pos = s.pos

	var buf bytes.Buffer
	for {
		ch := s.read()
		if ch == eof {
			break
		} else if !isIdentChar(ch) {
			s.unread()
			break
		}
		buf.WriteRune(ch)
	}
	lit = buf.String()

	// If the literal matches a keyword then return that keyword.
	if tok = Lookup(lit); tok != IDENT {
		return tok, pos, lit
	}

	return IDENT, pos, lit
}

// scanNumber consumes consecutive digits, optionally starting with a minus sign and up to one '.' character.
func (s *Scanner) scanNumber() (tok Token, pos Pos, lit string) {
	pos = s.pos
	tok = INTEGER

	var buf bytes.Buffer
	var seenDot bool
	first := true
	for {
		ch := s.read()
		if !isDigit(ch) && !(first && ch == '-') && (seenDot || ch != '.') {
			s.unread()
			break
		}
		if ch == '.' {
			seenDot = true
			tok = FLOAT
		}
		buf.WriteRune(ch)
		first = false
	}
	return tok, pos, buf.String()
}

// scanString consumes a single-quoted or double-quoted string.
func (s *Scanner) scanString() (tok Token, pos Pos, lit string) {
	pos = s.pos

	// This must be either a single- or double-quote.
	ending := s.read()

	var buf bytes.Buffer
	for {
		ch := s.read()
		if ch == ending {
			break
		} else if ch == '\n' || ch == eof {
			return BADSTRING, pos, buf.String()
		} else if ch == '\\' {
			next := s.read()
			if next == 'n' {
				buf.WriteRune('\n')
			} else if next == '\\' {
				buf.WriteRune('\\')
			} else if next == '"' {
				buf.WriteRune('"')
			} else if next == '\'' {
				buf.WriteRune('\'')
			} else {
				return BADSTRING, pos, buf.String()
			}
		} else {
			buf.WriteRune(ch)
		}
	}

	return STRING, pos, buf.String()
}

// bufScanner represents a wrapper for scanner to add a buffer.
// It provides a fixed-length circular buffer that can be unread.
type bufScanner struct {
	s   *Scanner
	i   int // buffer index
	n   int // buffer size
	buf [8]struct {
		tok Token
		pos Pos
		lit string
	}
}

// newBufScanner returns a new buffered scanner for a reader.
func newBufScanner(r io.Reader) *bufScanner {
	return &bufScanner{s: NewScanner(r)}
}

// Scan reads the next token from the scanner.
func (s *bufScanner) Scan() (tok Token, pos Pos, lit string) {
	// If we have unread tokens then read them off the buffer first.
	if s.n > 0 {
		s.n--
		return s.curr()
	}

	// Move buffer position forward and save the token.
	s.i = (s.i + 1) % len(s.buf)
	buf := &s.buf[s.i]
	buf.tok, buf.pos, buf.lit = s.s.Scan()

	return s.curr()
}

// unscan pushes the previously token back onto the buffer.
func (s *bufScanner) unscan() { s.n++ }

// curr returns the last read token.
func (s *bufScanner) curr() (tok Token, pos Pos, lit string) {
	buf := &s.buf[(s.i-s.n+len(s.buf))%len(s.buf)]
	return buf.tok, buf.pos, buf.lit
}

// pos returns the current position.
func (s *bufScanner) pos() Pos {
	_, pos, _ := s.curr()
	return pos
}

// isWhitespace returns true if the rune a Unicode space character.
func isWhitespace(ch rune) bool { return unicode.IsSpace(ch) }

// isLetter returns true if the rune is a letter.
func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

// isDigit returns true if the rune is a digit.
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

// isIdentChar returns true if the rune can be used in an unquoted identifier.
func isIdentChar(ch rune) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '-' || ch == '.'
}

// isIdentFirstChar returns true if the rune can be used as the first char in an identifier.
func isIdentFirstChar(ch rune) bool { return isLetter(ch) }

const eof = rune(0)
