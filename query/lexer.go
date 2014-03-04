package query

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	TYPE_FUNC    = iota
	TYPE_LP      = iota
	TYPE_RP      = iota
	TYPE_LB      = iota
	TYPE_RB      = iota
	TYPE_VALUE   = iota
	TYPE_KEYWORD = iota
	TYPE_EQUALS  = iota
	TYPE_COMMA   = iota
	TYPE_ERROR   = iota

	// Below types deprecated
	TYPE_ID      = iota
	TYPE_FRAME   = iota
	TYPE_PROFILE = iota
	TYPE_LIMIT   = iota
)

type Token struct {
	Text string
	Type int
}

type statefn func(lexer *Lexer) statefn

type Lexer struct {
	text  string     // the string being scanned.
	pos   int        // current position in the input.
	width int        // width of last rune read from input.
	start int        // start position of this item.
	state int        // current state of lexer NEEDED???
	ch    chan Token // channel of scanned items (Tokens).
}

func (lexer *Lexer) emit(typ int) {
	lexer.ch <- Token{lexer.text[lexer.start:lexer.pos], typ}
	lexer.start = lexer.pos
}

func (lexer *Lexer) acceptUntil(chars string, consume bool) (rune, error) {
	start_pos := lexer.pos

	for {
		next := lexer.next()

		if next == rune(' ') {
			lexer.ignore()
		}

		if next == 0 {
			lexer.pos = start_pos
			return 0, errors.New("Not found")
		}
		ch := strings.IndexRune(chars, next)
		if ch >= 0 {
			if consume {
				lexer.backup()
			} else {
				lexer.pos = start_pos
			}
			return []rune(chars)[ch], nil
		}
	}
}

func (lexer *Lexer) acceptRun(valid string) {
	for strings.IndexRune(valid, lexer.next()) >= 0 {
	}
	lexer.backup()
}

// next returns the next rune in the input.
func (lexer *Lexer) next() (runey rune) {
	if lexer.pos >= len(lexer.text) {
		lexer.width = 0
		return 0
	}
	runey, lexer.width = utf8.DecodeRuneInString(lexer.text[lexer.pos:])
	lexer.pos += lexer.width
	return runey
}

// ignore skips over the pending input before this point.
func (lexer *Lexer) ignore() {
	lexer.start = lexer.pos
}

// backup steps back one rune.
// Can be called only once per call of next.
func (lexer *Lexer) backup() {
	lexer.pos -= lexer.width
}

// peek returns but does not consume
// the next rune in the input.
func (lexer *Lexer) peek() rune {
	for {
		next_rune := lexer.next()
		// ignore spaces
		if next_rune != rune(' ') {
			lexer.backup()
			return next_rune
		}
		lexer.ignore()
	}
}

func stateError(err error) func(lexer *Lexer) statefn {
	return func(lexer *Lexer) statefn {
		lexer.ch <- Token{err.Error(), TYPE_ERROR}
		close(lexer.ch)
		return nil
	}
}

func stateFunc(lexer *Lexer) statefn {
	_, err := lexer.acceptUntil("(", true)
	if err != nil {
		return stateError(err)
	}
	lexer.emit(TYPE_FUNC)
	return stateLP
}

func stateLP(lexer *Lexer) statefn {
	lexer.pos += 1
	lexer.emit(TYPE_LP)
	// handle multiple LPs
	if lexer.peek() == rune('(') {
		return stateLP
	}
	return stateArgs
}

func stateLB(lexer *Lexer) statefn {
	lexer.acceptUntil("[", true)
	lexer.next()
	lexer.emit(TYPE_LB)
	for {
		r, err := lexer.acceptUntil(",]", true)
		if err != nil {
			return stateError(errors.New("Unclosed bracket!"))
		}
		lexer.emit(TYPE_VALUE)
		if r == ',' {
			lexer.next()
			lexer.emit(TYPE_COMMA)
		} else {
			lexer.next()
			lexer.emit(TYPE_RB)
			return stateArgs
		}
	}
}

func stateArgs(lexer *Lexer) statefn {
	r, err := lexer.acceptUntil("(),=[", false)
	if err != nil {
		return stateError(err)
	}
	switch r {
	case '(':
		return stateFunc
	case ')':
		return stateValue
	case ',':
		return stateValue
	case '=':
		return stateKeyword
	case '[':
		return stateLB
	default:
		return stateError(errors.New("Expecting arguments!"))
	}
	return nil
}

func stateKeyword(lexer *Lexer) statefn {
	_, err := lexer.acceptUntil("=", true)
	if err != nil {
		return stateError(err)
	}
	lexer.emit(TYPE_KEYWORD)
	return stateEquals
}

func stateEquals(lexer *Lexer) statefn {
	e := lexer.next()
	if e != '=' {
		return stateError(errors.New("Expecting '='!"))
	}
	lexer.emit(TYPE_EQUALS)
	return stateValue
}

func stateValue(lexer *Lexer) statefn {
	r, err := lexer.acceptUntil("(),[", false)
	if err != nil {
		return stateError(err)
	}
	switch r {
	case '(':
		return stateFunc
	case ')':
		lexer.acceptUntil(")", true)
		if lexer.pos > lexer.start {
			lexer.emit(TYPE_VALUE)
		}
		return stateRP
	case ',':
		lexer.acceptUntil(",", true)
		lexer.emit(TYPE_VALUE)
		return stateComma
	case '[':
		return stateLB
	default:
		return stateError(errors.New("Unexpected character!"))
	}
	return nil
}

func stateRP(lexer *Lexer) statefn {
	lexer.pos += 1
	lexer.emit(TYPE_RP)

	peeked := lexer.peek()
	if peeked == rune(',') {
		return stateRPComma
	} else if peeked == rune(')') {
		return stateRP
	} else {
		return stateEOF
	}
}

func stateRPComma(lexer *Lexer) statefn {
	lexer.pos += 1
	lexer.emit(TYPE_COMMA)
	return stateValue
}

func stateComma(lexer *Lexer) statefn {
	lexer.pos += 1
	lexer.emit(TYPE_COMMA)
	return stateArgs
}

func stateEOF(lexer *Lexer) statefn {
	close(lexer.ch)
	return nil
}

func (lexer *Lexer) Lex() (tokens []Token, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("query: %v", r)
			}
		}
	}()
	tokens = make([]Token, 0)
	state := stateFunc
	go func() {
		for {
			state = state(lexer)
			if state == nil {
				return
			}
		}
	}()
	for t := range lexer.ch {
		if t.Type == TYPE_ERROR {
			err = errors.New(t.Text)
		}
		tokens = append(tokens, t)
	}
	return tokens, err
}

func Lex(input string) ([]Token, error) {
	lexer := Lexer{input, 0, 0, 0, TYPE_FUNC, make(chan Token)}
	return lexer.Lex()
}
