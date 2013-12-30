package query

import (
	"errors"
	"log"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	TYPE_FUNC    = iota
	TYPE_LP      = iota
	TYPE_RP      = iota
	TYPE_ID      = iota
	TYPE_FRAME   = iota
	TYPE_PROFILE = iota
	TYPE_COMMA   = iota
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

func (lexer *Lexer) acceptUntil(chars string) error {
	for {
		if strings.HasPrefix(lexer.text[lexer.pos:], chars) {
			return nil
		}
		// if we receive a reserved character that we are not expecting, throw a parse error
		lexer.pos += 1
		if lexer.pos > len(lexer.text) {
			return errors.New("Parse error, expecting " + string(chars))
		}
	}
}

func (lexer *Lexer) acceptRun(valid string) {
	for strings.IndexRune(valid, lexer.next()) >= 0 {
	}
	lexer.backup()
}

func (lexer *Lexer) acceptNumber() {
	digits := "0123456789"
	lexer.acceptRun(digits)
}
func (lexer *Lexer) acceptText() {
	digits := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_." // TODO: make this more flexible. accept anything up to a space or RP: ")"
	lexer.acceptRun(digits)
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

func stateFunc(lexer *Lexer) statefn {
	err := lexer.acceptUntil("(")
	if err != nil {
		log.Fatal(err)
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

func stateArgs(lexer *Lexer) statefn {
	if unicode.IsNumber(lexer.peek()) {
		return stateID
	} else {
		return stateFunc
	}
}

func stateID(lexer *Lexer) statefn {
	lexer.acceptNumber()
	lexer.emit(TYPE_ID)
	// if next is comma
	peeked := lexer.peek()
	if peeked == rune(',') {
		return stateFrameComma
	} else if peeked == rune(')') {
		return stateRP
	} else {
		return stateID
	}
}

func stateProfile(lexer *Lexer) statefn {
	lexer.peek()
	lexer.acceptNumber()
	lexer.emit(TYPE_PROFILE)
	lexer.peek()
	return stateRP
}

func stateRP(lexer *Lexer) statefn {
	lexer.pos += 1
	lexer.emit(TYPE_RP)

	peeked := lexer.peek()
	if peeked == rune(',') {
		return stateComma
	} else if peeked == rune(')') {
		return stateRP
	} else {
		return stateEOF
	}
}

func stateFrameComma(lexer *Lexer) statefn {
	lexer.pos += 1
	lexer.emit(TYPE_COMMA)
	return stateFrameOrProfile
}

func stateProfileComma(lexer *Lexer) statefn {
	lexer.pos += 1
	lexer.emit(TYPE_COMMA)
	return stateProfile
}

func stateFrameOrProfile(lexer *Lexer) statefn {
	if unicode.IsNumber(lexer.peek()) {
		lexer.acceptNumber()
		lexer.emit(TYPE_PROFILE)
		lexer.peek()
	} else {
		lexer.acceptText()
		lexer.emit(TYPE_FRAME)
		if lexer.peek() == rune(',') {
			return stateProfileComma
		}
	}
	return stateRP
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

func (lexer *Lexer) Lex() []Token {
	tokens := make([]Token, 0)
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
		tokens = append(tokens, t)
	}
	return tokens
}

func Lex(input string) []Token {
	lexer := Lexer{input, 0, 0, 0, TYPE_FUNC, make(chan Token)}
	return lexer.Lex()
}
