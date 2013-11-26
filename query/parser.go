package query

import (
	"encoding/json"
	"errors"
	"log"
	"pilosa/db"
	"github.com/davecgh/go-spew/spew"
)

const (
	TYPE_FUNC = iota
	TYPE_LP = iota
	TYPE_RP = iota
	TYPE_ID = iota
)

type Token struct {
	Text string
	Type int
}

type statefn func(lexer *Lexer) statefn

type Lexer struct {
	text string
	pos int
	start int
	state int
	ch chan Token
}

func (lexer *Lexer) emit(typ int) {
	lexer.ch <- Token{lexer.text[lexer.start:lexer.pos], typ}
	lexer.start = lexer.pos
}

func (lexer *Lexer) accept(char uint8) error {
	for {
		if lexer.text[lexer.pos] == char {
			return nil
		}
		lexer.pos += 1
		if lexer.pos > len(lexer.text) {
			return errors.New("Parse error, expecting " + string(char))
		}
	}
}

func stateFunc(lexer *Lexer) statefn {
	err := lexer.accept('(')
	if err != nil {
		log.Fatal(err)
	}
	lexer.emit(TYPE_FUNC)
	return stateLP
}

func stateLP(lexer *Lexer) statefn {
	lexer.pos += 1
	lexer.emit(TYPE_LP)
	return stateID
}

func stateID(lexer *Lexer) statefn {
	err := lexer.accept(')')
	if err != nil {
		log.Fatal(err)
	}
	lexer.emit(TYPE_ID)
	return stateRP
}

func stateRP(lexer *Lexer) statefn {
	lexer.pos += 1
	lexer.emit(TYPE_RP)
	close(lexer.ch)
	return nil
}

func (lexer *Lexer) Lex() []Token{
	tokens := make([]Token, 0)
	state := stateFunc
	go func () {
		for {
			state = state(lexer)
			if state == nil {
				return
			}
		}
	}()
	for t := range lexer.ch {
		spew.Dump(t)
		tokens = append(tokens, t)
	}
	return tokens
}

func Lex(input string) []Token {
	lexer := Lexer{input, 0, 0, TYPE_FUNC, make(chan Token)}
	return lexer.Lex()
}

var InvalidQueryError = errors.New("Invalid query format.")

type QueryParser struct {
	QueryString string
}

func (q *QueryParser) Walk(data interface{}) (*Query, error) {
	query := new(Query)

	slice, ok := data.([]interface{})
	if !ok {
		return nil, InvalidQueryError
	}
	operation, ok := slice[0].(string)

	if !ok {
		return nil, InvalidQueryError
	}
	if operation == "union" || operation == "intersect" {
		query.Operation = operation
		inputs := slice[1:]
		query.Inputs = make([]QueryInput, len(inputs))
		for idx, input := range inputs {
			subquery, err := q.Walk(input)
			if err != nil {
				return nil, err
			}
			query.Inputs[idx] = subquery
		}
	} else if operation == "bitmap" {
		query.Operation = "get"
		frame, ok := slice[1].(string)
		if !ok {
			return nil, InvalidQueryError
		}
		id, ok := slice[2].(float64)
		if !ok {
			return nil, InvalidQueryError
		}
		id_int := int(id)
		query.Inputs = []QueryInput{db.Bitmap{frame, id_int}}
	}

	return query, nil
}

func (q *QueryParser) Parse() (*Query, error) {
	var data interface{}
	if err := json.Unmarshal([]byte(q.QueryString), &data); err != nil {
		return nil, err
	}
	return q.Walk(data)
}
