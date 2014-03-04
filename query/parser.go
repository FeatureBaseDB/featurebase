package query

import (
	"errors"
	"fmt"
	"strconv"
	"tux21b.org/v1/gocql/uuid"

	"github.com/davecgh/go-spew/spew"
)

var InvalidQueryError = errors.New("Invalid query format.")

type QueryParser struct {
	tokens []Token
	pos    int
}

func (self *QueryParser) next() *Token {
	self.pos += 1
	if self.pos > len(self.tokens) {
		return nil
	}
	return &self.tokens[self.pos-1]
}

func (self *QueryParser) peek() *Token {
	token := self.next()
	self.backup()
	return token
}

func (self *QueryParser) backup() {
	self.pos -= 1
}

func (self *QueryParser) Parse() (query *Query, err error) {
	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)
			if !ok {
				err = fmt.Errorf("query: %v", r)
			}
		}
	}()
	var token *Token

	id := uuid.RandomUUID()
	query = &Query{Id: &id, Subqueries: make([]Query, 0), Args: make(map[string]interface{})}

	token = self.next()
	if token.Type != TYPE_FUNC {
		return nil, fmt.Errorf("Expected function, found token %v.", token)
	}

	query.Operation = token.Text

	token = self.next()
	if token.Type != TYPE_LP {
		return nil, fmt.Errorf("Expected '(', found token %v.", token)
	}

ArgLoop:
	for {
		token = self.next()
		if token == nil {
			return nil, fmt.Errorf("Unclosed parentheses!")
		}

		switch token.Type {
		case TYPE_FUNC:
			self.backup()
			subquery, err := self.Parse()
			if err != nil {
				return nil, err
			}
			query.Subqueries = append(query.Subqueries, *subquery)
		case TYPE_VALUE:
			switch query.Operation {
			case "get":
				switch len(query.Args) {
				case 0:
					i, err := strconv.ParseUint(token.Text, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Expecting integer id! (%v)", err)
					}
					query.Args["id"] = i
				case 1:
					query.Args["frame"] = token.Text
				default:
					return nil, fmt.Errorf("Unexpected argument! (%v)", token)
				}
			case "set":
				switch len(query.Args) {
				case 0:
					i, err := strconv.ParseUint(token.Text, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Expecting integer id! (%v)", err)
					}
					query.Args["id"] = i
				case 1:
					query.Args["frame"] = token.Text
				case 2:
					i, err := strconv.ParseUint(token.Text, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Expecting integer id! (%v)", err)
					}
					query.Args["profile_id"] = i
				default:
					return nil, fmt.Errorf("Unexpected argument! (%v)", token)
				}

			default:
				spew.Dump("UNPROCESSED VALUE", token)
			}
			continue
		case TYPE_COMMA:
			continue
		case TYPE_RP:
			break ArgLoop
		case TYPE_KEYWORD:
			var value interface{}
			keyword := token.Text
			token = self.next()
			if token == nil || token.Type != TYPE_EQUALS {
				return nil, fmt.Errorf("Expecting equals sign!")
			}
			token = self.next()
			if token == nil || token.Type != TYPE_VALUE {
				return nil, fmt.Errorf("Expecting value!")
			}
			if keyword == "id" {
				value, err = strconv.ParseUint(token.Text, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("Expecting integer id! (%v)", err)
				}
			} else {
				value = token.Text
			}
			query.Args[keyword] = value
		case TYPE_LB:
			query.Args["ids"] = make([]uint64, 0)
			for {
				token = self.next()
				if token == nil {
					return nil, fmt.Errorf("Unclosed list!")
				}
				switch token.Type {
				case TYPE_COMMA:
					break
				case TYPE_VALUE:
					i, err := strconv.ParseUint(token.Text, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Expecting integer id! (%v)", err)
					}
					query.Args["ids"] = append(query.Args["ids"].([]uint64), i)
				case TYPE_RB:
					continue ArgLoop
				default:
					return nil, fmt.Errorf("Unexpected token! (%v)", token)
				}
			}
		default:
			spew.Dump("unexpected", token)
			panic(token)
		}
	}

	if query.Operation == "get" && query.Args["frame"] == nil {
		query.Args["frame"] = "general"
	}

	return query, nil
}

func Parse(tokens []Token) (*Query, error) {
	parser := QueryParser{tokens, 0}
	return parser.Parse()
}
