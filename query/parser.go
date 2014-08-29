package query

import (
	"errors"
	"fmt"
	"log"
	"pilosa/util"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
)

var InvalidQueryError = errors.New("Invalid query format.")

type QueryParser struct {
	tokens []Token
	pos    int
}

/* MASTER
if len(tokens) > 4 && tokens[2].Type == TYPE_FRAME && tokens[4].Type == TYPE_PROFILE {
	frame_type = tokens[2].Text
	profile_id, err = strconv.ParseUint(tokens[4].Text, 10, 64)
	if err != nil {
		panic(err)
	}
} else if len(tokens) > 2 && tokens[2].Type == TYPE_FRAME {
	frame_type = tokens[2].Text
} else if len(tokens) > 2 && tokens[2].Type == TYPE_PROFILE {
	profile_id, err = strconv.ParseUint(tokens[2].Text, 10, 64)
	if err != nil {
		panic(err)
	}
}
var filter int //TRAVIS the is for the category
filter = 0
bm := db.Bitmap{bitmap_id, frame_type, filter}
return []QueryInput{&bm}, uint64(profile_id), 0
*/
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

	id := util.RandomUUID()
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
	const shortForm = "2006-01-02T15:04"

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
			case "range":
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
					t, err := time.Parse(shortForm, token.Text)
					if err != nil {
						return nil, fmt.Errorf("Expecting integer DateTime (%v)", err)
					}
					query.Args["start"] = t
				case 3:
					t, err := time.Parse(shortForm, token.Text)
					if err != nil {
						return nil, fmt.Errorf("Expecting integer DateTime (%v)", err)
					}
					query.Args["end"] = t
				}
			case "mask":
				switch len(query.Args) {
				case 0:
					query.Args["frame"] = token.Text
				case 1:
					i, err := strconv.ParseUint(token.Text, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Expecting integer id! (%v)", err)
					}
					query.Args["start"] = i
				case 2:
					i, err := strconv.ParseUint(token.Text, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Expecting integer id! (%v)", err)
					}
					query.Args["end"] = i
				}
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
					query.Args["filter"] = i
				case 3:
					i, err := strconv.ParseUint(token.Text, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Expecting integer id! (%v)", err)
					}
					query.Args["profile_id"] = i
				default:
					return nil, fmt.Errorf("Unexpected argument! (%v)", token)
				}

			case "top-n":
				switch len(query.Args) {
				case 0:
					query.Args["frame"] = token.Text
				case 1:
					i, err := strconv.Atoi(token.Text)
					if err != nil {
						return nil, fmt.Errorf("Expecting integer! (%v)", err)
					}
					query.Args["n"] = i
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
			} else if keyword == "n" {
				value, err = strconv.Atoi(token.Text)
				if err != nil {
					return nil, fmt.Errorf("Expecting integer! (%v)", err)
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

			log.Println(spew.Sdump("unexpected", token))
			return nil, errors.New("BAD TOKEN")
		}
	}

	if query.Operation == "get" && query.Args["frame"] == nil {
		query.Args["frame"] = "general"
	}
	if len(query.Args) == 0 && len(query.Subqueries) == 0 {
		if query.Operation == "count" {
			return nil, fmt.Errorf("No Args Given")
		}
		if query.Operation == "intersect" {
			return nil, fmt.Errorf("No Args Given")
		}
		if query.Operation == "union" {
			return nil, fmt.Errorf("No Args Given")
		}
		if query.Operation == "difference" {
			return nil, fmt.Errorf("No Args Given")
		}
		if query.Operation == "range" {
			return nil, fmt.Errorf("No Args Given")
		}
		if query.Operation == "mask" {
			return nil, fmt.Errorf("No Args Given")
		}
		if query.Operation == "stash" {
			return nil, fmt.Errorf("No Args Given")
		}
		if query.Operation == "recall" {
			return nil, fmt.Errorf("No Args Given")
		}
	}
	return query, nil
}

func Parse(tokens []Token) (*Query, error) {
	parser := QueryParser{tokens, 0}
	return parser.Parse()
}
