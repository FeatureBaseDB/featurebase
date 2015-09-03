package query

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	log "github.com/cihub/seelog"
	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa"
)

var InvalidQueryError = errors.New("Invalid query format.")

type QueryParser struct {
	tokens []Token
	pos    int
}

func (self *QueryParser) next() *Token {
	log.Trace("QueryParser.next")
	self.pos += 1
	if self.pos > len(self.tokens) {
		return nil
	}
	return &self.tokens[self.pos-1]
}

func (self *QueryParser) peek() *Token {
	log.Trace("QueryParser.peek")
	token := self.next()
	self.backup()
	return token
}

func (self *QueryParser) backup() {
	log.Trace("QueryParser.backup")
	self.pos -= 1
}

func (self *QueryParser) Parse() (query *Query, err error) {
	log.Trace("QueryParser.Parse", query, err)
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

	id := pilosa.RandomUUID()
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
			case "clear":
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
			case "all":
				// do nothing
			case "recall": //need pair based list of frag_id,handle
				arg, ok := query.Args["stash"]
				if !ok {
					arg = &Stash{make([]CacheItem, 0), false}
					query.Args["stash"] = arg
				}

				recallArgs := arg.(*Stash)
				if !recallArgs.incomplete {
					i, err := strconv.ParseUint(token.Text, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Expecting fragment id! (%v)", err)
					}
					recallArgs.Add(pilosa.SUUID(i)) //constructs a new arg
				} else {
					i, err := strconv.ParseUint(token.Text, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("Expecting handle! (%v)", err)
					}
					recallArgs.Assign(pilosa.BitmapHandle(i)) //sets the value of the last created arg
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
			peeked := self.peek()
			// we currently support 2 types of values in square brackets:
			//    ids      <- list of integers (TYPE_VALUE)
			//    filters  <- list of queries (TYPE_FUNC)
			switch peeked.Type {
			case TYPE_VALUE:
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
			case TYPE_FUNC:
				query.Args["filters"] = make([]Query, 0)
				for {
					token = self.next()
					if token == nil {
						return nil, fmt.Errorf("Unclosed list!")
					}
					switch token.Type {
					case TYPE_COMMA:
						break
					case TYPE_FUNC:
						self.backup()
						filterquery, err := self.Parse()
						if err != nil {
							return nil, err
						}
						query.Args["filters"] = append(query.Args["filters"].([]Query), *filterquery)
					case TYPE_RB:
						continue ArgLoop
					default:
						return nil, fmt.Errorf("Unexpected token! (%v)", token)
					}
				}
			}
		case TYPE_RB:
			//
		default:
			log.Warn(spew.Sdump("unexpected", token))
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
	log.Trace("Parse", tokens)
	parser := QueryParser{tokens, 0}
	return parser.Parse()
}
