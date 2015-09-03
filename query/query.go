package query

import (
	"strings"

	log "github.com/cihub/seelog"
	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/db"
)

type QueryInput interface{}

type QueryResults struct {
	Data interface{}
}

type PqlList []PqlListItem

type PqlListItem struct {
	Id    *pilosa.GUID
	Label string
	PQL   string
}

type Query struct {
	Id         *pilosa.GUID
	Operation  string
	Args       map[string]interface{}
	Subqueries []Query
}

func QueryPlanForPQL(database *db.Database, pql string, destination *db.Location) (*QueryPlan, error) {
	log.Trace("QueryPlanFOrPQL", database, pql, destination)
	tokens, err := Lex(pql)
	if err != nil {
		return nil, err
	}
	return QueryPlanForTokens(database, tokens, destination)
}

func QueryForPQL(pql string) (*Query, error) {
	log.Trace("QueryForPQL", pql)
	tokens, err := Lex(pql)
	if err != nil {
		return nil, err
	}
	return QueryForTokens(tokens)
}

func QueryForTokens(tokens []Token) (*Query, error) {
	log.Trace("QueryForTokens", tokens)
	query, err := Parse(tokens)
	if err != nil {
		return nil, err
	}
	return query, nil
}

func QueryPlanForTokens(database *db.Database, tokens []Token, destination *db.Location) (*QueryPlan, error) {
	log.Trace("QueryPlanForTokens", database, tokens, destination)
	query, err := QueryForTokens(tokens)
	if err != nil {
		return nil, err
	}
	return QueryPlanForQuery(database, query, destination)
}

func QueryPlanForQuery(database *db.Database, query *Query, destination *db.Location) (*QueryPlan, error) {
	log.Trace("QueryPlanForQuery", database, query, destination)
	query_planner := QueryPlanner{Database: database, Query: query}
	id := pilosa.RandomUUID()
	query_plan, err := query_planner.Plan(query, &id, destination)
	if err != nil {
		return nil, err
	}
	return query_plan, nil
}

func TokensToFilterStrings(tokens []Token) (string, []string) {
	log.Trace("TokensToFilterStrings", tokens)
	var whole []string
	var filter string
	var filters []string
	var open_parens int
	var in_square_brackets bool
	var last_slice = 0

	open_parens = -1
	in_square_brackets = false
	for i, _ := range tokens {
		whole = append(whole, tokens[i].Text)
		if tokens[i].Type == TYPE_FUNC {
			last_slice = i
		} else if tokens[i].Type == TYPE_LP {
			open_parens += 1
		} else if tokens[i].Type == TYPE_RP {
			open_parens -= 1
			if open_parens == 0 {
				if !in_square_brackets {
					filter = strings.Join(whole[2:], "")
				} else {
					filters = append(filters, strings.Join(whole[last_slice:], ""))
				}
				last_slice = i
			}
		} else if open_parens == 0 && tokens[i].Type == TYPE_LB {
			in_square_brackets = true
		}
	}
	return filter, filters
}
