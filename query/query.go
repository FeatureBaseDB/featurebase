package query

import (
	"pilosa/db"
	"strings"

	"github.com/gocql/gocql/uuid"
)

type QueryInput interface{}

type QueryResults struct {
	Data interface{}
}

type PqlList []PqlListItem

type PqlListItem struct {
	Id    *uuid.UUID
	Label string
	PQL   string
}

type Query struct {
	Id         *uuid.UUID
	Operation  string
	Args       map[string]interface{}
	Subqueries []Query
}

func QueryPlanForPQL(database *db.Database, pql string, destination *db.Location) (*QueryPlan, error) {
	tokens, err := Lex(pql)
	if err != nil {
		return nil, err
	}
	return QueryPlanForTokens(database, tokens, destination)
}

func QueryForPQL(pql string) (*Query, error) {
	tokens, err := Lex(pql)
	if err != nil {
		return nil, err
	}
	return QueryForTokens(tokens)
}

func QueryForTokens(tokens []Token) (*Query, error) {
	query, err := Parse(tokens)
	if err != nil {
		return nil, err
	}
	return query, nil
}

func QueryPlanForTokens(database *db.Database, tokens []Token, destination *db.Location) (*QueryPlan, error) {
	query, err := QueryForTokens(tokens)
	if err != nil {
		return nil, err
	}
	return QueryPlanForQuery(database, query, destination)
}

func QueryPlanForQuery(database *db.Database, query *Query, destination *db.Location) (*QueryPlan, error) {
	query_planner := QueryPlanner{Database: database, Query: query}
	id := uuid.RandomUUID()
	query_plan, err := query_planner.Plan(query, &id, destination)
	if err != nil {
		return nil, err
	}
	return query_plan, nil
}

func TokensToString(tokens []Token) string {
	var str []string
	for i, _ := range tokens {
		str = append(str, tokens[i].Text)
	}
	// for now, we're just using this function to pull the filter out of the outer function "outerfunc(filter)"
	str = str[2 : len(str)-1]
	return strings.Join(str, "")
}
