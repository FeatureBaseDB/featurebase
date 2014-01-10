package query

import (
	"pilosa/db"

	"tux21b.org/v1/gocql/uuid"
)

type QueryInput interface{}

type QueryResults struct {
	Data interface{}
}

type Query struct {
	Id        *uuid.UUID
	Operation string
	Inputs    []QueryInput //"strconv"
	// Represents a parsed query. Inputs can be Query or Bitmap objects
	// Maybe Bitmap and Query objects should have different fields to avoid using interface{}
	ProfileId uint64
}

func QueryPlanForPQL(database *db.Database, pql string, destination *db.Location) *QueryPlan {
	tokens := Lex(pql)
	return QueryPlanForTokens(database, tokens, destination)
}

func QueryForPQL(pql string) *Query {
	tokens := Lex(pql)
	return QueryForTokens(tokens)
}

func QueryForTokens(tokens []Token) *Query {
	query_parser := QueryParser{}
	query, err := query_parser.Parse(tokens)
	if err != nil {
		panic(err)
	}
	return query
}

func QueryPlanForTokens(database *db.Database, tokens []Token, destination *db.Location) *QueryPlan {
	query := QueryForTokens(tokens)
	return QueryPlanForQuery(database, query, destination)
	/*
		//spew.Dump(query)
		query_planner := QueryPlanner{Database: database}
		id := uuid.RandomUUID()
		query_plan := query_planner.Plan(query, &id, destination)
		//spew.Dump(query_plan)
		return query_plan
	*/
}

func QueryPlanForQuery(database *db.Database, query *Query, destination *db.Location) *QueryPlan {
	//spew.Dump(query)
	query_planner := QueryPlanner{Database: database}
	id := uuid.RandomUUID()
	query_plan := query_planner.Plan(query, &id, destination)
	//spew.Dump(query_plan)
	return query_plan
}
