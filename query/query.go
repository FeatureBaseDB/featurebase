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
	Operation string
	Inputs    []QueryInput //"strconv"
	// Represents a parsed query. Inputs can be Query or Bitmap objects
	// Maybe Bitmap and Query objects should have different fields to avoid using interface{}
	ProfileId uint64
}

func QueryPlanForPQL(database *db.Database, pql string, destination *db.Location) *QueryPlan {
	tokens := Lex(pql)
	query_parser := QueryParser{}
	query, err := query_parser.Parse(tokens)
	if err != nil {
		panic(err)
	}
	query_planner := QueryPlanner{Database: database}
	id := uuid.RandomUUID()
	query_plan := query_planner.Plan(query, &id, destination)
	return query_plan
}
