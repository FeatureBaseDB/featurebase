package query

import (
	"pilosa/db"
	"pilosa/util"

	"github.com/davecgh/go-spew/spew"
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
	Profile_id int
}

func QueryPlanForPQL(database *db.Database, pql string) *QueryPlan {
	tokens := Lex(pql)
	query_parser := QueryParser{}
	query, err := query_parser.Parse(tokens)
	spew.Dump("-------------------------------------")
	spew.Dump(query)
	spew.Dump("-------------------------------------")
	if err != nil {
		panic(err)
	}
	query_planner := QueryPlanner{Database: database}
	id := uuid.RandomUUID()
	process_id := uuid.RandomUUID()
	fragment_id := util.SUUID(1)
	destination := db.Location{&process_id, fragment_id}
	query_plan := query_planner.Plan(query, &id, &destination)
	return query_plan
}
