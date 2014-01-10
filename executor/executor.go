package executor

import (
	"fmt"
	"io/ioutil"
	"log"
	"pilosa/config"
	"pilosa/core"
	"pilosa/db"
	"pilosa/query"
	"pilosa/util"
	"tux21b.org/v1/gocql/uuid"

	"github.com/davecgh/go-spew/spew"
)

type Executor struct {
	service *core.Service
	inbox   chan *db.Message
}

func (self *Executor) Init() error {
	log.Println("Starting Executor")
	return nil
}

func (self *Executor) Close() {
	log.Println("Shutting down Executor")
}

func (self *Executor) NewJob(job *db.Message) {
	switch job.Data.(type) {
	case query.CountQueryStep:
		self.service.CountQueryStepHandler(job)
	case query.UnionQueryStep:
		self.service.UnionQueryStepHandler(job)
	case query.IntersectQueryStep:
		self.service.IntersectQueryStepHandler(job)
	case query.CatQueryStep:
		self.service.CatQueryStepHandler(job)
	case query.GetQueryStep:
		self.service.GetQueryStepHandler(job)
	case query.SetQueryStep:
		self.service.SetQueryStepHandler(job)
	default:
		fmt.Println("unknown")
	}
}

type stringSlice []string

func (slice stringSlice) pos(value string) int {
	for p, v := range slice {
		if v == value {
			return p
		}
	}
	return -1
}

func (self *Executor) RunQueryTest(database_name string, pql string) string {
	return pql
}

type queryItem struct {
	label string
	pql   string
}
type queryItemCopy struct {
	id    *uuid.UUID
	label string
}
type QueryItemResult struct {
	Label  string
	Result interface{}
}

func (self *Executor) runQuery(database *db.Database, qry *query.Query) {
	process, err := self.service.GetProcess()
	if err != nil {
		spew.Dump(err)
	}
	process_id := process.Id()
	fragment_id := util.SUUID(0)
	destination := db.Location{&process_id, fragment_id}

	spew.Dump("QUERY.ID:", qry.Id)
	query_plan := query.QueryPlanForQuery(database, qry, &destination)
	// loop over the query steps and send to Transport
	for _, qs := range *query_plan {
		msg := new(db.Message)
		msg.Data = qs
		switch step := qs.(type) {
		case query.PortableQueryStep:
			self.service.Transport.Send(msg, step.GetLocation().ProcessId)
		}
	}
}

func (self *Executor) RunPQL(database_name string, pql string) interface{} {
	database := self.service.Cluster.GetOrCreateDatabase(database_name)

	// see if the outer query function is a custom query
	reserved_functions := stringSlice{"get", "set", "union", "intersect", "count"}
	tokens := query.Lex(pql)
	outer_token := tokens[0].Text

	if reserved_functions.pos(outer_token) != -1 {

		qry := query.QueryForTokens(tokens)
		go self.runQuery(database, qry)

		var final interface{}
		final, err := self.service.Hold.Get(qry.Id, 10)
		if err != nil {
			spew.Dump(err)
		}
		return final

	} else {
		macros_dir := config.Get("macros").(string)
		macros_file := macros_dir + "/" + outer_token + ".js"
		file_data, err := ioutil.ReadFile(macros_file)
		if err != nil {
			spew.Dump(err)
		}
		spew.Dump(file_data)

		// CUSTOM QUERY LIST ///////////////
		var query_list []queryItem
		query_list = append(query_list, queryItem{"set1", "set(20, 1)"})
		query_list = append(query_list, queryItem{"set2", "set(20, 1)"})
		query_list = append(query_list, queryItem{"set3", "set(20, 2)"})
		query_list = append(query_list, queryItem{"set4", "set(20, 3)"})
		query_list = append(query_list, queryItem{"set5", "set(20, 4)"})
		query_list = append(query_list, queryItem{"count", "count(get(20))"})
		var query_list_copy []*queryItemCopy
		////////////////////////////////////

		for _, qi := range query_list {
			qry := query.QueryForPQL(qi.pql)
			go self.runQuery(database, qry)
			query_list_copy = append(query_list_copy, &queryItemCopy{qry.Id, qi.label})
		}

		var final_result []*QueryItemResult
		for _, qlc := range query_list_copy {
			final, err := self.service.Hold.Get(qlc.id, 10)
			if err != nil {
				spew.Dump(err)
			}
			spew.Dump(final)
			final_result = append(final_result, &QueryItemResult{qlc.label, final})
		}

		return final_result
	}

}

func (self *Executor) Run() {
	log.Println("Executor Run...")
}

func NewExecutor(service *core.Service) *Executor {
	return &Executor{service, make(chan *db.Message)}
}
