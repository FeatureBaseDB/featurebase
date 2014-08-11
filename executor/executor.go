package executor

import (
	"fmt"
	"log"
	"pilosa/config"
	"pilosa/core"
	"pilosa/db"
	"pilosa/query"
	"pilosa/util"

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
	case query.TopNQueryStep:
		self.service.TopNQueryStepHandler(job)
	case query.UnionQueryStep:
		self.service.UnionQueryStepHandler(job)
	case query.IntersectQueryStep:
		self.service.IntersectQueryStepHandler(job)
	case query.DifferenceQueryStep:
		self.service.DifferenceQueryStepHandler(job)
	case query.CatQueryStep:
		self.service.CatQueryStepHandler(job)
	case query.GetQueryStep:
		self.service.GetQueryStepHandler(job)
	case query.SetQueryStep:
		self.service.SetQueryStepHandler(job)
	case query.RangeQueryStep:
		self.service.RangeQueryStepHandler(job)
		//	case query.MaskQueryStep:
		//		self.service.MaskQueryStepHandler(job)
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

func (self *Executor) runQuery(database *db.Database, qry *query.Query) error {
	process, err := self.service.GetProcess()
	if err != nil {
		return err
	}
	process_id := process.Id()
	fragment_id := util.SUUID(0)
	destination := db.Location{&process_id, fragment_id}

	query_plan, err := query.QueryPlanForQuery(database, qry, &destination)
	if err != nil {
		switch obj := err.(type) {
		//case *query.InvalidFrame:
		case *query.FragmentNotFound:
			self.service.TopologyMapper.MakeFragments(obj.Db, obj.Slice)
		}
		self.service.Hold.Set(qry.Id, err, 30)
		return err
	}
	// loop over the query steps and send to Transport
	for _, qs := range *query_plan {
		msg := new(db.Message)
		msg.Data = qs
		switch step := qs.(type) {
		case query.PortableQueryStep:
			self.service.Transport.Send(msg, step.GetLocation().ProcessId)
		}
	}
	return nil
}

func (self *Executor) RunPQL(database_name string, pql string) (interface{}, error) {
	database := self.service.Cluster.GetOrCreateDatabase(database_name)

	// see if the outer query function is a custom query
	reserved_functions := stringSlice{"get", "set", "union", "intersect", "difference", "count", "top-n", "mask", "range"}
	tokens, err := query.Lex(pql)
	if err != nil {
		return nil, err
	}
	outer_token := tokens[0].Text

	if reserved_functions.pos(outer_token) != -1 {

		qry, err := query.QueryForTokens(tokens)
		if err != nil {
			return nil, err
		}
		go self.runQuery(database, qry)

		var final interface{}
		final, err = self.service.Hold.Get(qry.Id, 10)
		if err != nil {
			return nil, err
		}
		return final, nil

	} else {
		plugins_dir := config.GetString("plugins")
		plugins_file := plugins_dir + "/" + outer_token + ".js"
		filter := query.TokensToString(tokens)
		query_list := GetMacro(plugins_file, filter).(query.PqlList)

		for i, _ := range query_list {
			qry, err := query.QueryForPQL(query_list[i].PQL)
			if err != nil {
				return nil, err
			}
			go self.runQuery(database, qry)
			query_list[i].Id = qry.Id
		}

		final_result := make(map[string]interface{})
		result := make(chan struct {
			final interface{}
			label string
			err   error
		})
		x := 0
		for i, _ := range query_list {
			x++
			go func(q query.PqlListItem, reply chan struct {
				final interface{}
				label string
				err   error
			}) {
				final, err := self.service.Hold.Get(q.Id, 10)
				result <- struct {
					final interface{}
					label string
					err   error
				}{final, q.Label, err}
			}(query_list[i], result)
			if err != nil {
				spew.Dump(err)
			}
		}
		for z := 0; z < x; z++ {
			ans := <-result
			final_result[ans.label] = ans.final
		}

		return final_result, nil
	}

}

func (self *Executor) Run() {
	log.Println("Executor Run...")
}

func NewExecutor(service *core.Service) *Executor {
	return &Executor{service, make(chan *db.Message)}
}
