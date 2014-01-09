package executor

import (
	"fmt"
	"log"
	"pilosa/core"
	"pilosa/db"
	"pilosa/query"
	"pilosa/util"
	"reflect"
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

func (self *Executor) RunQuery(database_name string, pql string) {
	database := self.service.Cluster.GetOrCreateDatabase(database_name)
	process, err := self.service.GetProcess()
	if err != nil {
		spew.Dump(err)
	}
	process_id := process.Id()
	fragment_id := util.SUUID(0)
	destination := db.Location{&process_id, fragment_id}

	query_plan := query.QueryPlanForPQL(database, pql, &destination)
	//spew.Dump(query_plan)

	// loop over the query steps and send to Transport
	var last_id *uuid.UUID
	for _, qs := range *query_plan {
		msg := new(db.Message)
		msg.Data = qs
		switch step := qs.(type) {
		case query.PortableQueryStep:
			self.service.Transport.Send(msg, step.GetLocation().ProcessId)
			if reflect.TypeOf(step) != reflect.TypeOf(query.SetQueryStep{}) {
				last_id = step.GetId()
			}
		}
	}

	// add an entry to my execute map[key] that is waiting for the final result
	if last_id != nil {
		final, err := self.service.Hold.Get(last_id, 10)
		if err != nil {
			spew.Dump(err)
		}
		spew.Dump("*******************************************************")
		spew.Dump("FINAL", final)
		spew.Dump("*******************************************************")
	}
}

func (self *Executor) Run() {
	log.Println("Executor Run...")
}

func NewExecutor(service *core.Service) *Executor {
	return &Executor{service, make(chan *db.Message)}
}
