package executor

import (
	"log"
	"pilosa/core"
	"pilosa/query"

	"github.com/davecgh/go-spew/spew"
)

type Job struct {
	query_plan *query.QueryPlan
	results_ch chan *query.QueryResults
}

type Executor struct {
	service *core.Service
	inbox   chan *Job
}

func (self *Executor) Init() error {
	log.Println("Starting Executor")
	return nil
}

func (self *Executor) Close() {
	log.Println("Shutting down Executor")
}

func (self *Executor) NewJob(qp *query.QueryPlan, results chan *query.QueryResults) {
	j := Job{qp, results}
	self.inbox <- &j
}

//func (self *Executor) ExecuteJob(qp *query.QueryPlan) {
func (self *Executor) executeJob(job *Job) {
	log.Println("Executor: ExecuteJob")
	//spew.Dump(job)
	query_plan := job.query_plan
	for _, qs := range *query_plan {
		spew.Dump(qs)
	}
	/*
		spew.Dump("sleep...")
		time.Sleep(4 * 1e9)
	*/
	// TODO: send the query steps out & wait for responses
	qr := new(query.QueryResults)
	qr.Data = 999
	job.results_ch <- qr
}

func (self *Executor) Run() {
	log.Println("Executor Run...")
	for {
		job := <-self.inbox
		go self.executeJob(job)
	}
}

func NewExecutor(service *core.Service) *Executor {
	return &Executor{service, make(chan *Job)}
}
