package executor

import (
	"fmt"
	"log"
	"pilosa/core"
	"pilosa/db"
	"pilosa/query"
	"time"
	"tux21b.org/v1/gocql/uuid"

	"github.com/davecgh/go-spew/spew"
)

/*
type Job struct {
	query_plan *query.QueryPlan
	results_ch chan *query.QueryResults
}
*/

type Executor struct {
	service *core.Service
	inbox   chan *db.Message
	qs_chan chan *query.QueryStep
}

func (self *Executor) Init() error {
	log.Println("Starting Executor")
	return nil
}

func (self *Executor) Close() {
	log.Println("Shutting down Executor")
}

func (self *Executor) NewJob(job *db.Message) {
	spew.Dump("NewJob")
	spew.Dump(job.Data)
	switch job.Data.(type) {
	case query.GetQueryStep:

		qs := job.Data.(query.GetQueryStep)
		fmt.Println("GET QUERYSTEP")
		// perform get query with index
		// push results to the map
		//query_results = ???

		// TEMP
		profile_id := uint64(7899)
		self.service.Index.SetBit(qs.Location.FragmentId, qs.Bitmap.Id, profile_id)
		// END TEMP

		spew.Dump("COUNT")
		bh, err := self.service.Index.Get(qs.Location.FragmentId, qs.Bitmap.Id)
		if err != nil {
			spew.Dump(err)
		}
		count, err := self.service.Index.Count(qs.Location.FragmentId, bh)
		if err != nil {
			spew.Dump(err)
		}
		spew.Dump(count)
		spew.Dump(qs.Id)

		//self.Set(qs.Id, count)
	case query.SetQueryStep:
		fmt.Println("SET QUERYSTEP")
		//self.Get()
	default:
		fmt.Println("unknown")

	}
}

func (self *Executor) NewQS(qs *query.QueryStep) {
	self.qs_chan <- qs
}

func (self *Executor) RunQuery(database_name string, pql string) {
	log.Println("RunQuery: PQL")
	spew.Dump(pql)

	database := self.service.Cluster.GetOrCreateDatabase(database_name)
	query_plan := query.QueryPlanForPQL(database, pql)
	//spew.Dump(query_plan)

	// loop over the query steps and send to Transport
	for _, qs := range *query_plan {
		spew.Dump(qs)
		msg := new(db.Message)
		msg.Data = qs
		self.service.Transport.Push(msg)
	}
	// TODO: add an entry to my execute map[key] that is waiting for the final result
	self.Get()

	time.Sleep(4 * 1e9)
	/*
		results_ch := make(chan *query.QueryResults)
		self.service.Executor.NewJob(query_plan, results_ch)
		results := <-results_ch
		spew.Dump("Results")
		spew.Dump(results)
		close(results_ch)

	*/

}

func (self *Executor) Get() {
	log.Println("Executor: Get")
}

func (self *Executor) Set(id *uuid.UUID, results *query.QueryResults) {
	log.Println("Executor: Set")
}

func (self *Executor) executeQueryPlan(job *db.Message) {
	log.Println("Executor: ExecuteJob")

	/*
		//spew.Dump(job)
		query_plan := job.query_plan
		for _, qs := range *query_plan {
			spew.Dump(qs)
			//query.HandleQueryStep(&qs)
			//res, err := self.service.Process.SetBit(fragment_id, bitmaps[0], profile_id)

			msg := new(db.Message)
			msg.Data = qs
			self.service.Transport.Push(msg)

		}
		// TODO: send the query steps out & wait for responses
		qr := new(query.QueryResults)
		qr.Data = 999
		job.results_ch <- qr
	*/
}

func (self *Executor) executeQS(qs *query.QueryStep) {
	spew.Dump("EXEC QS")
}

func (self *Executor) Run() {
	log.Println("Executor Run...")
	/*
		for {
			select {
			case job := <-self.inbox:
				go self.executeQueryPlan(job)
			case qs := <-self.qs_chan:
				go self.executeQS(qs)
			}
		}
	*/
}

func NewExecutor(service *core.Service) *Executor {
	return &Executor{service, make(chan *db.Message), make(chan *query.QueryStep)}
}
