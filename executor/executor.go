package executor

import (
	"fmt"
	"log"
	"pilosa/core"
	"pilosa/db"
	"pilosa/index"
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
	spew.Dump("NewJob")
	spew.Dump(job.Data)
	switch job.Data.(type) {
	case query.CatQueryStep:
		qs := job.Data.(query.CatQueryStep)
		fmt.Println("CAT QUERYSTEP")
		spew.Dump(qs)
		for _, input := range qs.Inputs {
			spew.Dump(input)
			bhi, err := self.service.Hold.Get(input, 10)
			bh := bhi.(index.BitmapHandle)
			// TODO: git rid of this count, need the cat to do a sum() or a true cat()
			count, err := self.service.Index.Count(qs.Location.FragmentId, bh)
			if err != nil {
				spew.Dump(err)
			}
			spew.Dump("COUNT", count)
		}
	case query.GetQueryStep:

		qs := job.Data.(query.GetQueryStep)
		fmt.Println("GET QUERYSTEP")

		// perform get query with index
		bh, err := self.service.Index.Get(qs.Location.FragmentId, qs.Bitmap.Id)
		//count, err := self.service.Index.Count(qs.Location.FragmentId, bh)
		if err != nil {
			spew.Dump(err)
		}
		//spew.Dump("COUNT", count)
		// push results to the map
		self.service.Hold.Set(qs.Id, bh, 10)

	case query.SetQueryStep:
		qs := job.Data.(query.SetQueryStep)
		fmt.Println("SET QUERYSTEP")
		self.service.Index.SetBit(qs.Location.FragmentId, qs.Bitmap.Id, qs.ProfileId)
	default:
		fmt.Println("unknown")

	}
}

func (self *Executor) RunQuery(database_name string, pql string) {
	log.Println("RunQuery: PQL")

	database := self.service.Cluster.GetOrCreateDatabase(database_name)

	process, err := self.service.GetProcess()
	if err != nil {
		spew.Dump(err)
	}
	process_id := process.Id()
	fragment_id := util.SUUID(0)
	destination := db.Location{&process_id, fragment_id}

	query_plan := query.QueryPlanForPQL(database, pql, &destination)
	spew.Dump(query_plan)

	// loop over the query steps and send to Transport
	for _, qs := range *query_plan {
		msg := new(db.Message)
		msg.Data = qs
		self.service.Transport.Push(msg)
	}

	// TODO: add an entry to my execute map[key] that is waiting for the final result
	//self.service.Hold.Get(??)

}

func (self *Executor) Run() {
	log.Println("Executor Run...")
}

func NewExecutor(service *core.Service) *Executor {
	return &Executor{service, make(chan *db.Message)}
}
