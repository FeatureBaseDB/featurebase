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
		var bhs []index.BitmapHandle
		use_sum := false
		sum := 0
		for _, input := range qs.Inputs {
			spew.Dump(input)
			bhi, _ := self.service.Hold.Get(input, 10)
			spew.Dump(bhi)
			switch bhi.(type) {
			case index.BitmapHandle:
				spew.Dump("BH")
				bhs = append(bhs, bhi.(index.BitmapHandle))
			case []byte:
				spew.Dump("RAW")
				bh, _ := self.service.Index.FromBytes(qs.Location.FragmentId, bhi.([]byte))
				bhs = append(bhs, bh)
			case int:
				use_sum = true
				sum += bhi.(int)
			}
		}

		if use_sum {
			spew.Dump("SUM", sum)
		} else {

			spew.Dump("BHS", bhs)

			unionbh, err := self.service.Index.Union(qs.Location.FragmentId, bhs)
			if err != nil {
				spew.Dump(err)
			}
			spew.Dump("UNION BH", unionbh)

			/*
				count, err := self.service.Index.Count(qs.Location.FragmentId, unionbh)
				if err != nil {
					spew.Dump(err)
				}
				spew.Dump("FINAL COUNT", count)
			*/
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

		if qs.LocIsDest() {
			self.service.Hold.Set(qs.Id, bh, 10)
		} else {
			bm, err := self.service.Index.GetBytes(qs.Location.FragmentId, bh)
			if err != nil {
				spew.Dump(err)
			}
			self.service.Hold.Set(qs.Id, bm, 10)
		}

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
