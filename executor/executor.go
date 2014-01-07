package executor

import (
	"fmt"
	"log"
	"pilosa/core"
	"pilosa/db"
	"pilosa/index"
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
		spew.Dump("COUNT QUERYSTEP")

		qs := job.Data.(query.CountQueryStep)
		input := qs.Input
		bhi, _ := self.service.Hold.Get(input, 10)
		var bh index.BitmapHandle
		switch bhi.(type) {
		case index.BitmapHandle:
			bh = bhi.(index.BitmapHandle)
		case []byte:
			bh, _ = self.service.Index.FromBytes(qs.Location.FragmentId, bhi.([]byte))
		}

		count, err := self.service.Index.Count(qs.Location.FragmentId, bh)
		if err != nil {
			spew.Dump(err)
		}
		spew.Dump("SLICE COUNT", count)
		// TODO: instead of adding to the local hold, we need to send result to transport (which may go to a remote process's hold)
		self.service.Hold.Set(qs.Id, count, 10)

	case query.CatQueryStep:
		qs := job.Data.(query.CatQueryStep)
		spew.Dump("CAT QUERYSTEP")
		spew.Dump(qs)
		var bhs []index.BitmapHandle
		use_sum := false
		var sum uint64
		for _, input := range qs.Inputs {
			bhi, _ := self.service.Hold.Get(input, 10)
			switch bhi.(type) {
			case index.BitmapHandle:
				spew.Dump("BH")
				bhs = append(bhs, bhi.(index.BitmapHandle))
			case []byte:
				spew.Dump("RAW")
				bh, _ := self.service.Index.FromBytes(qs.Location.FragmentId, bhi.([]byte))
				bhs = append(bhs, bh)
			case uint64:
				use_sum = true
				sum += bhi.(uint64)
			}
		}

		if use_sum {
			spew.Dump("SUM", sum)
			// TODO: instead of adding to the local hold, we need to send result to transport (which may go to a remote process's hold)
			self.service.Hold.Set(qs.Id, sum, 10)
		} else {
			unionbh, err := self.service.Index.Union(qs.Location.FragmentId, bhs)
			if err != nil {
				spew.Dump(err)
			}
			unionbm, err := self.service.Index.GetBytes(qs.Location.FragmentId, unionbh)
			if err != nil {
				spew.Dump(err)
			}
			// TODO: instead of adding to the local hold, we need to send result to transport (which may go to a remote process's hold)
			self.service.Hold.Set(qs.Id, unionbm, 10)
		}

	case query.GetQueryStep:

		qs := job.Data.(query.GetQueryStep)
		spew.Dump("GET QUERYSTEP")

		bh, err := self.service.Index.Get(qs.Location.FragmentId, qs.Bitmap.Id)
		if err != nil {
			spew.Dump(err)
		}
		if qs.LocIsDest() {
			self.service.Hold.Set(qs.Id, bh, 10)
		} else {
			bm, err := self.service.Index.GetBytes(qs.Location.FragmentId, bh)
			if err != nil {
				spew.Dump(err)
			}
			// TODO: instead of adding to the local hold, we need to send result to transport (which may go to a remote process's hold)
			self.service.Hold.Set(qs.Id, bm, 10)
		}

	case query.SetQueryStep:
		qs := job.Data.(query.SetQueryStep)
		spew.Dump("SET QUERYSTEP")
		self.service.Index.SetBit(qs.Location.FragmentId, qs.Bitmap.Id, qs.ProfileId)
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
	spew.Dump("--------query_plan-------------------")
	spew.Dump(query_plan)
	spew.Dump("-------------------------------------")
	//return

	// loop over the query steps and send to Transport
	var last_id *uuid.UUID
	for _, qs := range *query_plan {
		msg := new(db.Message)
		msg.Data = qs
		spew.Dump("qs", qs)
		switch step := qs.(type) {
		case query.CatQueryStep:
			last_id = step.Id
		}
		self.service.Transport.Push(msg)
	}

	// add an entry to my execute map[key] that is waiting for the final result
	if last_id != nil {
		final, err := self.service.Hold.Get(last_id, 10)
		if err != nil {
			spew.Dump(err)
		}
		spew.Dump("last_id", last_id)
		spew.Dump("GRAND FINAL", final)
	}
}

func (self *Executor) Run() {
	log.Println("Executor Run...")
}

func NewExecutor(service *core.Service) *Executor {
	return &Executor{service, make(chan *db.Message)}
}
