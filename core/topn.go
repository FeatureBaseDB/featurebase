package core

import (
	"encoding/gob"

	log "github.com/cihub/seelog"
	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/index"
	"github.com/umbel/pilosa/query"
	"github.com/umbel/pilosa/util"
)

type Task struct {
	processid util.GUID
	f         map[util.SUUID]index.FillArgs
	hold_id   util.GUID
}

type TopFill struct {
	Args            []index.FillArgs
	ReturnProcessId util.GUID
	QueryId         util.GUID
	DestProcessId   util.GUID
}

//portable query step
func (self *TopFill) GetId() *util.GUID {
	return &self.QueryId
}
func (self *TopFill) GetLocation() *db.Location {
	return &db.Location{&self.DestProcessId, 0} //this message is a broadcast to many fragments so i'm choosing fragmentzero
}

//

type hole struct {
	process  util.GUID
	handle   index.BitmapHandle
	fragment util.SUUID
}

func missing(fids map[util.SUUID]struct{}, all map[util.SUUID]struct {
	process util.GUID
	handle  index.BitmapHandle
}) []hole {
	results := make([]hole, 0, 0)

	for k, v := range all {
		_, ok := fids[k]
		if !ok {
			results = append(results, hole{v.process, v.handle, k})
		}
	}
	return results
}

func newtask(p util.GUID) *Task {
	result := new(Task)
	result.processid = p
	result.f = make(map[util.SUUID]index.FillArgs)
	result.hold_id = util.RandomUUID()
	return result
}

func (t *Task) Add(frag util.SUUID, bitmap_id uint64, handle index.BitmapHandle) {
	fa, ok := t.f[frag]
	if !ok {
		fa = index.FillArgs{frag, handle, make([]uint64, 0, 0)}
	}
	fa.Bitmaps = append(fa.Bitmaps, bitmap_id)
	t.f[frag] = fa
}

func BuildTask(merge_map map[uint64]uint64,
	slice_map map[uint64]map[util.SUUID]struct{},
	total_fragments map[util.SUUID]struct {
		process util.GUID
		handle  index.BitmapHandle
	}) map[util.GUID]*Task {

	tasks := make(map[util.GUID]*Task)
	for bitmap_id, _ := range merge_map { //for all brands
		//for fragment_id, reported_fragments := range slice_map[bitmap_id] { //find missing fragments
		reporting_fragments := slice_map[bitmap_id]
		//id slice ==> SUUID,BitmapHandle
		for _, p := range missing(reporting_fragments, total_fragments) {
			task, ok := tasks[p.process]
			if !ok {
				task = newtask(p.process)
				tasks[p.process] = task
			}
			task.Add(p.fragment, bitmap_id, p.handle)

		}
		//}
	}

	return tasks
}
func (self *Service) TopFillHandler(msg *db.Message) { //in order for this to get executed it needs to be a portable query step
	topfill := msg.Data.(TopFill)
	topn, err := self.Index.TopFillBatch(topfill.Args)
	if err != nil {
		log.Warn("TopFillHandler:", err)
	}

	result_message := db.Message{Data: query.FillResult{&query.BaseQueryResult{Id: &topfill.QueryId, Data: topn}}}
	self.Transport.Send(&result_message, &topfill.ReturnProcessId)
}

func SendRequest(process_id util.GUID, t *Task, service *Service) {
	args := make([]index.FillArgs, len(t.f), len(t.f))
	for _, v := range t.f {
		args = append(args, v)
	}
	msg := new(db.Message)
	p, _ := service.GetProcess()
	msg.Data = TopFill{args, p.Id(), t.hold_id, process_id}
	service.Transport.Send(msg, &process_id)
}

func FetchMissing(tasks map[util.GUID]*Task, service *Service) {
	for k, v := range tasks {
		go SendRequest(k, v, service)
	}
}

func GatherResults(tasks map[util.GUID]*Task, service *Service) map[uint64]uint64 {
	results := make(map[uint64]uint64)
	answers := make(chan []index.Pair)
	for _, task := range tasks {
		go func(id util.GUID) {
			value, err := service.Hold.Get(&id, 10) //eiher need to be the frame process or the handler process?
			if value == nil {
				log.Warn("Bad TopN Result:", err)
				empty := make([]index.Pair, 0, 0)
				answers <- empty

			} else {
				answers <- value.([]index.Pair)
			}
		}(task.hold_id)
	}
	for i := 0; i < len(tasks); i++ {
		batch := <-answers
		for _, pair := range batch {
			results[pair.Key] += pair.Count
		}
	}
	close(answers)
	return results
}

type TopNPackage struct {
	ProcessId  util.GUID
	FragmentId util.SUUID
	Pairs      []index.Pair
	HBitmap    index.BitmapHandle
}

func init() {
	gob.Register(TopNPackage{})
	gob.Register(TopFill{})
}

func (self *Service) TopNQueryStepHandler(msg *db.Message) {
	qs := msg.Data.(query.TopNQueryStep)
	var bh index.BitmapHandle
	var topnPackage TopNPackage

	// if we have an input, hold for it. if we don't, we assume an all() query
	if qs.Input == nil {
		topn, err := self.Index.TopNAll(qs.Location.FragmentId, qs.N*2, qs.Filters)
		if err != nil {
			log.Warn(spew.Sdump(err))
		}
		topnPackage = TopNPackage{*qs.Location.ProcessId, qs.Location.FragmentId, topn, bh}
	} else {
		input := qs.Input
		value, _ := self.Hold.Get(input, 10)
		//var bh index.BitmapHandle
		switch val := value.(type) {
		case index.BitmapHandle:
			bh = val
		case []byte:
			bh, _ = self.Index.FromBytes(qs.Location.FragmentId, val)
		}

		topn, err := self.Index.TopN(qs.Location.FragmentId, bh, qs.N*2, qs.Filters)
		if err != nil {
			log.Warn(spew.Sdump(err))
		}
		topnPackage = TopNPackage{*qs.Location.ProcessId, qs.Location.FragmentId, topn, bh}
	}

	result_message := db.Message{Data: query.TopNQueryResult{&query.BaseQueryResult{Id: qs.Id, Data: topnPackage}}}
	self.Transport.Send(&result_message, qs.Destination.ProcessId)
}

func canCompile() bool {
	return true
}
