package core

import (
	"encoding/gob"

	log "github.com/cihub/seelog"
	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/db"
)

type RemoteSetBit struct {
	requests []remote_task
	cluster  map[*pilosa.GUID][]BitmapRequestItem

	ID         pilosa.GUID
	ProcessMap *ProcessMap

	Hold interface {
		Get(id *pilosa.GUID, timeout int) (interface{}, error)
	}

	Transport interface {
		Send(message *db.Message, host *pilosa.GUID)
	}
}

func init() {
	gob.Register(BitmapRequestItem{})
	gob.Register(BitsRequest{})
	gob.Register(BitsResponse{})
}

type BitsRequest struct {
	Bits            []BitmapRequestItem
	ReturnProcessId pilosa.GUID
	QueryId         pilosa.GUID
	DestProcessId   pilosa.GUID
}
type BitmapRequestItem struct {
	Fragment_id pilosa.SUUID
	Bitmap_id   uint64
	Profile_id  uint64
	Filter      uint64
	Frame       string
	SetUnset    bool
}

func NewRemoteSetBit() *RemoteSetBit {
	obj := new(RemoteSetBit)
	obj.cluster = make(map[*pilosa.GUID][]BitmapRequestItem)
	return obj
}

func (self *RemoteSetBit) Request() {
	self.requests = make([]remote_task, 0)
	source_process, _ := self.ProcessMap.GetProcess(&self.ID)
	for process, request := range self.cluster {
		random_id := pilosa.NewGUID()
		msg := new(db.Message)
		msg.Data = BitsRequest{
			Bits:            request,
			ReturnProcessId: source_process.Id(),
			QueryId:         random_id,
			DestProcessId:   *process,
		}
		wait := len(request) * 10
		if wait < 10 {
			wait = 10
		}
		self.requests = append(self.requests, remote_task{random_id, wait})
		self.Transport.Send(msg, process)
	}

}

type remote_task struct {
	id        pilosa.GUID
	wait_time int
}

func (self *RemoteSetBit) MergeResults(local_results []SBResult) []SBResult {
	answers := make(chan []SBResult)
	for _, task := range self.requests {
		go func(task remote_task) {
			value, err := self.Hold.Get(&task.id, task.wait_time) //eiher need to be the frame process or the handler process?
			if value == nil {
				log.Warn("Bad RemoteSetBit Result:", err)
				empty := make([]SBResult, 0, 0)
				answers <- empty

			} else {
				answers <- value.([]SBResult)
			}
		}(task)
	}
	for i := 0; i < len(self.requests); i++ {
		batch := <-answers
		for _, item := range batch {
			local_results = append(local_results, item)
		}
	}
	close(answers)
	return local_results

}

func (self *RemoteSetBit) Add(frag *db.Fragment, bitmap_id, profile_id, filter uint64, frame string, SetUnset bool) {
	x, found := self.cluster[frag.GetProcessId()]
	if !found {
		x = make([]BitmapRequestItem, 0)
	}
	x = append(x, BitmapRequestItem{frag.GetId(), bitmap_id, profile_id, filter, frame, SetUnset})
	self.cluster[frag.GetProcessId()] = x

}

type BitsResponse struct {
	Id    *pilosa.GUID
	Items []SBResult
}

func (self *BitsResponse) ResultId() *pilosa.GUID {
	return self.Id
}
func (self *BitsResponse) ResultData() interface{} {
	return self.Items
}
