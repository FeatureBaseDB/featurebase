package core

import (
	//	"github.com/davecgh/go-spew/spew"
	"encoding/gob"
	"log"
	"pilosa/db"
	"pilosa/util"
)

type RemoteSetBit struct {
	requests []util.GUID
	cluster  map[*util.GUID][]BitmapRequestItem
	service  *Service
}

func init() {
	gob.Register(BitmapRequestItem{})
	gob.Register(BitsRequest{})
	gob.Register(BitsResponse{})
}

type BitsRequest struct {
	Bits            []BitmapRequestItem
	ReturnProcessId util.GUID
	QueryId         util.GUID
	DestProcessId   util.GUID
}
type BitmapRequestItem struct {
	Fragment_id util.SUUID
	Bitmap_id   uint64
	Profile_id  uint64
	Filter      uint64
	Frame       string
}

func NewRemoteSetBit(s *Service) *RemoteSetBit {
	obj := new(RemoteSetBit)
	obj.cluster = make(map[*util.GUID][]BitmapRequestItem)
	obj.service = s
	return obj
}

func (self *RemoteSetBit) Request() {
	self.requests = make([]util.GUID, 0)
	source_process, _ := self.service.GetProcess()
	for process, request := range self.cluster {
		random_id := util.RandomUUID()
		msg := new(db.Message)
		msg.Data = BitsRequest{
			Bits:            request,
			ReturnProcessId: source_process.Id(),
			QueryId:         random_id,
			DestProcessId:   *process,
		}
		self.requests = append(self.requests, random_id)
		self.service.Transport.Send(msg, process)
	}

}

func (self *RemoteSetBit) MergeResults(local_results []SBResult) []SBResult {
	answers := make(chan []SBResult)
	for _, task := range self.requests {
		go func(id util.GUID) {
			value, err := self.service.Hold.Get(&id, 10) //eiher need to be the frame process or the handler process?
			if value == nil {
				log.Println("Bad RemoteSetBit Result:", err)
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

func (self *RemoteSetBit) Add(frag *db.Fragment, bitmap_id, profile_id, filter uint64, frame string) {
	x, found := self.cluster[frag.GetProcessId()]
	if !found {
		x = make([]BitmapRequestItem, 0)
	}
	x = append(x, BitmapRequestItem{frag.GetId(), bitmap_id, profile_id, filter, frame})
	self.cluster[frag.GetProcessId()] = x

}

type BitsResponse struct {
	Id    *util.GUID
	Items []SBResult
}

func (self *BitsResponse) ResultId() *util.GUID {
	return self.Id
}
func (self *BitsResponse) ResultData() interface{} {
	return self.Items
}
