package dispatch

import (
	log "github.com/cihub/seelog"
	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa/core"
	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/query"
)

type Dispatch struct {
	service *core.Service
}

func (self *Dispatch) Init() error {
	log.Warn("Starting Dispatcher")
	return nil
}

func (self *Dispatch) Close() {
	log.Warn("Shutting down Dispatcher")
}

// The Local Route

func (self *Dispatch) Run() {
	log.Warn("Dispatch Run...")
	for {
		message := self.service.Transport.Receive()
		switch data := message.Data.(type) {
		case core.BatchRequest:
			log.Trace("Dispatch.Run BatchRequest")
			response := db.Message{Data: core.BatchResponse{Id: data.Id}}
			self.service.Index.LoadBitmap(data.Fragment_id, data.Bitmap_id, data.Compressed_bitmap, data.Filter)
			self.service.Transport.Send(&response, data.Source)
		case core.BitsRequest:
			log.Trace("Dispatch.Run BitsRequest")
			var results []core.SBResult
			result := false

			for _, v := range data.Bits {
				if v.SetUnset {
					result, _ = self.service.Index.SetBit(v.Fragment_id, v.Bitmap_id, v.Profile_id, uint64(v.Filter))
				} else {
					result, _ = self.service.Index.ClearBit(v.Fragment_id, v.Bitmap_id, v.Profile_id)
				}
				bundle := core.SBResult{v.Bitmap_id, v.Frame, v.Filter, v.Profile_id, result}
				results = append(results, bundle)
			}
			response := db.Message{Data: core.BitsResponse{Id: &data.QueryId, Items: results}}
			self.service.Transport.Send(&response, &data.ReturnProcessId)
		case core.PingRequest:
			log.Trace("Dispatch.Run Ping")
			pong := db.Message{Data: core.PongRequest{Id: data.Id}}
			self.service.Transport.Send(&pong, data.Source)
		case db.HoldResult:
			log.Trace("Dispatch.Run HoldResult")
			self.service.Hold.Set(data.ResultId(), data.ResultData(), 30)
		case query.PortableQueryStep:
			log.Trace("Dispatch.Run PortableQueryStep")
			go self.service.Executor.NewJob(message)
		case core.TopFill:
			log.Trace("Dispatch.Run TopFill")
			go self.service.TopFillHandler(message)
		case core.BitsResponse:
			self.service.Hold.Set(data.ResultId(), data.ResultData(), 30)
		default:
			spew.Dump(data)
			log.Warn("Unprocessed message", data)
		}
	}
}

func NewDispatch(service *core.Service) *Dispatch {
	return &Dispatch{service}
}
