package dispatch

import (
	"log"
	"pilosa/core"
	"pilosa/db"
	"pilosa/query"

	"github.com/davecgh/go-spew/spew"
)

type Dispatch struct {
	service *core.Service
}

func (self *Dispatch) Init() error {
	log.Println("Starting Dispatcher")
	return nil
}

func (self *Dispatch) Close() {
	log.Println("Shutting down Dispatcher")
}

func (self *Dispatch) Run() {
	log.Println("Dispatch Run...")
	for {
		message := self.service.Transport.Receive()
		switch data := message.Data.(type) {
		case core.PingRequest:
			pong := db.Message{Data: core.PongRequest{Id: data.Id}}
			self.service.Transport.Send(&pong, data.Source)
		case db.HoldResult:
			spew.Dump("HOLD-SET", data.ResultId())
			self.service.Hold.Set(data.ResultId(), data.ResultData(), 30)
		case query.PortableQueryStep:
			go self.service.Executor.NewJob(message)
		default:
			log.Println("Unprocessed message", data)
		}
	}
}

func NewDispatch(service *core.Service) *Dispatch {
	return &Dispatch{service}
}
