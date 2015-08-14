package core

import (
	"encoding/gob"
	"time"

	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/util"
)

type PingRequest struct {
	Id     *util.GUID
	Source *util.GUID
}

type PongRequest struct {
	Id *util.GUID
}

func (self PongRequest) ResultId() *util.GUID {
	return self.Id
}
func (self PongRequest) ResultData() interface{} {
	return self.Id
}

func init() {
	gob.Register(PingRequest{})
	gob.Register(PongRequest{})
}

type Pinger struct {
	ID util.GUID

	Hold interface {
		Get(id *util.GUID, timeout int) (interface{}, error)
	}

	Transport interface {
		Send(message *db.Message, host *util.GUID)
	}
}

func NewPinger(id util.GUID) *Pinger {
	return &Pinger{
		ID: id,
	}
}

func (self *Pinger) Ping(process_id *util.GUID) (*time.Duration, error) {
	id := util.RandomUUID()
	ping := db.Message{Data: PingRequest{Id: &id, Source: &self.ID}}
	start := time.Now()
	self.Transport.Send(&ping, process_id)
	_, err := self.Hold.Get(&id, 60)
	if err != nil {
		return nil, err
	}
	end := time.Now()
	dur := end.Sub(start)
	return &dur, nil
}
