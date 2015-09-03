package core

import (
	"encoding/gob"
	"time"

	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/db"
)

type PingRequest struct {
	Id     *pilosa.GUID
	Source *pilosa.GUID
}

type PongRequest struct {
	Id *pilosa.GUID
}

func (self PongRequest) ResultId() *pilosa.GUID {
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
	ID pilosa.GUID

	Hold interface {
		Get(id *pilosa.GUID, timeout time.Duration) (interface{}, error)
	}

	Transport interface {
		Send(message *db.Message, host *pilosa.GUID)
	}
}

func NewPinger(id pilosa.GUID) *Pinger {
	return &Pinger{
		ID: id,
	}
}

func (self *Pinger) Ping(process_id *pilosa.GUID) (*time.Duration, error) {
	id := pilosa.RandomUUID()
	ping := db.Message{Data: PingRequest{Id: &id, Source: &self.ID}}
	start := time.Now()
	self.Transport.Send(&ping, process_id)
	_, err := self.Hold.Get(&id, 60*time.Second)
	if err != nil {
		return nil, err
	}
	end := time.Now()
	dur := end.Sub(start)
	return &dur, nil
}
