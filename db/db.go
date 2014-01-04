package db

import (
	"encoding/gob"

	"github.com/nu7hatch/gouuid"
)

type Message struct {
	Key         string      `json:key`
	Data        interface{} `json:data`
	Destination Location
}

type PingRequest struct {
	Id     *uuid.UUID
	Source *uuid.UUID
}

type PongRequest struct {
	Id *uuid.UUID
}

func init() {
	gob.Register(Message{})
	gob.Register(PingRequest{})
	gob.Register(PongRequest{})
}
