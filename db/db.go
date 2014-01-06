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

type HoldResult interface {
	ResultId() *uuid.UUID
}

func init() {
	gob.Register(Message{})
}
