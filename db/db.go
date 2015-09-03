package db

import (
	"encoding/gob"

	"github.com/umbel/pilosa"
)

type Message struct {
	Data interface{} `json:"data"`
}

type Envelope struct {
	Message *Message
	Host    *pilosa.GUID
}

type HoldResult interface {
	ResultId() *pilosa.GUID
	ResultData() interface{}
}

func init() {
	gob.Register(Message{})
}
