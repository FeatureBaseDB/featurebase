package db

import (
	"encoding/gob"

	. "github.com/umbel/pilosa/util"
)

type Message struct {
	Data interface{} `json:"data"`
}

type Envelope struct {
	Message *Message
	Host    *GUID
}

type HoldResult interface {
	ResultId() *GUID
	ResultData() interface{}
}

func init() {
	gob.Register(Message{})
}
