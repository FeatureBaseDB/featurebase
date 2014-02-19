package db

import (
	"encoding/gob"

	"tux21b.org/v1/gocql/uuid"
)

type Message struct {
	Data interface{} `json:data`
}

type Envelope struct {
	Message *Message
	Host    *uuid.UUID
}

type HoldResult interface {
	ResultId() *uuid.UUID
	ResultData() interface{}
}

func init() {
	gob.Register(Message{})
}
