package db

import (
	"encoding/gob"

	"tux21b.org/v1/gocql/uuid"
)

type Message struct {
	Data interface{} `json:data`
}

/*
import "pilosa/core"

type Message interface {
	Handle(*core.Service)
}


type Message struct {
	Key         string      `json:key`
	Data        interface{} `json:data`
	Destination Location
}
*/

type HoldResult interface {
	ResultId() *uuid.UUID
}

func init() {
	gob.Register(Message{})
}
