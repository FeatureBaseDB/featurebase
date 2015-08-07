package interfaces

import (
	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/util"
)

type Transporter interface {
	Run()
	Close()
	Send(*db.Message, *util.GUID)
	Receive() *db.Message
	Push(*db.Message)
}

type Dispatcher interface {
	Init() error
	Close()
	Run()
}

type Executorer interface {
	Init() error
	Close()
	Run()
	NewJob(*db.Message)
	RunPQL(string, string) (interface{}, error)
}
