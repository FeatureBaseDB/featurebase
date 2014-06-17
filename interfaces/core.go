package interfaces

import (
	"pilosa/db"

	"github.com/gocql/gocql/uuid"
)

type Transporter interface {
	Run()
	Close()
	Send(*db.Message, *uuid.UUID)
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
