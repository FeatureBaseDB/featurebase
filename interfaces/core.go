package interfaces

import (
	"pilosa/db"

	"tux21b.org/v1/gocql/uuid"
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
	RunQuery(string, string)
}
