package interfaces

import (
	"pilosa/db"
)

type Transporter interface {
	Init() error
	Close()
	Send(*db.Message)
	Receive() (*db.Message)
}

type Dispatcher interface {
	Init() error
	Close()
	Run()
}
