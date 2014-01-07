package interfaces

import "pilosa/db"

type Transporter interface {
	Run()
	Close()
	Send(*db.Message)
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
