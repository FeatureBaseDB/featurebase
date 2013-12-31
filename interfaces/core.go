package interfaces

import (
	"pilosa/db"
	"pilosa/query"
)

type Transporter interface {
	Run()
	Close()
	Send(*db.Message)
	Receive() *db.Message
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
	NewJob(*query.QueryPlan, chan *query.QueryResults)
}
