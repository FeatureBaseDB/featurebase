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
	//NewJob(*query.QueryPlan, chan *query.QueryResults)
	NewJob(*db.Message)
	NewQS(*query.QueryStep)
	RunQuery(string, string)
}
