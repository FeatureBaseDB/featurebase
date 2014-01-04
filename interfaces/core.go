package interfaces

import (
	"pilosa/db"
	"pilosa/query"

	"github.com/nu7hatch/gouuid"
)

type Transporter interface {
	Run()
	Close()
	Send(*db.Message, *uuid.UUID)
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
