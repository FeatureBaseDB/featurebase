package cruncher

import (
	"github.com/umbel/pilosa/core"
	"github.com/umbel/pilosa/dispatch"
	"github.com/umbel/pilosa/executor"
	"github.com/umbel/pilosa/transport"
)

type Cruncher struct {
	*core.Service
	close_chan chan bool
}

func (cruncher *Cruncher) Run() {
	cruncher.Service.Run()
}

func NewCruncher() *Cruncher {
	service := core.NewService()
	cruncher := Cruncher{service, make(chan bool)}
	cruncher.Transport = transport.NewTcpTransport(service)
	cruncher.Dispatch = dispatch.NewDispatch(service)
	cruncher.Executor = executor.NewExecutor(service)
	return &cruncher
}
