package cruncher

import (
	"pilosa/core"
	"pilosa/dispatch"
	"pilosa/executor"
	"pilosa/index"
	"pilosa/transport"

	"github.com/davecgh/go-spew/spew"
)

type Cruncher struct {
	*core.Service
	close_chan chan bool
	api        *index.FragmentContainer
}

func (cruncher *Cruncher) Run() {
	spew.Dump("Cruncher.Run")
	cruncher.Service.Run()
}

func NewCruncher() *Cruncher {
	service := core.NewService()
	fragment_container := index.NewFragmentContainer()
	cruncher := Cruncher{service, make(chan bool), fragment_container}
	cruncher.Transport = transport.NewTcpTransport(service)
	cruncher.Dispatch = dispatch.NewDispatch(service)
	cruncher.Executor = executor.NewExecutor(service)
	return &cruncher
}
