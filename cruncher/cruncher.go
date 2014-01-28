package cruncher

import (
	"pilosa/core"
	"pilosa/dispatch"
	"pilosa/executor"
	"pilosa/transport"

	"github.com/davecgh/go-spew/spew"
)

type Cruncher struct {
	*core.Service
	close_chan chan bool
	//	api        *index.FragmentContainer
}

func (cruncher *Cruncher) Run() {
	spew.Dump("Cruncher.Run")
	//	go cruncher.Cleanup()
	cruncher.Service.Run()
}

/*
func (cruncher *Cruncher) Cleanup() {
	exit, done := cruncher.Service.GetExitChannels()
	for {
		select {
		case <-exit:
			spew.Dump(cruncher)
			cruncher.api.Shutdown()
			done <- 1
		}
	}
}
*/

func NewCruncher() *Cruncher {
	service := core.NewService()
	//fragment_container := index.NewFragmentContainer()
	cruncher := Cruncher{service, make(chan bool)}
	cruncher.Transport = transport.NewTcpTransport(service)
	cruncher.Dispatch = dispatch.NewDispatch(service)
	cruncher.Executor = executor.NewExecutor(service)
	return &cruncher
}
