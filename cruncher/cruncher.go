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
	//	api        *index.FragmentContainer
}

func (cruncher *Cruncher) Run() {
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
