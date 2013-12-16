package cruncher

import (
	"github.com/davecgh/go-spew/spew"
	"pilosa/core"
	"pilosa/dispatch"
	"pilosa/index"
	"pilosa/transport"
)

type Cruncher struct {
	core.Service
	close_chan chan bool
	api        *index.FragmentContainer
}

func (cruncher *Cruncher) Run(port int) {
	spew.Dump("Cruncher.Run")
	spew.Dump(port)

	cruncher.api = index.NewFragmentContainer()
	/*
	   bh = api.Get(frag,tileid)
	   api.SetBit(frag,bh,1)
	   api.Count(frag,bh)
	   api.Union(frag,[bh1,bh2])
	   api.Intersect(frag,[bh1,bh2])
	*/

	cruncher.Service.Run()
}

func NewCruncher() *Cruncher {
	service := core.NewService()
	fragment_container := index.NewFragmentContainer()
	cruncher := Cruncher{*service, make(chan bool), fragment_container}
	cruncher.Transport = transport.NewTcpTransport(service)
	cruncher.Dispatch = dispatch.NewCruncherDispatch(service)
	return &cruncher
}
