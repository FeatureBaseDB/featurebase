package cruncher

import (
    "github.com/davecgh/go-spew/spew"
    "pilosa/index"
	"pilosa/core"
)


type Cruncher struct {
	core.Service
    close_chan chan bool
}

func (cruncher *Cruncher) Run(port int) {
	spew.Dump("Cruncher.Run")
	spew.Dump(port)
	web_api:= index.NewFragmentContainer()
	started:= make(chan bool)
	go web_api.RunServer(port , cruncher.close_chan ,started ) 
	cruncher.Service.Run()
	<-started
}

func NewCruncher() *Cruncher {
	service := core.NewService()
	cruncher := Cruncher{*service, make(chan bool)}
	return &cruncher
}
