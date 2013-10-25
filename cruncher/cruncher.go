package cruncher

import (
	"pilosa/core"
	"pilosa/db"
	"log"
)

type Cruncher struct {
	core.Service
}

func (c *Cruncher) Init() {
	log.Println("Initializing cruncher...")
}

func (c *Cruncher) Run() {
	log.Println("Running cruncher...")
	c.SetupEtcd()
	//go r.SyncEtcd()
	go c.WatchEtcd()
	go c.HandleConnections()
	c.SetupNetwork()
	go c.Serve()
	go c.HandleInbox()
	go c.ServeHTTP()

	sigterm, sighup := c.GetSignals()
	for {
		select {
			case <- sighup:
				log.Println("SIGHUP! Reloading configuration...")
				// TODO: reload configuration
			case <- sigterm:
				log.Println("SIGTERM! Cleaning up...")
				c.Stop()
				return
		}
	}
}

func NewCruncher(tcp, http *db.Location) *Cruncher {
	service := core.NewService(tcp, http)
	cruncher := Cruncher{*service}
	return &cruncher
}

func (c *Cruncher) HandleInbox() {
	for {
		select {
		case message := <-c.Inbox:
			log.Println("process", message)
		}
	}
}
