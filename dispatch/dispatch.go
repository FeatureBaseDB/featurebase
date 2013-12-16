package dispatch

import (
	"log"
	"pilosa/core"
)

type CruncherDispatch struct {
	service *core.Service
}

func (self *CruncherDispatch) Init() error {
	log.Println("Starting Dispatcher")
	return nil
}

func (self *CruncherDispatch) Close() {
	log.Println("Shutting down Dispatcher")
}

func (self *CruncherDispatch) Run() {
	for {
		message := self.service.Transport.Receive()
		log.Println("Processing ", message)
	}
}

func NewCruncherDispatch(service *core.Service) *CruncherDispatch {
	return &CruncherDispatch{service}
}
