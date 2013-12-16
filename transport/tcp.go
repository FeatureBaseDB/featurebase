package transport

import (
	"log"
	"pilosa/core"
	"pilosa/db"
)

type TcpTransport struct {
	port  int
	inbox chan *db.Message
}

func (self *TcpTransport) Init() error {
	log.Println("Initializing TCP transport")
	return nil
}

func (self *TcpTransport) Close() {
	log.Println("Shutting down TCP transport")
}

func (self *TcpTransport) Send(message *db.Message) {
	log.Println("Send", message)
}

func (self *TcpTransport) Receive() *db.Message {
	return <-self.inbox
}

func NewTcpTransport(service *core.Service) *TcpTransport {
	return &TcpTransport{12000, make(chan *db.Message)} //TODO: make port configurable
}
