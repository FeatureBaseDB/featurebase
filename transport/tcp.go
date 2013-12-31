package transport

import (
	"log"
	"pilosa/config"
	"pilosa/core"
	"pilosa/db"
)

type TcpTransport struct {
	port  int
	inbox chan *db.Message
	stop  chan bool
}

func (self *TcpTransport) Run() {
	log.Println("Initializing TCP transport")
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
	return &TcpTransport{config.GetInt("port_tcp"), make(chan *db.Message), nil}
}
