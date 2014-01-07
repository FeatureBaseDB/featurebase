package transport

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"pilosa/config"
	"pilosa/core"
	"pilosa/db"
	"time"

	"tux21b.org/v1/gocql/uuid"
)

type qmessage struct {
	message *db.Message
	host    *uuid.UUID
}

type TcpTransport struct {
	service *core.Service
	port    int
	inbox   chan *db.Message
	outbox  chan qmessage
	stop    chan bool
}

func (self *TcpTransport) Run() {
	log.Println("Initializing TCP transport")
	go self.listen()
	for {
		select {
		case qmessage := <-self.outbox:
			// todo: persistent connections
			process, err := self.service.ProcessMap.GetProcess(qmessage.host)
			if err != nil {
				log.Println("transport/tcp", err)
				return
			}
			host_string := fmt.Sprintf("%s:%d", process.Host(), process.PortTcp())
			conn, err := net.Dial("tcp", host_string)
			encoder := gob.NewEncoder(conn)
			err = encoder.Encode(qmessage.message)
			if err != nil {
				log.Println(err.Error())
				return
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func (self *TcpTransport) listen() {
	port_string := fmt.Sprintf(":%d", self.port)
	l, e := net.Listen("tcp", port_string)
	if e != nil {
		log.Fatal("Cannot bind to port!", self.port)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("Cannot accept message!")
		}
		go self.manage(&conn)
	}
}

func (self *TcpTransport) manage(conn *net.Conn) {
	decoder := gob.NewDecoder(*conn)
	var mess *db.Message
	err := decoder.Decode(&mess)
	if err != nil {
		log.Println("tcp/transport", err.Error())
		return
	}
	self.inbox <- mess
}

func (self *TcpTransport) Close() {
	log.Println("Shutting down TCP transport")
}

func (self *TcpTransport) Send(message *db.Message, host *uuid.UUID) {
	self.outbox <- qmessage{message, host}
}

func (self *TcpTransport) Receive() *db.Message {
	return <-self.inbox
}

func (self *TcpTransport) Push(message *db.Message) {
	self.inbox <- message
}

func NewTcpTransport(service *core.Service) *TcpTransport {
	return &TcpTransport{service, config.GetInt("port_tcp"), make(chan *db.Message), make(chan qmessage), nil}
}
