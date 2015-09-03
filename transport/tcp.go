package transport

import (
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"time"

	notify "github.com/bitly/go-notify"
	log "github.com/cihub/seelog"
	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/core"
	"github.com/umbel/pilosa/db"
)

const DefaultTCPPort = 12001

type connection struct {
	transport *TcpTransport
	inbox     chan *db.Message
	outbox    chan *db.Message
	conn      *net.Conn
	process   *pilosa.GUID
}

type newconnection struct {
	id         *pilosa.GUID
	connection *connection
}

func init() {
	gob.Register(pilosa.GUID{})
}

func (self *connection) manage() {
BeginManageConnection:
	for {
		if self.conn == nil {
			process, err := self.transport.ProcessMap.GetProcess(self.process)
			if err != nil {
				log.Warn("transport/tcp: error getting process, retrying in 2 seconds... ", self.process, err)
				time.Sleep(2 * time.Second)
				continue
			}
			host_string := fmt.Sprintf("%s:%d", process.Host(), process.PortTcp())
			conn, err := net.Dial("tcp", host_string)
			if err != nil {
				log.Warn("transport/tcp: error dialing: ", host_string, " Retrying in 2 seconds...")
				time.Sleep(2 * time.Second)
				continue
			}
			self.conn = &conn
			go func() {
				self.outbox <- &db.Message{Data: self.transport.ID.String()}
			}()
		}
		encoder := gob.NewEncoder(*self.conn)
		decoder := gob.NewDecoder(*self.conn)
		var exit = make(chan int)
		go func() {
			for {
				var mess *db.Message
				err := decoder.Decode(&mess)
				if err != nil {
					log.Warn("transport/tcp: error decoding message: ", err.Error())
					exit <- 1
					return
				}
				self.inbox <- mess
			}
		}()
		for {
			select {
			case message := <-self.outbox:
				err := encoder.Encode(message)
				if err != nil {
					log.Warn(err.Error())
					return
				}
			case message := <-self.inbox:
				identifier, ok := message.Data.(pilosa.GUID)
				if ok {
					// message is connection registration; bypass inbox and register
					self.process = &identifier
					self.transport.reg <- &newconnection{&identifier, self}
				} else {
					self.transport.inbox <- message
				}
			case <-exit:
				if self.process != nil {
					self.conn = nil
					continue BeginManageConnection
				} else {
					return
				}
			}
		}
	}
}

type TcpTransport struct {
	inbox       chan *db.Message
	outbox      chan db.Envelope
	connections map[pilosa.GUID]*connection
	reg         chan *newconnection

	ID         pilosa.GUID
	Port       int
	ProcessMap *core.ProcessMap
}

func NewTcpTransport(id pilosa.GUID) *TcpTransport {
	return &TcpTransport{
		inbox:       make(chan *db.Message, 100),
		outbox:      make(chan db.Envelope, 100),
		connections: make(map[pilosa.GUID]*connection),
		reg:         make(chan *newconnection),

		ID:   id,
		Port: DefaultTCPPort,
	}
}

func (self *TcpTransport) Run() {
	log.Warn("Initializing TCP transport")
	go self.listen()
	for {
		select {
		case env := <-self.outbox:
			con, ok := self.connections[*(env.Host)]
			if !ok {
				con = &connection{self, make(chan *db.Message, 100), make(chan *db.Message, 100), nil, env.Host}
				go con.manage()
				self.connections[*env.Host] = con
			}
			con.outbox <- env.Message
		case nc := <-self.reg:
			self.connections[*nc.id] = nc.connection
		}
	}
}

func (self *TcpTransport) listen() {
	port_string := fmt.Sprintf(":%d", self.Port)
	l, e := net.Listen("tcp", port_string)
	if e != nil {
		log.Critical("Cannot bind to port! ", self.Port)
		os.Exit(-1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Warn("Error accepting, trying again in 2 sec... ", err)
			time.Sleep(2 * time.Second)
			continue
		}
		go self.manage(&conn)
	}
}

func (self *TcpTransport) manage(conn *net.Conn) {
	con := &connection{self, make(chan *db.Message, 1024), make(chan *db.Message, 1024), conn, nil}
	con.manage()
}

func (self *TcpTransport) Close() {
	log.Warn("Shutting down TCP transport")
}

func (self *TcpTransport) Send(message *db.Message, host *pilosa.GUID) {
	log.Trace("TcpTransport.Send", message, host)
	envelope := db.Envelope{Message: message, Host: host}
	notify.Post("outbox", &envelope)
	self.outbox <- envelope
}

func (self *TcpTransport) Receive() *db.Message {
	log.Trace("TcpTransport.Receive")
	message := <-self.inbox
	notify.Post("inbox", message)
	return message
}

func (self *TcpTransport) Push(message *db.Message) {
	self.inbox <- message
}
