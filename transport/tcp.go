package transport

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"pilosa/config"
	"pilosa/core"
	"pilosa/db"
	. "pilosa/util"
	"runtime/debug"
	"sync"

	"time"

	notify "github.com/bitly/go-notify"
)

type connection struct {
	transport *TcpTransport
	inbox     chan *db.Message
	outbox    chan *db.Message
	conn      net.Conn
	process   *GUID
	id        int
	terminate bool
	exit      chan int
}

type newconnection struct {
	id         *GUID
	connection *connection
}

func init() {
	gob.Register(GUID{})
}

func newConnection(transport *TcpTransport, conn net.Conn, proc *GUID) *connection {
	println("New Connection", proc)
	p := new(connection)
	p.transport = transport
	p.outbox = make(chan *db.Message, 2048)
	p.inbox = make(chan *db.Message, 2048)
	p.conn = conn
	p.process = proc
	p.exit = make(chan int)
	return p
}

func (self *connection) manage() {
	for {
		self.exit = make(chan int)
		self.serviceConnection()
		close(self.exit)
		time.Sleep(2 * time.Second)
		self.conn = nil
		if self.terminate {
			break
		}

	}
}

func (self *connection) Shutdown() {
	self.terminate = true
	if self.conn != nil {
		self.conn.Close()
	}

}
func (self *connection) serviceConnection() {
	var host_string string
	if self.conn == nil {
		println("Service Connection", self.process)
		process, err := self.transport.service.ProcessMap.GetProcess(self.process)
		if err != nil {
			log.Println("transport/tcp: error getting process, retrying in 2 seconds... ", self.process, err)
			return
		}
		host_string = fmt.Sprintf("%s:%d", process.Host(), process.PortTcp())
		log.Println("Connecting:", host_string)
		conn, err := net.Dial("tcp", host_string)
		if err != nil {
			log.Println("transport/tcp: error dialing: ", host_string, " Retrying in 2 seconds...")
			return
		}
		self.conn = conn
		go func() {
			//register on server
			self.outbox <- &db.Message{self.transport.service.Id}
		}()
	}
	encoder := gob.NewEncoder(self.conn)
	decoder := gob.NewDecoder(self.conn)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			var mess *db.Message
			err := decoder.Decode(&mess)
			if err != nil {
				log.Println("transport/tcp: error decoding message: ", host_string, err.Error())
				self.exit <- 1
				wg.Done()
				return
			}
			self.inbox <- mess
		}
	}()
	breakout := true
	for breakout {
		select {
		case message := <-self.outbox:
			err := encoder.Encode(message)
			if err != nil {
				log.Println("Sending to ", host_string, err.Error())
				breakout = false
			}
		case message := <-self.inbox:
			identifier, ok := message.Data.(GUID)
			if ok {
				// message is connection registration; bypass inbox and register
				self.process = &identifier
				self.transport.reg <- &newconnection{&identifier, self}
			} else {
				self.transport.inbox <- message
			}
		case <-self.exit:
			breakout = false
		}
	}
	self.conn.Close()
	wg.Wait()
}

type TcpTransport struct {
	service     *core.Service
	port        int
	inbox       chan *db.Message
	outbox      chan db.Envelope
	connections map[GUID]*connection
	reg         chan *newconnection
	enc         *gob.Encoder
	dec         *gob.Decoder
}

func NewTcpTransport(service *core.Service) *TcpTransport {
	p := new(TcpTransport)
	var network bytes.Buffer // Stand-in for a network connection
	p.service = service
	p.port = config.GetInt("port_tcp")
	p.inbox = make(chan *db.Message, 2048)
	p.outbox = make(chan db.Envelope, 2048)
	p.connections = make(map[GUID]*connection)
	p.reg = make(chan *newconnection)
	p.enc = gob.NewEncoder(&network) // Will write to network.
	p.dec = gob.NewDecoder(&network) // Will read from network.
	return p
}

func (self *TcpTransport) Run() {
	log.Println("Initializing TCP transport")
	go self.listen()
	for {
		select {
		case env := <-self.outbox: //transport outbox
			con, ok := self.connections[*(env.Host)]
			if !ok {
				con = newConnection(self, nil, env.Host)
				go con.manage()
				self.connections[*env.Host] = con
			}
			con.outbox <- env.Message
		case nc := <-self.reg:
			before, present := self.connections[*nc.id]
			if present {
				before.Shutdown()
			}
			self.connections[*nc.id] = nc.connection
		}
	}
}

func (self *TcpTransport) listen() {
	port_string := fmt.Sprintf(":%d", self.port)
	l, e := net.Listen("tcp", port_string)
	if e != nil {
		log.Fatal("Cannot bind to port! ", self.port)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting, trying again in 2 sec... ", err)
			time.Sleep(2 * time.Second)
			continue
		}
		go self.manage(conn)
	}
}

func (self *TcpTransport) manage(c net.Conn) {
	con := newConnection(self, c, nil)
	con.manage()
}

func (self *TcpTransport) Close() {
	log.Println("Shutting down TCP transport")
}
func (self *TcpTransport) adjust(in *db.Message) *db.Message {
	err := self.enc.Encode(in)
	var out db.Message
	err = self.dec.Decode(&out)
	if err != nil {
		log.Println(err)
	}
	return &out

}

func (self *TcpTransport) Send(message *db.Message, host *GUID) {
	if Equal(host, self.service.Id) {
		self.inbox <- self.adjust(message)
	} else {
		envelope := db.Envelope{message, host}
		notify.Post("outbox", &envelope)
		self.outbox <- envelope
	}
}

func (self *TcpTransport) Receive() *db.Message {
	message := <-self.inbox
	notify.Post("inbox", message)
	return message
}

func (self *TcpTransport) Push(message *db.Message) {
	self.inbox <- message
}
