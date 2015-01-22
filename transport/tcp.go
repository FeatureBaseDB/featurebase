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
	"sync"

	"time"

	notify "github.com/bitly/go-notify"
)

type connection struct {
	inbox      chan *db.Message
	outbox     chan *db.Message
	conn       net.Conn
	encoder    *gob.Encoder
	decoder    *gob.Decoder
	transport  *TcpTransport
	process_id *GUID
	exit       chan int
}

func init() {
	gob.Register(GUID{})
}

func newConnection(conn net.Conn, enc *gob.Encoder, dec *gob.Decoder, t *TcpTransport, g *GUID) *connection {
	p := new(connection)
	p.outbox = make(chan *db.Message, 2048)
	p.inbox = make(chan *db.Message, 2048)
	p.conn = conn
	p.encoder = enc
	p.decoder = dec
	p.transport = t
	p.process_id = g
	p.exit = make(chan int)
	return p
}

func (self *connection) Close() {
	close(self.outbox)
	close(self.inbox)
}

func (self *connection) run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			var mess *db.Message
			err := self.decoder.Decode(&mess)
			if err != nil {
				log.Println("transport/tcp: error decoding message: ", err.Error())
				time.Sleep(2 * time.Second)
				self.exit <- 1
				wg.Done()
				return
			} else {
				self.inbox <- mess
			}
		}
	}()
	breakout := true
	for breakout {
		select {
		case message := <-self.outbox:
			err := self.encoder.Encode(message)
			if err != nil {
				log.Println("Failed to Send on Socket Transport")
				breakout = false
			}
		case message := <-self.inbox:
			self.transport.inbox <- message
		case <-self.exit:
			breakout = false
		}
	}
	self.conn.Close()
	wg.Wait()
	self.transport.removeConnection(self)
}

type TcpTransport struct {
	service     *core.Service
	port        int
	inbox       chan *db.Message
	outbox      chan db.Envelope
	connections map[*GUID]*connection
	mutex       sync.Mutex
	enc         *gob.Encoder
	dec         *gob.Decoder
}

func NewTcpTransport(service *core.Service) *TcpTransport {
	p := new(TcpTransport)
	p.service = service
	p.port = config.GetInt("port_tcp")
	p.inbox = make(chan *db.Message, 2048)
	p.outbox = make(chan db.Envelope, 2048)
	p.connections = make(map[*GUID]*connection)
	var network bytes.Buffer         // Stand-in for a network connection
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
			con, need := self.getConnection(env.Host)
			if need {
				con = self.connectRemotePeer(env.Host)
			}
			if con != nil {
				con.outbox <- env.Message
			}
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
		self.addPeer(conn)
	}
}
func (self *TcpTransport) connectRemotePeer(remoteProcessId *GUID) *connection {
	process, err := self.service.ProcessMap.GetProcess(remoteProcessId)
	host_string := fmt.Sprintf("%s:%d", process.Host(), process.PortTcp())
	conn, err := net.Dial("tcp", host_string)
	if err != nil {
		log.Println("transport/tcp: error dialing: ", host_string, " Retrying in 2 seconds...")
		return nil
	}
	encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	err = encoder.Encode(self.service.Id) //send registration
	if err != nil {
		log.Println("Error sending processid:", host_string)
	}
	acon := newConnection(conn, encoder, decoder, self, remoteProcessId)
	self.addConnection(acon)
	return acon

}

func (self *TcpTransport) addPeer(conn net.Conn) {
	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)
	var processid GUID
	err := decoder.Decode(&processid)
	if err != nil {
		log.Println("Failed to recieve remote processid")
		return
	}
	acon := newConnection(conn, encoder, decoder, self, &processid)
	self.addConnection(acon)
}
func (self *TcpTransport) getConnection(guid *GUID) (*connection, bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	con, ok := self.connections[guid]
	return con, !ok
}

func (self *TcpTransport) addConnection(c *connection) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.connections[c.process_id] = c
	go c.run()
}
func (self *TcpTransport) removeConnection(c *connection) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	delete(self.connections, c.process_id)
	c.Close()

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
