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

/*
func (self *TcpTransport) RunServer(porti int) {
	http.Handle("/", self)
	port := fmt.Sprintf(":%d", porti)

	s := &http.Server{
		Addr:           port,
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	l, e := net.Listen("tcp", port)
	if e != nil {
		log.Panicf(e.Error())
	}
	self.stop = make(chan bool)
	go s.Serve(l)
	select {
	case <-self.stop:
		log.Printf("Server thread exit")
		l.Close()
		// Shutdown()
		return
		break
	}
}

func (self *TcpTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//fmt.Fprintf(w, "URL Path: %s\n", r.URL.Path[1:])
	path := r.URL.Path[1:]

	msg := new(db.Message)
	msg.Key = "path"
	msg.Data = path
	self.inbox <- msg
}
*/

func (self *TcpTransport) Run() {
	log.Println("Initializing TCP transport")
	//self.RunServer(self.port)
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
