package transport

import (
	"log"

	"github.com/umbel/pilosa/db"
)

type HttpTransport struct {
	port   int
	outbox chan *db.Message
	done   chan int
}

func (trans *HttpTransport) Init() error {
	log.Println("Bind to port", trans.port)
	trans.done = make(chan int)
	go trans.Loop()
	return nil
}

func (trans *HttpTransport) Loop() {
	var message *db.Message
	for {
		select {
		case message = <-trans.outbox:
			log.Println(message)
		case <-trans.done:
			return
		}
	}
}

func (trans *HttpTransport) Close() {
	log.Println("Closing HTTP transport.")
	trans.done <- 1
}

func (trans *HttpTransport) Send(node string, message *db.Message) error {
	log.Println("Send", message, "to", node)
	trans.outbox <- message
	return nil
}

func (trans *HttpTransport) Receive() (*db.Message, error) {
	return &db.Message{}, nil
}

func NewHttpTransport(port int) *HttpTransport {
	trans := new(HttpTransport)
	trans.port = port
	trans.outbox = make(chan *db.Message, 10)
	return trans
}
