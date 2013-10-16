package core

import (
	"github.com/coreos/go-etcd/etcd"
	"github.com/coreos/etcd/store"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"strings"
	"sync"
	"net"
	"encoding/gob"
	//"net"
	//"flag"
	//"encoding/gob"
	//"net/http"
	//"encoding/json"
	//"io"
)
type Message struct {
	Key string `json:key`
	Data interface{} `json:data`
	Destination Location
}

type Envelope struct {
	Message *Message
	Location *Location
}

type Connection struct {
	Conn net.Conn
	Encoder *gob.Encoder
	Decoder *gob.Decoder
}

type Stopper struct {
	TermChans []chan int
	DoneChans []chan int
	Mutex sync.RWMutex
}

func (stopper *Stopper) Stop() {
	var i chan int
	var o chan int
	stopper.Mutex.RLock()
	for _, i = range stopper.TermChans {
		go func() {
			i <- 1
		}()
	}
	for _, o = range stopper.DoneChans {
		<-o
	}
	stopper.Mutex.RUnlock()
	return
}

func (stopper *Stopper) GetExitChannels() (chan int, chan int) {
	termchan := make(chan int, 1)
	donechan := make(chan int, 1)
	stopper.Mutex.Lock()
	stopper.TermChans = append(stopper.TermChans, termchan)
	stopper.DoneChans = append(stopper.DoneChans, donechan)
	stopper.Mutex.Unlock()
	return termchan, donechan
}

type Service struct {
	Stopper
	Handler func(Message)
	Mailbox chan Message
	Inbox chan *Message
	Port string
	PortHttp string
	Etcd *etcd.Client
	Location *Location
	NodeMap NodeMap
	NodeMapMutex sync.RWMutex
	Outbox chan *Envelope
	ConnectionMap map[Location]*Connection
	ConnectionMapMutex sync.RWMutex
	Listener net.Listener
	ConnectionRegisterChannel chan *PersistentConnection
	//Cluster query.Cluster
}

func NewService(location *Location) *Service {
	service := new(Service)
	service.Location = location
	service.Outbox = make(chan *Envelope)
	service.Inbox = make(chan *Message)
	return service
}

func (service *Service) GetSignals() (chan os.Signal, chan os.Signal) {
	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	return termChan, hupChan
}

func (service *Service) SendMessage(message *Message) error {
	if message.Destination == *service.Location {
		return service.DeliverMessage(message)
	}
	router := service.GetRouterLocation(message.Destination)
	var dest *Location
	if router == *service.Location {
		dest = &message.Destination
	} else {
		dest = &router
	}
	return service.DoSendMessage(message, dest)
}

func (service *Service) DeliverMessage(message *Message) error {
	log.Println("do handle of", message)
	switch message.Key {
	case "ping":
		location, ok := message.Data.(Location)
		if !ok {
			log.Println("Invalid ping request!", message)
			return nil
		}
		service.SendMessage(&Message{"pong", nil, location})
	}
	return nil
}

func (service *Service) DoSendMessage(message *Message, destination *Location) error {
	log.Println("send message", message, "to", destination)
	service.Outbox <- &Envelope{message, destination}
	return nil
}

type PersistentConnection struct {
	Outbox chan *Message
	Inbox chan *Message
	Encoder *gob.Encoder
	Decoder *gob.Decoder
	Location Location
	Service *Service
	Connection *net.Conn
	Identified bool
}

func NewPersistentConnection(service *Service, location *Location) *PersistentConnection {
	conn := &PersistentConnection{}
	conn.Service = service
	conn.Identified = true
	conn.Outbox = make(chan *Message)
	conn.Inbox = make(chan *Message)
	if location != nil {
		conn.Location = *location
	}
	return conn
}

func (conn *PersistentConnection) TryConnect() error {
	locationString := conn.Location.ToString()
	log.Println("Dialing", locationString)
	connection, err := net.Dial("tcp", locationString)
    if err != nil {
		log.Println("Error in dial!")
		return err
    }
	conn.Connection = &connection
	conn.Encoder = gob.NewEncoder(connection)
	conn.Decoder = gob.NewDecoder(connection)
	return nil
}

func (conn *PersistentConnection) Manage() {
	log.Println("Starting manager")
	var connectionError chan error
	var err error

	go func() {
		var message *Message
		var err error
		for {
			err = conn.Decoder.Decode(&message)
			if err != nil {
				connectionError <- err
				return
			}
			conn.Inbox <- message
		}
	}()

	var message *Message
	for {
		select {
		case message = <-conn.Outbox:
			log.Println("sending", message)
			err = conn.Encoder.Encode(message)
			if err != nil {
//				if e, ok := err.(*net.OpError); ok {
//					if e.Err == syscall.EPIPE {
//						// Client disconnected

				log.Println("error sending", err)
				go func() { conn.Outbox <- message }() // Resend failed message
				if !conn.Identified {
					log.Println("Stopping because connection not identified")
					return
				}
				conn.Connect()
			}
		case message = <-conn.Inbox:
			log.Println("receiving", message)
			if message.Key == "identify" {
				log.Println("Registering connection")
				conn.Location = message.Data.(Location)
				go conn.Service.RegisterConnection(conn)
			} else {
				conn.Service.Inbox <- message
			}
		case err = <-connectionError:
			log.Println("error receiving", err)
			if !conn.Identified {
				log.Println("Stopping because connection not identified")
				return
			}
			conn.Connect()
		}
	}
}

func (conn *PersistentConnection) Connect() {
	log.Println("Connnecting to", conn.Location)
	err := conn.TryConnect()
	if err != nil {
		log.Println(err)
		log.Println("Connection failed! Waiting 1 second...")
		time.Sleep(time.Second)
		// Infinite recursion if node never comes up. Not sure if this is a problem.
		conn.Connect()
	} else {
		log.Println("Send identify message")
		conn.Encoder.Encode(Message{"identify", *conn.Service.Location, conn.Location})
		//go func() { conn.Outbox <- &Message{"identify", *conn.Service.Location, conn.Location} }()
	}
	log.Println("Connect ending")
}

type ConnectionMapping map[Location]*PersistentConnection

func (service *Service) RegisterConnection(conn *PersistentConnection) {
	service.ConnectionRegisterChannel <- conn
}
func (service *Service) HandleConnections() {
	connections := ConnectionMapping{}

	log.Println("Handling connections...")
	for {
		select {
		case envelope := <-service.Outbox:
			conn, ok := connections[*envelope.Location]
			if !ok {
				conn = NewPersistentConnection(service, envelope.Location)
				connections[*envelope.Location] = conn
				conn.Identified = true
				go func () {
					conn.Connect()
					conn.Manage()
				}()
			}
			// Spawning new goroutine so it doesn't block the main event loop while it's sending.
			// This emulates an infinitely buffered channel.
			go func() { conn.Outbox <- envelope.Message }()
		case conn := <-service.ConnectionRegisterChannel:
			connections[conn.Location] = conn
		}
	}
}

func (service *Service) SetupEtcd() {
	gob.Register(Location{})
	service.Etcd = etcd.NewClient()
	service.NodeMapMutex.Lock()
	defer service.NodeMapMutex.Unlock()
	service.NodeMap = NodeMap{}

	nodes, err := service.Etcd.Get("nodes")
	if err != nil {
		log.Fatal(err)
	}
	for _, node := range nodes {
		nodestring := strings.Split(node.Key, "/")[2]
		location, err := NewLocation(nodestring)
		if err != nil {
			log.Fatal(err)
		}
		routerlocation, err := NewLocation(node.Value)
		if err != nil {
			log.Fatal(err)
		}
		service.NodeMap[*location] = *routerlocation
	}
	log.Println(service.NodeMap)
}

func (service *Service) WatchEtcd() {
	var receiver = make(chan *store.Response)
	var stop chan bool
	go func () {
		_, err := service.Etcd.Watch("nodes/", 0, receiver, stop)
		if err != nil {
			log.Fatal(err)
		}
	}()

	exit, done := service.GetExitChannels()

	for {
		select {
			case response := <-receiver:
				switch response.Action {
				case "SET":
					nodestring := strings.Split(response.Key, "/")[2]
					node, err := NewLocation(nodestring)
					if err != nil {
						log.Fatal(err)
					}
					router, err := NewLocation(response.Value)
					if err != nil {
						log.Fatal(err)
					}
					service.NodeMapMutex.Lock()
					service.NodeMap[*node] = *router
					service.NodeMapMutex.Unlock()
				case "DELETE":
					nodestring := strings.Split(response.Key, "/")[2]
					node, err := NewLocation(nodestring)
					if err != nil {
						log.Fatal(err)
					}
					service.NodeMapMutex.Lock()
					delete(service.NodeMap, *node)
					service.NodeMapMutex.Unlock()
				default:
					log.Println("unhandled etcd message", response)
				}
				//log.Println(response.Action, response.Key, response.Value)
				log.Println(service.NodeMap)
			case <-exit:
				log.Println("cleaning up watchetcd service thing.")
				time.Sleep(2*time.Second)
				log.Println("done!")
				done <- 1
		}
	}
}

func (service *Service) GetRouterLocation(node Location) Location {
	service.NodeMapMutex.RLock()
	defer service.NodeMapMutex.RUnlock()
	location, ok := service.NodeMap[node]
	if !ok {
		location, ok = service.NodeMap[*service.Location]
		if !ok {
			log.Fatal("Cannot find router!!!")
		}
	}
	return location
}

func (service *Service) SetupNetwork() {
	log.Println("Setup network")
	var err error
	locationString := service.Location.ToString()
	service.Listener, err = net.Listen("tcp", locationString)
	if err != nil {
		log.Fatal(err)
	}
}
func (service *Service) Serve() {
	for {
		conn, err := service.Listener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		con := NewPersistentConnection(service, nil)
		con.Connection = &conn
		con.Encoder = gob.NewEncoder(*con.Connection)
		con.Decoder = gob.NewDecoder(*con.Connection)
		go con.Manage()
	}
}

//func (app *Application) serveHTTP() {
//	http.HandleFunc("/message", func(w http.ResponseWriter, r *http.Request) {
//		if r.Method != "POST" {
//			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
//			return
//		}
//		var message Message
//		decoder := json.NewDecoder(r.Body)
//		if decoder.Decode(&message) != nil {
//			http.Error(w, "Invalid JSON", http.StatusBadRequest)
//			return
//		}
//		app.Mailbox <- message
//	})
//	http.ListenAndServe(app.PortHttp, nil)
//}
//

