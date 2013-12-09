package core

import (
	"github.com/coreos/go-etcd/etcd"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"sync"
	"net"
	"encoding/gob"
	"pilosa/db"
	//"net"
	//"flag"
	//"encoding/gob"
	//"io"
)

type Stats struct {
	MessageInCount int
	MessageOutCount int
	MessageProcessedCount int
	Uptime int
	MemoryUsed int
}

type Connection struct {
	Conn net.Conn
	Encoder *gob.Encoder
	Decoder *gob.Decoder
}

type Service struct {
	Stopper
	Handler func(db.Message)
	Mailbox chan db.Message
	Inbox chan *db.Message
	Port string
	PortHttp string
	Etcd *etcd.Client
	Location *db.Location
	HttpLocation *db.Location
	NodeMap db.NodeMap
	NodeMapMutex sync.RWMutex
	Outbox chan *db.Envelope
	ConnectionMap map[db.Location]*Connection
	ConnectionMapMutex sync.RWMutex
	Listener net.Listener
	ConnectionRegisterChannel chan *PersistentConnection
	Stats *Stats
	//Cluster query.Cluster
}

func NewService(tcp, http *db.Location) *Service {
	service := new(Service)
	service.Location = tcp
	service.HttpLocation = http
	service.Outbox = make(chan *db.Envelope)
	service.Inbox = make(chan *db.Message)
	service.Stats = new(Stats)
	return service
}

func (service *Service) GetSignals() (chan os.Signal, chan os.Signal) {
	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	return termChan, hupChan
}

func (service *Service) SendMessage(message *db.Message) error {
	if message.Destination == *service.Location {
		return service.DeliverMessage(message)
	}
	router := service.GetRouterLocation(message.Destination)
	var dest *db.Location
	if router == *service.Location {
		dest = &message.Destination
	} else {
		dest = &router
	}
	return service.DoSendMessage(message, dest)
}

func (service *Service) DeliverMessage(message *db.Message) error {
	log.Println("do handle of", message)
	switch message.Key {
	case "ping":
		location, ok := message.Data.(db.Location)
		if !ok {
			log.Println("Invalid ping request!", message)
			return nil
		}
		service.SendMessage(&db.Message{"pong", nil, location})
	}
	return nil
}

func (service *Service) DoSendMessage(message *db.Message, destination *db.Location) error {
	log.Println("send message", message, "to", destination)
	service.Outbox <- &db.Envelope{message, destination}
	return nil
}

type PersistentConnection struct {
	Outbox chan *db.Message
	Inbox chan *db.Message
	Encoder *gob.Encoder
	Decoder *gob.Decoder
	Location db.Location
	Service *Service
	Connection *net.Conn
	Identified bool
}

func NewPersistentConnection(service *Service, location *db.Location) *PersistentConnection {
	conn := &PersistentConnection{}
	conn.Service = service
	conn.Identified = true
	conn.Outbox = make(chan *db.Message)
	conn.Inbox = make(chan *db.Message)
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
		var message *db.Message
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

	var message *db.Message
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
				conn.Location = message.Data.(db.Location)
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
		conn.Encoder.Encode(db.Message{"identify", *conn.Service.Location, conn.Location})
		//go func() { conn.Outbox <- &db.Message{"identify", *conn.Service.Location, conn.Location} }()
	}
	log.Println("Connect ending")
}

type ConnectionMapping map[db.Location]*PersistentConnection

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

func (service *Service) GetRouterLocation(node db.Location) db.Location {
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

func (service *Service) GetStats() *Stats {
	return service.Stats
}

func (service *Service) NewListener() chan *db.Message {
	ch := make(chan *db.Message)
	return ch
}


////////////////////////////////////////////////


func (service *Service) Run() {
    log.Println("Running service...")
    service.SetupEtcd()
    //go r.SyncEtcd()
    //go service.WatchEtcd()
    //go service.HandleConnections()
    //service.SetupNetwork()
    //go service.Serve()
    //go service.HandleInbox()
    //go service.ServeHTTP()
    go service.MetaWatcher()

    sigterm, sighup := service.GetSignals()
    for {
        select {
            case <- sighup:
                log.Println("SIGHUP! Reloading configuration...")
                // TODO: reload configuration
            case <- sigterm:
                log.Println("SIGTERM! Cleaning up...")
                service.Stop()
                return
        }
    }
}

func (service *Service) HandleInbox() {
    for {
        select {
        case message := <-service.Inbox:
            log.Println("process", message)
        }
    }
}
