package core

import (
	"github.com/coreos/go-etcd/etcd"
	"log"
	"os"
	"os/signal"
	"syscall"
	"net"
	"encoding/gob"
	"pilosa/db"
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
	Port string
	PortHttp string
	Etcd *etcd.Client
	Cluster *db.Cluster
	TopologyMapper *TopologyMapper
	ProcessMapper *ProcessMapper
	ProcessMap *ProcessMap
	Transport *Transporter
	Dispatcher *Dispatcher
}

func NewService() *Service {
	service := new(Service)
	service.Etcd = etcd.NewClient(nil)
	service.Cluster = db.NewCluster()
	service.TopologyMapper = &TopologyMapper{service, "/pilosa/0"}
	service.ProcessMapper = NewProcessMapper(service)
	return service
}

func (service *Service) GetSignals() (chan os.Signal, chan os.Signal) {
	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	return termChan, hupChan
}
////////////////////////////////////////////////


func (service *Service) Run() {
    log.Println("Running service...")
    go service.TopologyMapper.Run()

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
