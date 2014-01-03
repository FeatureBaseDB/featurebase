package core

import (
	"log"
	"os"
	"os/signal"
	"pilosa/config"
	"pilosa/db"
	"pilosa/index"
	"pilosa/interfaces"
	"syscall"

	"github.com/coreos/go-etcd/etcd"
	"github.com/davecgh/go-spew/spew"
	"github.com/nu7hatch/gouuid"
)

type Service struct {
	Stopper
	Id             *uuid.UUID
	Etcd           *etcd.Client
	Cluster        *db.Cluster
	TopologyMapper *TopologyMapper
	ProcessMapper  *ProcessMapper
	ProcessMap     *ProcessMap
	Transport      interfaces.Transporter
	Dispatch       interfaces.Dispatcher
	Executor       interfaces.Executorer
	WebService     *WebService
	process_id     *uuid.UUID
	Index          *index.FragmentContainer
}

func NewService() *Service {
	spew.Dump("NewService")
	service := new(Service)
	service.init_id()
	service.Etcd = etcd.NewClient(nil)
	service.Cluster = db.NewCluster()
	service.TopologyMapper = NewTopologyMapper(service, "/pilosa/0")
	service.ProcessMapper = NewProcessMapper(service, "/pilosa/0")
	service.ProcessMap = NewProcessMap()
	service.WebService = NewWebService(service)
	service.process_id = config.GetUUID("process_id")
	service.Index = index.NewFragmentContainer()
	return service
}

func (service *Service) init_id() {
	var id *uuid.UUID
	var err error
	id_string := config.GetString("id")
	if id_string == "" {
		log.Println("Service id not configured, generating...")
		id, err = uuid.NewV4()
		if err != nil {
			log.Fatal("problem generating uuid")
		}
	} else {
		id, err = uuid.ParseHex(id_string)
		if err != nil {
			log.Fatalf("Service id '%s' not valid", id_string)
		}
	}
	service.Id = id
}

func (service *Service) GetSignals() (chan os.Signal, chan os.Signal) {
	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	return termChan, hupChan
}

func (service *Service) Run() {
	log.Println("Running service...")
	go service.TopologyMapper.Run()
	go service.ProcessMapper.Run()
	go service.WebService.Run()
	go service.Transport.Run()
	go service.Dispatch.Run()
	go service.Executor.Run()

	sigterm, sighup := service.GetSignals()
	for {
		select {
		case <-sighup:
			log.Println("SIGHUP! Reloading configuration...")
			// TODO: reload configuration
		case <-sigterm:
			log.Println("SIGTERM! Cleaning up...")
			service.Stop()
			return
		}
	}
}

type Message interface {
	Handle(*Service)
}
