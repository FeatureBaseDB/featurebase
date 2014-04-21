package core

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"pilosa/config"
	"pilosa/db"
	"pilosa/hold"
	"pilosa/index"
	"pilosa/interfaces"
	"pilosa/util"
	"syscall"

	"github.com/coreos/go-etcd/etcd"
	"tux21b.org/v1/gocql/uuid"
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
	Index          *index.FragmentContainer
	Hold           *hold.Holder
	version        string
	name           string
}

func NewService() *Service {
	service := new(Service)
	service.init_id()
	etc_hosts := config.GetStringArrayDefault("etcd_servers", []string{})

	service.Etcd = etcd.NewClient(etc_hosts)
	service.Cluster = db.NewCluster()
	service.TopologyMapper = NewTopologyMapper(service, "/pilosa/0")
	service.ProcessMapper = NewProcessMapper(service, "/pilosa/0")
	service.ProcessMap = NewProcessMap()
	service.WebService = NewWebService(service)
	service.Index = index.NewFragmentContainer()
	service.Hold = hold.NewHolder()
	service.version = "0.0.25"
	service.name = "Cruncher"
	service.PrepareLogging()
	fmt.Printf("Pilosa %s\n", service.version)
	return service
}

func (self *Service) PrepareLogging() {
	base_path := config.GetString("log_path")
	if base_path == "" {
		base_path = "/tmp"
	}
	f, err := os.OpenFile(fmt.Sprintf("%s/%s.%s", base_path, self.name, self.Id), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("error opening file: %v", err)
	}
	//defer f.Close()
	log.SetOutput(f)
}

func (service *Service) init_id() {
	var id uuid.UUID
	var err error
	id_string := config.GetString("id")
	if id_string == "" {
		log.Println("Service id not configured, generating...")
		id = uuid.RandomUUID()
		if err != nil {
			log.Fatal("problem generating uuid")
		}
	} else {
		id, err = uuid.ParseUUID(id_string)
		if err != nil {
			log.Fatalf("Service id '%s' not valid", id_string)
		}
	}
	service.Id = &id
}

func (self *Service) GetProcess() (*db.Process, error) {
	return self.ProcessMap.GetProcess(self.Id)
}

func (service *Service) GetSignals() (chan os.Signal, chan os.Signal) {
	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	return termChan, hupChan
}

func (service *Service) Run() {
	log.Println("Running service...", service.version)
	go service.TopologyMapper.Run()
	go service.ProcessMapper.Run()
	go service.WebService.Run()
	go service.Transport.Run()
	go service.Dispatch.Run()
	go service.Executor.Run()
	go service.Hold.Run()

	sigterm, sighup := service.GetSignals()
	for {
		select {
		case <-sighup:
			log.Println("SIGHUP! Reloading configuration...")
			// TODO: reload configuration
		case <-sigterm:
			log.Println("SIGTERM! Cleaning up...")
			service.Index.Shutdown()
			util.ShutdownStats()
			service.Stop()
			return
		}
	}
}

type Message interface {
	Handle(*Service)
}
