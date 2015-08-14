package core

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/cihub/seelog"
	"github.com/coreos/go-etcd/etcd"
	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/hold"
	"github.com/umbel/pilosa/index"
	"github.com/umbel/pilosa/interfaces"
	"github.com/umbel/pilosa/util"
)

type Service struct {
	Stopper
	Id             *util.GUID
	Etcd           *etcd.Client
	Cluster        *db.Cluster
	TopologyMapper *TopologyMapper
	ProcessMapper  *ProcessMapper
	ProcessMap     *ProcessMap
	Executor       interfaces.Executorer
	WebService     *WebService
	Index          *index.FragmentContainer
	Hold           *hold.Holder
	version        string
	name           string
}

var Build string

func (service *Service) GetSignals() (chan os.Signal, chan os.Signal) {
	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	return termChan, hupChan
}

func (service *Service) Run() {
	log.Info("Setup service...", service.version)
	service.TopologyMapper.Setup()
	log.Info("Running service...", service.version)
	go service.TopologyMapper.Run()
	go service.ProcessMapper.Run()
	go service.WebService.Run()
	go service.Executor.Run()
	go service.Hold.Run()

	sigterm, sighup := service.GetSignals()
	for {
		select {
		case <-sighup:
			log.Info("SIGHUP! Reloading configuration...")
			// TODO: reload configuration
		case <-sigterm:
			log.Info("SIGTERM! Cleaning up...")
			service.Index.Shutdown()
			service.WebService.Shutdown()
			util.ShutdownStats()
			service.Stop()
			return
		}
	}
	log.Info("Service stopping")
}

type Message interface {
	Handle(*Service)
}
