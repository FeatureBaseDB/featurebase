package core

import (
	"fmt"
	//"log"
	log "github.com/cihub/seelog"
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
)

type Service struct {
	Stopper
	Id             *util.GUID
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

var Build string

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
	service.version = Build
	service.name = "Cruncher"
	service.PrepareLogging()
	fmt.Printf("Pilosa %s\n", service.version)
	return service
}

func (self *Service) getProduction() string {
	base_path := config.GetString("log_path")
	if base_path == "" {
		base_path = "/tmp"
	}
	fname := fmt.Sprintf("%s/%s.%s", base_path, self.name, self.Id)
	//<seelog minlevel="debug" maxlevel="error">
	log_level := config.GetStringDefault("log_level", "info")
	prod_config := fmt.Sprintf(`<seelog minlevel="%s">
	<outputs>
		<rollingfile type="size" filename="%s" maxsize="524288000" maxrolls="4" formatid="format1" />
	</outputs> `, log_level, fname)
	prod_config += `<formats>
	    <format id="format1" format="%Date/%Time [%LEV] %Msg%n"/>
	 </formats>
	</seelog>`
	//fmt.Println(prod_config)
	return prod_config
}

func (self *Service) getDev() string {
	return `<seelog>
	<outputs>
	<console />
	</outputs>
	</seelog>
	`
}
func (self *Service) PrepareLogging() {
	logger, _ := log.LoggerFromConfigAsBytes([]byte(self.getProduction()))
	//	logger, _ := log.LoggerFromConfigAsBytes([]byte(self.getDev()))
	log.ReplaceLogger(logger)
}

func (service *Service) init_id() {
	var id util.GUID
	var err error
	id_string := config.GetString("id")
	if id_string == "" {
		log.Info("Service id not configured, generating...")
		id = util.RandomUUID()
		if err != nil {
			log.Critical("problem generating uuid")
			os.Exit(-1)

		}
	} else {
		id, err = util.ParseGUID(id_string)
		if err != nil {
			log.Critical("Service id not valid:", id_string)
			os.Exit(-1)
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
	log.Info("Setup service...", service.version)
	service.TopologyMapper.Setup()
	log.Info("Running service...", service.version)
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
