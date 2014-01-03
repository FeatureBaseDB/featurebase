package core

import (
	"errors"
	"log"
	"pilosa/config"
	"pilosa/db"
	"pilosa/util"
	"strconv"
	"strings"
	"sync"

	"github.com/coreos/go-etcd/etcd"
	"github.com/davecgh/go-spew/spew"
	"tux21b.org/v1/gocql/uuid"
)

type TopologyMapper struct {
	service   *Service
	namespace string
}

func (self *TopologyMapper) Run() {
	log.Println(self.namespace + "/db")
	resp, err := self.service.Etcd.Get(self.namespace+"/db", false, true)
	if err != nil {
		log.Fatal(err)
	}
	for _, node := range flatten(resp.Node) {
		err := self.handlenode(node)
		if err != nil {
			log.Println(err)
		}
	}
	receiver := make(chan *etcd.Response)
	stop := make(chan bool)
	go func() {
		// TODO: error check and restart watcher
		// TODO: use modindex to make sure watch catches everything
		_, _ = self.service.Etcd.Watch(self.namespace+"/db", 0, true, receiver, stop)
	}()
	go func() {
		for resp = range receiver {
			switch resp.Action {
			case "set":
				self.handlenode(resp.Node)
			}
			// TODO: handle deletes
		}
	}()
}

func NewTopologyMapper(service *Service, namespace string) *TopologyMapper {
	return &TopologyMapper{service, namespace}
}

func (self *TopologyMapper) handlenode(node *etcd.Node) error {
	key := node.Key[len(self.namespace)+1:]
	bits := strings.Split(key, "/")
	var database *db.Database
	var frame *db.Frame
	var fragment *db.Fragment
	var fragment_id util.SUUID
	var slice *db.Slice
	var slice_int int
	var process_uuid uuid.UUID
	var process *db.Process
	var err error

	if len(bits) <= 1 || bits[0] != "db" {
		return nil
	}
	if len(bits) > 1 {
		database = self.service.Cluster.GetOrCreateDatabase(bits[1])
	}
	if len(bits) > 2 {
		if bits[2] != "frame" {
			return errors.New("no frame")
		}
	}
	if len(bits) > 3 {
		frame = database.GetOrCreateFrame(bits[3])
	}
	if len(bits) > 4 {
		if bits[4] != "slice" {
			return errors.New("no slice")
		}
	}
	if len(bits) > 5 {
		slice_int, err = strconv.Atoi(bits[5])
		if err != nil {
			return err
		}
		slice = database.GetOrCreateSlice(slice_int)
	}
	if len(bits) > 6 {
		if bits[6] != "fragment" {
			return errors.New("no fragment")
		}
	}
	if len(bits) > 7 {
		fragment_id = util.Hex_to_SUUID(bits[7])
		fragment = database.GetOrCreateFragment(frame, slice, fragment_id)
	}

	if len(bits) > 8 {
		if bits[8] != "process" {
			return errors.New("no process")
		}
		process_uuid, err = uuid.ParseUUID(node.Value)
		if err != nil {
			return err
		}
		process = db.NewProcess(&process_uuid)
		fragment.SetProcess(process)

		if self.service.Id.String() == process_uuid.String() {
			self.service.Index.AddFragment(bits[1], bits[3], slice_int, fragment_id)
		}

	}
	return err
}

func flatten(node *etcd.Node) []*etcd.Node {
	nodes := []*etcd.Node{node}
	for i := 0; i < len(node.Nodes); i++ {
		nodes = append(nodes, flatten(&node.Nodes[i])...)
	}
	return nodes
}

type Node struct {
	id        *uuid.UUID
	ip        string
	port_tcp  int
	port_http int
}

type ProcessMap struct {
	nodes map[uuid.UUID]*db.Process
	mutex sync.Mutex
}

func NewProcessMap() *ProcessMap {
	p := ProcessMap{}
	p.nodes = make(map[uuid.UUID]*db.Process)
	return &p
}

func (self *ProcessMap) AddProcess(process *db.Process) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.nodes[process.Id()] = process
}

func (self *ProcessMap) GetProcess(id *uuid.UUID) (*db.Process, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	process, ok := self.nodes[*id]
	if !ok {
		return nil, errors.New("No such process")
	}
	return process, nil
}

func (self *ProcessMap) GetOrAddProcess(id *uuid.UUID) *db.Process {
	process, err := self.GetProcess(id)
	if err != nil {
		process = db.NewProcess(id)
		self.AddProcess(process)
	}
	return process
}

func (self *ProcessMap) GetHost(id *uuid.UUID) (string, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	process, ok := self.nodes[*id]
	if !ok {
		return "", errors.New("Process does not exist")
	}
	return process.Host(), nil
}

func (self *ProcessMap) GetPortTcp(id *uuid.UUID) (int, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	process, ok := self.nodes[*id]
	if !ok {
		return 0, errors.New("Process does not exist")
	}
	return process.PortTcp(), nil
}

func (self *ProcessMap) GetPortHttp(id *uuid.UUID) (int, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	process, ok := self.nodes[*id]
	if !ok {
		return 0, errors.New("Process does not exist")
	}
	return process.PortHttp(), nil
}

func (self *ProcessMap) GetMetadata() map[string]map[string]interface{} {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	out := make(map[string]map[string]interface{})
	for id, process := range self.nodes {
		pdata := make(map[string]interface{})
		pdata["host"] = process.Host()
		pdata["port_tcp"] = process.PortTcp()
		pdata["port_http"] = process.PortHttp()
		out[id.String()] = pdata
	}
	return out
}

type ProcessMapper struct {
	service   *Service
	receiver  chan etcd.Response
	commands  chan ProcessMapperCommand
	namespace string
}

func NewProcessMapper(service *Service, namespace string) *ProcessMapper {
	return &ProcessMapper{
		service:   service,
		receiver:  make(chan etcd.Response),
		commands:  make(chan ProcessMapperCommand),
		namespace: namespace,
	}
}

type ProcessMapperCommand struct {
	key string
}

func getKey(input string) string {
	bits := strings.Split(input, "/")
	return bits[len(bits)-1]
}

func (self *ProcessMapper) getnode(u *uuid.UUID) *Node {
	return new(Node)
}

func crash_on_error(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func (self *ProcessMapper) handlenode(node *etcd.Node) error {
	var err error
	var process *db.Process
	key := node.Key[len(self.namespace)+1:]
	bits := strings.Split(key, "/")
	if len(bits) <= 1 || bits[0] != "process" {
		return nil
	}
	if len(bits) >= 2 {
		id_string := bits[1]
		id, err := uuid.ParseUUID(id_string)
		if err != nil {
			return errors.New("Invalid UUID: " + id_string)
		}
		process = self.service.ProcessMap.GetOrAddProcess(&id)
	}
	if len(bits) >= 3 {
		switch bits[2] {
		case "port_tcp":
			port_tcp, _ := strconv.Atoi(node.Value)
			process.SetPortTcp(port_tcp)
		case "port_http":
			port_http, _ := strconv.Atoi(node.Value)
			process.SetPortHttp(port_http)
		case "host":
			host := node.Value
			process.SetHost(host)
		}
	}

	return err
}

func (self *ProcessMapper) Run() {
	id_string := self.service.Id.String()
	path := self.namespace + "/process"
	self_path := path + "/" + id_string

	log.Println("Writing configuration to etcd...")
	log.Println(self_path)

	var err error
	_, err = self.service.Etcd.Set(self_path+"/port_tcp", strconv.Itoa(config.GetInt("port_tcp")), 0)
	crash_on_error(err)
	_, err = self.service.Etcd.Set(self_path+"/port_http", strconv.Itoa(config.GetInt("port_http")), 0)
	crash_on_error(err)
	_, err = self.service.Etcd.Set(self_path+"/host", config.GetString("host"), 0)
	crash_on_error(err)

	response, err := self.service.Etcd.Get(path, false, true)
	for _, node := range flatten(response.Node) {
		err := self.handlenode(node)
		if err != nil {
			spew.Dump(node)
			log.Println(err)
		}
	}

	receiver := make(chan *etcd.Response)
	stop := make(chan bool)
	go func() {
		// TODO: error check and restart watcher
		// TODO: use modindex to make sure watch catches everything
		_, _ = self.service.Etcd.Watch(path, 0, true, receiver, stop)
	}()

	go func() {
		for response = range receiver {
			switch response.Action {
			case "set":
				self.handlenode(response.Node)
			}
			// TODO: handle deletes
		}
	}()
}
