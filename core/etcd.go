package core

import (
	"errors"
	"github.com/coreos/go-etcd/etcd"
	"github.com/davecgh/go-spew/spew"
	"github.com/nu7hatch/gouuid"
	"log"
	"pilosa/db"
	"strconv"
	"strings"
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
			spew.Dump(node)
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
	var fragment_uuid *uuid.UUID
	var slice *db.Slice
	var process_uuid *uuid.UUID
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
		slice_int, err := strconv.Atoi(bits[5])
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
		fragment_uuid, err = uuid.ParseHex(bits[7])
		if err != nil {
			return err
		}
		fragment = database.GetOrCreateFragment(frame, slice, fragment_uuid)
	}

	if len(bits) > 8 {
		if bits[8] != "process" {
			return errors.New("no process")
		}
		process_uuid, err = uuid.ParseHex(node.Value)
		if err != nil {
			return err
		}
		process = db.NewProcess(process_uuid)
		fragment.SetProcess(process)
	}
	return err
}

func flatten(node *etcd.Node) []*etcd.Node {
	nodes := make([]*etcd.Node, 0)
	nodes = append(nodes, node)
	for _, node := range node.Nodes {
		nodes = append(nodes, flatten(&node)...)
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
	nodes []*db.Process
}

type ProcessMapper struct {
	etcd     *etcd.Client
	nodes    []Node
	receiver chan *etcd.Response
	commands chan *ProcessMapperCommand
}

func NewProcessMapper(service *Service) *ProcessMapper {
	return &ProcessMapper{}
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

func (self *ProcessMapper) Run() {
	return
	var modindex uint64
	response, err := self.etcd.Get("nodes", false, true)
	if err != nil {
		log.Fatal(err)
	}
	//modindex = response.ModifiedIndex
	log.Println(modindex)
	nodes := make([]Node, 0)
	spew.Dump(response)
	for _, noderef := range response.Node.Nodes {
		nodestring := getKey(noderef.Key)
		u, err := uuid.ParseHex(nodestring)
		if err != nil {
			log.Fatal("Not a valid UUID: ", nodestring)
		}
		node := Node{id: u}
		for _, prop := range noderef.Nodes {
			switch getKey(prop.Key) {
			case "port_tcp":
				node.port_tcp, _ = strconv.Atoi(prop.Value)
			case "port_http":
				node.port_http, _ = strconv.Atoi(prop.Value)
			case "ip":
				node.ip = prop.Value
			}
		}
		nodes = append(nodes, node)
	}

	self.nodes = nodes
	spew.Dump(self.nodes)
	go func() {
		stop := make(chan bool)
		_, err := self.etcd.Watch("nodes/", 0, true, self.receiver, stop)
		if err != nil {
			log.Fatal(err)
		}
	}()
	go func() {
		for {
			select {
			case cmd := <-self.commands:
				spew.Dump(cmd)
			case response := <-self.receiver:
				switch response.Action {
				case "set":
					bits := strings.Split(response.Node.Key, "/")
					if len(bits) != 4 {
						log.Fatal("bug in etcd sync or etcd data")
					}
					//router, err := db.NewLocation(response.Node.Value)
					u, err := uuid.ParseHex(bits[2])
					node := self.getnode(u)
					if err != nil {
						log.Fatal(err)
					}
					switch bits[3] {
					case "port_tcp":
						node.port_tcp, _ = strconv.Atoi(response.Node.Value)
					case "port_http":
						node.port_http, _ = strconv.Atoi(response.Node.Value)
					case "ip":
						node.ip = response.Node.Value
					}
					spew.Dump(node)
				case "delete":
					spew.Dump("delete", response)

				default:
					spew.Dump("unhandled", response)
				}
			}
			//_, err = self.etcd.Get("nodes", false, false)
			//if err != nil {
			//	  log.Fatal(err)
			//}
			//spew.Dump(nodes)
		}
	}()
}
