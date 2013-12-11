package core

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/coreos/go-etcd/etcd"
	"pilosa/db"
	"log"
	"strings"
	"github.com/nu7hatch/gouuid"
	"strconv"
	"errors"
)

type MetaWatcher struct {
	service *Service
	namespace string
}

func (self *MetaWatcher) Run() {
	log.Println(self.namespace + "/db")
	resp, err := self.service.Etcd.Get(self.namespace + "/db", false, true)
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
		_, _ = self.service.Etcd.Watch(self.namespace + "/db", 0, true, receiver, stop)
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

func NewMetaWatcher(service *Service, namespace string) *MetaWatcher {
	return &MetaWatcher{service, namespace}
}

func (self *MetaWatcher) handlenode(node *etcd.Node) error {
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
