package core

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/coreos/go-etcd/etcd"
	"encoding/gob"
	"pilosa/db"
	"log"
	"strings"
	"github.com/nu7hatch/gouuid"
	"strconv"
	"errors"
)

func (service *Service) SetupEtcd() {
	gob.Register(db.Location{})
	service.Etcd = etcd.NewClient(nil)
}

func flatten(node *etcd.Node) []*etcd.Node {
	nodes := make([]*etcd.Node, 0)
	nodes = append(nodes, node)
	for _, node := range node.Nodes {
		nodes = append(nodes, flatten(&node)...)
	}
	return nodes
}

func handlenode(node *etcd.Node, namespace string, cluster *db.Cluster) error {
	key := node.Key[len(namespace)+1:]
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
		database = cluster.GetOrCreateDatabase(bits[1])
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

func (service *Service) MetaWatcher() {
	namespace := "/pilosa/0"
	log.Println(namespace + "/db")
	cluster := db.NewCluster()
	resp, err := service.Etcd.Get(namespace + "/db", false, true)
	if err != nil {
		log.Fatal(err)
	}
	for _, node := range flatten(resp.Node) {
		err := handlenode(node, namespace, cluster)
		if err != nil {
			spew.Dump(node)
			log.Println(err)
		}
	}
	receiver := make(chan *etcd.Response)
	stop := make(chan bool)
	go func() {
		_, _ = service.Etcd.Watch(namespace + "/db", 0, true, receiver, stop)
	}()
	go func() {
		for resp = range receiver {
			switch resp.Action {
			case "set":
				handlenode(resp.Node, namespace, cluster)
			}
		}
	}()
}
