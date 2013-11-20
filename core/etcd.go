package core

import (
	"github.com/coreos/go-etcd/etcd"
	"log"
	"strings"
	"time"
	"encoding/gob"
	"pilosa/db"
)

func (service *Service) SetupEtcd() {
	gob.Register(db.Location{})
	service.Etcd = etcd.NewClient(nil)
	service.NodeMapMutex.Lock()
	defer service.NodeMapMutex.Unlock()
	service.NodeMap = db.NodeMap{}

	nodes, err := service.Etcd.Get("nodes", false)
	if err != nil {
		log.Fatal(err)
	}
	for _, node := range nodes.Kvs {
		nodestring := strings.Split(node.Key, "/")[2]
		location, err := db.NewLocation(nodestring)
		if err != nil {
			log.Fatal(err)
		}
		routerlocation, err := db.NewLocation(node.Value)
		if err != nil {
			log.Fatal(err)
		}
		service.NodeMap[*location] = *routerlocation
	}
	log.Println(service.NodeMap)
}

func (service *Service) WatchEtcd() {
	var receiver = make(chan *etcd.Response)
	var stop chan bool
	go func () {
		_, err := service.Etcd.Watch("nodes/", 0, receiver, stop)
		if err != nil {
			log.Fatal(err)
		}
	}()

	exit, done := service.GetExitChannels()

	for {
		select {
			case response := <-receiver:
				switch response.Action {
				case "SET":
					nodestring := strings.Split(response.Key, "/")[2]
					node, err := db.NewLocation(nodestring)
					if err != nil {
						log.Fatal(err)
					}
					router, err := db.NewLocation(response.Value)
					if err != nil {
						log.Fatal(err)
					}
					service.NodeMapMutex.Lock()
					service.NodeMap[*node] = *router
					service.NodeMapMutex.Unlock()
				case "DELETE":
					nodestring := strings.Split(response.Key, "/")[2]
					node, err := db.NewLocation(nodestring)
					if err != nil {
						log.Fatal(err)
					}
					service.NodeMapMutex.Lock()
					delete(service.NodeMap, *node)
					service.NodeMapMutex.Unlock()
				default:
					log.Println("unhandled etcd message", response)
				}
				//log.Println(response.Action, response.Key, response.Value)
				log.Println(service.NodeMap)
			case <-exit:
				log.Println("cleaning up watchetcd service thing.")
				time.Sleep(time.Second/2)
				log.Println("done!")
				done <- 1
		}
	}
}
