package router

import (
	"log"
	"time"
	//"strings"
	"pilosa/core"
)

type Router struct {
	core.Service
}

func (r *Router) Init() {
	log.Println("Initializing router...")
}

func (r *Router) Run() {
	log.Println("Running router...")
	r.SetupEtcd()
	//go r.SyncEtcd()
	go r.WatchEtcd()
	go r.HandleConnections()

	go func() {
		for {
			r.SendMessage(&core.Message{"ping", core.Location{"127.0.0.1", 1200}, core.Location{"127.0.0.1", 1300}})
			time.Sleep(2*time.Second)
			//log.Println(r.GetRouterLocation(core.Location{"127.0.0.1", 1200}))
		}
	}()

	sigterm, sighup := r.GetSignals()
	for {
		select {
			case <- sighup:
				log.Println("SIGHUP! Reloading configuration...")
				// TODO: reload configuration
			case <- sigterm:
				log.Println("SIGTERM! Cleaning up...")
				r.Stop()
				return
		}
	}
}

func (r *Router) SetupEtcd() {
	r.Service.SetupEtcd()
	//routers, err := r.Etcd.Get("topology")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//for _, router := range routers {
	//	routerstring := strings.Split(router.Key, "/")[2]
	//	routerlocation, err := core.NewLocation(routerstring)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	nodes, err := r.Etcd.Get("topology/" + routerstring)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	for _, node := range nodes {
	//		nodestring := strings.Split(node.Key, "/")[3]
	//		nodelocation, err := core.NewLocation(nodestring)
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//		r.NodeMap[*nodelocation] = *routerlocation
	//	}
	//}
}

func (r *Router) HandleMessage(m *core.Message) {
	log.Println(m)
}

func NewRouter(tcp, http *core.Location) *Router {
	service := core.NewService(tcp, http)
	router := Router{*service}
	return &router
}
