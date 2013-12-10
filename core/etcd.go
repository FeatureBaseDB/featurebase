package core

import (
	"github.com/coreos/go-etcd/etcd"
	"encoding/gob"
	"pilosa/db"
	"github.com/davecgh/go-spew/spew"
	"github.com/nu7hatch/gouuid"
	"strconv"
	"log"
)

func (service *Service) SetupEtcd() {
	gob.Register(db.Location{})
	service.Etcd = etcd.NewClient(nil)
	//service.NodeMapMutex.Lock()
	//defer service.NodeMapMutex.Unlock()
	//service.NodeMap = db.NodeMap{}

	//nodes, err := service.Etcd.Get("nodes", false)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//for _, node := range nodes.Kvs {
	//	nodestring := strings.Split(node.Key, "/")[2]
	//	location, err := db.NewLocation(nodestring)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	routerlocation, err := db.NewLocation(node.Value)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	service.NodeMap[*location] = *routerlocation
	//}
	//log.Println(service.NodeMap)
}

//func (service *Service) WatchEtcd() {
//	var receiver = make(chan *etcd.Response)
//	var stop chan bool
//	go func () {
//		_, err := service.Etcd.Watch("nodes/", 0, receiver, stop)
//		if err != nil {
//			log.Fatal(err)
//		}
//	}()
//
//	exit, done := service.GetExitChannels()
//
//	for {
//		select {
//			case response := <-receiver:
//				switch response.Action {
//				case "SET":
//					nodestring := strings.Split(response.Key, "/")[2]
//					node, err := db.NewLocation(nodestring)
//					if err != nil {
//						log.Fatal(err)
//					}
//					router, err := db.NewLocation(response.Value)
//					if err != nil {
//						log.Fatal(err)
//					}
//					service.NodeMapMutex.Lock()
//					service.NodeMap[*node] = *router
//					service.NodeMapMutex.Unlock()
//				case "DELETE":
//					nodestring := strings.Split(response.Key, "/")[2]
//					node, err := db.NewLocation(nodestring)
//					if err != nil {
//						log.Fatal(err)
//					}
//					service.NodeMapMutex.Lock()
//					delete(service.NodeMap, *node)
//					service.NodeMapMutex.Unlock()
//				default:
//					log.Println("unhandled etcd message", response)
//				}
//				//log.Println(response.Action, response.Key, response.Value)
//				log.Println(service.NodeMap)
//			case <-exit:
//				log.Println("cleaning up watchetcd service thing.")
//				time.Sleep(time.Second/2)
//				log.Println("done!")
//				done <- 1
//		}
//	}
//}


func (service *Service) MetaWatcher() {
	namespace := "/pilosa/0"
	log.Println(namespace + "/db")
	resp, err := service.Etcd.Get(namespace + "/db", false, true)
	if err != nil {
		log.Fatal(err)
	}
	cluster := db.NewCluster()

	for _, database_ref := range resp.Node.Nodes {
		database_name := database_ref.Key[len(namespace)+4:]
		database := cluster.AddDatabase(database_name)
		for _, database_attr_ref := range database_ref.Nodes {
			key := database_attr_ref.Key[len(database_ref.Key)+1:]
			if key == "frame" {
				for _, frame_ref := range database_attr_ref.Nodes {
					frame_name := frame_ref.Key[len(database_attr_ref.Key)+1:]
					frame := database.AddFrame(frame_name)
					for _, frame_attr_ref := range frame_ref.Nodes {
						key = frame_attr_ref.Key[len(frame_ref.Key)+1:]
						if key == "slice" {
							for _, slice_ref := range frame_attr_ref.Nodes {
								slice_name := slice_ref.Key[len(frame_attr_ref.Key)+1:]
								slice_id, err := strconv.Atoi(slice_name)
								if err != nil {
									log.Fatal(err)
								}
								slice := database.AddSlice(slice_id)
								for _, slice_attr_ref := range slice_ref.Nodes {
									key = slice_attr_ref.Key[len(slice_ref.Key)+1:]
									if key == "fragment" {
										for _, fragment_ref := range slice_attr_ref.Nodes {
                                            /*
											fragment_name := fragment_ref.Key[len(slice_attr_ref.Key)+1:]
											fragment_id, err := strconv.Atoi(fragment_name)
											if err != nil {
												log.Fatal(err)
											}
                                            */
											for _, fragment_attr_ref := range fragment_ref.Nodes {
												key = fragment_attr_ref.Key[len(fragment_ref.Key)+1:]
												if key == "node" {
													uuid, err := uuid.ParseHex(fragment_attr_ref.Value)
													if err != nil {
														log.Fatal(err)
													}
													process := uuid
													database.AddFragment(frame, slice, process)
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	database, _ := cluster.GetDatabase("main")
	spew.Dump(database)
	database.OldGetFragment(db.Bitmap{1200, "general"}, 1)

	receiver := make(chan *etcd.Response)
	stop := make(chan bool)
	go func() {
		_, _ = service.Etcd.Watch(namespace + "/db", 0, true, receiver, stop)
	}()
	go func() {
		for x := range receiver {
			spew.Dump(x)
		}
	}()
}
