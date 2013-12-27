package core

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"pilosa/config"
	"pilosa/db"
	"pilosa/query"
	"strconv"

	"github.com/davecgh/go-spew/spew"
)

type WebService struct {
	service *Service
}

func NewWebService(service *Service) *WebService {
	return &WebService{service}
}

func (self *WebService) Run() {
	port_string := strconv.Itoa(config.GetInt("port_http"))
	log.Printf("Serving HTTP on port %s...\n", port_string)
	mux := http.NewServeMux()
	mux.HandleFunc("/message", self.HandleMessage)
	mux.HandleFunc("/query", self.HandleQuery)
	mux.HandleFunc("/stats", self.HandleStats)
	mux.HandleFunc("/info", self.HandleInfo)
	mux.HandleFunc("/processes", self.HandleProcesses)
	mux.HandleFunc("/listen", self.HandleListen)
	s := &http.Server{
		Addr:    ":" + port_string,
		Handler: mux,
	}
	s.ListenAndServe()
}

func (self *WebService) HandleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	var message db.Message
	decoder := json.NewDecoder(r.Body)
	if decoder.Decode(&message) != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	//service.Inbox <- &message
}

func (self *WebService) HandleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, "Error reading POST data", http.StatusBadRequest)
		return
	}

	cluster := self.service.Cluster
	database := cluster.GetOrCreateDatabase("main")
	pql := string(body)

	query.Execute(database, pql)
}

func (self *WebService) HandleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	encoder := json.NewEncoder(w)
	//stats := service.GetStats()
	stats := ""
	err := encoder.Encode(stats)
	if err != nil {
		log.Fatal("Error encoding stats")
	}
}

func (self *WebService) HandleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	spew.Fdump(w, self.service.Cluster)
}

func (self *WebService) HandleProcesses(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	encoder := json.NewEncoder(w)
	processes := self.service.ProcessMap.GetMetadata()
	err := encoder.Encode(processes)
	if err != nil {
		log.Fatal("Error encoding stats")
	}
}

func (self *WebService) HandleListen(w http.ResponseWriter, r *http.Request) {
	//listener := service.NewListener()
	//encoder := json.NewEncoder(w)
	//for {
	//	select {
	//	case message := <-listener:
	//		err := encoder.Encode(message)
	//		if err != nil {
	//			log.Println("Error sending message")
	//			return
	//		}
	//	}
	//}
}
