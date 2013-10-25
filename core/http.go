package core

import (
	"net/http"
	"encoding/json"
	"log"
	"strconv"
	"io/ioutil"
	"pilosa/db"
	"pilosa/query"
)

func (service *Service) HandleMessage(w http.ResponseWriter, r *http.Request) {
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
	service.Inbox <- &message
}

func (service *Service) HandleQuery(w http.ResponseWriter, r *http.Request) {
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
	q := query.QueryParser{string(body)}
	log.Println(q) // TODO: parse and perform
}

func (service *Service) HandleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	encoder := json.NewEncoder(w)
	stats := service.GetStats()
	err := encoder.Encode(stats)
	if err != nil {
		log.Fatal("Error encoding stats")
	}
}

func (service *Service) HandleListen(w http.ResponseWriter, r *http.Request) {
	listener := service.NewListener()
	encoder := json.NewEncoder(w)
	for {
		select {
		case message := <-listener:
			err := encoder.Encode(message)
			if err != nil {
				log.Println("Error sending message")
				return
			}
		}
	}
}

func (service *Service) ServeHTTP() {
	log.Println("Serving HTTP...")
	http.HandleFunc("/message", service.HandleMessage)
	http.HandleFunc("/query", service.HandleQuery)
	http.HandleFunc("/stats", service.HandleStats)
	http.HandleFunc("/listen", service.HandleListen)
	http.ListenAndServe(":" + strconv.Itoa(service.HttpLocation.Port), nil)
}
