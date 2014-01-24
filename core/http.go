package core

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"pilosa/config"
	"pilosa/db"
	"runtime"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"tux21b.org/v1/gocql/uuid"
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
	mux.HandleFunc("/test", self.HandleTest)
	mux.HandleFunc("/version", self.HandleVersion)
	mux.HandleFunc("/ping", self.HandlePing)
	mux.HandleFunc("/batch", self.HandleBatch)
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

func (self *WebService) HandleBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	/*   	values.Set("db", database)
	values.Set("id", id)
	values.Set("frame", fragment_type)
	values.Set("slice", fmt.Sprintf("%d", slice))
	values.Set("bitmap", compressed_string)
	*/

	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	database_name := r.Form.Get("db")
	if database_name == "" {
		http.Error(w, "Provide a database (db)", http.StatusNotFound)
		return
	}

	bitmap_id_string := r.Form.Get("id")
	if bitmap_id_string == "" {
		http.Error(w, "Provide a bitmap id (id)", http.StatusNotFound)
		return
	}
	bitmap_id, _ := strconv.ParseUint(bitmap_id_string, 10, 64)

	slice_string := r.Form.Get("slice")
	if bitmap_id_string == "" {
		http.Error(w, "Provide a slice (slice)", http.StatusNotFound)
		return
	}
	slice, _ := strconv.ParseInt(slice_string, 10, 32)

	frame := r.Form.Get("frame")
	if bitmap_id_string == "" {
		http.Error(w, "Provide a frame (frame)", http.StatusNotFound)
		return
	}

	compressed_bitmap := r.Form.Get("bitmap")
	if bitmap_id_string == "" {
		http.Error(w, "Provide a compressed base64 bitmap (bitmap)", http.StatusNotFound)
		return
	}

	results := self.service.Batch(database_name, frame, compressed_bitmap, bitmap_id, int(slice))

	encoder := json.NewEncoder(w)
	err = encoder.Encode(results)
	if err != nil {
		log.Println("Error Batch results")
	    log.Println(spew.Sdump(r.Form)
		err = encoder.Encode("Bad Batch Request")
	}

}

func (self *WebService) HandleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	database_name := r.Form.Get("db")
	if database_name == "" {
		database_name = config.GetString("default_db")
	}
	if database_name == "" {
		http.Error(w, "Provide a database (db)", http.StatusNotFound)
		return
	}
	pql := r.Form.Get("pql")
	if pql == "" {
		http.Error(w, "Provide a valid query string (pql)", http.StatusNotFound)
		return
	}

	results := self.service.Executor.RunPQL(database_name, pql)

	encoder := json.NewEncoder(w)
	err = encoder.Encode(results)
	if err != nil {
		log.Fatal("Error encoding stats")
	}

}

func (self *WebService) HandleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	encoder := json.NewEncoder(w)
	//stats := service.GetStats()
	//	stats := ""
	m := &runtime.MemStats{}
	runtime.ReadMemStats(m)
	stats := map[string]interface{}{"num_goroutines": runtime.NumGoroutine(), "Memory Aquired": m.Sys, "Memory Used": m.Alloc}

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

func (self *WebService) HandleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	fmt.Fprintf(w, "Pilosa v."+self.service.version+"\n")
}

func (self *WebService) HandleTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	spew.Dump("TEST!")

	msg := new(db.Message)
	msg.Data = "mystring"
	self.service.Transport.Push(msg)

	msg2 := new(db.Message)
	msg2.Data = 789
	self.service.Transport.Push(msg2)
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

func (self *WebService) HandlePing(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	process_string := r.Form.Get("process")
	process_id, err := uuid.ParseUUID(process_string)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_, err = self.service.ProcessMap.GetProcess(&process_id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	duration, err := self.service.Ping(&process_id)
	if err != nil {
		spew.Fdump(w, err)
		return
	}
	encoder := json.NewEncoder(w)
	encoder.Encode(map[string]float64{"duration": duration.Seconds()})
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
