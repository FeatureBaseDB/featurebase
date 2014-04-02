package core

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"pilosa/config"
	"pilosa/db"
	"reflect"
	"runtime"
	"strconv"

	notify "github.com/bitly/go-notify"
	"github.com/davecgh/go-spew/spew"
	"github.com/gorilla/websocket"
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
	mux.HandleFunc("/status", self.HandleStatus)
	mux.HandleFunc("/info", self.HandleInfo)
	mux.HandleFunc("/processes", self.HandleProcesses)
	mux.HandleFunc("/listen/ws", self.HandleListenWS)
	mux.HandleFunc("/listen/stream", self.HandleListenStream)
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

	filter_string := r.Form.Get("filter")
	filter, _ := strconv.ParseInt(filter_string, 10, 32)
	if bitmap_id_string == "" {
		http.Error(w, "Provide a filter for categories", http.StatusNotFound)
		return
	}

	results := self.service.Batch(database_name, frame, compressed_bitmap, bitmap_id, int(slice), int(filter))

	encoder := json.NewEncoder(w)
	err = encoder.Encode(results)
	if err != nil {
		log.Println("Error Batch results")
		log.Println(spew.Sdump(r.Form))
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
	m := &runtime.MemStats{}
	runtime.ReadMemStats(m)

	//self.Report(fmt.Sprintf("%s.goroutines", prefix),
	//	float64(runtime.NumGoroutine()), now, context, dimensions)
	//self.Report(fmt.Sprintf("%s.memory.allocated", prefix),
	//	float64(memStats.Alloc), now, context, dimensions)
	//self.Report(fmt.Sprintf("%s.memory.mallocs", prefix),
	//	float64(memStats.Mallocs), now, context, dimensions)
	//self.Report(fmt.Sprintf("%s.memory.frees", prefix),
	//	float64(memStats.Frees), now, context, dimensions)
	//self.Report(fmt.Sprintf("%s.memory.gc.total_pause", prefix),
	//	float64(memStats.PauseTotalNs)/nsInMs, now, context, dimensions)
	//self.Report(fmt.Sprintf("%s.memory.heap", prefix),
	//	float64(memStats.HeapAlloc), now, context, dimensions)
	//self.Report(fmt.Sprintf("%s.memory.stack", prefix),
	//	float64(memStats.StackInuse), now, context, dimensions)

	//stats := map[string]interface{}{
	//	"num_goroutines": runtime.NumGoroutine(),
	//	"memory_allocated": m.Sys,
	//	"memory_": m.Alloc
	//}

	err := encoder.Encode(m)
	if err != nil {
		panic("Error encoding stats")
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
		panic("Error encoding stats")
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
	w.Write([]byte(`
	<html><head><title>Pilosa - streaming client</title></head><body>
	<style type="text/css">
		dt.inbox { background-color: #efe; }
		dt.outbox { background-color: #eef; }
	</style>
	<script src="//ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
	<script type="text/javascript">
		$dl = $("<dl/>")
		$dl.on('click', 'dt', function(e) {
			$(this).next().toggle()
		})
		$("body").append($dl)
		ws = new WebSocket("ws://" + window.location.host + "/listen/ws")
		ws.onmessage = function(mes) {
			obj = JSON.parse(mes.data)
			if (obj.host) {
				$dl.append('<dt class="outbox">&rarr;' + obj.type + ' (to: ' + obj.host + ')</dt>')
			} else {
				$dl.append('<dt class="inbox">&larr;' + obj.type + '</dt>')
			}
			$dl.append('<dd style="display:none;"><pre>' + obj.dump + obj.host + '</pre></dd>')
		}
	</script>
	</body></html>
	`))
}

func (self *WebService) streamer(writer func(map[string]interface{}) error) {
	inbox := make(chan interface{}, 10)
	outbox := make(chan interface{}, 10)
	notify.Start("inbox", inbox)
	notify.Start("outbox", outbox)
	var obj interface{}
	var data interface{}
	var inmessage *db.Message
	var outmessage *db.Envelope
	var host string
	for {
		select {
		case obj = <-outbox:
			outmessage = obj.(*db.Envelope)
			data = outmessage.Message.Data
			host = outmessage.Host.String()
		case obj = <-inbox:
			inmessage = obj.(*db.Message)
			data = inmessage.Data
			host = ""
		}

		typ := reflect.TypeOf(data)

		err := writer(map[string]interface{}{
			"type": typ.String(),
			"dump": spew.Sdump(data),
			"host": host,
		})
		if err != nil {
			log.Println("stopping")
			notify.Stop("inbox", inbox)
			notify.Stop("outbox", outbox)
			drain(inbox)
			drain(outbox)
			return
		}
	}
}

func (self *WebService) HandleListenWS(w http.ResponseWriter, r *http.Request) {
	defer func() {
		err := recover()
		spew.Dump(err)
	}()
	ws, err := websocket.Upgrade(w, r, nil, 1024, 1024)
	if _, ok := err.(websocket.HandshakeError); ok {
		http.Error(w, "Not a websocket handshake", 400)
		return
	} else if err != nil {
		log.Println(err)
		return
	}
	self.streamer(func(data map[string]interface{}) error {
		return ws.WriteJSON(data)
	})
}

func drain(ch chan interface{}) {
	for {
		switch {
		case <-ch:
		default:
			return
		}
	}
}

func (self *WebService) HandleListenStream(w http.ResponseWriter, r *http.Request) {
	defer func() {
		err := recover()
		spew.Dump(err)
	}()
	writer := json.NewEncoder(w)
	flusher := w.(http.Flusher)
	self.streamer(func(data map[string]interface{}) error {
		err := writer.Encode(data)
		flusher.Flush()
		return err
	})
}

func (self *WebService) HandleStatus(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(`
	<html><head><title>Pilosa - status</title></head><body>
	<style type="text/css">
	td {
		background: #eee;
	}
	</style>
	<script src="//ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
	<script type="text/javascript">
		$table = $("<table><tr><th>process id</th><th>host</th><th>tcp port</th><th>http port</th><th>latency</th></tr></table>")
		$("body").append($table)
		$.ajax('/processes', {
			dataType: "json",
			success: function(resp) {
				$.each(resp, function(index, value) {
					$table.append('<tr><td>' + index + '</td><td>' + value.host + '</td><td>' + value.port_tcp + '</td><td>' + value.port_http + '</td><td><button class="pinger"/></td></tr>')
				})
			}
		})
		$table.on('click', 'button.pinger', function(e) {
			var $td = $(this).parent()
			var $tr = $td.parent()
			var id = $tr.children('td').eq(0).text()

			$.ajax('/ping?process=' +  id, {
				dataType: 'json',
				success: function(resp) {
					$td.html(resp.duration)
				}
			})
		})
	</script>
	</body></html>
	`))
}
