package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type stringslice []string

func (self *stringslice) String() string {
	return fmt.Sprintf("%d", *self)
}

func (self *stringslice) Set(value string) error {
	*self = append(*self, value)
	return nil
}

var port uint
var blocksize uint64
var etcd_nodes stringslice

func init() {
	flag.UintVar(&port, "port", 9000, "Port to run HTTP server on")
	flag.Uint64Var(&blocksize, "blocksize", 64, "Block size")
	flag.Var(&etcd_nodes, "etcd", "Etcd server")
	flag.Parse()
}

type Req interface{}

type IncReq struct {
	id  int
	ret chan uint64
}

type DelReq struct {
	id int
}

type Nexter struct {
	reqchan chan Req
	done    chan bool
}

func (self *Nexter) countloop(ch chan uint64, id int, client *etcd.Client) {
	var start uint64
	var end uint64
	path := "nexter/" + strconv.Itoa(id)

	for {
		node, err := client.Get(path, false, false)
		if err != nil {
			ee, ok := err.(*etcd.EtcdError)
			if ok && ee.ErrorCode == 100 { // node does not exist
				_, err := client.Create(path, "0", 0)
				if err != nil {
					ee, ok := err.(*etcd.EtcdError)
					// Catch race condition where another node did the same client.Create()
					if ok && ee.ErrorCode == 105 { // Node has been created
						continue
					} else {
						log.Fatal(err)
					}
				}
				continue
			} else {
				log.Fatal(err)
			}
		} else { // No error, get start of series from etcd node
			start, err = strconv.ParseUint(node.Node.Value, 10, 0)
			if err != nil {
				log.Fatal(err)
			}
			end = start + blocksize
		}
		_, err = client.CompareAndSwap(path, strconv.FormatUint(end, 10), 0, strconv.FormatUint(start, 10), 0)
		if err != nil {
			log.Println("Error with CompareAndSet! Trying again in 1 second...")
			time.Sleep(time.Second)
			continue
		}
		for c := start; c < end; c += 1 {
			ch <- c
		}
	}
}

func (self *Nexter) loop() {
	counters := make(map[int]chan uint64)
	client := etcd.NewClient(etcd_nodes)
	for {
		select {
		case req := <-self.reqchan:
			switch req.(type) {
			case *IncReq:
				countreq := req.(*IncReq)
				counter, ok := counters[countreq.id]
				if !ok {
					counter = make(chan uint64)
					counters[countreq.id] = counter
					go self.countloop(counter, countreq.id, client)
				}
				go func() { countreq.ret <- <-counter }()
			case *DelReq:
				delreq := req.(*DelReq)
				delete(counters, delreq.id)
				path := "nexter/" + strconv.Itoa(delreq.id)
				client.Delete(path, true)
			default:
			}
		case <-self.done:
			break
		}
	}
}

func (self *Nexter) GetCount(id int) uint64 {
	ret := make(chan uint64)
	self.reqchan <- &IncReq{id, ret}
	return <-ret
}
func (self *Nexter) Delete(id int) {
	self.reqchan <- &DelReq{id}
}
func (self *Nexter) Stop() {
	self.done <- true
}

func NewNexter() *Nexter {
	nexter := &Nexter{make(chan Req), make(chan bool)}
	go nexter.loop()
	return nexter
}

func main() {
	log.Println("Starting Nexter...")
	nexter := NewNexter()
	http.HandleFunc("/nexter/", func(w http.ResponseWriter, r *http.Request) {
		url := r.URL.Path
		splits := strings.Split(url, "/")
		if len(splits) < 3 {
			http.Error(w, "Missing property id", http.StatusBadRequest)
			return
		}
		id_string := splits[2]
		id, err := strconv.Atoi(id_string)
		if err != nil {
			http.Error(w, "Property id is not a number", http.StatusBadRequest)
			return
		}

		num := nexter.GetCount(id)
		dec := json.NewEncoder(w)
		dec.Encode(num)
	})
	port_string := strconv.FormatUint(uint64(port), 10)
	http.ListenAndServe(":"+port_string, nil)
}
