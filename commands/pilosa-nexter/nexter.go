package main

import (
	"github.com/coreos/go-etcd/etcd"
	"log"
	"net/http"
	"encoding/json"
	"strings"
	"strconv"
	"time"
)

const blocksize = 64

type Req struct {
	id int
	ret chan uint64
}

type Nexter struct {
	reqchan chan *Req
}

func (self *Nexter) countloop(ch chan uint64, id int, client *etcd.Client) {
	var start uint64
	var end uint64
	for {
		path := "nexter/" + strconv.Itoa(id)
		node, err := client.Get(path, false)
		if err != nil {
			ee, ok := err.(etcd.EtcdError)
			if ok && ee.ErrorCode == 100 { // node does not exist
				start = 0
				end = blocksize - 1
				client.Set(path, "0", 0) // TODO: error check
			} else {
				log.Fatal(err)
			}
		} else { // No error, get start of series from etcd node
			start, err = strconv.ParseUint(node.Kvs[0].Value, 10, 0)
			end = start + blocksize
			if err != nil {
				log.Fatal(err)
			}
		}
		for {
			newval, err := client.CompareAndSwap(path, strconv.FormatUint(end, 10), 0, strconv.FormatUint(start, 10), 0)
			if err != nil {
				break
			} else {
				log.Println("Error with CompareAndSet! Trying again in 1 second...")
				time.Sleep(time.Second)
				start, err = strconv.ParseUint(newval.Value, 10, 0)
				if err != nil {
					log.Fatal(err)
				}
				end = start + blocksize
			}
		}
		for c := start; c < end; c += 1 {
			ch <- c
		}
	}
}

func (self *Nexter) loop() {
	counters := make(map[int]chan uint64)
	client := etcd.NewClient(nil)
	for {
		select {
		case countreq := <-self.reqchan:
			counter, ok := counters[countreq.id]
			if !ok {
				counter = make(chan uint64)
				counters[countreq.id] = counter
				go self.countloop(counter, countreq.id, client)
			}
			go func() {countreq.ret <- <-counter}()
		}
	}
}
func (self *Nexter) GetCount(id int) uint64 {
	ret := make(chan uint64)
	self.reqchan <- &Req{id, ret}
	return <-ret
}

func NewNexter() *Nexter {
	nexter := &Nexter{make(chan *Req)}
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
	http.ListenAndServe(":9000", nil)
}
