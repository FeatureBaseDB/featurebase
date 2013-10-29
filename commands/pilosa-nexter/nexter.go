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

type Command struct {
	property int
	retchan chan int
}

type Property struct {
	id int
	current int
	max int
}

func (p *Property) Increment() int {
	p.current += 1
	if p.current < p.max {
		return p.current
	} else {
		p.Sync()
		p.current += 1
		return p.current
	}
}

func (p *Property) Sync() {
	path := "nexter/" + strconv.Itoa(p.id)
	max := 0
	key, err := client.Get(path)
	if err != nil {
		ee, ok := err.(etcd.EtcdError)
		if ok {
			if ee.ErrorCode == 100 {
				client.Set(path, "0", 0)
			}
		} else {
			log.Fatal(err)
		}
	} else {
		max, err = strconv.Atoi(key[0].Value)
		if err != nil {
			log.Fatal(err)
		}
	}
	for {
		newval, ok, err := client.TestAndSet(path, strconv.Itoa(max), strconv.Itoa(max+5), 0)
		if err != nil {
			log.Fatal(err)
		}
		if ok {
			break
		} else {
			time.Sleep(time.Second)
			max, err = strconv.Atoi(newval.Value)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	p.max = max
	p.current = max - 5
}

func NewProperty(id int) *Property {
	prop := &Property{id:id}
	prop.Sync()
	return prop
}

var commandchan chan *Command
var client *etcd.Client

func Nextifier() {
	prop_map := make(map[int]*Property)
	for {
		select {
		case c := <-commandchan:
			log.Println(prop_map)
			prop, ok := prop_map[c.property]
			if !ok {
				prop = NewProperty(c.property)
				prop_map[c.property] = prop
			}
			c.retchan <- prop.Increment()
		}
	}
}

func Nexter(w http.ResponseWriter, r *http.Request) {
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
	retchan := make(chan int)
	commandchan <- &Command{id, retchan}
	num := <-retchan
	dec := json.NewEncoder(w)
	dec.Encode(num)
}

func main() {
	client = etcd.NewClient(nil)
	commandchan = make(chan *Command)
	go Nextifier()
	log.Println("Starting Nexter...")
	http.HandleFunc("/nexter/", Nexter)
	http.ListenAndServe(":9000", nil)
}
