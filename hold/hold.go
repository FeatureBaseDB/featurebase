package hold

import (
	"errors"
	. "pilosa/util"
	"time"
)

type holdchan chan interface{}
type gethold struct {
	id    *GUID
	reply chan holdchan
}
type delhold struct {
	id *GUID
}

type Holder struct {
	data    map[GUID]holdchan
	getchan chan gethold
	delchan chan delhold
}

//var Hold Holder

func (self *Holder) DelChan(id *GUID) {
	req := delhold{id}
	self.delchan <- req
}

func (self *Holder) GetChan(id *GUID) holdchan {
	reply := make(chan holdchan)
	req := gethold{id, reply}
	self.getchan <- req
	return <-reply
}

func (self *Holder) Get(id *GUID, timeout int) (interface{}, error) {
	ch := self.GetChan(id)
	select {
	case val := <-ch:
		return val, nil
	case <-time.After(time.Duration(timeout) * time.Second):
		self.DelChan(id)
		return nil, errors.New("Timeout getting from holder")
	}
}

func (self *Holder) Set(id *GUID, value interface{}, timeout int) {
	ch := self.GetChan(id)
	go func() {
		select {
		case ch <- value:
		case <-time.After(time.Duration(timeout) * time.Second):
		}
		self.DelChan(id)
	}()
}

func (self *Holder) Run() {
	var greq gethold
	var dreq delhold
	for {
		select {
		case greq = <-self.getchan:
			item, ok := self.data[*greq.id]
			if !ok {
				item = make(holdchan)
				self.data[*greq.id] = item
			}
			greq.reply <- item
		case dreq = <-self.delchan:
			delete(self.data, *dreq.id)
		}
	}
}

func NewHolder() *Holder {
	h := Holder{make(map[GUID]holdchan), make(chan gethold), make(chan delhold)}
	return &h
}

/*
func init() {
	Hold = Holder{make(map[GUID]holdchan), make(chan gethold), make(chan delhold)}
	go Hold.Run()
}
*/
