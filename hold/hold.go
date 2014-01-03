package hold

import (
	"errors"
	"time"

	"github.com/nu7hatch/gouuid"
)

type holdchan chan interface{}
type gethold struct {
	id    *uuid.UUID
	reply chan holdchan
}
type delhold struct {
	id *uuid.UUID
}

type Holder struct {
	data    map[uuid.UUID]holdchan
	getchan chan gethold
	delchan chan delhold
}

var Hold Holder

func (self *Holder) DelChan(id *uuid.UUID) {
	req := delhold{id}
	self.delchan <- req
}

func (self *Holder) GetChan(id *uuid.UUID) holdchan {
	reply := make(chan holdchan)
	req := gethold{id, reply}
	self.getchan <- req
	return <-reply
}

func (self *Holder) Get(id *uuid.UUID, timeout int) (interface{}, error) {
	ch := self.GetChan(id)
	select {
	case val := <-ch:
		return val, nil
	case <-time.After(time.Duration(timeout) * time.Second):
		self.DelChan(id)
		return nil, errors.New("Timeout getting from holder")
	}
}

func (self *Holder) Set(id *uuid.UUID, value interface{}, timeout int) {
	ch := self.GetChan(id)
	go func() {
		select {
		case ch <- value:
		case <-time.After(time.Duration(timeout) * time.Second):
		}
		self.DelChan(id)
	}()
}

func (self *Holder) run() {
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

func init() {
	Hold = Holder{make(map[uuid.UUID]holdchan), make(chan gethold), make(chan delhold)}
	go Hold.run()
}
