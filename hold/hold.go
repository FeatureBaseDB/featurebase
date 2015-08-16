package hold

import (
	"errors"
	"time"

	log "github.com/cihub/seelog"
	. "github.com/umbel/pilosa/util"
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

func NewHolder() *Holder {
	return &Holder{
		data:    make(map[GUID]holdchan),
		getchan: make(chan gethold),
		delchan: make(chan delhold),
	}
}

func (self *Holder) DelChan(id *GUID) {
	log.Trace("Holder.DelChan", id)
	req := delhold{id}
	self.delchan <- req
}

func (self *Holder) GetChan(id *GUID) holdchan {
	log.Trace("Holder.GetChan", id)
	reply := make(chan holdchan)
	req := gethold{id, reply}
	self.getchan <- req
	return <-reply
}

func (self *Holder) Get(id *GUID, timeout int) (interface{}, error) {
	log.Trace("Holder.Get", id, timeout)
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
	log.Trace("Holder.Set", id, value, timeout)
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
