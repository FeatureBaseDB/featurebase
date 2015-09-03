package hold

import (
	"errors"
	"time"

	log "github.com/cihub/seelog"
	"github.com/umbel/pilosa"
)

type holdchan chan interface{}
type gethold struct {
	id    *pilosa.GUID
	reply chan holdchan
}
type delhold struct {
	id *pilosa.GUID
}

type Holder struct {
	data    map[pilosa.GUID]holdchan
	getchan chan gethold
	delchan chan delhold
}

func NewHolder() *Holder {
	return &Holder{
		data:    make(map[pilosa.GUID]holdchan),
		getchan: make(chan gethold),
		delchan: make(chan delhold),
	}
}

func (self *Holder) DelChan(id *pilosa.GUID) {
	log.Trace("Holder.DelChan", id)
	req := delhold{id}
	self.delchan <- req
}

func (self *Holder) GetChan(id *pilosa.GUID) holdchan {
	log.Trace("Holder.GetChan", id)
	reply := make(chan holdchan)
	req := gethold{id, reply}
	self.getchan <- req
	return <-reply
}

func (self *Holder) Get(id *pilosa.GUID, timeout time.Duration) (interface{}, error) {
	log.Trace("Holder.Get", id, timeout.String())
	ch := self.GetChan(id)
	select {
	case val := <-ch:
		return val, nil
	case <-time.After(timeout):
		self.DelChan(id)
		return nil, errors.New("Timeout getting from holder")
	}
}

func (self *Holder) Set(id *pilosa.GUID, value interface{}, timeout time.Duration) {
	log.Trace("Holder.Set", id, value, timeout.String())
	ch := self.GetChan(id)
	go func() {
		select {
		case ch <- value:
		case <-time.After(timeout):
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
