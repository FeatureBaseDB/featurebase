package roaring

import (
	"github.com/pilosa/fast-skiplist"
)

func NewSkipListContainers() *SkipListContainers {
	return &SkipListContainers{
		list: skiplist.New(),
	}
}

type SkipListContainers struct {
	list *skiplist.SkipList
}

func (slc *SkipListContainers) Get(key uint64) *container {
	return slc.list.Get(key).Value().(*container)
}

func (slc *SkipListContainers) Put(key uint64, c *container) {
	slc.list.Set(key, c)
}

func (slc *SkipListContainers) Remove(key uint64) {
	slc.list.Remove(key)
}

func (slc *SkipListContainers) GetOrCreate(key uint64) *container {
	el := slc.list.Get(key)
	if el == nil {
		return slc.list.Set(key, newContainer()).Value().(*container)
	}
	return el.Value().(*container)
}

func (slc *SkipListContainers) Clone() Containers {
	nslc := NewSkipListContainers()
	for c := slc.list.Front(); c != nil; c = c.Next() {
		nslc.list.Set(c.Key(), c.Value().(*container).clone())
	}
	return nslc
}

func (slc *SkipListContainers) Last() (key uint64, c *container) {
	el := slc.list.Last()
	return el.Key(), el.Value().(*container)
}

func (slc *SkipListContainers) Size() int {
	return slc.list.Length()
}

func (slc *SkipListContainers) Iterator(key uint64) (citer Contiterator, found bool) {
	el := slc.list.GetNext(key)
	if el.Key() == key {
		found = true
	}

	return &SLCIterator{
		el: el,
	}, found

}

type SLCIterator struct {
	started bool
	el      *skiplist.Element
}

func (i *SLCIterator) Next() bool {
	if i.el == nil {
		return false
	}
	if !i.started {
		i.started = true
		return true
	}
	i.el = i.el.Next()
	return i.el != nil
}

func (i *SLCIterator) Value() (uint64, *container) {
	return i.el.Key(), i.el.Value().(*container)
}
