package core

import (
	"sync"
)

type Stopper struct {
	TermChans []chan int
	DoneChans []chan int
	Mutex     sync.RWMutex
}

func (stopper *Stopper) Stop() {
	var i chan int
	var o chan int
	stopper.Mutex.RLock()
	for _, i = range stopper.TermChans {
		go func(i chan int) {
			i <- 1
		}(i)
	}
	for _, o = range stopper.DoneChans {
		<-o
	}
	stopper.Mutex.RUnlock()
	return
}

func (stopper *Stopper) GetExitChannels() (chan int, chan int) {
	termchan := make(chan int, 1)
	donechan := make(chan int, 1)
	stopper.Mutex.Lock()
	stopper.TermChans = append(stopper.TermChans, termchan)
	stopper.DoneChans = append(stopper.DoneChans, donechan)
	stopper.Mutex.Unlock()
	return termchan, donechan
}
