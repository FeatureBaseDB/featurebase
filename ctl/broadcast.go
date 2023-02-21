package ctl

import (
	"io"
	"io/ioutil"
)

type Broadcaster struct {
	source  io.Reader
	Readers []io.Reader
	Writers []io.Writer
}

func (b *Broadcaster) Consume() {
	if b.source == nil {
		return
	}
	io.Copy(ioutil.Discard, b.source)
	// maybe acknolwede completfe?  but it doesn't know its complete
	for i := range b.Writers {
		r := b.Readers[i].(*io.PipeReader)
		w := b.Writers[i].(*io.PipeWriter)
		r.Close()
		w.Close()
	}
}

func NewBroadcaster(reader io.Reader, numSubscribers int) *Broadcaster {
	b := &Broadcaster{Readers: make([]io.Reader, numSubscribers), Writers: make([]io.Writer, numSubscribers)}
	if numSubscribers == 1 {
		b.Readers[0] = reader
		return b
	}
	for i := 0; i < len(b.Readers); i++ {
		r, w := io.Pipe()
		b.Readers[i] = r
		b.Writers[i] = w
	}
	b.source = io.TeeReader(reader, io.MultiWriter(b.Writers...))
	return b
}
