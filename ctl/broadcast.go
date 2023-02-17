package ctl

import (
	"io"
)

type Broadcaster struct {
	source  io.Reader
	Readers []io.Reader
}

func (b *Broadcaster) Consume() {
	if len(b.Readers) == 1 {
		return
	}
	buf := make([]byte, 65535)
	for {
		n, err := b.source.Read(buf)
		if err == io.EOF {
			// close and shutdown
			b.shutdown()
			return
		}
		buf = buf[:n]
		for _, s := range b.Readers {
			subscriber := s.(*Subscriber)
			// possibly a timeout channel here
			subscriber.src <- buf
		}

	}
}

func (b *Broadcaster) shutdown() {
	for _, s := range b.Readers {
		subscriber := s.(*Subscriber)
		close(subscriber.src)
	}
}

type Subscriber struct {
	src   chan []byte
	local []byte
	i     int
}

func (s *Subscriber) Read(b []byte) (n int, err error) {
	if len(s.local) == 0 {
		s.local = <-s.src
	}
	if len(s.local) == 0 {
		return 0, io.EOF
	}
	n = copy(b, s.local)
	s.local = s.local[n:]
	return n, nil
}

func NewBroadcaster(reader io.Reader, numSubscribers int) *Broadcaster {
	b := &Broadcaster{Readers: make([]io.Reader, numSubscribers)}
	if numSubscribers == 1 {
		b.Readers[0] = reader
		return b
	}
	for i := 0; i < len(b.Readers); i++ {
		b.Readers[i] = &Subscriber{src: make(chan []byte)}
	}
	b.source = reader
	return b
}
