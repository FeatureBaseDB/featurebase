package messenger

import (
	"sync"

	"github.com/gogo/protobuf/proto"
)

type Messenger interface {
	SendMessage(m proto.Message) error
	ReceiveMessage(b []byte) error
}

// default Messenger ignores all messages
type defaultMessenger struct{}

func NewDefaultMessenger() Messenger {
	return &defaultMessenger{}
}

func (msgr *defaultMessenger) SendMessage(m proto.Message) error {
	return nil
}

func (msgr *defaultMessenger) ReceiveMessage(b []byte) error {
	return nil
}

// singletons
var instances map[string]Messenger
var onces *safeOnces

//var onces map[string]*sync.Once
type safeOnces struct {
	mu      sync.Mutex
	oncemap map[string]*sync.Once
}

func (s *safeOnces) createOnce(key string) {
	if _, exists := s.oncemap[key]; exists {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.oncemap[key]; !exists {
		s.oncemap[key] = new(sync.Once)
	}
}

func init() {
	instances = make(map[string]Messenger)
	onces = new(safeOnces)
	onces.oncemap = make(map[string]*sync.Once)
}

func SetMessenger(key string, msgr Messenger) {
	onces.createOnce(key)
	onces.oncemap[key].Do(func() {
		instances[key] = msgr
	})
}

func GetMessenger(key string) Messenger {
	onces.createOnce(key)
	onces.oncemap[key].Do(func() {
		// if a Messenger hasn't been set, use the default
		instances[key] = NewDefaultMessenger()
	})
	return instances[key]
}
