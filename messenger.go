package pilosa

import (
	"fmt"
	"reflect"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

func init() {
	NopMessenger = &nopMessenger{}
}

var NopMessenger Messenger

// nopMessenger represents a Messenger that doesn't do anything.
type nopMessenger struct{}

func (c *nopMessenger) SendMessage(pb proto.Message) error {
	fmt.Println("NOPMessenger: Send")
	return nil
}
func (c *nopMessenger) ReceiveMessage(pb proto.Message) error {
	fmt.Println("NOPMessenger: Receive")
	return nil
}

type Messenger interface {
	SendMessage(pb proto.Message) error
	ReceiveMessage(pb proto.Message) error
}

const (
	MessageTypeCreateSlice = 1
	MessageTypeDeleteDB    = 2
	MessageTypeCreateDB    = 3
)

func MarshalMessage(m proto.Message) ([]byte, error) {
	var typ uint8
	switch obj := m.(type) {
	case *internal.CreateSliceMessage:
		typ = MessageTypeCreateSlice
	case *internal.DeleteDBMessage:
		typ = MessageTypeDeleteDB
	case *internal.CreateDBMessage:
		typ = MessageTypeCreateDB
	default:
		return nil, fmt.Errorf("message type not implemented for marshalling: %s", reflect.TypeOf(obj))
	}
	buf, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	return append([]byte{typ}, buf...), nil
}

func UnmarshalMessage(buf []byte) (proto.Message, error) {
	typ, buf := buf[0], buf[1:]

	var m proto.Message
	switch typ {
	case MessageTypeCreateSlice:
		m = &internal.CreateSliceMessage{}
	case MessageTypeDeleteDB:
		m = &internal.DeleteDBMessage{}
	case MessageTypeCreateDB:
		m = &internal.CreateDBMessage{}
	default:
		return nil, fmt.Errorf("invalid message type: %d", typ)
	}

	if err := proto.Unmarshal(buf, m); err != nil {
		return nil, err
	}
	return m, nil
}
