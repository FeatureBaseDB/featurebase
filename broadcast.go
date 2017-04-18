package pilosa

import (
	"fmt"
	"reflect"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// Broadcaster is an interface for broadcasting messages.
type Broadcaster interface {
	SendSync(pb proto.Message) error
	SendAsync(pb proto.Message) error
}

func init() {
	NopBroadcaster = &nopBroadcaster{}
}

var NopBroadcaster Broadcaster

// nopBroadcaster represents a Broadcaster that doesn't do anything.
type nopBroadcaster struct{}

// SendSync A no-op implemenetation of Broadcaster SendSync method.
func (c *nopBroadcaster) SendSync(pb proto.Message) error {
	fmt.Println("NOPBroadcaster: SendSync") // TODO remove or log properly?
	return nil
}

// SendAsync A no-op implemenetation of Broadcaster SendAsync method.
func (c *nopBroadcaster) SendAsync(pb proto.Message) error {
	fmt.Println("NOPBroadcaster: SendAsync") // TODO remove or log properly?
	return nil
}

// BroadcastHandler is the interface for the pilosa object which knows how to
// handle broadcast messages. (Hint: this is implemented by pilosa.Server)
type BroadcastHandler interface {
	ReceiveMessage(pb proto.Message) error
}

// BroadcastReceiver is the interface for the object which will listen for and
// decode broadcast messages before passing them to pilosa to handle. The
// implementation of this could be an http server which listens for messages,
// gets the protobuf payload, and then passes it to
// BroadcastHandler.ReceiveMessage.
type BroadcastReceiver interface {
	// Start starts listening for broadcast messages - it should return
	// immediately, spawning a goroutine if necessary.
	Start(BroadcastHandler) error
}

type nopBroadcastReceiver struct{}

func (n *nopBroadcastReceiver) Start(b BroadcastHandler) error { return nil }

var NopBroadcastReceiver = &nopBroadcastReceiver{}

const (
	MessageTypeCreateSlice = 1
	MessageTypeCreateDB    = 2
	MessageTypeDeleteDB    = 3
	MessageTypeCreateFrame = 4
	MessageTypeDeleteFrame = 5
)

func MarshalMessage(m proto.Message) ([]byte, error) {
	var typ uint8
	switch obj := m.(type) {
	case *internal.CreateSliceMessage:
		typ = MessageTypeCreateSlice
	case *internal.CreateDBMessage:
		typ = MessageTypeCreateDB
	case *internal.DeleteDBMessage:
		typ = MessageTypeDeleteDB
	case *internal.CreateFrameMessage:
		typ = MessageTypeCreateFrame
	case *internal.DeleteFrameMessage:
		typ = MessageTypeDeleteFrame
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
	case MessageTypeCreateDB:
		m = &internal.CreateDBMessage{}
	case MessageTypeDeleteDB:
		m = &internal.DeleteDBMessage{}
	case MessageTypeCreateFrame:
		m = &internal.CreateFrameMessage{}
	case MessageTypeDeleteFrame:
		m = &internal.DeleteFrameMessage{}
	default:
		return nil, fmt.Errorf("invalid message type: %d", typ)
	}

	if err := proto.Unmarshal(buf, m); err != nil {
		return nil, err
	}
	return m, nil
}
