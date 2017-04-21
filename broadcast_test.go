package pilosa_test

import (
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
)

// Ensure a message can be marshaled and unmarshaled.
func TestMessage_Marshal(t *testing.T) {

	testMessageMarshal(t, &internal.CreateSliceMessage{
		DB:    "d",
		Slice: 8,
	})

	testMessageMarshal(t, &internal.DeleteDBMessage{
		DB: "d",
	})
}

func testMessageMarshal(t *testing.T, m proto.Message) {
	marshalled, err := pilosa.MarshalMessage(m)
	if err != nil {
		t.Fatal(err)
	}
	unmarshalled, err := pilosa.UnmarshalMessage(marshalled)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(unmarshalled, m) {
		t.Fatalf("unexpected message marshalling: %s", unmarshalled)
	}
}

// Ensure that BroadcastReceiver can register a BroadcastHandler.
func TestBroadcast_BroadcastReceiver(t *testing.T) {

	s := pilosa.NewServer()

	sbr := NewSimpleBroadcastReceiver()
	sbh := NewSimpleBroadcastHandler()

	s.BroadcastReceiver = sbr
	s.BroadcastReceiver.Start(sbh)

	msg := &internal.DeleteDBMessage{
		DB: "d",
	}

	s.BroadcastReceiver.(*SimpleBroadcastReceiver).Receive(msg)

	// Make sure the message received is what was sentd
	if !reflect.DeepEqual(sbh.receivedMessage, msg) {
		t.Fatalf("unexpected message: %s", sbh.receivedMessage)
	}
}

type SimpleBroadcastReceiver struct {
	broadcastHandler pilosa.BroadcastHandler
}

func NewSimpleBroadcastReceiver() *SimpleBroadcastReceiver {
	return &SimpleBroadcastReceiver{}
}

func (r *SimpleBroadcastReceiver) Start(h pilosa.BroadcastHandler) error {
	r.broadcastHandler = h
	return nil
}

func (r *SimpleBroadcastReceiver) Receive(pb proto.Message) error {
	r.broadcastHandler.ReceiveMessage(pb)
	return nil
}

type SimpleBroadcastHandler struct {
	receivedMessage proto.Message
}

func NewSimpleBroadcastHandler() *SimpleBroadcastHandler {
	return &SimpleBroadcastHandler{}
}

func (h *SimpleBroadcastHandler) ReceiveMessage(pb proto.Message) error {
	h.receivedMessage = pb.(proto.Message)
	return nil
}
