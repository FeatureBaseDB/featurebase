package messenger_test

import (
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/messenger"
)

var tm1 *testMessenger

func init() {
	tm1 = &testMessenger{}
}

func TestMessenger_SetGet(t *testing.T) {
	tm2 := NewTestMessenger()

	messenger.SetMessenger("test1", tm1)
	messenger.SetMessenger("test1", tm2)

	if tm := messenger.GetMessenger("test1"); tm != tm1 {
		t.Fatalf("unexpected messenger: %d", tm)
	}

	return
}

func TestMessenger_SendReceive(t *testing.T) {

	messenger.SetMessenger("test2", tm1)

	protoMessage := &TestProtoMessage{Msg: "ABC"}
	messenger.GetMessenger("test2").SendMessage(protoMessage)

	if msg := tm1.GetHold(); !reflect.DeepEqual(msg, []byte("ABC")) {
		t.Fatalf("unexpected message: %d", msg)
	}
	return
}

func TestMessenger_DefaultMessenger(t *testing.T) {

	unknown := messenger.GetMessenger("UNKNOWNKEY")
	dm := messenger.NewDefaultMessenger()

	if reflect.TypeOf(unknown) != reflect.TypeOf(dm) {
		t.Fatal("Get before Set is not returning the default messenger")
	}

	protoMessage := &TestProtoMessage{Msg: "ABC"}
	if dm.SendMessage(protoMessage) != nil {
		t.Fatal("default SendMessage not returning nil")
	}
	if dm.ReceiveMessage([]byte("XYZ")) != nil {
		t.Fatal("default ReceiveMessage not returning nil")
	}

	return
}

// test Messenger passes messages directly from SendMessage to ReceiveMessage
type testMessenger struct {
	hold []byte
}

func NewTestMessenger() messenger.Messenger {
	return &testMessenger{}
}

func (msgr *testMessenger) SendMessage(m proto.Message) error {
	buf, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	if err := msgr.ReceiveMessage(buf); err != nil {
		return err
	}
	return nil
}

func (msgr *testMessenger) ReceiveMessage(b []byte) error {
	msgr.hold = b
	return nil
}

func (msgr *testMessenger) GetHold() []byte {
	return msgr.hold
}

// Test Proto Message
type TestProtoMessage struct {
	Msg string
}

func (m *TestProtoMessage) Reset()         { *m = TestProtoMessage{} }
func (m *TestProtoMessage) String() string { return "" }
func (*TestProtoMessage) ProtoMessage()    {}
func (m *TestProtoMessage) Marshal() (d []byte, err error) {
	return []byte(m.Msg), nil
}
