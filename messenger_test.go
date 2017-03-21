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
