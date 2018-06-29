// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	testMessageMarshal(t, &internal.CreateShardMessage{
		Index: "i",
		Shard: 8,
	})

	testMessageMarshal(t, &internal.DeleteIndexMessage{
		Index: "i",
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
