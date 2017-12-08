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

package pilosa

import (
	"fmt"
	"reflect"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// NodeSet represents an interface for Node membership and inter-node communication.
type NodeSet interface {
	// Returns a list of all Nodes in the cluster
	Nodes() []*Node

	// Open starts any network activity implemented by the NodeSet
	Open() error
}

// StaticNodeSet represents a basic NodeSet for testing.
type StaticNodeSet struct {
	nodes []*Node
}

// NewStaticNodeSet creates a statically defined NodeSet.
func NewStaticNodeSet() *StaticNodeSet {
	return &StaticNodeSet{}
}

// Nodes implements the NodeSet interface and returns a list of nodes in the cluster.
func (s *StaticNodeSet) Nodes() []*Node {
	return s.nodes
}

// Open implements the NodeSet interface to start network activity, but for a static NodeSet it does nothing.
func (s *StaticNodeSet) Open() error {
	return nil
}

// Join sets the NodeSet nodes to the slice of Nodes passed in.
func (s *StaticNodeSet) Join(nodes []*Node) error {
	s.nodes = nodes
	return nil
}

// Broadcaster is an interface for broadcasting messages.
type Broadcaster interface {
	SendSync(pb proto.Message) error
	SendAsync(pb proto.Message) error
}

func init() {
	NopBroadcaster = &nopBroadcaster{}
	NopGossiper = &nopGossiper{}
}

// NopBroadcaster represents a Broadcaster that doesn't do anything.
var NopBroadcaster Broadcaster

type nopBroadcaster struct{}

// SendSync A no-op implemenetation of Broadcaster SendSync method.
func (n *nopBroadcaster) SendSync(pb proto.Message) error {
	return nil
}

// SendAsync A no-op implemenetation of Broadcaster SendAsync method.
func (n *nopBroadcaster) SendAsync(pb proto.Message) error {
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

// NopBroadcastReceiver is a no-op implementation of the BroadcastReceiver.
var NopBroadcastReceiver = &nopBroadcastReceiver{}

// Gossiper is an interface for sharing messages via gossip.
type Gossiper interface {
	SendAsync(pb proto.Message) error
}

// NopBroadcaster represents a Broadcaster that doesn't do anything.
var NopGossiper Gossiper

type nopGossiper struct{}

// SendAsync A no-op implemenetation of Gossiper SendAsync method.
func (n *nopGossiper) SendAsync(pb proto.Message) error {
	return nil
}

// Broadcast message types.
const (
	MessageTypeCreateSlice           = 1
	MessageTypeCreateIndex           = 2
	MessageTypeDeleteIndex           = 3
	MessageTypeCreateFrame           = 4
	MessageTypeDeleteFrame           = 5
	MessageTypeCreateInputDefinition = 6
	MessageTypeDeleteInputDefinition = 7
	MessageTypeDeleteView            = 8
)

// MarshalMessage encodes the protobuf message into a byte slice.
func MarshalMessage(m proto.Message) ([]byte, error) {
	var typ uint8
	switch obj := m.(type) {
	case *internal.CreateSliceMessage:
		typ = MessageTypeCreateSlice
	case *internal.CreateIndexMessage:
		typ = MessageTypeCreateIndex
	case *internal.DeleteIndexMessage:
		typ = MessageTypeDeleteIndex
	case *internal.CreateFrameMessage:
		typ = MessageTypeCreateFrame
	case *internal.DeleteFrameMessage:
		typ = MessageTypeDeleteFrame
	case *internal.CreateInputDefinitionMessage:
		typ = MessageTypeCreateInputDefinition
	case *internal.DeleteInputDefinitionMessage:
		typ = MessageTypeDeleteInputDefinition
	case *internal.DeleteViewMessage:
		typ = MessageTypeDeleteView
	default:
		return nil, fmt.Errorf("message type not implemented for marshalling: %s", reflect.TypeOf(obj))
	}
	buf, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	return append([]byte{typ}, buf...), nil
}

// UnmarshalMessage decodes the byte slice into a protobuf message.
func UnmarshalMessage(buf []byte) (proto.Message, error) {
	typ, buf := buf[0], buf[1:]

	var m proto.Message
	switch typ {
	case MessageTypeCreateSlice:
		m = &internal.CreateSliceMessage{}
	case MessageTypeCreateIndex:
		m = &internal.CreateIndexMessage{}
	case MessageTypeDeleteIndex:
		m = &internal.DeleteIndexMessage{}
	case MessageTypeCreateFrame:
		m = &internal.CreateFrameMessage{}
	case MessageTypeDeleteFrame:
		m = &internal.DeleteFrameMessage{}
	case MessageTypeCreateInputDefinition:
		m = &internal.CreateInputDefinitionMessage{}
	case MessageTypeDeleteInputDefinition:
		m = &internal.DeleteInputDefinitionMessage{}
	case MessageTypeDeleteView:
		m = &internal.DeleteViewMessage{}
	default:
		return nil, fmt.Errorf("invalid message type: %d", typ)
	}

	if err := proto.Unmarshal(buf, m); err != nil {
		return nil, err
	}
	return m, nil
}
