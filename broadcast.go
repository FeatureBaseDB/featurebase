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

// MemberSet represents an interface for Node membership and inter-node communication.
type MemberSet interface {
	// Open starts any network activity implemented by the MemberSet
	// Node is the local node, used for membership broadcasts.
	Open(n *Node) error
}

// StaticMemberSet represents a basic MemberSet for testing.
type StaticMemberSet struct {
	nodes []*Node
}

// NewStaticMemberSet creates a statically defined MemberSet.
func NewStaticMemberSet(nodes []*Node) *StaticMemberSet {
	return &StaticMemberSet{
		nodes: nodes,
	}
}

// Open implements the MemberSet interface to start network activity, but for a static MemberSet it does nothing.
func (s *StaticMemberSet) Open(n *Node) error {
	return nil
}

// Broadcaster is an interface for broadcasting messages.
type Broadcaster interface {
	SendSync(pb proto.Message) error
	SendAsync(pb proto.Message) error
	SendTo(to *Node, pb proto.Message) error
}

func init() {
	NopBroadcaster = &nopBroadcaster{}
	NopGossiper = &nopGossiper{}
}

// NopBroadcaster represents a Broadcaster that doesn't do anything.
var NopBroadcaster Broadcaster

type nopBroadcaster struct{}

// SendSync A no-op implementation of Broadcaster SendSync method.
func (n *nopBroadcaster) SendSync(pb proto.Message) error {
	return nil
}

// SendAsync A no-op implementation of Broadcaster SendAsync method.
func (n *nopBroadcaster) SendAsync(pb proto.Message) error {
	return nil
}

// SendTo is a no-op implementation of Broadcaster SendTo method.
func (c *nopBroadcaster) SendTo(to *Node, pb proto.Message) error {
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

// SendAsync A no-op implementation of Gossiper SendAsync method.
func (n *nopGossiper) SendAsync(pb proto.Message) error {
	return nil
}

// Broadcast message types.
const (
	MessageTypeCreateSlice = iota
	MessageTypeCreateIndex
	MessageTypeDeleteIndex
	MessageTypeCreateFrame
	MessageTypeDeleteFrame
	MessageTypeCreateView
	MessageTypeDeleteView
	MessageTypeCreateField
	MessageTypeDeleteField
	MessageTypeCreateInputDefinition
	MessageTypeDeleteInputDefinition
	MessageTypeClusterStatus
	MessageTypeResizeInstruction
	MessageTypeResizeInstructionComplete
	MessageTypeSetCoordinator
	MessageTypeUpdateCoordinator
	MessageTypeNodeState
	MessageTypeRecalculateCaches
	MessageTypeNodeEvent
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
	case *internal.CreateViewMessage:
		typ = MessageTypeCreateView
	case *internal.DeleteViewMessage:
		typ = MessageTypeDeleteView
	case *internal.CreateFieldMessage:
		typ = MessageTypeCreateField
	case *internal.DeleteFieldMessage:
		typ = MessageTypeDeleteField
	case *internal.CreateInputDefinitionMessage:
		typ = MessageTypeCreateInputDefinition
	case *internal.DeleteInputDefinitionMessage:
		typ = MessageTypeDeleteInputDefinition
	case *internal.ClusterStatus:
		typ = MessageTypeClusterStatus
	case *internal.ResizeInstruction:
		typ = MessageTypeResizeInstruction
	case *internal.ResizeInstructionComplete:
		typ = MessageTypeResizeInstructionComplete
	case *internal.SetCoordinatorMessage:
		typ = MessageTypeSetCoordinator
	case *internal.UpdateCoordinatorMessage:
		typ = MessageTypeUpdateCoordinator
	case *internal.NodeStateMessage:
		typ = MessageTypeNodeState
	case *internal.RecalculateCaches:
		typ = MessageTypeRecalculateCaches
	case *internal.NodeEventMessage:
		typ = MessageTypeNodeEvent
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
	case MessageTypeCreateView:
		m = &internal.CreateViewMessage{}
	case MessageTypeDeleteView:
		m = &internal.DeleteViewMessage{}
	case MessageTypeCreateField:
		m = &internal.CreateFieldMessage{}
	case MessageTypeDeleteField:
		m = &internal.DeleteFieldMessage{}
	case MessageTypeCreateInputDefinition:
		m = &internal.CreateInputDefinitionMessage{}
	case MessageTypeDeleteInputDefinition:
		m = &internal.DeleteInputDefinitionMessage{}
	case MessageTypeClusterStatus:
		m = &internal.ClusterStatus{}
	case MessageTypeResizeInstruction:
		m = &internal.ResizeInstruction{}
	case MessageTypeResizeInstructionComplete:
		m = &internal.ResizeInstructionComplete{}
	case MessageTypeSetCoordinator:
		m = &internal.SetCoordinatorMessage{}
	case MessageTypeUpdateCoordinator:
		m = &internal.UpdateCoordinatorMessage{}
	case MessageTypeNodeState:
		m = &internal.NodeStateMessage{}
	case MessageTypeRecalculateCaches:
		m = &internal.RecalculateCaches{}
	case MessageTypeNodeEvent:
		m = &internal.NodeEventMessage{}
	default:
		return nil, fmt.Errorf("invalid message type: %d", typ)
	}

	if err := proto.Unmarshal(buf, m); err != nil {
		return nil, err
	}
	return m, nil
}
