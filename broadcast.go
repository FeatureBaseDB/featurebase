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
	"github.com/pkg/errors"
)

// broadcaster is an interface for broadcasting messages.
type broadcaster interface {
	SendSync(Message) error
	SendAsync(Message) error
	SendTo(*Node, Message) error
}

// Message is the interface implemented by all core pilosa types which can be serialized to messages.
// TODO add at least a single "isMessage()" method.
type Message interface{}

func init() {
	NopBroadcaster = &nopBroadcaster{}
}

// NopBroadcaster represents a Broadcaster that doesn't do anything.
var NopBroadcaster broadcaster

type nopBroadcaster struct{}

// SendSync A no-op implementation of Broadcaster SendSync method.
func (nopBroadcaster) SendSync(Message) error { return nil }

// SendAsync A no-op implementation of Broadcaster SendAsync method.
func (nopBroadcaster) SendAsync(Message) error { return nil }

// SendTo is a no-op implementation of Broadcaster SendTo method.
func (nopBroadcaster) SendTo(*Node, Message) error { return nil }

// Broadcast message types.
const (
	messageTypeCreateShard = iota
	messageTypeCreateIndex
	messageTypeDeleteIndex
	messageTypeCreateField
	messageTypeDeleteField
	messageTypeCreateView
	messageTypeDeleteView
	messageTypeClusterStatus
	messageTypeResizeInstruction
	messageTypeResizeInstructionComplete
	messageTypeSetCoordinator
	messageTypeUpdateCoordinator
	messageTypeNodeState
	messageTypeRecalculateCaches
	messageTypeNodeEvent
	messageTypeNodeStatus
)

// MarshalMessage encodes the protobuf message into a byte slice.
func MarshalMessage(m proto.Message) ([]byte, error) {
	var typ uint8
	switch obj := m.(type) {
	case *internal.CreateShardMessage:
		typ = messageTypeCreateShard
	case *internal.CreateIndexMessage:
		typ = messageTypeCreateIndex
	case *internal.DeleteIndexMessage:
		typ = messageTypeDeleteIndex
	case *internal.CreateFieldMessage:
		typ = messageTypeCreateField
	case *internal.DeleteFieldMessage:
		typ = messageTypeDeleteField
	case *internal.CreateViewMessage:
		typ = messageTypeCreateView
	case *internal.DeleteViewMessage:
		typ = messageTypeDeleteView
	case *internal.ClusterStatus:
		typ = messageTypeClusterStatus
	case *internal.ResizeInstruction:
		typ = messageTypeResizeInstruction
	case *internal.ResizeInstructionComplete:
		typ = messageTypeResizeInstructionComplete
	case *internal.SetCoordinatorMessage:
		typ = messageTypeSetCoordinator
	case *internal.UpdateCoordinatorMessage:
		typ = messageTypeUpdateCoordinator
	case *internal.NodeStateMessage:
		typ = messageTypeNodeState
	case *internal.RecalculateCaches:
		typ = messageTypeRecalculateCaches
	case *internal.NodeEventMessage:
		typ = messageTypeNodeEvent
	case *internal.NodeStatus:
		typ = messageTypeNodeStatus
	default:
		return nil, fmt.Errorf("message type not implemented for marshalling: %s", reflect.TypeOf(obj))
	}
	buf, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling")
	}
	return append([]byte{typ}, buf...), nil
}

func encode(m Message) proto.Message {
	switch mt := m.(type) {
	case *CreateShardMessage:
		return encodeCreateShardMessage(mt)
	case *CreateIndexMessage:
		return encodeCreateIndexMessage(mt)
	case *DeleteIndexMessage:
		return encodeDeleteIndexMessage(mt)
	case *CreateFieldMessage:
		return encodeCreateFieldMessage(mt)
	case *DeleteFieldMessage:
		return encodeDeleteFieldMessage(mt)
	case *CreateViewMessage:
		return encodeCreateViewMessage(mt)
	case *DeleteViewMessage:
		return encodeDeleteViewMessage(mt)
	case *ClusterStatus:
		return encodeClusterStatus(mt)
	case *ResizeInstruction:
		return encodeResizeInstruction(mt)
	case *ResizeInstructionComplete:
		return encodeResizeInstructionComplete(mt)
	case *SetCoordinatorMessage:
		return encodeSetCoordinatorMessage(mt)
	case *UpdateCoordinatorMessage:
		return encodeUpdateCoordinatorMessage(mt)
	case *NodeStateMessage:
		return encodeNodeStateMessage(mt)
	case *RecalculateCaches:
		return encodeRecalculateCaches(mt)
	case *nodeEvent:
		return encodeNodeEventMessage(mt)
	case *NodeStatus:
		return encodeNodeStatus(mt)
	}
	return nil
}

// UnmarshalMessage decodes the byte slice into a protobuf message.
func UnmarshalMessage(buf []byte) (proto.Message, error) {
	typ, buf := buf[0], buf[1:]
	var m proto.Message
	switch typ {
	case messageTypeCreateShard:
		m = &internal.CreateShardMessage{}
	case messageTypeCreateIndex:
		m = &internal.CreateIndexMessage{}
	case messageTypeDeleteIndex:
		m = &internal.DeleteIndexMessage{}
	case messageTypeCreateField:
		m = &internal.CreateFieldMessage{}
	case messageTypeDeleteField:
		m = &internal.DeleteFieldMessage{}
	case messageTypeCreateView:
		m = &internal.CreateViewMessage{}
	case messageTypeDeleteView:
		m = &internal.DeleteViewMessage{}
	case messageTypeClusterStatus:
		m = &internal.ClusterStatus{}
	case messageTypeResizeInstruction:
		m = &internal.ResizeInstruction{}
	case messageTypeResizeInstructionComplete:
		m = &internal.ResizeInstructionComplete{}
	case messageTypeSetCoordinator:
		m = &internal.SetCoordinatorMessage{}
	case messageTypeUpdateCoordinator:
		m = &internal.UpdateCoordinatorMessage{}
	case messageTypeNodeState:
		m = &internal.NodeStateMessage{}
	case messageTypeRecalculateCaches:
		m = &internal.RecalculateCaches{}
	case messageTypeNodeEvent:
		m = &internal.NodeEventMessage{}
	case messageTypeNodeStatus:
		m = &internal.NodeStatus{}
	default:
		return nil, fmt.Errorf("invalid message type: %d", typ)
	}

	if err := proto.Unmarshal(buf, m); err != nil {
		return nil, errors.Wrap(err, "unmarshalling")
	}
	return m, nil
}

func decode(m proto.Message) Message {
	switch mt := m.(type) {
	case *internal.CreateShardMessage:
		return decodeCreateShardMessage(mt)
	case *internal.CreateIndexMessage:
		return decodeCreateIndexMessage(mt)
	case *internal.DeleteIndexMessage:
		return decodeDeleteIndexMessage(mt)
	case *internal.CreateFieldMessage:
		return decodeCreateFieldMessage(mt)
	case *internal.DeleteFieldMessage:
		return decodeDeleteFieldMessage(mt)
	case *internal.CreateViewMessage:
		return decodeCreateViewMessage(mt)
	case *internal.DeleteViewMessage:
		return decodeDeleteViewMessage(mt)
	case *internal.ClusterStatus:
		return decodeClusterStatus(mt)
	case *internal.ResizeInstruction:
		return decodeResizeInstruction(mt)
	case *internal.ResizeInstructionComplete:
		return decodeResizeInstructionComplete(mt)
	case *internal.SetCoordinatorMessage:
		return decodeSetCoordinatorMessage(mt)
	case *internal.UpdateCoordinatorMessage:
		return decodeUpdateCoordinatorMessage(mt)
	case *internal.NodeStateMessage:
		return decodeNodeStateMessage(mt)
	case *internal.RecalculateCaches:
		return decodeRecalculateCaches(mt)
	case *internal.NodeEventMessage:
		return decodeNodeEventMessage(mt)
	case *internal.NodeStatus:
		return decodeNodeStatus(mt)
	}
	return nil
}
