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

// Broadcaster is an interface for broadcasting messages.
type Broadcaster interface {
	SendSync(pb proto.Message) error
	SendAsync(pb proto.Message) error
	SendTo(to *Node, pb proto.Message) error
}

func init() {
	NopBroadcaster = &nopBroadcaster{}
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
	default:
		return nil, fmt.Errorf("message type not implemented for marshalling: %s", reflect.TypeOf(obj))
	}
	buf, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling")
	}
	return append([]byte{typ}, buf...), nil
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
	default:
		return nil, fmt.Errorf("invalid message type: %d", typ)
	}

	if err := proto.Unmarshal(buf, m); err != nil {
		return nil, errors.Wrap(err, "unmarshalling")
	}
	return m, nil
}
