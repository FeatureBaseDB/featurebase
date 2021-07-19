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

	"github.com/molecula/featurebase/v2/topology"
	"github.com/pkg/errors"
)

// Serializer is an interface for serializing pilosa types to bytes and back.
type Serializer interface {
	Marshal(Message) ([]byte, error)
	Unmarshal([]byte, Message) error
}

// NopSerializer represents a Serializer that doesn't do anything.
var NopSerializer Serializer = &nopSerializer{}

type nopSerializer struct{}

// Marshal is a no-op implementation of Serializer Marshal method.
func (*nopSerializer) Marshal(Message) ([]byte, error) { return nil, nil }

// Unmarshal is a no-op implementation of Serializer Unmarshal method.
func (*nopSerializer) Unmarshal([]byte, Message) error { return nil }

// broadcaster is an interface for broadcasting messages.
type broadcaster interface {
	SendSync(Message) error
	SendAsync(Message) error
	SendTo(*topology.Node, Message) error
}

// Message is the interface implemented by all core pilosa types which can be serialized to messages.
// TODO add at least a single "isMessage()" method.
type Message interface{}

// NopBroadcaster represents a Broadcaster that doesn't do anything.
var NopBroadcaster broadcaster = &nopBroadcaster{}

type nopBroadcaster struct{}

// SendSync A no-op implementation of Broadcaster SendSync method.
func (nopBroadcaster) SendSync(Message) error { return nil }

// SendAsync A no-op implementation of Broadcaster SendAsync method.
func (nopBroadcaster) SendAsync(Message) error { return nil }

// SendTo is a no-op implementation of Broadcaster SendTo method.
func (nopBroadcaster) SendTo(*topology.Node, Message) error { return nil }

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
	messageTypeNodeState
	messageTypeRecalculateCaches
	messageTypeLoadSchemaMessage
	messageTypeNodeEvent
	messageTypeNodeStatus
	messageTypeTransaction
	messageTypeResizeNodeMessage
	messageTypeResizeAbortMessage
)

// MarshalInternalMessage serializes the pilosa message and adds pilosa internal
// type info which is used by the internal messaging stuff.
func MarshalInternalMessage(m Message, s Serializer) ([]byte, error) {
	typ := getMessageType(m)
	buf, err := s.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling")
	}
	return append([]byte{typ}, buf...), nil
}

func getMessage(typ byte) Message {
	switch typ {
	case messageTypeCreateShard:
		return &CreateShardMessage{}
	case messageTypeCreateIndex:
		return &CreateIndexMessage{}
	case messageTypeDeleteIndex:
		return &DeleteIndexMessage{}
	case messageTypeCreateField:
		return &CreateFieldMessage{}
	case messageTypeDeleteField:
		return &DeleteFieldMessage{}
	case messageTypeCreateView:
		return &CreateViewMessage{}
	case messageTypeDeleteView:
		return &DeleteViewMessage{}
	case messageTypeClusterStatus:
		return &ClusterStatus{}
	case messageTypeResizeInstruction:
		return &ResizeInstruction{}
	case messageTypeResizeInstructionComplete:
		return &ResizeInstructionComplete{}
	case messageTypeNodeState:
		return &NodeStateMessage{}
	case messageTypeRecalculateCaches:
		return &RecalculateCaches{}
	case messageTypeLoadSchemaMessage:
		return &LoadSchemaMessage{}
	case messageTypeNodeEvent:
		return &NodeEvent{}
	case messageTypeNodeStatus:
		return &NodeStatus{}
	case messageTypeTransaction:
		return &TransactionMessage{}
	case messageTypeResizeNodeMessage:
		return &ResizeNodeMessage{}
	case messageTypeResizeAbortMessage:
		return &ResizeAbortMessage{}
	default:
		panic(fmt.Sprintf("unknown message type %d", typ))
	}
}

func getMessageType(m Message) byte {
	switch m.(type) {
	case *CreateShardMessage:
		return messageTypeCreateShard
	case *CreateIndexMessage:
		return messageTypeCreateIndex
	case *DeleteIndexMessage:
		return messageTypeDeleteIndex
	case *CreateFieldMessage:
		return messageTypeCreateField
	case *DeleteFieldMessage:
		return messageTypeDeleteField
	case *CreateViewMessage:
		return messageTypeCreateView
	case *DeleteViewMessage:
		return messageTypeDeleteView
	case *ClusterStatus:
		return messageTypeClusterStatus
	case *ResizeInstruction:
		return messageTypeResizeInstruction
	case *ResizeInstructionComplete:
		return messageTypeResizeInstructionComplete
	case *NodeStateMessage:
		return messageTypeNodeState
	case *RecalculateCaches:
		return messageTypeRecalculateCaches
	case *LoadSchemaMessage:
		return messageTypeLoadSchemaMessage
	case *NodeEvent:
		return messageTypeNodeEvent
	case *NodeStatus:
		return messageTypeNodeStatus
	case *TransactionMessage:
		return messageTypeTransaction
	case *ResizeNodeMessage:
		return messageTypeResizeNodeMessage
	case *ResizeAbortMessage:
		return messageTypeResizeAbortMessage
	default:
		panic(fmt.Sprintf("don't have type for message %#v", m))
	}
}
