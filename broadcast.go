// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/disco"
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
	SendTo(*disco.Node, Message) error
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
func (nopBroadcaster) SendTo(*disco.Node, Message) error { return nil }

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
	messageTypeUNUSED0 // used to be ResizeInstruction
	messageTypeUNUSED1 // used to be ResizeInstructionComplete
	messageTypeNodeState
	messageTypeRecalculateCaches
	messageTypeLoadSchemaMessage
	messageTypeNodeEvent
	messageTypeNodeStatus
	messageTypeTransaction
	messageTypeUNUSED2 // used to be ResizeNodeMessage
	messageTypeUNUSED3 // used to be ResizeAbortMessage
	messageTypeUpdateField
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
	case messageTypeUpdateField:
		return &UpdateFieldMessage{}
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
	case *UpdateFieldMessage:
		return messageTypeUpdateField
	default:
		panic(fmt.Sprintf("don't have type for message %#v", m))
	}
}
