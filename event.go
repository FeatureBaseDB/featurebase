// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import "github.com/molecula/featurebase/v3/disco"

// NodeEventType are the types of node events.
type NodeEventType int

// Constant node event types.
const (
	NodeJoin NodeEventType = iota
	NodeLeave
	NodeUpdate
)

// NodeEvent is a single event related to node activity in the cluster.
type NodeEvent struct {
	Event NodeEventType
	Node  *disco.Node
}
