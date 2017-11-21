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

package gossip

import (
	"fmt"
	"io"
	"log"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
)

// GossipMemberSet represents a gossip implementation of MemberSet using memberlist
// GossipMemberSet also represents a gossip implementation of pilosa.Broadcaster
// GossipMemberSet also represents an implementation of memberlist.Delegate
type GossipMemberSet struct {
	memberlist *memberlist.Memberlist
	handler    pilosa.BroadcastHandler

	broadcasts *memberlist.TransmitLimitedQueue

	statusHandler pilosa.StatusHandler
	config        *gossipConfig

	// The writer for any logging.
	LogOutput io.Writer
}

// Nodes implements the MemberSet interface and returns a list of nodes in the cluster.
func (g *GossipMemberSet) Nodes() []*pilosa.Node {
	a := make([]*pilosa.Node, 0, g.memberlist.NumMembers())
	for _, n := range g.memberlist.Members() {
		uri, _ := pilosa.NewURIFromAddress(n.Name)
		// TODO don't swallow the error above
		a = append(a, &pilosa.Node{URI: *uri})
	}
	return a
}

// Start implements the BroadcastReceiver interface and sets the BroadcastHandler.
func (g *GossipMemberSet) Start(h pilosa.BroadcastHandler) error {
	g.handler = h
	return nil
}

// Open implements the MemberSet interface to start network activity.
func (g *GossipMemberSet) Open() error {
	if g.handler == nil {
		return fmt.Errorf("opening GossipMemberSet: you must call Start(pilosa.BroadcastHandler) before calling Open()")
	}

	err := error(nil)
	g.memberlist, err = memberlist.Create(g.config.memberlistConfig)
	if err != nil {
		return err
	}

	g.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return g.memberlist.NumMembers()
		},
		RetransmitMult: 3,
	}

	uri, err := pilosa.NewURIFromAddress(g.config.gossipSeed)
	if err != nil {
		return err
	}

	// attach to gossip seed node
	nodes := []*pilosa.Node{&pilosa.Node{URI: *uri}} //TODO: support a list of seeds

	err = g.joinWithRetry(pilosa.NodeSet(pilosa.Nodes(nodes).URIs()).ToHostPortStrings())
	if err != nil {
		return err
	}
	return nil
}

// joinWithRetry wraps the standard memberlist Join function in a retry.
func (g *GossipMemberSet) joinWithRetry(hosts []string) error {
	err := retry(60, 2*time.Second, func() error {
		_, err := g.memberlist.Join(hosts)
		return err
	})
	return err
}

// retry periodically retries function fn a specified number of attempts.
func retry(attempts int, sleep time.Duration, fn func() error) (err error) {
	for i := 0; ; i++ {
		err = fn()
		if err == nil {
			return
		}
		if i >= (attempts - 1) {
			break
		}
		time.Sleep(sleep)
		log.Println("retrying after error:", err)
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

// logger returns a logger for the GossipMemberSet.
func (g *GossipMemberSet) logger() *log.Logger {
	return log.New(g.LogOutput, "", log.LstdFlags)
}

////////////////////////////////////////////////////////////////

type gossipConfig struct {
	gossipSeed       string
	memberlistConfig *memberlist.Config
}

// NewGossipMemberSet returns a new instance of GossipMemberSet.
func NewGossipMemberSet(name string, gossipHost string, gossipPort int, gossipSeed string, server *pilosa.Server, secretKey []byte) *GossipMemberSet {
	g := &GossipMemberSet{
		LogOutput: server.LogOutput,
	}

	//TODO: pull memberlist config from pilosa.cfg file
	g.config = &gossipConfig{
		memberlistConfig: memberlist.DefaultLocalConfig(),
		gossipSeed:       gossipSeed,
	}
	g.config.memberlistConfig.Name = name
	g.config.memberlistConfig.BindAddr = gossipHost
	g.config.memberlistConfig.BindPort = gossipPort
	g.config.memberlistConfig.AdvertiseAddr = pilosa.HostToIP(gossipHost)
	g.config.memberlistConfig.AdvertisePort = gossipPort
	//g.config.memberlistConfig.PushPullInterval = 0 * time.Second // Default is 15s in DefaultLocalConfig. // TODO travis: change this from 0
	g.config.memberlistConfig.Delegate = g
	g.config.memberlistConfig.SecretKey = secretKey
	g.config.memberlistConfig.Events = server.Cluster.EventReceiver.(memberlist.EventDelegate)

	g.statusHandler = server

	return g
}

// SendSync implementation of the Broadcaster interface.
func (g *GossipMemberSet) SendSync(pb proto.Message) error {
	msg, err := pilosa.MarshalMessage(pb)
	if err != nil {
		return err
	}

	mlist := g.memberlist

	// Direct sends the message directly to every node.
	// An error from any node raises an error on the entire operation.
	//
	// Gossip uses the gossip protocol to eventually deliver the message
	// to every node.
	var eg errgroup.Group
	for _, n := range mlist.Members() {
		// Don't send the message to the local node.
		if n == mlist.LocalNode() {
			continue
		}
		node := n
		eg.Go(func() error {
			return mlist.SendToTCP(node, msg)
		})
	}
	return eg.Wait()
}

// SendAsync implementation of the Broadcaster interface.
func (g *GossipMemberSet) SendAsync(pb proto.Message) error {
	msg, err := pilosa.MarshalMessage(pb)
	if err != nil {
		return err
	}

	b := &broadcast{
		msg:    msg,
		notify: nil,
	}
	g.broadcasts.QueueBroadcast(b)
	return nil
}

// SendTo implementation of the Broadcaster interface.
func (g *GossipMemberSet) SendTo(to *pilosa.Node, pb proto.Message) error {
	msg, err := pilosa.MarshalMessage(pb)
	if err != nil {
		return err
	}

	mlist := g.memberlist

	// Get the memberlist.Node from the pilosa.Node.
	for _, node := range mlist.Members() {
		if node.Name == to.URI.String() {
			return mlist.SendToTCP(node, msg)
		}
	}

	return nil
}

// NodeMeta implementation of the memberlist.Delegate interface.
func (g *GossipMemberSet) NodeMeta(limit int) []byte {
	return []byte{}
}

// NotifyMsg implementation of the memberlist.Delegate interface
// called when a user-data message is received.
func (g *GossipMemberSet) NotifyMsg(b []byte) {
	m, err := pilosa.UnmarshalMessage(b)
	if err != nil {
		g.logger().Printf("unmarshal message error: %s", err)
		return
	}
	if err := g.handler.ReceiveMessage(m); err != nil {
		g.logger().Printf("receive message error: %s", err)
		return
	}
}

// GetBroadcasts implementation of the memberlist.Delegate interface
// called when user data messages can be broadcast.
func (g *GossipMemberSet) GetBroadcasts(overhead, limit int) [][]byte {
	return g.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState implementation of the memberlist.Delegate interface
// sends this Node's state data.
func (g *GossipMemberSet) LocalState(join bool) []byte {
	pb, err := g.statusHandler.LocalStatus()
	if err != nil {
		g.logger().Printf("error getting local state, err=%s", err)
		return []byte{}
	}

	// Marshal nodestate data to bytes.
	buf, err := proto.Marshal(pb)
	if err != nil {
		g.logger().Printf("error marshalling nodestate data, err=%s", err)
		return []byte{}
	}
	return buf
}

// MergeRemoteState implementation of the memberlist.Delegate interface
// receive and process the remote side's LocalState.
func (g *GossipMemberSet) MergeRemoteState(buf []byte, join bool) {
	// Unmarshal nodestate data.
	var pb internal.NodeStatus
	if err := proto.Unmarshal(buf, &pb); err != nil {
		g.logger().Printf("error unmarshalling nodestate data, err=%s", err)
		return
	}
	err := g.statusHandler.HandleRemoteStatus(&pb)
	if err != nil {
		g.logger().Printf("merge state error: %s", err)
	}
}

// GossipEventReceiver is used to enable an application to receive
// events about joins and leaves over a channel.
//
// Care must be taken that events are processed in a timely manner from
// the channel, since this delegate will block until an event can be sent.
type GossipEventReceiver struct {
	ch           chan memberlist.NodeEvent
	eventHandler pilosa.EventHandler
}

// NewGossipEventReceiver returns a new instance of GossipEventReceiver.
func NewGossipEventReceiver() *GossipEventReceiver {
	return &GossipEventReceiver{
		ch: make(chan memberlist.NodeEvent, 1),
	}
}

func (g *GossipEventReceiver) NotifyJoin(n *memberlist.Node) {
	g.ch <- memberlist.NodeEvent{memberlist.NodeJoin, n}
}

func (g *GossipEventReceiver) NotifyLeave(n *memberlist.Node) {
	g.ch <- memberlist.NodeEvent{memberlist.NodeLeave, n}
}

func (g *GossipEventReceiver) NotifyUpdate(n *memberlist.Node) {
	g.ch <- memberlist.NodeEvent{memberlist.NodeUpdate, n}
}

// Start implements the pilosa.EventReceiver interface and sets the EventHandler.
func (g *GossipEventReceiver) Start(h pilosa.EventHandler) error {
	g.eventHandler = h
	go g.listen()
	return nil
}

func (g *GossipEventReceiver) listen() {
	var nodeEventType pilosa.NodeEventType
	for {
		e := <-g.ch
		switch e.Event {
		case memberlist.NodeJoin:
			nodeEventType = pilosa.NodeJoin
		case memberlist.NodeLeave:
			nodeEventType = pilosa.NodeLeave
		case memberlist.NodeUpdate:
			nodeEventType = pilosa.NodeUpdate
		default:
			continue
		}

		uri, _ := pilosa.NewURIFromAddress(e.Node.Name)
		// TODO: don't swallow this error

		ne := &pilosa.NodeEvent{
			Event: nodeEventType,
			URI:   *uri,
		}
		g.eventHandler.ReceiveEvent(ne)
	}
}

// broadcast represents an implementation of memberlist.Broadcast
type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}
