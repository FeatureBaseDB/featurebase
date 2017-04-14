package pilosa

import (
	"io"
	"log"
	"os"

	"golang.org/x/sync/errgroup"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/pilosa/pilosa/internal"
)

// GossipNodeSet represents a gossip implementation of NodeSet using memberlist
// GossipNodeSet also represents an implementation of memberlist.Delegate
type GossipNodeSet struct {
	memberlist *memberlist.Memberlist

	config *GossipConfig

	// The writer for any logging.
	LogOutput io.Writer
}

func (g *GossipNodeSet) AttachBroker(mb *GossipMessageBroker) {
	g.config.memberlistConfig.Delegate = mb
}

func (g *GossipNodeSet) Nodes() []*Node {
	a := make([]*Node, 0, g.memberlist.NumMembers())
	for _, n := range g.memberlist.Members() {
		a = append(a, &Node{Host: n.Name})
	}
	return a
}

func (g *GossipNodeSet) Open() error {
	ml, err := memberlist.Create(g.config.memberlistConfig)
	if err != nil {
		return err
	}
	g.memberlist = ml

	// attach to gossip seed node
	nodes := []*Node{&Node{Host: g.config.gossipSeed}} //TODO: support a list of seeds
	_, err = g.memberlist.Join(Nodes(nodes).Hosts())
	if err != nil {
		return err
	}
	return nil
}

// logger returns a logger for the GossipNodeSet.
func (g *GossipNodeSet) logger() *log.Logger {
	return log.New(g.LogOutput, "", log.LstdFlags)
}

////////////////////////////////////////////////////////////////

type GossipConfig struct {
	gossipSeed       string
	memberlistConfig *memberlist.Config
}

// NewGossipNodeSet returns a new instance of GossipNodeSet.
func NewGossipNodeSet(name string, gossipHost string, gossipPort int, gossipSeed string) *GossipNodeSet {
	g := &GossipNodeSet{
		LogOutput: os.Stderr,
	}

	//TODO: pull memberlist config from pilosa.cfg file
	g.config = &GossipConfig{
		memberlistConfig: memberlist.DefaultLocalConfig(),
		gossipSeed:       gossipSeed,
	}
	g.config.memberlistConfig.Name = name
	g.config.memberlistConfig.BindAddr = gossipHost
	g.config.memberlistConfig.BindPort = gossipPort
	g.config.memberlistConfig.AdvertiseAddr = gossipHost
	g.config.memberlistConfig.AdvertisePort = gossipPort

	return g
}

////////////////////////////////////////////////////////////////

// GossipMessageBroker represents a gossip implementation of pilosa.MessageBroker
// GossipMessageBroker also represents an implementation of memberlist.Delegate
type GossipMessageBroker struct {
	broadcasts *memberlist.TransmitLimitedQueue

	messenger *Messenger

	// The writer for any logging.
	LogOutput io.Writer
}

// implementation of the messenger.Messenger interface
func (g *GossipMessageBroker) Send(pb proto.Message, method string) error {
	msg, err := MarshalMessage(pb)
	if err != nil {
		return err
	}

	mlist := g.messenger.Cluster.NodeSet.(*GossipNodeSet).memberlist

	// Direct sends the message directly to every node.
	// An error from any node raises an error on the entire operation.
	//
	// Gossip uses the gossip protocol to eventually deliver the message
	// to every node.
	switch method {
	case "direct":
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
	case "gossip":
		b := &broadcast{
			msg:    msg,
			notify: nil,
		}
		g.broadcasts.QueueBroadcast(b)
	}

	return nil
}

func (g *GossipMessageBroker) Receive(pb proto.Message) error {
	if err := g.messenger.ReceiveMessage(pb); err != nil {
		return err
	}
	return nil
}

// implementation of the memberlist.Delegate interface
func (g *GossipMessageBroker) NodeMeta(limit int) []byte {
	return []byte{}
}

func (g *GossipMessageBroker) NotifyMsg(b []byte) {
	m, err := UnmarshalMessage(b)
	if err != nil {
		g.logger().Printf("unmarshal message error: %s", err)
		return
	}
	if err := g.Receive(m); err != nil {
		g.logger().Printf("receive message error: %s", err)
		return
	}
}

func (g *GossipMessageBroker) GetBroadcasts(overhead, limit int) [][]byte {
	return g.broadcasts.GetBroadcasts(overhead, limit)
}

func (g *GossipMessageBroker) LocalState(join bool) []byte {
	pb, err := g.messenger.LocalState()
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

func (g *GossipMessageBroker) MergeRemoteState(buf []byte, join bool) {
	// Unmarshal nodestate data.
	var pb internal.NodeState
	if err := proto.Unmarshal(buf, &pb); err != nil {
		g.logger().Printf("error unmarshalling nodestate data, err=%s", err)
		return
	}
	err := g.messenger.HandleRemoteState(&pb)
	if err != nil {
		g.logger().Printf("merge state error: %s", err)
	}
}

// logger returns a logger for the GossipMessageBroker.
func (g *GossipMessageBroker) logger() *log.Logger {
	return log.New(g.LogOutput, "", log.LstdFlags)
}

////////////////////////////////////////////////////////////////

// NewGossipMessageBroker returns a new instance of GossipMessageBroker.
func NewGossipMessageBroker(m *Messenger) *GossipMessageBroker {
	g := &GossipMessageBroker{
		LogOutput: os.Stderr,
		messenger: m,
	}

	g.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return g.messenger.Cluster.NodeSet.(*GossipNodeSet).memberlist.NumMembers()
		},
		RetransmitMult: 3,
	}

	return g
}

////////////////////////////////////////////////////////////////

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
