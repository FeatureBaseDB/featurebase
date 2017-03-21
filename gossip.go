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
	broadcasts *memberlist.TransmitLimitedQueue

	config *GossipConfig

	messageHandler     func(m proto.Message) error
	remoteStateHandler func(m proto.Message) error
	localStateSource   func() (proto.Message, error)

	// The writer for any logging.
	LogOutput io.Writer
}

func (g *GossipNodeSet) Nodes() []*Node {
	a := make([]*Node, 0, g.memberlist.NumMembers())
	for _, n := range g.memberlist.Members() {
		a = append(a, &Node{Host: n.Name})
	}
	return a
}

func (g *GossipNodeSet) Join(nodes []*Node) (int, error) {
	return g.memberlist.Join(Nodes(nodes).Hosts())
}

func (g *GossipNodeSet) Open() error {
	ml, err := memberlist.Create(g.config.memberlistConfig)
	if err != nil {
		return err
	}
	g.memberlist = ml

	// attach to gossip seed node
	g.Join([]*Node{&Node{Host: g.config.gossipSeed}}) //TODO: support a list of seeds

	g.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return g.memberlist.NumMembers()
		},
		RetransmitMult: 3,
	}
	return nil
}

func (g *GossipNodeSet) SetMessageHandler(f func(proto.Message) error) {
	g.messageHandler = f
}

func (g *GossipNodeSet) SetRemoteStateHandler(f func(proto.Message) error) {
	g.remoteStateHandler = f
}

func (g *GossipNodeSet) SetLocalStateSource(f func() (proto.Message, error)) {
	g.localStateSource = f
}

// implementation of the messenger.Messenger interface
func (g *GossipNodeSet) SendMessage(pb proto.Message, method string) error {
	msg, err := MarshalMessage(pb)
	if err != nil {
		return err
	}

	// Broadcast asyncronously sends the message directly to each node.
	// An error from any node raises an error on the entire operation.
	// This is a blocking operation.
	//
	// Gossip uses the gossip protocol to eventually deliver the message
	// to every node.
	switch method {
	case "broadcast":
		var eg errgroup.Group
		for _, n := range g.memberlist.Members() {
			// Don't send the message to the local node.
			if n == g.memberlist.LocalNode() {
				continue
			}
			node := n
			eg.Go(func() error {
				return g.memberlist.SendToTCP(node, msg)
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

func (g *GossipNodeSet) ReceiveMessage(pb proto.Message) error {
	err := g.messageHandler(pb)
	if err != nil {
		return err
	}
	return nil
}

// implementation of the memberlist.Delegate interface
func (g *GossipNodeSet) NodeMeta(limit int) []byte {
	return []byte{}
}

func (g *GossipNodeSet) NotifyMsg(b []byte) {
	m, err := UnmarshalMessage(b)
	if err != nil {
		g.logger().Printf("unmarshal message error: %s", err)
		return
	}
	if err := g.ReceiveMessage(m); err != nil {
		g.logger().Printf("receive message error: %s", err)
		return
	}
}

func (g *GossipNodeSet) GetBroadcasts(overhead, limit int) [][]byte {
	return g.broadcasts.GetBroadcasts(overhead, limit)
}

func (g *GossipNodeSet) LocalState(join bool) []byte {

	pb, err := g.localStateSource()
	if err != nil {
		g.logger().Printf("error getting local state, err=%s", err)
		return []byte{}
	}

	// Marshal nodestate data to bytes.
	buf, err := proto.Marshal(pb)
	if err != nil {
		g.logger().Printf("error marshaling nodestate data, err=%s", err)
		return []byte{}
	}
	return buf
}

func (g *GossipNodeSet) MergeRemoteState(buf []byte, join bool) {
	// Unmarshal nodestate data.
	var pb internal.NodeState
	if err := proto.Unmarshal(buf, &pb); err != nil {
		g.logger().Printf("error unmarshaling nodestate data, err=%s", err)
		return
	}
	err := g.remoteStateHandler(&pb)
	if err != nil {
		g.logger().Printf("merge state error: %s", err)
	}
	return
}

// logger returns a logger for the GossipNodeSet.
func (g *GossipNodeSet) logger() *log.Logger {
	return log.New(g.LogOutput, "", log.LstdFlags)
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
	g.config.memberlistConfig.Delegate = g

	return g
}
