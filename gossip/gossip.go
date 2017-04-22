package gossip

import (
	"fmt"
	"io"
	"log"
	"os"

	"golang.org/x/sync/errgroup"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
)

// StateHandler specifies two methods which an object must implement to share
// state in the cluster. These are used by the GossipNodeSet to implement the
// LocalState and MergeRemoteState methods of memberlist.Delegate
type StateHandler interface {
	LocalState() (proto.Message, error)
	HandleRemoteState(proto.Message) error
}

// GossipNodeSet represents a gossip implementation of NodeSet using memberlist
// GossipNodeSet also represents a gossip implementation of pilosa.Broadcaster
// GossipNodeSet also represents an implementation of memberlist.Delegate
type GossipNodeSet struct {
	memberlist *memberlist.Memberlist
	handler    pilosa.BroadcastHandler

	broadcasts *memberlist.TransmitLimitedQueue

	stateHandler StateHandler
	config       *GossipConfig

	// The writer for any logging.
	LogOutput io.Writer
}

func (g *GossipNodeSet) Nodes() []*pilosa.Node {
	a := make([]*pilosa.Node, 0, g.memberlist.NumMembers())
	for _, n := range g.memberlist.Members() {
		a = append(a, &pilosa.Node{Host: n.Name})
	}
	return a
}

func (g *GossipNodeSet) Start(h pilosa.BroadcastHandler) error {
	g.handler = h
	return nil
}

func (g *GossipNodeSet) Open() error {
	if g.handler == nil {
		return fmt.Errorf("opening GossipNodeSet: you must call Start(pilosa.BroadcastHandler) before calling Open()")
	}
	ml, err := memberlist.Create(g.config.memberlistConfig)
	if err != nil {
		return err
	}
	g.memberlist = ml

	// attach to gossip seed node
	nodes := []*pilosa.Node{&pilosa.Node{Host: g.config.gossipSeed}} //TODO: support a list of seeds
	_, err = g.memberlist.Join(pilosa.Nodes(nodes).Hosts())
	if err != nil {
		return err
	}
	g.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return ml.NumMembers()
		},
		RetransmitMult: 3,
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
func NewGossipNodeSet(name string, gossipHost string, gossipPort int, gossipSeed string, sh StateHandler) *GossipNodeSet {
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

	g.stateHandler = sh

	return g
}

// SendSync implementation of the Broadcaster interface
func (g *GossipNodeSet) SendSync(pb proto.Message) error {
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

// SendAsync implementation of the Broadcaster interface
func (g *GossipNodeSet) SendAsync(pb proto.Message) error {
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

// implementation of the memberlist.Delegate interface
func (g *GossipNodeSet) NodeMeta(limit int) []byte {
	return []byte{}
}

func (g *GossipNodeSet) NotifyMsg(b []byte) {
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

func (g *GossipNodeSet) GetBroadcasts(overhead, limit int) [][]byte {
	return g.broadcasts.GetBroadcasts(overhead, limit)
}

func (g *GossipNodeSet) LocalState(join bool) []byte {
	pb, err := g.stateHandler.LocalState()
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

func (g *GossipNodeSet) MergeRemoteState(buf []byte, join bool) {
	// Unmarshal nodestate data.
	var pb internal.NodeState
	if err := proto.Unmarshal(buf, &pb); err != nil {
		g.logger().Printf("error unmarshalling nodestate data, err=%s", err)
		return
	}
	err := g.stateHandler.HandleRemoteState(&pb)
	if err != nil {
		g.logger().Printf("merge state error: %s", err)
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
