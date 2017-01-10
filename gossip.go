package pilosa

import (
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/memberlist"
)

// GossipNodeSet represents a gossip implementation of NodeSet using memberlist
// GossipNodeSet also represents an implementation of memberlist.Delegate
type GossipNodeSet struct {
	Memberlist *memberlist.Memberlist
	Broadcasts *memberlist.TransmitLimitedQueue

	//config *memberlist.Config
	config *GossipConfig

	messageHandler func(m proto.Message) error
}

func (g *GossipNodeSet) Nodes() []*Node {
	a := make([]*Node, 0, g.Memberlist.NumMembers())
	for _, n := range g.Memberlist.Members() {
		a = append(a, &Node{Host: n.Name})
	}
	return a
}

func (g *GossipNodeSet) Join(nodes []*Node) (int, error) {
	return g.Memberlist.Join(Nodes(nodes).Hosts())
}

func (g *GossipNodeSet) Open() error {
	ml, err := memberlist.Create(g.config.memberlistConfig)
	if err != nil {
		return err
	}
	g.Memberlist = ml

	// attach to gossip seed node
	g.Join([]*Node{&Node{Host: g.config.gossipSeed}}) //TODO: support a list of seeds

	g.Broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return g.Memberlist.NumMembers()
		},
		RetransmitMult: 3,
	}
	return nil
}

func (g *GossipNodeSet) SetMessageHandler(f func(proto.Message) error) {
	g.messageHandler = f
}

// implementation of the messenger.Messenger interface
func (g *GossipNodeSet) SendMessage(pb proto.Message) error {
	msg, err := MarshalMessage(pb)
	if err != nil {
		return err
	}

	b := &broadcast{
		msg:    msg,
		notify: nil,
	}
	g.Broadcasts.QueueBroadcast(b)
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
		return // TODO: this error is getting swallowed
	}
	g.ReceiveMessage(m)
}

func (g *GossipNodeSet) GetBroadcasts(overhead, limit int) [][]byte {
	return g.Broadcasts.GetBroadcasts(overhead, limit)
}

func (g *GossipNodeSet) LocalState(join bool) []byte {
	return []byte{}
}

func (g *GossipNodeSet) MergeRemoteState(buf []byte, join bool) {
	return
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
func NewGossipNodeSet(name string, gossipPort int, gossipSeed string) *GossipNodeSet {
	g := &GossipNodeSet{}

	//TODO: pull memberlist config from pilosa.cfg file
	g.config = &GossipConfig{
		memberlistConfig: memberlist.DefaultLocalConfig(),
		gossipSeed:       gossipSeed,
	}
	g.config.memberlistConfig.Name = name
	g.config.memberlistConfig.BindPort = gossipPort
	g.config.memberlistConfig.GossipNodes = 1
	g.config.memberlistConfig.Delegate = g

	return g
}
