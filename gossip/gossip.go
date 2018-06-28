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
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/toml"
	"github.com/pkg/errors"
)

// Ensure GossipMemberSet implements interfaces.
var _ memberlist.Delegate = &GossipMemberSet{}

// GossipMemberSet represents a gossip implementation of MemberSet using memberlist.
type GossipMemberSet struct {
	mu         sync.RWMutex
	memberlist *memberlist.Memberlist
	handler    pilosa.BroadcastHandler

	broadcasts *memberlist.TransmitLimitedQueue

	pserver *pilosa.Server
	config  *gossipConfig

	Logger pilosa.Logger

	logger    *log.Logger
	transport *Transport

	gossipEventReceiver *gossipEventReceiver
}

// Open implements the MemberSet interface to start network activity.
func (g *GossipMemberSet) Open() (err error) {
	g.mu.Lock()
	g.memberlist, err = memberlist.Create(g.config.memberlistConfig)
	g.mu.Unlock()
	if err != nil {
		return errors.Wrap(err, "creating memberlist")
	}

	g.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			g.mu.RLock()
			defer g.mu.RUnlock()
			return g.memberlist.NumMembers()
		},
		RetransmitMult: 3,
	}

	var uris = make([]*pilosa.URI, len(g.config.gossipSeeds))
	for i, addr := range g.config.gossipSeeds {
		uris[i], err = pilosa.NewURIFromAddress(addr)
		if err != nil {
			return fmt.Errorf("new uri from address: %s", err)
		}
	}

	var nodes = make([]*pilosa.Node, len(uris))
	for i, uri := range uris {
		nodes[i] = &pilosa.Node{URI: *uri}
	}

	g.mu.RLock()
	err = g.joinWithRetry(pilosa.URIs(pilosa.Nodes(nodes).URIs()).HostPortStrings())
	g.mu.RUnlock()
	if err != nil {
		return errors.Wrap(err, "joinWithRetry")
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

////////////////////////////////////////////////////////////////

type gossipConfig struct {
	gossipSeeds      []string
	memberlistConfig *memberlist.Config
}

// GossipMemberSetOption describes a functional option for GossipMemberSet.
type GossipMemberSetOption func(*GossipMemberSet) error

// WithTransport is a functional option for providing a transport to NewGossipMemberSet.
func WithTransport(transport *Transport) GossipMemberSetOption {
	return func(g *GossipMemberSet) error {
		g.transport = transport
		return nil
	}
}

// WithLogger is a functional option for providing a logger to NewGossipMemberSet.
func WithLogger(logger *log.Logger) GossipMemberSetOption {
	return func(g *GossipMemberSet) error {
		g.logger = logger
		return nil
	}
}

// NewGossipMemberSet returns a new instance of GossipMemberSet based on options.
func NewGossipMemberSet(cfg Config, s *pilosa.Server, options ...GossipMemberSetOption) (*GossipMemberSet, error) {
	host := s.Node().URI.Host()
	g := &GossipMemberSet{
		Logger: pilosa.NopLogger,
	}

	// options
	for _, opt := range options {
		if err := opt(g); err != nil {
			return nil, errors.Wrap(err, "executing option")
		}
	}
	ger := newGossipEventReceiver(g.logger, s)
	g.gossipEventReceiver = ger

	if g.transport == nil {
		port, err := strconv.Atoi(cfg.Port)
		if err != nil {
			return nil, fmt.Errorf("convert port: %s", err)
		}

		// Set up the transport.
		transport, err := NewTransport(host, port, g.logger)
		if err != nil {
			return nil, fmt.Errorf("new tranport: %s", err)
		}

		g.transport = transport
	}

	port := g.transport.Net.GetAutoBindPort()

	var gossipKey []byte
	var err error
	if cfg.Key != "" {
		gossipKey, err = ioutil.ReadFile(cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("reading gossip key: %s", err)
		}
	}

	// memberlist config
	conf := memberlist.DefaultWANConfig()
	conf.Transport = g.transport.Net
	conf.Name = s.Node().ID
	conf.BindAddr = s.Node().URI.Host()
	conf.BindPort = port
	conf.AdvertisePort = port
	conf.AdvertiseAddr = hostToIP(s.Node().URI.Host())
	//
	conf.TCPTimeout = time.Duration(cfg.StreamTimeout)
	conf.SuspicionMult = cfg.SuspicionMult
	conf.PushPullInterval = time.Duration(cfg.PushPullInterval)
	conf.ProbeTimeout = time.Duration(cfg.ProbeTimeout)
	conf.ProbeInterval = time.Duration(cfg.ProbeInterval)
	conf.GossipNodes = cfg.Nodes
	conf.GossipInterval = time.Duration(cfg.Interval)
	conf.GossipToTheDeadTime = time.Duration(cfg.ToTheDeadTime)
	//
	conf.Delegate = g
	conf.SecretKey = gossipKey
	conf.Events = ger
	conf.Logger = g.logger

	g.config = &gossipConfig{
		memberlistConfig: conf,
		gossipSeeds:      cfg.Seeds,
	}

	g.pserver = s

	return g, nil
}

// NodeMeta implementation of the memberlist.Delegate interface.
func (g *GossipMemberSet) NodeMeta(limit int) []byte {
	buf, err := proto.Marshal(pilosa.EncodeNode(g.pserver.Node()))
	if err != nil {
		g.Logger.Printf("marshal message error: %s", err)
		return []byte{}
	}
	return buf
}

// NotifyMsg implementation of the memberlist.Delegate interface
// called when a user-data message is received.
func (g *GossipMemberSet) NotifyMsg(b []byte) {
	m, err := pilosa.UnmarshalMessage(b)
	if err != nil {
		g.Logger.Printf("unmarshal message error: %s", err)
		return
	}
	if err := g.handler.ReceiveMessage(m); err != nil {
		g.Logger.Printf("receive message error: %s", err)
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
	pb, err := g.pserver.LocalStatus()
	if err != nil {
		g.Logger.Printf("error getting local state, err=%s", err)
		return []byte{}
	}

	// Marshal nodestate data to bytes.
	buf, err := proto.Marshal(pb)
	if err != nil {
		g.Logger.Printf("error marshalling nodestate data, err=%s", err)
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
		g.Logger.Printf("error unmarshalling nodestate data, err=%s", err)
		return
	}
	err := g.pserver.HandleRemoteStatus(&pb)
	if err != nil {
		g.Logger.Printf("merge state error: %s", err)
	}
}

// gossipEventReceiver is used to enable an application to receive
// events about joins and leaves over a channel.
//
// Care must be taken that events are processed in a timely manner from
// the channel, since this delegate will block until an event can be sent.
type gossipEventReceiver struct {
	ch           chan memberlist.NodeEvent
	eventHandler *pilosa.Server

	logger *log.Logger
}

// newGossipEventReceiver returns a new instance of GossipEventReceiver.
func newGossipEventReceiver(logger *log.Logger, pserver *pilosa.Server) *gossipEventReceiver {
	ger := &gossipEventReceiver{
		ch:           make(chan memberlist.NodeEvent, 1),
		logger:       logger,
		eventHandler: pserver,
	}
	go ger.listen()
	return ger
}

func (g *gossipEventReceiver) NotifyJoin(n *memberlist.Node) {
	g.ch <- memberlist.NodeEvent{memberlist.NodeJoin, n}
}

func (g *gossipEventReceiver) NotifyLeave(n *memberlist.Node) {
	g.ch <- memberlist.NodeEvent{memberlist.NodeLeave, n}
}

func (g *gossipEventReceiver) NotifyUpdate(n *memberlist.Node) {
	g.ch <- memberlist.NodeEvent{memberlist.NodeUpdate, n}
}

func (g *gossipEventReceiver) listen() {
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

		// Get the node from the event.Node meta data.
		var n internal.Node
		if err := proto.Unmarshal(e.Node.Meta, &n); err != nil {
			panic("failed to unmarshal event node meta data")
		}
		// node := pilosa.DecodeNode(&n)

		ne := &internal.NodeEventMessage{
			Event: uint32(nodeEventType),
			Node:  &n,
		}
		if err := g.eventHandler.ReceiveMessage(ne); err != nil {
			g.logger.Printf("receive event error: %s", err)
		}
	}
}

// Transport is a gossip transport for binding to a port.
type Transport struct {
	//memberlist.Transport
	Net *memberlist.NetTransport
	URI *pilosa.URI
}

// NewTransport returns a NetTransport based on the given host and port.
// It will dynamically bind to a port if port is 0.
// This is useful for test cases where specifying a port is not reasonable.
//func NewTransport(host string, port int) (*memberlist.NetTransport, error) {
func NewTransport(host string, port int, logger *log.Logger) (*Transport, error) {
	// memberlist config
	conf := memberlist.DefaultWANConfig()
	conf.BindAddr = host
	conf.BindPort = port
	conf.AdvertisePort = port
	conf.Logger = logger

	net, err := newTransport(conf)
	if err != nil {
		return nil, fmt.Errorf("new transport: %s", err)
	}

	uri, err := pilosa.NewURIFromHostPort(host, uint16(net.GetAutoBindPort()))
	if err != nil {
		return nil, fmt.Errorf("new uri from host port: %s", err)
	}

	return &Transport{
		Net: net,
		URI: uri,
	}, nil
}

// newTransport returns a NetTransport based on the memberlist configuration.
// It will dynamically bind to a port if conf.BindPort is 0.
func newTransport(conf *memberlist.Config) (*memberlist.NetTransport, error) {
	nc := &memberlist.NetTransportConfig{
		BindAddrs: []string{conf.BindAddr},
		BindPort:  conf.BindPort,
		Logger:    conf.Logger,
	}

	// See comment below for details about the retry in here.
	makeNetRetry := func(limit int) (*memberlist.NetTransport, error) {
		var err error
		for try := 0; try < limit; try++ {
			var nt *memberlist.NetTransport
			if nt, err = memberlist.NewNetTransport(nc); err == nil {
				return nt, nil
			}
			if strings.Contains(err.Error(), "address already in use") {
				conf.Logger.Printf("[DEBUG] Got bind error: %v", err)
				continue
			}
		}

		return nil, fmt.Errorf("failed to obtain an address: %v", err)
	}

	// The dynamic bind port operation is inherently racy because
	// even though we are using the kernel to find a port for us, we
	// are attempting to bind multiple protocols (and potentially
	// multiple addresses) with the same port number. We build in a
	// few retries here since this often gets transient errors in
	// busy unit tests.
	limit := 1
	if conf.BindPort == 0 {
		limit = 10
	}

	nt, err := makeNetRetry(limit)
	if err != nil {
		return nil, fmt.Errorf("Could not set up network transport: %v", err)
	}

	return nt, nil
}

// Config holds toml-friendly memberlist configuration.
type Config struct {
	// Port indicates the port to which pilosa should bind for internal state sharing.
	Port  string   `toml:"port"`
	Seeds []string `toml:"seeds"`
	Key   string   `toml:"key"`
	// StreamTimeout is the timeout for establishing a stream connection with
	// a remote node for a full state sync, and for stream read and write
	// operations. Maps to memberlist TCPTimeout.
	StreamTimeout toml.Duration `toml:"stream-timeout"`
	// SuspicionMult is the multiplier for determining the time an
	// inaccessible node is considered suspect before declaring it dead.
	// The actual timeout is calculated using the formula:
	//
	//   SuspicionTimeout = SuspicionMult * log(N+1) * ProbeInterval
	//
	// This allows the timeout to scale properly with expected propagation
	// delay with a larger cluster size. The higher the multiplier, the longer
	// an inaccessible node is considered part of the cluster before declaring
	// it dead, giving that suspect node more time to refute if it is indeed
	// still alive.
	SuspicionMult int `toml:"suspicion-mult"`
	// PushPullInterval is the interval between complete state syncs.
	// Complete state syncs are done with a single node over TCP and are
	// quite expensive relative to standard gossiped messages. Setting this
	// to zero will disable state push/pull syncs completely.
	//
	// Setting this interval lower (more frequent) will increase convergence
	// speeds across larger clusters at the expense of increased bandwidth
	// usage.
	PushPullInterval toml.Duration `toml:"push-pull-interval"`
	// ProbeInterval and ProbeTimeout are used to configure probing behavior
	// for memberlist.
	//
	// ProbeInterval is the interval between random node probes. Setting
	// this lower (more frequent) will cause the memberlist cluster to detect
	// failed nodes more quickly at the expense of increased bandwidth usage.
	//
	// ProbeTimeout is the timeout to wait for an ack from a probed node
	// before assuming it is unhealthy. This should be set to 99-percentile
	// of RTT (round-trip time) on your network.
	ProbeInterval toml.Duration `toml:"probe-interval"`
	ProbeTimeout  toml.Duration `toml:"probe-timeout"`

	// Interval and Nodes are used to configure the gossip
	// behavior of memberlist.
	//
	// Interval is the interval between sending messages that need
	// to be gossiped that haven't been able to piggyback on probing messages.
	// If this is set to zero, non-piggyback gossip is disabled. By lowering
	// this value (more frequent) gossip messages are propagated across
	// the cluster more quickly at the expense of increased bandwidth.
	//
	// Nodes is the number of random nodes to send gossip messages to
	// per Interval. Increasing this number causes the gossip messages
	// to propagate across the cluster more quickly at the expense of
	// increased bandwidth.
	//
	// ToTheDeadTime is the interval after which a node has died that
	// we will still try to gossip to it. This gives it a chance to refute.
	Interval      toml.Duration `toml:"interval"`
	Nodes         int           `toml:"nodes"`
	ToTheDeadTime toml.Duration `toml:"to-the-dead-time"`
}

// hostToIP converts host to an IP4 address based on net.LookupIP().
func hostToIP(host string) string {
	// if host is not an IP addr, check net.LookupIP()
	if net.ParseIP(host) == nil {
		hosts, err := net.LookupIP(host)
		if err != nil {
			return host
		}
		for _, h := range hosts {
			// this restricts pilosa to IP4
			if h.To4() != nil {
				return h.String()
			}
		}
	}
	return host
}
