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
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pkg/errors"
)

// Ensure GossipMemberSet implements interfaces.
var _ pilosa.BroadcastReceiver = &GossipMemberSet{}
var _ pilosa.Gossiper = &GossipMemberSet{}
var _ memberlist.Delegate = &GossipMemberSet{}

// GossipMemberSet represents a gossip implementation of MemberSet using memberlist.
type GossipMemberSet struct {
	mu         sync.RWMutex
	node       *pilosa.Node
	memberlist *memberlist.Memberlist
	handler    pilosa.BroadcastHandler

	broadcasts *memberlist.TransmitLimitedQueue

	statusHandler pilosa.StatusHandler
	config        *gossipConfig

	Logger pilosa.Logger

	logger    *log.Logger
	transport *Transport
}

// Start implements the BroadcastReceiver interface and sets the BroadcastHandler.
func (g *GossipMemberSet) Start(h pilosa.BroadcastHandler) error {
	g.handler = h
	return nil
}

// GetBindAddr returns the gossip bind address based on config and auto bind port.
// This method is currently only used in a test scenario where a second node needs
// the auto-bind address of the first node to use as its gossip seed.
func (g *GossipMemberSet) GetBindAddr() string {
	return fmt.Sprintf("%s:%d", g.config.memberlistConfig.BindAddr, g.config.memberlistConfig.BindPort)
}

// Open implements the MemberSet interface to start network activity.
func (g *GossipMemberSet) Open(n *pilosa.Node) error {
	if g.handler == nil {
		return fmt.Errorf("must call Start(pilosa.BroadcastHandler) before calling Open()")
	}

	g.node = n

	err := error(nil)
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
func WithTransport(transport *Transport) func(*GossipMemberSet) error {
	return func(g *GossipMemberSet) error {
		g.transport = transport
		return nil
	}
}

// WithLogger is a functional option for providing a logger to NewGossipMemberSet.
func WithLogger(logger *log.Logger) func(*GossipMemberSet) error {
	return func(g *GossipMemberSet) error {
		g.logger = logger
		return nil
	}
}

// NewGossipMemberSet returns a new instance of GossipMemberSet based on options.
func NewGossipMemberSet(name string, cfg *pilosa.Config, server *pilosa.Server, options ...GossipMemberSetOption) (*GossipMemberSet, error) {

	g := &GossipMemberSet{
		Logger: server.Logger,
	}

	// options
	for _, opt := range options {
		if err := opt(g); err != nil {
			return nil, err
		}
	}

	if g.transport == nil {
		port, err := strconv.Atoi(cfg.Gossip.Port)
		if err != nil {
			return nil, fmt.Errorf("convert port: %s", err)
		}

		bindURI, err := pilosa.NewURIFromAddress(cfg.Bind)
		if err != nil {
			return nil, fmt.Errorf("getting uri from bind address: %s", err)
		}
		host := bindURI.Host()

		// Set up the transport.
		transport, err := NewTransport(host, port, g.logger)
		if err != nil {
			return nil, fmt.Errorf("new tranport: %s", err)
		}

		g.transport = transport
	}

	port := g.transport.Net.GetAutoBindPort()

	bindURI, err := pilosa.NewURIFromAddress(cfg.Bind)
	if err != nil {
		return nil, fmt.Errorf("getting uri from bind address (with transport): %s", err)
	}
	host := bindURI.Host()

	var gossipKey []byte
	if cfg.Gossip.Key != "" {
		gossipKey, err = ioutil.ReadFile(cfg.Gossip.Key)
		if err != nil {
			return nil, fmt.Errorf("reading gossip key: %s", err)
		}
	}

	// memberlist config
	conf := memberlist.DefaultWANConfig()
	conf.Transport = g.transport.Net
	conf.Name = name
	conf.BindAddr = host
	conf.BindPort = port
	conf.AdvertisePort = port
	conf.AdvertiseAddr = pilosa.HostToIP(host)
	//
	conf.TCPTimeout = time.Duration(cfg.Gossip.StreamTimeout)
	conf.SuspicionMult = cfg.Gossip.SuspicionMult
	conf.PushPullInterval = time.Duration(cfg.Gossip.PushPullInterval)
	conf.ProbeTimeout = time.Duration(cfg.Gossip.ProbeTimeout)
	conf.ProbeInterval = time.Duration(cfg.Gossip.ProbeInterval)
	conf.GossipNodes = cfg.Gossip.Nodes
	conf.GossipInterval = time.Duration(cfg.Gossip.Interval)
	conf.GossipToTheDeadTime = time.Duration(cfg.Gossip.ToTheDeadTime)
	//
	conf.Delegate = g
	conf.SecretKey = gossipKey
	conf.Events = server.Cluster.EventReceiver.(memberlist.EventDelegate)
	conf.Logger = g.logger

	g.config = &gossipConfig{
		memberlistConfig: conf,
		gossipSeeds:      cfg.Gossip.Seeds,
	}

	g.statusHandler = server

	return g, nil
}

// SendSync implementation of the Broadcaster interface.
func (g *GossipMemberSet) SendSync(pb proto.Message) error {
	msg, err := pilosa.MarshalMessage(pb)
	if err != nil {
		return fmt.Errorf("marshal message: %s", err)
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

// SendAsync implementation of the Gossiper interface.
func (g *GossipMemberSet) SendAsync(pb proto.Message) error {
	msg, err := pilosa.MarshalMessage(pb)
	if err != nil {
		return fmt.Errorf("marshal message: %s", err)
	}

	b := &broadcast{
		msg:    msg,
		notify: nil,
	}
	g.broadcasts.QueueBroadcast(b)
	return nil
}

// NodeMeta implementation of the memberlist.Delegate interface.
func (g *GossipMemberSet) NodeMeta(limit int) []byte {
	buf, err := proto.Marshal(pilosa.EncodeNode(g.node))
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
	pb, err := g.statusHandler.LocalStatus()
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
	err := g.statusHandler.HandleRemoteStatus(&pb)
	if err != nil {
		g.Logger.Printf("merge state error: %s", err)
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

	Logger pilosa.Logger
}

// NewGossipEventReceiver returns a new instance of GossipEventReceiver.
func NewGossipEventReceiver(logger pilosa.Logger) *GossipEventReceiver {
	return &GossipEventReceiver{
		ch:     make(chan memberlist.NodeEvent, 1),
		Logger: logger,
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

		// Get the node from the event.Node meta data.
		var n internal.Node
		if err := proto.Unmarshal(e.Node.Meta, &n); err != nil {
			panic("failed to unmarshal event node meta data")
		}
		node := pilosa.DecodeNode(&n)

		ne := &pilosa.NodeEvent{
			Event: nodeEventType,
			Node:  node,
		}
		if err := g.eventHandler.ReceiveEvent(ne); err != nil {
			g.Logger.Printf("receive event error: %s", err)
		}
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

// Transport is a gossip transport for binding to a port.
type Transport struct {
	//memberlist.Transport
	Net *memberlist.NetTransport
	URI *pilosa.URI
}

// NewTransport returns a NetTransport based on the given host and port.
// It will dynamically bind to a port if port is 0.
// This is useful for test cases where specifiying a port is not reasonable.
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
