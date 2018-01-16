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
	"os"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pkg/errors"
)

// Ensure GossipNodeSet implements interfaces.
var _ pilosa.BroadcastReceiver = &GossipNodeSet{}
var _ pilosa.Gossiper = &GossipNodeSet{}
var _ memberlist.Delegate = &GossipNodeSet{}

// GossipNodeSet represents a gossip implementation of NodeSet using memberlist
// GossipNodeSet also represents a gossip implementation of pilosa.Broadcaster
// GossipNodeSet also represents an implementation of memberlist.Delegate
type GossipNodeSet struct {
	memberlist *memberlist.Memberlist
	handler    pilosa.BroadcastHandler

	broadcasts *memberlist.TransmitLimitedQueue

	statusHandler pilosa.StatusHandler
	config        *gossipConfig

	// The writer for any logging.
	LogOutput io.Writer
}

// Nodes implements the NodeSet interface and returns a list of nodes in the cluster.
func (g *GossipNodeSet) Nodes() []*pilosa.Node {
	a := make([]*pilosa.Node, 0, g.memberlist.NumMembers())
	for _, n := range g.memberlist.Members() {
		a = append(a, &pilosa.Node{Scheme: "gossip", Host: n.Name})
	}
	return a
}

// Start implements the BroadcastReceiver interface and sets the BroadcastHandler
func (g *GossipNodeSet) Start(h pilosa.BroadcastHandler) error {
	g.handler = h
	return nil
}

// Seed returns the gossipSeed determined by the config.
func (g *GossipNodeSet) Seed() string {
	return g.config.gossipSeed
}

// Open implements the NodeSet interface to start network activity.
func (g *GossipNodeSet) Open() error {
	if g.handler == nil {
		return fmt.Errorf("opening GossipNodeSet: you must call Start(pilosa.BroadcastHandler) before calling Open()")
	}
	ml, err := memberlist.Create(g.config.memberlistConfig)
	if err != nil {
		return errors.Wrap(err, "creating memberlist")
	}
	g.memberlist = ml
	g.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return ml.NumMembers()
		},
		RetransmitMult: 3,
	}

	// attach to gossip seed node
	nodes := []*pilosa.Node{&pilosa.Node{Scheme: "gossip", Host: g.config.gossipSeed}} //TODO: support a list of seeds
	err = g.joinWithRetry(pilosa.Nodes(nodes).Hosts())
	if err != nil {
		return errors.Wrap(err, "joinWithRetry")
	}
	return nil
}

// joinWithRetry wraps the standard memberlist Join function in a retry.
func (g *GossipNodeSet) joinWithRetry(hosts []string) error {
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

// logger returns a logger for the GossipNodeSet.
func (g *GossipNodeSet) logger() *log.Logger {
	return log.New(g.LogOutput, "", log.LstdFlags)
}

////////////////////////////////////////////////////////////////

type gossipConfig struct {
	gossipSeed       string
	memberlistConfig *memberlist.Config
}

// newTransport returns a NetTransport based on the memberlist configuration.
// It will dynamically bind to a port if conf.BindPort is 0.
// This is useful for test cases where specifiying a port is not reasonable.
func newTransport(conf *memberlist.Config) (*memberlist.NetTransport, error) {
	if conf.LogOutput != nil && conf.Logger != nil {
		return nil, fmt.Errorf("Cannot specify both LogOutput and Logger. Please choose a single log configuration setting.")
	}

	logDest := conf.LogOutput
	if logDest == nil {
		logDest = os.Stderr
	}

	logger := conf.Logger
	if logger == nil {
		logger = log.New(logDest, "", log.LstdFlags)
	}

	nc := &memberlist.NetTransportConfig{
		BindAddrs: []string{conf.BindAddr},
		BindPort:  conf.BindPort,
		Logger:    logger,
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
				logger.Printf("[DEBUG] Got bind error: %v", err)
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
	if conf.BindPort == 0 {
		port := nt.GetAutoBindPort()
		conf.BindPort = port
		conf.AdvertisePort = port
		logger.Printf("[DEBUG] Using dynamic bind port %d", port)
	}

	return nt, nil
}

// NewGossipNodeSet returns a new instance of GossipNodeSet.
func NewGossipNodeSet(name string, gossipHost string, gossipPort int, gossipSeed string, server *pilosa.Server, secretKey []byte) (*GossipNodeSet, error) {
	g := &GossipNodeSet{
		LogOutput: server.LogOutput,
	}

	conf := memberlist.DefaultWANConfig()
	conf.BindPort = gossipPort
	conf.AdvertisePort = gossipPort

	//TODO: pull memberlist config from pilosa.cfg file
	g.config = &gossipConfig{
		memberlistConfig: conf,
		gossipSeed:       gossipSeed,
	}

	g.config.memberlistConfig.Name = name
	g.config.memberlistConfig.BindAddr = gossipHost
	g.config.memberlistConfig.AdvertiseAddr = pilosa.HostToIP(gossipHost)
	g.config.memberlistConfig.Delegate = g
	g.config.memberlistConfig.SecretKey = secretKey

	g.statusHandler = server

	// set up the transport
	transport, err := newTransport(g.config.memberlistConfig)
	if err != nil {
		return nil, err
	}
	g.config.memberlistConfig.Transport = transport

	// If no gossipSeed is provided, use local host:port.
	if gossipSeed == "" {
		g.config.gossipSeed = fmt.Sprintf("%s:%d", gossipHost, g.config.memberlistConfig.BindPort)
	}

	return g, nil
}

// SendAsync implementation of the Gossiper interface.
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

// NodeMeta implementation of the memberlist.Delegate interface.
func (g *GossipNodeSet) NodeMeta(limit int) []byte {
	return []byte{}
}

// NotifyMsg implementation of the memberlist.Delegate interface
// called when a user-data message is received.
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

// GetBroadcasts implementation of the memberlist.Delegate interface
// called when user data messages can be broadcast.
func (g *GossipNodeSet) GetBroadcasts(overhead, limit int) [][]byte {
	return g.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState implementation of the memberlist.Delegate interface
// sends this Node's state data.
func (g *GossipNodeSet) LocalState(join bool) []byte {
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
// receive and process the remote side side's LocalState.
func (g *GossipNodeSet) MergeRemoteState(buf []byte, join bool) {
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
