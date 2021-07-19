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

package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/topology"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/clientv3util"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/etcdserver/api/v3client"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/transport"
	"go.etcd.io/etcd/pkg/types"
)

type Options struct {
	Name         string `toml:"name"`
	Dir          string `toml:"dir"`
	LClientURL   string `toml:"listen-client-url"`
	AClientURL   string `toml:"advertise-client-url"`
	LPeerURL     string `toml:"listen-peer-url"`
	APeerURL     string `toml:"advertise-peer-url"`
	ClusterURL   string `toml:"cluster-url"`
	InitCluster  string `toml:"initial-cluster"`
	ClusterName  string `toml:"cluster-name"`
	HeartbeatTTL int64  `toml:"heartbeat-ttl"`
	// TLS provided tls files
	TrustedCAFile  string `toml:"tls-trusted-cafile"`
	ClientCertFile string `toml:"tls-cert-file"`
	ClientKeyFile  string `toml:"tls-key-file"`
	PeerCertFile   string `toml:"tls-peer-cert-file"`
	PeerKeyFile    string `toml:"tls-peer-key-file"`

	LPeerSocket   []*net.TCPListener
	LClientSocket []*net.TCPListener

	BootstrapTimeout time.Duration
}

var (
	_ disco.DisCo     = &Etcd{}
	_ disco.Schemator = &Etcd{}
	_ disco.Stator    = &Etcd{}
	_ disco.Metadator = &Etcd{}
	_ disco.Resizer   = &Etcd{}
	_ disco.Sharder   = &Etcd{}
)

const (
	// We put all the things the node-watcher watches in /node so we
	// can use a single watcher for them.
	nodePrefix      = "/node/"
	heartbeatPrefix = nodePrefix + "heartbeat/"
	schemaPrefix    = "/schema/"
	resizePrefix    = nodePrefix + "resize/"
	metadataPrefix  = nodePrefix + "metadata/"
	shardPrefix     = "/shard/"
)

var (
	etcdLeaderChanged = etcdserver.ErrLeaderChanged.Error()
)

// nodeData is an internal tracker of the data we're keeping about
// nodes in etcd, which we update from data collected either directly
// from the KV, or via heartbeats.
//
// Any change to the topology.Node should create a new Node rather
// than reusing the old one, so we can return the structure and not worry
// about data races.
//
// We have to track the revisions of individual components so we can
// discard updates which are genuinely out-of-order for a given field,
// but still handle cases where we get updates to several fields that
// reach us out of order.
type nodeData struct {
	heartbeatState string
	resizeState    string
	metadata       []byte
	topologyNode   *topology.Node
}

func (n *nodeData) computedState() disco.NodeState {
	if n.resizeState != "" {
		return disco.NodeStateResizing
	}
	if n.heartbeatState != "" {
		return disco.NodeState(n.heartbeatState)
	}
	return disco.NodeStateUnknown
}

type Etcd struct {
	options  Options
	replicas int

	e     *embed.Etcd
	cli   *clientv3.Client
	cliMu sync.Mutex

	heartbeatLeasedKV, resizeLeasedKV *leasedKV

	// We have a watcher running. watchCancel() cancels its context.
	watchCancel func()

	// knownNodes and sortedNodes get updated by data coming in from
	// watchers. Any change to the contents of a *topology.Node here
	// should be implemented by making a new one and replacing the pointer,
	// so the old pointer stays valid and can be used.
	nodeMu          sync.Mutex
	nodeRev         int64
	knownNodes      map[string]*nodeData
	sortedNodes     []*topology.Node
	nodeStates      map[string]disco.NodeState
	nodeStatesDirty bool // do we need to remake the nodeStates map to use it?

	// we want to inherit parent's logging functionality
	logger logger.Logger
}

func NewEtcd(opt Options, logger logger.Logger, replicas int) *Etcd {
	e := &Etcd{
		options:    opt,
		logger:     logger,
		replicas:   replicas,
		knownNodes: make(map[string]*nodeData),
		nodeStates: make(map[string]disco.NodeState),
	}

	if e.options.HeartbeatTTL == 0 {
		e.options.HeartbeatTTL = 5 // seconds
	}
	return e
}

// Close implements io.Closer
func (e *Etcd) Close() error {
	if e.watchCancel != nil {
		e.watchCancel()
	}
	if e.e != nil {
		if e.resizeLeasedKV != nil {
			e.resizeLeasedKV.Stop()
			e.resizeLeasedKV = nil
		}
		if e.heartbeatLeasedKV != nil {
			e.heartbeatLeasedKV.Stop()
		}

		e.e.Close()
		<-e.e.Server.StopNotify()
	}

	if e.cli != nil {
		e.cli.Close()
	}

	return nil
}

// retryClient attempts to do a thing, but also tries to handle the
// specific case where the client fails because of a leader election,
// in which case we need to restart the client and retry the thing.
//
// We have to let go of the lock while calling `fn` because some fn are
// long-lasting ones, like watchNodesOnce. So we grab a local copy of
// the client object, then call things on that object. This should error
// out sanely instead of panicing if we close the client while something
// is running on it.
func (e *Etcd) retryClient(fn func(cli *clientv3.Client) error) (err error) {
	e.cliMu.Lock()
	cli := e.cli
	e.cliMu.Unlock()
	if err = fn(cli); err == nil || err.Error() != etcdLeaderChanged {
		// either it's nil or it's an error we don't try to handle here
		return err
	}
	// we can't do much with an error from closing e.cli at this point, so
	// we try again.
	e.cliMu.Lock()
	if cli != e.cli {
		cli = e.cli
		e.cliMu.Unlock()
		return fn(cli)
	}
	_ = cli.Close()
	cli = v3client.New(e.e.Server)
	e.cli = cli
	e.cliMu.Unlock()
	return fn(cli)
}

func parseOptions(opt Options) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Debug = false // true gives data races on grpc.EnableTracing in etcd
	cfg.LogLevel = "error"
	cfg.Logger = "zap"
	cfg.Name = opt.Name
	cfg.Dir = opt.Dir
	cfg.InitialClusterToken = opt.ClusterName
	cfg.BootstrapTimeout = opt.BootstrapTimeout
	cfg.LCUrls = types.MustNewURLs([]string{opt.LClientURL})
	if opt.AClientURL != "" {
		cfg.ACUrls = types.MustNewURLs([]string{opt.AClientURL})
	} else {
		cfg.ACUrls = cfg.LCUrls
	}
	cfg.LPUrls = types.MustNewURLs([]string{opt.LPeerURL})
	if opt.APeerURL != "" {
		cfg.APUrls = types.MustNewURLs([]string{opt.APeerURL})
	} else {
		cfg.APUrls = cfg.LPUrls
	}

	lps := make([]*net.TCPListener, len(opt.LPeerSocket))
	copy(lps, opt.LPeerSocket)
	cfg.LPeerSocket = lps

	lcs := make([]*net.TCPListener, len(opt.LPeerSocket))
	copy(lcs, opt.LClientSocket)
	cfg.LClientSocket = lcs

	if opt.InitCluster != "" {
		cfg.InitialCluster = opt.InitCluster
		cfg.ClusterState = embed.ClusterStateFlagNew
	} else {
		cfg.InitialCluster = cfg.Name + "=" + opt.APeerURL
	}

	if opt.ClusterURL != "" {
		cfg.ClusterState = embed.ClusterStateFlagExisting

		cli, err := clientv3.NewFromURL(opt.ClusterURL)
		if err != nil {
			panic(err)
		}
		defer cli.Close()

		log.Println("Cluster Members:")
		mIDs, mNames, mURLs := memberList(cli)
		for i, id := range mIDs {
			log.Printf("\tid: %d, name: %s, url: %s\n", id, mNames[i], mURLs[i])
			cfg.InitialCluster += "," + mNames[i] + "=" + mURLs[i]
		}

		log.Println("Joining Cluster:")
		id, name := memberAdd(cli, opt.APeerURL)
		log.Printf("\tid: %d, name: %s\n", id, name)
	}
	// can only use tls if not using pre-configured listeners
	cfg.ClientTLSInfo = transport.TLSInfo{
		TrustedCAFile: opt.TrustedCAFile,
		CertFile:      opt.ClientCertFile,
		KeyFile:       opt.ClientKeyFile,
	}
	cfg.PeerTLSInfo = transport.TLSInfo{
		TrustedCAFile: opt.TrustedCAFile,
		CertFile:      opt.PeerCertFile,
		KeyFile:       opt.PeerKeyFile,
	}

	return cfg
}

// Start starts etcd and hearbeat
func (e *Etcd) Start(ctx context.Context) (_ disco.InitialClusterState, err error) {
	opts := parseOptions(e.options)
	state := disco.InitialClusterState(opts.ClusterState)

	e.e, err = embed.StartEtcd(opts)
	if err != nil {
		return state, errors.Wrap(err, "starting etcd")
	}
	// If we are returning an error, the caller won't be shutting us down
	// later, so we have to stop the server ourselves.
	defer func() {
		if err != nil {
			e.e.Server.Stop()
		}
	}()
	e.cli = v3client.New(e.e.Server)

	select {
	case <-ctx.Done():
		return state, ctx.Err()

	case err := <-e.e.Err():
		return state, err

	case <-e.e.Server.ReadyNotify():
		members := e.e.Server.Cluster().Members()
		e.nodeMu.Lock()
		defer e.nodeMu.Unlock()
		// mark everything unknown so we show a state for nodes we haven't
		// heard from yet.
		for _, member := range members {
			peerID := member.ID.String()
			e.knownNodes[peerID] = &nodeData{
				topologyNode: &topology.Node{
					ID:    peerID,
					State: disco.NodeStateUnknown,
				},
			}
			e.nodeStates[peerID] = disco.NodeStateUnknown
		}
		e.nodeStatesDirty = true
		return state, e.startHeartbeatAndWatcher(ctx)
	}
}

// startHeartbeatAndWatcher spins up the heartbeat, and also a background
// watcher that watches for changes to events we care about.
func (e *Etcd) startHeartbeatAndWatcher(ctx context.Context) error {
	key := heartbeatPrefix + e.e.Server.ID().String()
	e.heartbeatLeasedKV = newLeasedKV(e, key, e.options.HeartbeatTTL)

	if err := e.heartbeatLeasedKV.Start(string(disco.NodeStateStarting)); err != nil {
		return errors.Wrap(err, "startHeartbeat: starting a new heartbeat")
	}
	// WatchNodes does not check for an error, and will need to be shut
	// down later. We only get this far at a point where we're returning
	// a nil error, and thus, the caller is expected to cleanly shut down
	// the server later.
	go e.WatchNodes()
	return nil
}

func (e *Etcd) NodeState(ctx context.Context, peerID string) (disco.NodeState, error) {
	return e.nodeState(ctx, peerID)
}

func (e *Etcd) nodeState(ctx context.Context, peerID string) (disco.NodeState, error) {
	e.nodeMu.Lock()
	defer e.nodeMu.Unlock()
	err := e.populateNodeStates(ctx)
	return e.nodeStates[peerID], err
}

func (e *Etcd) NodeStates(ctx context.Context) (map[string]disco.NodeState, error) {
	e.nodeMu.Lock()
	defer e.nodeMu.Unlock()
	err := e.populateNodeStates(ctx)
	return e.nodeStates, err
}

func (e *Etcd) Started(ctx context.Context) (err error) {
	return e.heartbeatLeasedKV.Set(ctx, string(disco.NodeStateStarted))
}

func (e *Etcd) ID() string {
	if e.e == nil || e.e.Server == nil {
		return ""
	}
	return e.e.Server.ID().String()
}

func (e *Etcd) Peers() []*disco.Peer {
	var peers []*disco.Peer
	for _, member := range e.e.Server.Cluster().Members() {
		peers = append(peers, &disco.Peer{ID: member.ID.String(), URL: member.PickPeerURL()})
	}
	return peers
}

func (e *Etcd) IsLeader() bool {
	if e.e == nil || e.e.Server == nil {
		return false
	}
	return e.e.Server.Leader() == e.e.Server.ID()
}

func (e *Etcd) Leader() *disco.Peer {
	id := e.e.Server.Leader()
	peer := &disco.Peer{ID: id.String()}

	if m := e.e.Server.Cluster().Member(id); m != nil {
		peer.URL = m.PickPeerURL()
	}

	return peer
}

func (e *Etcd) ClusterState(ctx context.Context) (out disco.ClusterState, err error) {
	if e.e == nil {
		return disco.ClusterStateUnknown, nil
	}

	var (
		heartbeats int = 0
		resize     bool
		starting   bool
	)
	e.nodeMu.Lock()
	err = e.populateNodeStates(ctx)
	states := e.nodeStates
	e.nodeMu.Unlock()
	if err != nil {
		e.logger.Printf("ClusterState %q: getting node states: %v", e.options.Name, states)
		return disco.ClusterStateUnknown, err
	}
	for _, state := range states {
		switch state {
		case disco.NodeStateStarting:
			starting = true
		case disco.NodeStateResizing:
			resize = true
		case disco.NodeStateUnknown:
			continue
		}

		heartbeats++
	}

	if resize {
		return disco.ClusterStateResizing, nil
	}

	if starting {
		return disco.ClusterStateStarting, nil
	}

	if heartbeats < len(states) {
		if len(states)-heartbeats >= e.replicas {
			return disco.ClusterStateDown, nil
		}

		return disco.ClusterStateDegraded, nil
	}

	return disco.ClusterStateNormal, nil
}

func (e *Etcd) Resize(ctx context.Context) (func([]byte) error, error) {
	key := path.Join(resizePrefix, e.e.Server.ID().String())
	if e.resizeLeasedKV == nil {
		e.resizeLeasedKV = newLeasedKV(e, key, e.options.HeartbeatTTL)
	}

	if err := e.resizeLeasedKV.Start(""); err != nil {
		return nil, errors.Wrap(err, "Resize: creates a new hearbeat")
	}

	return func(value []byte) error {
		log.Println("Update progress:", key, string(value))
		return e.putKey(ctx, key, string(value), clientv3.WithIgnoreLease())
	}, nil
}

func (e *Etcd) DoneResize() error {
	if e.resizeLeasedKV != nil {
		e.resizeLeasedKV.Stop()
	}

	e.resizeLeasedKV = nil
	return nil
}

func (e *Etcd) Watch(ctx context.Context, peerID string, onUpdate func([]byte) error) error {
	key := path.Join(resizePrefix, peerID)
	for resp := range e.cli.Watch(ctx, key) {
		if err := resp.Err(); err != nil {
			return errors.Wrapf(err, "Watch: key (%s) response", key)
		}

		for _, ev := range resp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				if onUpdate != nil && ev.Kv.Value != nil {
					if err := onUpdate(ev.Kv.Value); err != nil {
						return err
					}
				}

			case mvccpb.DELETE:
				// nothing to watch - key was deleted
				return errors.WithMessagef(disco.ErrKeyDeleted, "Watch key %s", key)
			}
		}
	}

	return nil
}

// parseNodeKey reads heartbeatPrefix + "23" and yields (heartbeatPrefix, "23", nil).
func parseNodeKey(key []byte) (prefix string, peerID string, err error) {
	// we're looking for things starting with nodePrefix
	if !bytes.HasPrefix(key, []byte(nodePrefix)) {
		return "", "", fmt.Errorf("not a node key: %q", key)
	}
	peerIndex := bytes.LastIndex(key, []byte("/"))
	if peerIndex < 6 {
		return "", "", fmt.Errorf("not a valid node key: %q", key)
	}
	return string(key[:peerIndex+1]), string(key[peerIndex+1:]), nil
}

// deleteNodeData is like putNodeData, but handles deletes rather than cases
// where a value exists. you should call it with the node mutex locked.
func (e *Etcd) deleteNodeData(key []byte, revision int64) error {
	prefix, peerID, err := parseNodeKey(key)
	if err != nil {
		return err
	}
	if revision > e.nodeRev {
		e.nodeRev = revision
	}
	switch prefix {
	case heartbeatPrefix:
		if e.knownNodes[peerID] == nil {
			e.knownNodes[peerID] = &nodeData{}
		}
		e.knownNodes[peerID].heartbeatState = ""
		e.nodeStatesDirty = true
	case metadataPrefix:
		if e.knownNodes[peerID] == nil {
			e.knownNodes[peerID] = &nodeData{}
		}
		e.knownNodes[peerID].metadata = nil
		e.knownNodes[peerID].topologyNode = &topology.Node{}
		e.nodeStatesDirty = true
	case resizePrefix:
		if e.knownNodes[peerID] == nil {
			e.knownNodes[peerID] = &nodeData{}
		}
		e.knownNodes[peerID].resizeState = ""
		e.nodeStatesDirty = true
	default:
		return fmt.Errorf("node watch: invalid prefix %q\n", prefix)
	}
	return nil
}

// putNodeData does the actual updating of the node state maps, etc,
// given an incoming heartbeat, metadata, or resizing change. It requires
// that you already hold the node mutex.
func (e *Etcd) putNodeData(key []byte, value []byte, revision int64) (err error) {
	prefix, peerID, err := parseNodeKey(key)
	if err != nil {
		return err
	}
	if revision > e.nodeRev {
		e.nodeRev = revision
	}
	switch prefix {
	case heartbeatPrefix:
		if e.knownNodes[peerID] == nil {
			e.knownNodes[peerID] = &nodeData{}
		}
		e.knownNodes[peerID].heartbeatState = string(value)
		e.nodeStatesDirty = true
	case metadataPrefix:
		if e.knownNodes[peerID] == nil {
			e.knownNodes[peerID] = &nodeData{}
		}
		e.knownNodes[peerID].metadata = value
		var newNode topology.Node
		err := json.Unmarshal(value, &newNode)
		if err != nil {
			return fmt.Errorf("json unmarshal of node metadata: %v\n", err)
		}
		e.knownNodes[peerID].topologyNode = &newNode
		// This saves us one remake of the node later, probably.
		e.knownNodes[peerID].topologyNode.State = e.knownNodes[peerID].computedState()
		e.nodeStatesDirty = true
	case resizePrefix:
		if e.knownNodes[peerID] == nil {
			e.knownNodes[peerID] = &nodeData{}
		}
		e.knownNodes[peerID].resizeState = string(value)
		e.nodeStatesDirty = true
	default:
		return fmt.Errorf("node watch: invalid prefix %q\n", prefix)
	}
	return nil
}

// compute the states of all the nodes. we compute all of them because
// we might have returned the old map in response to a query, so we want to
// make a new one. You should have the node state lock held when you call this.
func (e *Etcd) populateNodeStates(ctx context.Context) error {
	if !e.nodeStatesDirty {
		return nil
	}
	e.nodeStates = make(map[string]disco.NodeState, len(e.knownNodes))
	e.sortedNodes = make([]*topology.Node, 0, len(e.knownNodes))
	for peerID, data := range e.knownNodes {
		newState := data.computedState()
		e.nodeStates[peerID] = newState
		// update the state with the current state, so we can
		// reuse these nodes later. sortedNodes may end up shorter
		// than the whole node list if we don't have all the nodes
		// yet!
		if data.topologyNode != nil {
			if data.topologyNode.State != newState {
				newNode := *data.topologyNode
				newNode.State = newState
				data.topologyNode = &newNode
			}
			e.sortedNodes = append(e.sortedNodes, data.topologyNode)
		}
	}
	// sort list by ID. list now contains sorted nodes which have their
	// current states.
	sort.Sort(topology.ByID(e.sortedNodes))
	e.nodeStatesDirty = false
	return nil
}

// watchNodesOnce is a helper function to use with the retry logic
// to let us restart the client if we need to.
func (e *Etcd) watchNodesOnce(ctx context.Context, cli *clientv3.Client) (err error) {
	e.nodeMu.Lock()
	// we are looking for revisions HIGHER than the highest revision we've
	// currently seen, we don't want one equal to it.
	minRev := e.nodeRev + 1
	e.nodeMu.Unlock()
	for resp := range cli.Watch(ctx, nodePrefix, clientv3.WithPrefix(), clientv3.WithRev(minRev)) {
		if err := resp.Err(); err != nil {
			return err
		}
		// lock the node mutex for this whole process of updating so
		// we never see partial updates; everything that comes into the
		// watcher as a single message will be processed atomically.
		e.nodeMu.Lock()
		for _, ev := range resp.Events {
			switch ev.Type {
			case mvccpb.PUT:
				err := e.putNodeData(ev.Kv.Key, ev.Kv.Value, ev.Kv.ModRevision)
				if err != nil {
					e.logger.Printf("put event: %v", err)
				}
			case mvccpb.DELETE:
				err := e.deleteNodeData(ev.Kv.Key, ev.Kv.ModRevision)
				if err != nil {
					e.logger.Printf("delete event: %v", err)
				}
			default:
				e.logger.Printf("watchp %q: unknown event %#v", e.options.Name, ev)
			}
		}
		e.nodeMu.Unlock()

	}
	return nil
}

// WatchNodes monitors changes to /heartbeat/, /resizing/, and /metadata/;
// basically, it catches changes to cluster state, but ignores the schema.
func (e *Etcd) WatchNodes() {
	ctx, cancel := context.WithCancel(context.Background())
	e.watchCancel = cancel
	watchInContext := func(cli *clientv3.Client) error {
		return e.watchNodesOnce(ctx, cli)
	}
	// retryClient will retry on leader failure, but not for other failures
	// such as ErrCompacted which can terminate a watch. But we want to resume
	// watching again as long as our context isn't cancelled. The context
	// should get cancelled when this Etcd gets shut down.
	for ctx.Err() == nil {
		err := e.retryClient(watchInContext)
		if err != nil {
			e.logger.Printf("WatchNodes: error from watch client: %v", err)
		}
		// delay slightly on error so we don't go completely crazy
		time.Sleep(1 * time.Second)
	}
}

func (e *Etcd) DeleteNode(ctx context.Context, nodeID string) error {
	id, err := types.IDFromString(nodeID)
	if err != nil {
		return err
	}

	_, err = e.cli.MemberRemove(ctx, uint64(id))
	if err != nil {
		return errors.Wrap(err, "DeleteNode: removes an existing member from the cluster")
	}

	return nil
}

func (e *Etcd) Schema(ctx context.Context) (disco.Schema, error) {
	keys, vals, err := e.getKeyWithPrefix(ctx, schemaPrefix)
	if err != nil {
		return nil, err
	}

	// The logic in the following for loop assumes that the list of keys is
	// ordered such that index comes before field, which comes before view.
	// For example:
	//   /index1
	//   /index1/field1
	//   /index1/field1/view1
	//   /index1/field1/view2
	//   /index1/field2
	//   /index2
	//   /index2/field1
	//
	m := make(disco.Schema)
	for i, k := range keys {
		tokens := strings.Split(strings.Trim(k, "/"), "/")
		// token[0] contains the schemaPrefix

		// token[1]: index
		index := tokens[1]
		if _, ok := m[index]; !ok {
			m[index] = &disco.Index{
				Data:   vals[i],
				Fields: make(map[string]*disco.Field),
			}
			continue
		}
		flds := m[index].Fields

		// token[2]: field
		if len(tokens) > 2 {
			field := tokens[2]
			if _, ok := flds[field]; !ok {
				flds[field] = &disco.Field{
					Data:  vals[i],
					Views: make(map[string]struct{}),
				}
				continue
			}
			views := flds[field].Views

			// token[3]: view
			if len(tokens) > 3 {
				view := tokens[3]
				views[view] = struct{}{}
			}
		}
	}
	return m, nil
}

func (e *Etcd) Metadata(ctx context.Context, peerID string) ([]byte, error) {
	e.nodeMu.Lock()
	defer e.nodeMu.Unlock()
	err := e.populateNodeStates(ctx)
	if err != nil {
		return nil, err
	}
	data, ok := e.knownNodes[peerID]
	if !ok {
		return nil, errors.New("node not found")
	}
	return data.metadata, nil
}

func (e *Etcd) SetMetadata(ctx context.Context, metadata []byte) error {
	err := e.putKey(ctx, path.Join(metadataPrefix,
		e.e.Server.ID().String()),
		string(metadata),
	)
	if err != nil {
		return errors.Wrap(err, "SetMetadata")
	}

	return nil
}

func (e *Etcd) CreateIndex(ctx context.Context, name string, val []byte) error {
	key := schemaPrefix + name

	// Set up Op to write index value as bytes.
	op := clientv3.OpPut(key, "")
	op.WithValueBytes(val)

	// Check for key existence, and execute Op within a transaction.
	var resp *clientv3.TxnResponse
	err := e.retryClient(func(cli *clientv3.Client) (err error) {
		resp, err = cli.Txn(ctx).
			If(clientv3util.KeyMissing(key)).
			Then(op).
			Commit()
		return err
	})
	if err != nil {
		return errors.Wrap(err, "executing transaction")
	}

	if !resp.Succeeded {
		return disco.ErrIndexExists
	}

	return nil
}

func (e *Etcd) Index(ctx context.Context, name string) ([]byte, error) {
	return e.getKeyBytes(ctx, schemaPrefix+name)
}

func (e *Etcd) DeleteIndex(ctx context.Context, name string) (err error) {
	key := schemaPrefix + name
	// Deleting index and fields in one transaction.

	err = e.retryClient(func(cli *clientv3.Client) error {
		_, err = cli.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(key), ">", -1)).
			Then(
				clientv3.OpDelete(key+"/", clientv3.WithPrefix()), // deleting index fields
				clientv3.OpDelete(key),                            // deleting index
			).Commit()
		return err
	})

	return errors.Wrap(err, "DeleteIndex")
}

func (e *Etcd) Field(ctx context.Context, indexName string, name string) ([]byte, error) {
	key := schemaPrefix + indexName + "/" + name
	return e.getKeyBytes(ctx, key)
}

func (e *Etcd) CreateField(ctx context.Context, indexName string, name string, val []byte) error {
	key := schemaPrefix + indexName + "/" + name

	// Set up Op to write field value as bytes.
	op := clientv3.OpPut(key, "")
	op.WithValueBytes(val)

	// Check for key existence, and execute Op within a transaction.
	var resp *clientv3.TxnResponse

	err := e.retryClient(func(cli *clientv3.Client) (err error) {
		resp, err = cli.Txn(ctx).
			If(clientv3util.KeyMissing(key)).
			Then(op).
			Commit()
		return err
	})
	if err != nil {
		return errors.Wrap(err, "executing transaction")
	}

	if !resp.Succeeded {
		return disco.ErrFieldExists
	}

	return nil
}

func (e *Etcd) DeleteField(ctx context.Context, indexname string, name string) (err error) {
	key := schemaPrefix + indexname + "/" + name
	// Deleting field and views in one transaction.
	err = e.retryClient(func(cli *clientv3.Client) (err error) {
		_, err = cli.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(key), ">", -1)).
			Then(
				clientv3.OpDelete(key+"/", clientv3.WithPrefix()), // deleting field views
				clientv3.OpDelete(key),                            // deleting field
			).Commit()
		return err
	})

	return errors.Wrap(err, "DeleteField")
}

func (e *Etcd) View(ctx context.Context, indexName, fieldName, name string) (bool, error) {
	key := schemaPrefix + indexName + "/" + fieldName + "/" + name
	return e.keyExists(ctx, key)
}

// CreateView differs from CreateIndex and CreateField in that it does not
// return an error if the view already exists. If this logic needs to be
// changed, we likely need to return disco.ErrViewExists.
func (e *Etcd) CreateView(ctx context.Context, indexName, fieldName, name string) (err error) {
	key := schemaPrefix + indexName + "/" + fieldName + "/" + name

	// Check for key existence, and execute Op within a transaction.
	err = e.retryClient(func(cli *clientv3.Client) (err error) {
		_, err = cli.Txn(ctx).
			If(clientv3util.KeyMissing(key)).
			Then(clientv3.OpPut(key, "")).
			Commit()
		return err
	})
	if err != nil {
		return errors.Wrap(err, "executing transaction")
	}

	return nil
}

func (e *Etcd) DeleteView(ctx context.Context, indexName, fieldName, name string) error {
	return e.delKey(ctx, schemaPrefix+indexName+"/"+fieldName+"/"+name, false)
}

func (e *Etcd) putKey(ctx context.Context, key, val string, opts ...clientv3.OpOption) error {
	err := e.retryClient(func(cli *clientv3.Client) (err error) {
		_, err = cli.Txn(ctx).
			Then(clientv3.OpPut(key, val, opts...)).
			Commit()
		return err
	})
	return errors.Wrapf(err, "putKey: Put(%s, %s)", key, val)
}

func (e *Etcd) getKeyBytes(ctx context.Context, key string) ([]byte, error) {
	// Get the current value for the key.
	op := clientv3.OpGet(key)
	var resp *clientv3.TxnResponse
	err := e.retryClient(func(cli *clientv3.Client) (err error) {
		resp, err = cli.Txn(ctx).Then(op).Commit()
		return err
	})
	if err != nil {
		return nil, err
	}

	if len(resp.Responses) == 0 {
		return nil, disco.ErrKeyDoesNotExist
	}

	kvs := resp.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 {
		return nil, disco.ErrKeyDoesNotExist
	}

	return kvs[0].Value, nil
}

func (e *Etcd) getKeyWithPrefix(ctx context.Context, key string) (keys []string, values [][]byte, err error) {
	op := clientv3.OpGet(key, clientv3.WithPrefix())
	var resp *clientv3.TxnResponse
	err = e.retryClient(func(cli *clientv3.Client) (err error) {
		resp, err = cli.Txn(ctx).Then(op).Commit()
		return err
	})
	if err != nil {
		return nil, nil, errors.Wrapf(err, "getKeyWithPrefix(%s)", key)
	}

	if len(resp.Responses) == 0 {
		return nil, nil, disco.ErrKeyDoesNotExist
	}

	kvs := resp.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 {
		return nil, nil, nil
	}

	keys = make([]string, len(kvs))
	values = make([][]byte, len(kvs))
	for i, kv := range kvs {
		keys[i] = string(kv.Key)
		values[i] = kv.Value
	}

	return keys, values, nil
}

func (e *Etcd) keyExists(ctx context.Context, key string) (bool, error) {
	var resp *clientv3.TxnResponse
	err := e.retryClient(func(cli *clientv3.Client) (err error) {
		resp, err = cli.Txn(ctx).
			If(clientv3util.KeyExists(key)).
			Then(clientv3.OpGet(key, clientv3.WithCountOnly())).
			Commit()
		return err
	})
	if err != nil {
		return false, err
	}
	if !resp.Succeeded {
		return false, nil
	}

	if len(resp.Responses) == 0 {
		return false, nil
	}
	return resp.Responses[0].GetResponseRange().Count > 0, nil
}

func (e *Etcd) delKey(ctx context.Context, key string, withPrefix bool) (err error) {
	if withPrefix {
		_, err = e.cli.Delete(ctx, key, clientv3.WithPrefix())
	} else {
		_, err = e.cli.Delete(ctx, key)
	}
	return err
}

func memberList(cli *clientv3.Client) (ids []uint64, names []string, urls []string) {
	ml, err := cli.MemberList(context.TODO())
	if err != nil {
		panic(err)
	}
	n := len(ml.Members)
	ids = make([]uint64, n)
	names = make([]string, n)
	urls = make([]string, n)

	for i, m := range ml.Members {
		ids[i], names[i], urls[i] = m.ID, m.Name, m.PeerURLs[0]
	}
	return
}

func memberAdd(cli *clientv3.Client, peerURL string) (id uint64, name string) {
	ma, err := cli.MemberAdd(context.TODO(), []string{peerURL})
	if err != nil {
		return 0, ""
	}

	return ma.Member.ID, ma.Member.Name
}

// Shards implements the Sharder interface.
func (e *Etcd) Shards(ctx context.Context, index, field string) ([][]byte, error) {
	key := path.Join(shardPrefix, index, field)
	_, vals, err := e.getKeyWithPrefix(ctx, key)

	if errors.Cause(err) == disco.ErrKeyDoesNotExist {
		e.logger.Warnf("key: %s, err: %v", key, err)
		return nil, nil
	}

	return vals, nil
}

// SetShards implements the Sharder interface.
func (e *Etcd) SetShards(ctx context.Context, index, field string, shards []byte) error {
	key := path.Join(shardPrefix, index, field, e.e.Server.ID().String())

	op := clientv3.OpPut(key, "")
	op.WithValueBytes(shards)
	return e.retryClient(func(cli *clientv3.Client) (err error) {
		_, err = cli.Txn(ctx).Then(op).Commit()
		return
	})
}

// Nodes implements the Noder interface. It returns the sorted list of nodes
// based on the etcd peers.
func (e *Etcd) Nodes() []*topology.Node {
	e.nodeMu.Lock()
	defer e.nodeMu.Unlock()
	err := e.populateNodeStates(context.TODO())
	if err != nil {
		return nil
	}
	return e.sortedNodes
}

// PrimaryNodeID implements the Noder interface.
func (e *Etcd) PrimaryNodeID(hasher topology.Hasher) string {
	return topology.PrimaryNodeID(e.NodeIDs(), hasher)
}

// NodeIDs returns the list of node IDs in the etcd cluster.
func (e *Etcd) NodeIDs() []string {
	peers := e.Peers()
	ids := make([]string, len(peers))
	for i, peer := range peers {
		ids[i] = peer.ID
	}
	return ids
}

// SetNodes implements the Noder interface as NOP
// (because we can't force to set nodes for etcd).
func (e *Etcd) SetNodes(nodes []*topology.Node) {}

// AppendNode implements the Noder interface as NOP
// (because resizer is responsible for adding new nodes).
func (e *Etcd) AppendNode(node *topology.Node) {}

// RemoveNode implements the Noder interface as NOP
// (because resizer is responsible for removing existing nodes)
func (e *Etcd) RemoveNode(nodeID string) bool {
	return false
}
