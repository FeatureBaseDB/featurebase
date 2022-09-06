// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/monitor"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	clientv3util "go.etcd.io/etcd/client/v3/clientv3util"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
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

	UnsafeNoFsync bool   `toml:"no-fsync"`
	Cluster       string `toml:"static-cluster"`
	EtcdHosts     string `toml:"etcd-hosts"`
	Id            string
}

var (
	_ disco.DisCo     = &Etcd{}
	_ disco.Schemator = &Etcd{}
	_ disco.Noder     = &Etcd{}
	_ disco.Sharder   = &Etcd{}
)

const (
	// We put all the things the node-watcher watches in /node so we
	// can use a single watcher for them.
	nodePrefix      = "/node/"
	heartbeatPrefix = nodePrefix + "heartbeat/"
	schemaPrefix    = "/schema/"
	metadataPrefix  = nodePrefix + "metadata/"
	shardPrefix     = "/shard/"
)

var errEtcdShuttingDown = errors.New("etcd shutting down")

// nodeData is an internal tracker of the data we're keeping about
// nodes in etcd, which we update from data collected either directly
// from the KV, or via heartbeats.
//
// Any change to the disco.Node should create a new Node rather
// than reusing the old one, so we can return the structure and not worry
// about data races.
//
// We have to track the revisions of individual components so we can
// discard updates which are genuinely out-of-order for a given field,
// but still handle cases where we get updates to several fields that
// reach us out of order.
type nodeData struct {
	heartbeat disco.NodeState
	metadata  []byte
	node      *disco.Node
}

// EtcdServicer provides access to either an embeded etcd server or an external host
type EtcdServicer interface {
	Shutdown()
	Peers() []*disco.Peer
	IsLeader() bool
	Leader() *disco.Peer
	ID() string
	Startup(ctx context.Context, state disco.InitialClusterState) (disco.InitialClusterState, error)
	NewClient() (*clientv3.Client, error)
}
type EmbeddedEtcd struct {
	e      *embed.Etcd
	parent *Etcd
}

func NewEmbeddedEtcd(p *Etcd, opts *embed.Config) (*EmbeddedEtcd, error) {
	e, err := embed.StartEtcd(opts)
	if err != nil {
		return nil, errors.Wrap(err, "starting etcd")
	}
	this := &EmbeddedEtcd{}
	this.parent = p
	this.e = e
	p.e = e
	return this, nil
}

func (e *EmbeddedEtcd) ID() string {
	if e.e == nil || e.e.Server == nil {
		return ""
	}
	return e.e.Server.ID().String()
}

func (e *EmbeddedEtcd) Shutdown() {
	if e.e != nil {
		e.e.Close()
		<-e.e.Server.StopNotify()
	}
}

func (e *EmbeddedEtcd) Peers() []*disco.Peer {
	var peers []*disco.Peer
	for _, member := range e.e.Server.Cluster().Members() {
		peers = append(peers, &disco.Peer{ID: member.ID.String(), URL: member.PickPeerURL()})
	}
	return peers
}

func (e *EmbeddedEtcd) IsLeader() bool {
	if e.e == nil || e.e.Server == nil {
		return false
	}
	return e.e.Server.Leader() == e.e.Server.ID()
}

func (e *EmbeddedEtcd) Leader() *disco.Peer {
	id := e.e.Server.Leader()
	peer := &disco.Peer{ID: id.String()}

	if m := e.e.Server.Cluster().Member(id); m != nil {
		peer.URL = m.PickPeerURL()
	}

	return peer
}

func (e *EmbeddedEtcd) Startup(ctx context.Context, state disco.InitialClusterState) (disco.InitialClusterState, error) {
	select {
	case <-ctx.Done():
		return state, ctx.Err()

	case err := <-e.e.Err():
		return state, err

	case <-e.e.Server.ReadyNotify():
		members := e.e.Server.Cluster().Members()
		e.parent.nodeMu.Lock()
		defer e.parent.nodeMu.Unlock()
		// mark everything unknown so we show a state for nodes we haven't
		// heard from yet.
		for _, member := range members {
			peerID := member.ID.String()
			_ = e.parent.seeNode(peerID)
		}
		e.parent.nodesDirty = true
		return state, e.parent.startHeartbeatAndWatcher(ctx)
	}
}

func (e *EmbeddedEtcd) NewClient() (*clientv3.Client, error) {
	return v3client.New(e.e.Server), nil
}

type Etcd struct {
	options  Options
	replicas int

	e       *embed.Etcd // TODO (twg) factor out
	service EtcdServicer
	cli     *clientv3.Client
	cliMu   sync.Mutex

	heartbeatLeasedKV *leasedKV

	// A context for our children (node watcher, lease keepalive)
	childContext context.Context
	// function to cancel the child contexts when we're done
	childCancel func()

	// knownNodes and sortedNodes get updated by data coming in from
	// watchers. Any change to the contents of a *disco.Node here
	// should be implemented by making a new one and replacing the pointer,
	// so the old pointer stays valid and can be used.
	nodeMu      sync.Mutex
	nodeRev     int64
	knownNodes  map[string]*nodeData
	sortedNodes []*disco.Node // immutable nodes kept in sorted order
	nodesDirty  bool          // do we need to recompute sortedNodes?

	version string

	// we want to inherit parent's logging functionality
	logger logger.Logger
}

func NewEtcd(opt Options, logger logger.Logger, replicas int, version string) *Etcd {
	e := &Etcd{
		options:    opt,
		logger:     logger,
		replicas:   replicas,
		knownNodes: make(map[string]*nodeData),
		version:    version,
	}

	if e.options.HeartbeatTTL == 0 {
		e.options.HeartbeatTTL = 5 // seconds
	}
	return e
}

// Close implements io.Closer
func (e *Etcd) Close() error {
	// tell the heartbeat to stop. we do this before canceling
	// the context because we want the heartbeat to get a chance
	// to notify other nodes that it's down.
	if e.heartbeatLeasedKV != nil {
		e.heartbeatLeasedKV.Stop()
	}
	// cancel the contexts that heartbeat and watcher are using.
	if e.childCancel != nil {
		e.childCancel()
	}
	// shut down the server, if we have one.
	e.service.Shutdown()
	// shut down the client, if we have one. if something's still
	// using it, we anticipate the client failing its current call,
	// and any retry will be checking for the child context being
	// cancelled, first, we hope.
	if e.cli != nil {
		e.cli.Close()
	}

	return nil
}

const etcdRetryTimes = 3

// newClient requests a new client which is different from the one
// passed in. if we've already changed our client (say, because someone
// else already did that) we just return that new one.
func (e *Etcd) newClient(cli *clientv3.Client) *clientv3.Client {
	e.cliMu.Lock()
	defer e.cliMu.Unlock()
	if cli != e.cli {
		cli = e.cli
		// someone else already reopened. retry.
		return cli
	}
	_ = cli.Close()
	e.cli, _ = e.service.NewClient()
	return e.cli
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
//
// New feature: retryClient can also retry on errTimeout.
func (e *Etcd) retryClient(fn func(cli *clientv3.Client) error) (err error) {
	e.cliMu.Lock()
	cli := e.cli
	e.cliMu.Unlock()
	for tries := 0; tries < etcdRetryTimes; tries++ {
		start := time.Now()
		err = fn(cli)
		switch err {
		case etcdserver.ErrLeaderChanged:
			cli = e.newClient(cli)
		case nil:
			return nil
		default:
			msg := err.Error()
			// this shouldn't be necessary, but empirically, we sometimes
			// get an error message which has this text, but the error itself
			// isn't actually etcdserver.ErrLeaderChanged.
			if strings.Contains(msg, "etcdserver: leader changed") {
				cli = e.newClient(cli)
				break
			}
			// check that the request hasn't timed out. this can happen with either of these errors
			if !strings.Contains(msg, "etcdserver: request timed out") && !strings.Contains(msg, "context deadline exceeded") && !strings.Contains(msg, errEtcdShuttingDown.Error()) {
				// not a known error, also not a wrapped timeout
				return errors.Wrap(err, "non-retryable error")
			}
			fallthrough // treat this as being one of the ErrTimeout derivatives, possibly wrapped.
		case etcdserver.ErrTimeout, etcdserver.ErrTimeoutDueToLeaderFail, etcdserver.ErrTimeoutDueToConnectionLost, etcdserver.ErrTimeoutLeaderTransfer:
			// sporadic timeouts are concerning but not necessarily fatal
			// and can usually be retried.
			elapsed := time.Since(start)
			retrying := ""
			if tries < etcdRetryTimes {
				retrying = fmt.Sprintf(" (retrying, n=%d)", tries)
			}
			e.logger.Warnf("timeout (%v elapsed) on etcd query%s", elapsed, retrying)
			// Sleep just a touch longer to give things a time to
			// stabilize. We're mostly relying on the fact that this is a
			// timeout to give us a reasonable backoff period and keep us
			// from spamming these.
			time.Sleep(100 * time.Millisecond)
		}
	}
	// if we got here, we got a total of three of some combination of
	// ErrTimeout or ErrLeaderChanged, and we're giving up.
	return errors.Wrap(err, "exhausted all retries")
}

func (e *Etcd) parseOptions() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.LogLevel = "error"
	cfg.Logger = "zap"
	cfg.Name = e.options.Name
	cfg.Dir = e.options.Dir
	cfg.InitialClusterToken = e.options.ClusterName
	var err error
	cfg.LCUrls, err = types.NewURLs([]string{e.options.LClientURL})
	if err != nil {
		return nil, fmt.Errorf("parsing listen client URL %q: %v", e.options.LClientURL, err)
	}
	cfg.UnsafeNoFsync = e.options.UnsafeNoFsync
	if e.options.AClientURL != "" {
		cfg.ACUrls, err = types.NewURLs([]string{e.options.AClientURL})
		if err != nil {
			return nil, fmt.Errorf("parsing advertise client URL %q: %v", e.options.AClientURL, err)
		}
	} else {
		cfg.ACUrls = cfg.LCUrls
	}
	cfg.LPUrls, err = types.NewURLs([]string{e.options.LPeerURL})
	if err != nil {
		return nil, fmt.Errorf("parsing listen peer URL %q: %v", e.options.LPeerURL, err)
	}
	if e.options.APeerURL != "" {
		cfg.APUrls, err = types.NewURLs([]string{e.options.APeerURL})
		if err != nil {
			return nil, fmt.Errorf("parsing advertise peer URL %q: %v", e.options.APeerURL, err)
		}
	} else {
		cfg.APUrls = cfg.LPUrls
	}
	if e.options.InitCluster != "" {
		// Checks if FB is running the single-node free version or the multi-node
		// enterprise version. Sentry.io is enabled on single-node.
		if AllowCluster() == false {
			// %% begin sonarcloud ignore %%

			monitor.InitErrorMonitor(e.version)
			e.logger.Infof("Initializing Monitor: Capturing usage metrics")
			// check for multiple nodes in the cluster and error if present
			nodes := strings.Split(e.options.InitCluster, ",")
			if len(nodes) > 1 {
				return nil, fmt.Errorf("multiple cluster nodes detected - this version of FeatureBase only supports single node. %+v", e.options.InitCluster)
			}
			// %% end sonarcloud ignore %%
		}
		cfg.InitialCluster = e.options.InitCluster
		cfg.ClusterState = embed.ClusterStateFlagNew
	} else {
		cfg.InitialCluster = cfg.Name + "=" + e.options.APeerURL
	}

	if e.options.ClusterURL != "" {
		return nil, errors.New("joining an existing cluster is unsupported")
	}
	// can only use tls if not using pre-configured listeners
	cfg.ClientTLSInfo = transport.TLSInfo{
		TrustedCAFile: e.options.TrustedCAFile,
		CertFile:      e.options.ClientCertFile,
		KeyFile:       e.options.ClientKeyFile,
	}
	cfg.PeerTLSInfo = transport.TLSInfo{
		TrustedCAFile: e.options.TrustedCAFile,
		CertFile:      e.options.PeerCertFile,
		KeyFile:       e.options.PeerKeyFile,
	}

	// We might get an error from Validate. etcd docs don't tell us what
	// that error might be, though!
	return cfg, cfg.Validate()
}

// Start starts etcd and hearbeat
func (e *Etcd) Start(ctx context.Context) (_ disco.InitialClusterState, err error) {
	opts, err := e.parseOptions()
	if err != nil {
		return disco.InitialClusterStateNew, err
	}
	state := disco.InitialClusterState(opts.ClusterState)
	// create a context that can be used for our watch processes, etcetera.
	e.childContext, e.childCancel = context.WithCancel(context.Background())
	if e.options.EtcdHosts != "" {
		e.service, err = NewExternalEtcd(e, e.options)
		if err != nil {
			return state, errors.Wrap(err, "connecting etcd")
		}
		e.logger.Infof("using external etcd %v with fixed fb cluster %v", e.options.EtcdHosts, e.options.Cluster)
	} else {

		e.service, err = NewEmbeddedEtcd(e, opts)
		if err != nil {
			return state, errors.Wrap(err, "starting etcd")
		}
		// If we are returning an error, the caller won't be shutting us down
		// later, so we have to stop the server ourselves.
		defer func() {
			if err != nil {
				// shut down everything on our way out.
				e.Close()
			}
		}()
	}

	e.cli, _ = e.service.NewClient()

	return e.service.Startup(ctx, state)
}

// startHeartbeatAndWatcher spins up the heartbeat, and also a background
// watcher that watches for changes to events we care about.
func (e *Etcd) startHeartbeatAndWatcher(ctx context.Context) error {
	key := heartbeatPrefix + e.service.ID()
	e.heartbeatLeasedKV = newLeasedKV(e, e.childContext, key, e.options.HeartbeatTTL)

	if err := e.heartbeatLeasedKV.Start(string(disco.NodeStateStarting)); err != nil {
		return errors.Wrap(err, "startHeartbeat: starting a new heartbeat")
	}
	// watchNodes does not check for an error, and will need to be shut
	// down later. We only get this far at a point where we're returning
	// a nil error, and thus, the caller is expected to cleanly shut down
	// the server later.
	go e.watchNodes()
	return nil
}

func (e *Etcd) SetState(ctx context.Context, state disco.NodeState) (err error) {
	return e.heartbeatLeasedKV.Set(ctx, string(state))
}

func (e *Etcd) ID() string {
	return e.service.ID()
}

func (e *Etcd) Peers() []*disco.Peer {
	return e.service.Peers()
}

func (e *Etcd) IsLeader() bool {
	return e.service.IsLeader()
}

func (e *Etcd) Leader() *disco.Peer {
	return e.service.Leader()
}

func (e *Etcd) ClusterState(ctx context.Context) (out disco.ClusterState, err error) {
	if len(e.options.EtcdHosts) == 0 {
		// only valid if not in external etcd mode
		if e.e == nil {
			return disco.ClusterStateUnknown, nil
		}
	}
	var (
		heartbeats int = 0
		starting   bool
	)
	e.nodeMu.Lock()
	nodes := e.populateNodeStates(ctx)
	e.nodeMu.Unlock()
	if err != nil {
		e.logger.Errorf("requesting cluster state %q: getting node states: %v", e.options.Name, err)
		return disco.ClusterStateUnknown, err
	}
	for _, node := range nodes {
		switch node.State {
		case disco.NodeStateStarting:
			starting = true
		case disco.NodeStateUnknown:
			continue
		}

		heartbeats++
	}

	if starting {
		return disco.ClusterStateStarting, nil
	}

	if heartbeats < len(e.knownNodes) {
		if len(e.knownNodes)-heartbeats >= e.replicas {
			return disco.ClusterStateDown, nil
		}

		return disco.ClusterStateDegraded, nil
	}

	return disco.ClusterStateNormal, nil
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

// seeNode encapsulates the practice of creating a new node, when needed,
// and checking for duplicates.
func (e *Etcd) seeNode(peerID string) *nodeData {
	var node *nodeData
	if node = e.knownNodes[peerID]; node == nil {
		e.logger.Debugf("previously unseen node, peer ID %s", peerID)
		node = &nodeData{
			node: &disco.Node{
				ID:    peerID,
				State: disco.NodeStateUnknown,
			},
			heartbeat: disco.NodeStateUnknown,
		}
		e.knownNodes[peerID] = node
	}
	return node
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
		node := e.seeNode(peerID)
		// mark state as unknown because we deleted the heartbeat.
		node.heartbeat = disco.NodeStateUnknown
		e.nodesDirty = true
	case metadataPrefix:
		e.logger.Infof("deleting a previously-seen node, peer ID %q", peerID)
		delete(e.knownNodes, peerID)
		e.nodesDirty = true
	default:
		return fmt.Errorf("node watch: invalid prefix %q", prefix)
	}
	return nil
}

// putNodeData does the actual updating of the node state maps, etc,
// given an incoming heartbeat or metadata change. It requires
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
		node := e.seeNode(peerID)
		node.heartbeat = disco.NodeState(value)
		e.nodesDirty = true
	case metadataPrefix:
		node := e.seeNode(peerID)
		node.metadata = value
		var newNode disco.Node
		err := json.Unmarshal(value, &newNode)
		if err != nil {
			return fmt.Errorf("json unmarshal of node metadata: %v", err)
		}
		// start with the heartbeat state
		newNode.State = node.heartbeat
		node.node = &newNode
		e.nodesDirty = true
	default:
		return fmt.Errorf("node watch: invalid prefix %q", prefix)
	}
	return nil
}

// compute the states of all the nodes. we compute all of them because
// we might have returned the old map in response to a query, so we want to
// make a new one. You should have the node state lock held when you call this.
// Returns an immutable sorted list of nodes; future updates will not
// modify the slice or the nodes in it.
func (e *Etcd) populateNodeStates(ctx context.Context) []*disco.Node {
	if !e.nodesDirty {
		return e.sortedNodes
	}
	e.sortedNodes = make([]*disco.Node, 0, len(e.knownNodes))
	for _, data := range e.knownNodes {
		newState := data.heartbeat
		// update the state with the current state, so we can
		// reuse these nodes later. sortedNodes may end up shorter
		// than the whole node list if we don't have all the nodes
		// yet!
		if data.node != nil {
			// The only part that should ever change is the state, which
			// will be either "unknown" or the state from a heartbeat.
			// If that computed state is different, we make a new node
			// at this point. The reason is that, if we previously returned
			// the sorted list of nodes, someone else could have a
			// pointer to the existing node. We don't want to clone these
			// every time anyone reads them, so instead we make them
			// immutable and copy-on-write.
			if data.node.State != newState {
				newNode := *data.node
				newNode.State = newState
				data.node = &newNode
			}
			e.sortedNodes = append(e.sortedNodes, data.node)
		}
	}
	// sort list by ID. list now contains sorted nodes which have their
	// current states.
	sort.Sort(disco.ByID(e.sortedNodes))
	e.nodesDirty = false
	return e.sortedNodes
}

// watchNodesOnce is a helper function to use with the retry logic
// to let us restart the client if we need to.
func (e *Etcd) watchNodesOnce(cli *clientv3.Client) (err error) {
	e.nodeMu.Lock()
	// we are looking for revisions HIGHER than the highest revision we've
	// currently seen, we don't want one equal to it.
	minRev := e.nodeRev + 1
	done := e.childContext.Done()
	e.nodeMu.Unlock()
	watcher := cli.Watch(clientv3.WithRequireLeader(e.childContext), nodePrefix, clientv3.WithPrefix(), clientv3.WithRev(minRev))
	for {
		select {
		case <-done:
			// we're done, this is not an error
			return nil
		case resp := <-watcher:
			if err := resp.Err(); err != nil {
				if resp.CompactRevision > minRev {
					e.logger.Infof("watching node status, wanted rev %d, minimum now %d",
						minRev, resp.CompactRevision)
					// We've been told that any request with a revision under
					// CompactRevision will always fail. Set nodeRev to one less than that,
					// so we'll specify it as the minimum when we retry.
					//
					// We currently have no obvious way to verify that this will work.
					e.nodeRev = resp.CompactRevision - 1
				}
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
						e.logger.Warnf("put event: %v", err)
					}
				case mvccpb.DELETE:
					err := e.deleteNodeData(ev.Kv.Key, ev.Kv.ModRevision)
					if err != nil {
						e.logger.Warnf("delete event: %v", err)
					}
				default:
					e.logger.Warnf("watchp %q: unknown event %#v", e.options.Name, ev)
				}
			}
			e.nodeMu.Unlock()
		}
	}
}

// watchNodes monitors changes to /heartbeat/ and /metadata/;
// basically, it catches changes to cluster state, but ignores the schema.
func (e *Etcd) watchNodes() {
	// retryClient will retry on leader failure, but not for other failures
	// such as ErrCompacted which can terminate a watch. But we want to resume
	// watching again as long as our context isn't cancelled. The context
	// should get cancelled when this Etcd gets shut down.
	for e.childContext.Err() == nil {
		err := e.retryClient(func(cli *clientv3.Client) error {
			return e.watchNodesOnce(cli)
		})
		if err != nil {
			e.logger.Warnf("watchNodes: error from watch client: %v", err)
		}
		// delay slightly on watch termination so we don't go completely crazy
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

func (e *Etcd) SetMetadata(ctx context.Context, node *disco.Node) error {
	// Set metadata for this node.
	data, err := json.Marshal(node)
	if err != nil {
		return errors.Wrap(err, "marshaling json metadata")
	}
	err = e.putKey(ctx, path.Join(metadataPrefix,
		e.service.ID()),
		string(data),
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

func (e *Etcd) UpdateField(ctx context.Context, indexName string, name string, val []byte) error {
	key := schemaPrefix + indexName + "/" + name

	// Set up Op to write field value as bytes.
	op := clientv3.OpPut(key, "")
	op.WithValueBytes(val)

	// Check for key existence, and execute Op within a transaction.
	var resp *clientv3.TxnResponse

	err := e.retryClient(func(cli *clientv3.Client) (err error) {
		resp, err = cli.Txn(ctx).
			If(clientv3util.KeyExists(key)).
			Then(op).
			Commit()
		return err
	})
	if err != nil {
		return errors.Wrap(err, "executing transaction")
	}

	if !resp.Succeeded {
		return disco.ErrFieldDoesNotExist
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
	key := path.Join(shardPrefix, index, field, e.service.ID())

	op := clientv3.OpPut(key, "")
	op.WithValueBytes(shards)
	return e.retryClient(func(cli *clientv3.Client) (err error) {
		_, err = cli.Txn(ctx).Then(op).Commit()
		return
	})
}

// Nodes implements the Noder interface. It returns the sorted list of nodes
// based on the etcd peers.
func (e *Etcd) Nodes() []*disco.Node {
	e.nodeMu.Lock()
	defer e.nodeMu.Unlock()
	return e.populateNodeStates(context.TODO())
}

// PrimaryNodeID implements the Noder interface.
func (e *Etcd) PrimaryNodeID(hasher disco.Hasher) string {
	return disco.PrimaryNodeID(e.NodeIDs(), hasher)
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
