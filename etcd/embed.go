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
	"time"

	"github.com/pilosa/pilosa/v2/disco"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/topology"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/clientv3util"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/mvcc/mvccpb"
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

	LPeerSocket   []*net.TCPListener
	LClientSocket []*net.TCPListener
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
	heartbeatPrefix = "/heartbeat/"
	schemaPrefix    = "/schema/"
	resizePrefix    = "/resize/"
	metadataPrefix  = "/metadata/"
	shardPrefix     = "/shard/"
	lockPrefix      = "/lock/"
)

type leaseMetadata struct {
	started bool
}

type Etcd struct {
	options  Options
	replicas int

	heartbeatID     clientv3.LeaseID
	heartbeatCancel context.CancelFunc

	resizeCancel context.CancelFunc

	lm leaseMetadata

	e *embed.Etcd
}

func NewEtcd(opt Options, replicas int) *Etcd {
	e := &Etcd{
		options:  opt,
		replicas: replicas,
	}
	return e
}

// Close implements io.Closer
func (e *Etcd) Close() error {
	if e.e != nil {
		if e.resizeCancel != nil {
			e.resizeCancel()
		}
		if e.heartbeatCancel != nil {
			e.heartbeatCancel()
		}
		e.e.Server.Stop()
		e.e.Close()
		<-e.e.Server.StopNotify()
	}

	return nil
}

func parseOptions(opt Options) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Debug = false // true gives data races on grpc.EnableTracing in etcd
	cfg.LogLevel = "error"
	cfg.Logger = "zap"
	cfg.Name = opt.Name
	cfg.Dir = opt.Dir
	cfg.InitialClusterToken = opt.ClusterName
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

	return cfg
}

// Start starts etcd and hearbeat
func (e *Etcd) Start(ctx context.Context) (disco.InitialClusterState, error) {
	opts := parseOptions(e.options)
	state := disco.InitialClusterState(opts.ClusterState)

	etcd, err := embed.StartEtcd(opts)
	if err != nil {
		return state, errors.Wrap(err, "starting etcd")
	}
	e.e = etcd

	select {
	case <-ctx.Done():
		e.e.Server.Stop()
		return state, ctx.Err()

	case err := <-e.e.Err():
		return state, err

	case <-e.e.Server.ReadyNotify():
		return state, e.startHeartbeat()
	}
}

func (e *Etcd) startHeartbeat() error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "startHeartbeat: creates a new client")
	}
	defer cli.Close()

	heartbeatID, heartbeatFunc, err := e.leaseKeepAlive(e.options.HeartbeatTTL)
	if err != nil {
		return errors.Wrap(err, "startHeartbeat: creates a new hearbeat")
	}

	ctx, heartbeatCancel := context.WithCancel(context.Background())
	key, value := heartbeatPrefix+e.e.Server.ID().String(), disco.ClusterStateStarting
	if e.e.Config().ClusterState == embed.ClusterStateFlagExisting {
		value = disco.ClusterStateResizing
	}

	if _, err := cli.Put(ctx, key, string(value), clientv3.WithLease(heartbeatID)); err != nil {
		heartbeatCancel()
		return errors.Wrapf(err, "startHeartbeat: puts a key-value (%s, %s) with lease (%v)", key, value, heartbeatID)
	}

	e.heartbeatID, e.heartbeatCancel = heartbeatID, heartbeatCancel
	go heartbeatFunc(ctx, time.Second)

	return nil
}

func (e *Etcd) NodeState(ctx context.Context, peerID string) (disco.NodeState, error) {
	cli, err := e.client()
	if err != nil {
		return disco.NodeStateUnknown, errors.Wrap(err, "NodeState: creates a new client")
	}
	defer cli.Close()

	return e.nodeState(ctx, cli, peerID)
}

func (e *Etcd) nodeState(ctx context.Context, cli *clientv3.Client, peerID string) (disco.NodeState, error) {
	resp, err := cli.Get(ctx, path.Join(resizePrefix, peerID), clientv3.WithCountOnly())
	if err != nil {
		return disco.NodeStateUnknown, err
	}
	if resp.Count > 0 {
		return disco.NodeStateResizing, nil
	}

	resp, err = cli.Get(ctx, path.Join(heartbeatPrefix, peerID))
	if err != nil {
		return disco.NodeStateUnknown, err
	}

	if len(resp.Kvs) > 1 {
		return disco.NodeStateUnknown, disco.ErrTooManyResults
	}

	if len(resp.Kvs) == 0 {
		return disco.NodeStateUnknown, disco.ErrNoResults
	}

	return disco.NodeState(resp.Kvs[0].Value), nil
}

func (e *Etcd) NodeStates(ctx context.Context) (map[string]disco.NodeState, error) {
	out := make(map[string]disco.NodeState)

	cli, err := e.client()
	if err != nil {
		return nil, errors.Wrap(err, "NodeStates")
	}
	defer cli.Close()

	members := e.e.Server.Cluster().Members()
	for _, member := range members {
		s, err := e.nodeState(ctx, cli, member.ID.String())
		if err != nil {
			log.Println("NodeStates get node state", member.ID.String(), err.Error())
		}

		out[member.ID.String()] = s
	}

	return out, nil
}

func (e *Etcd) Started(ctx context.Context) error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "Started")
	}
	defer cli.Close()

	key, value := heartbeatPrefix+e.e.Server.ID().String(), disco.NodeStateStarted
	if _, err = cli.Put(ctx, key, string(value), clientv3.WithLease(e.heartbeatID)); err == nil {
		e.lm.started = true
	}
	return err
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
	m := e.e.Server.Cluster().Member(id)
	return &disco.Peer{ID: id.String(), URL: m.PickPeerURL()}
}

func (e *Etcd) ClusterState(ctx context.Context) (disco.ClusterState, error) {
	if e.e == nil {
		return disco.ClusterStateUnknown, nil
	}

	cli, err := e.client()
	if err != nil {
		return disco.ClusterStateUnknown, errors.WithMessage(err, "ClusterState: creates a new client")
	}
	defer cli.Close()

	var (
		heartbeats int = 0
		resize     bool
		starting   bool
	)
	members := e.e.Server.Cluster().Members()
	for _, m := range members {
		ns, err := e.nodeState(ctx, cli, m.ID.String())
		if err != nil {
			log.Println("ClusterState get node state", err.Error())
			continue
		}

		heartbeats++

		if ns == disco.NodeStateStarting {
			starting = true
		}

		if ns == disco.NodeStateResizing {
			resize = true
		}
	}

	if resize {
		return disco.ClusterStateResizing, nil
	}

	if starting {
		return disco.ClusterStateStarting, nil
	}

	if heartbeats < len(members) {
		if len(members)-heartbeats >= e.replicas {
			return disco.ClusterStateDown, nil
		}

		return disco.ClusterStateDegraded, nil
	}

	return disco.ClusterStateNormal, nil
}

func (e *Etcd) Resize(ctx context.Context) (func([]byte) error, error) {
	cli, err := e.client()
	if err != nil {
		return nil, errors.Wrap(err, "Resize: creates a new client")
	}
	defer cli.Close()

	resizeID, resizeFunc, err := e.leaseKeepAlive(e.options.HeartbeatTTL)
	if err != nil {
		return nil, errors.Wrap(err, "Resize: creates a new hearbeat")
	}

	ctx, resizeCancel := context.WithCancel(ctx)
	// Check if key exists - maybe we are still resizing
	key := path.Join(resizePrefix, e.e.Server.ID().String())
	txnResp, err := cli.Txn(ctx).
		If(clientv3util.KeyMissing(key)).
		Then(clientv3.OpPut(key, "", clientv3.WithLease(resizeID))).
		Commit()
	if err != nil {
		resizeCancel()
		return nil, errors.Wrapf(err, "Resize: txn puts key (%s) with lease (%v)", key, resizeID)
	}

	if !txnResp.Succeeded {
		resizeCancel()
		return nil, errors.Errorf("Resize: key (%s) exists - maybe node (%s) is resizing", key, e.ID())
	}

	e.resizeCancel = resizeCancel
	go resizeFunc(ctx, time.Second)

	return func(value []byte) error {
		log.Println("Update progress:", key, string(value))
		return e.putKey(ctx, key, string(value), clientv3.WithLease(resizeID))
	}, nil
}

func (e *Etcd) DoneResize() error {
	if e.resizeCancel != nil {
		e.resizeCancel()
	}
	return nil
}

func (e *Etcd) Watch(ctx context.Context, peerID string, onUpdate func([]byte) error) error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "Watch: creates a new client")
	}
	defer cli.Close()

	key := path.Join(resizePrefix, peerID)
	for resp := range cli.Watch(ctx, key) {
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

func (e *Etcd) DeleteNode(ctx context.Context, nodeID string) error {
	id, err := types.IDFromString(nodeID)
	if err != nil {
		return err
	}

	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "DeleteNode: creates a new client")
	}
	defer cli.Close()

	_, err = cli.MemberRemove(ctx, uint64(id))
	if err != nil {
		return errors.Wrap(err, "DeleteNode: removes an existing member from the cluster")
	}

	return nil
}

func (e *Etcd) Schema(ctx context.Context) (map[string]*disco.Index, error) {
	keys, vals, err := e.getKey(ctx, schemaPrefix)
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
	m := make(map[string]*disco.Index)
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
					Views: make(map[string][]byte),
				}
				continue
			}
			views := flds[field].Views

			// token[3]: view
			if len(tokens) > 3 {
				view := tokens[3]
				views[view] = vals[i]
			}
		}
	}
	return m, nil
}

func (e *Etcd) Metadata(ctx context.Context, peerID string) ([]byte, error) {
	cli, err := e.client()
	if err != nil {
		return nil, errors.Wrap(err, "Metadata")
	}
	defer cli.Close()

	resp, err := cli.Get(ctx, path.Join(metadataPrefix, peerID))
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) > 1 {
		return nil, disco.ErrTooManyResults
	}

	if len(resp.Kvs) == 0 {
		return nil, disco.ErrNoResults
	}

	return resp.Kvs[0].Value, nil
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
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "CreateIndex: creating client")
	}
	defer cli.Close()

	key := schemaPrefix + name

	// Set up Op to write index value as bytes.
	op := clientv3.OpPut(key, "")
	op.WithValueBytes(val)

	// Check for key existence, and execute Op within a transaction.
	resp, err := cli.KV.Txn(ctx).
		If(clientv3util.KeyMissing(key)).
		Then(op).
		Commit()
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

func (e *Etcd) DeleteIndex(ctx context.Context, name string) error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "DeleteIndex: creating client")
	}
	defer cli.Close()

	key := schemaPrefix + name
	// Deleting index and fields in one transaction.
	_, err = cli.KV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), ">", -1)).
		Then(
			clientv3.OpDelete(key+"/", clientv3.WithPrefix()), // deleting index fields
			clientv3.OpDelete(key),                            // deleting index
		).Commit()

	return errors.Wrap(err, "DeleteIndex")
}

func (e *Etcd) Field(ctx context.Context, indexName string, name string) ([]byte, error) {
	key := schemaPrefix + indexName + "/" + name
	return e.getKeyBytes(ctx, key)
}

func (e *Etcd) CreateField(ctx context.Context, indexName string, name string, val []byte) error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "CreateField: creating client")
	}
	defer cli.Close()

	key := schemaPrefix + indexName + "/" + name

	// Set up Op to write field value as bytes.
	op := clientv3.OpPut(key, "")
	op.WithValueBytes(val)

	// Check for key existence, and execute Op within a transaction.
	resp, err := cli.KV.Txn(ctx).
		If(clientv3util.KeyMissing(key)).
		Then(op).
		Commit()
	if err != nil {
		return errors.Wrap(err, "executing transaction")
	}

	if !resp.Succeeded {
		return disco.ErrFieldExists
	}

	return nil
}

func (e *Etcd) DeleteField(ctx context.Context, indexname string, name string) error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "DeleteField: creating client")
	}
	defer cli.Close()

	key := schemaPrefix + indexname + "/" + name
	// Deleting field and views in one transaction.
	_, err = cli.KV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), ">", -1)).
		Then(
			clientv3.OpDelete(key+"/", clientv3.WithPrefix()), // deleting field views
			clientv3.OpDelete(key),                            // deleting field
		).Commit()

	return errors.Wrap(err, "DeleteField")
}

func (e *Etcd) View(ctx context.Context, indexName, fieldName, name string) ([]byte, error) {
	key := schemaPrefix + indexName + "/" + fieldName + "/" + name
	return e.getKeyBytes(ctx, key)
}

// CreateView differs from CreateIndex and CreateField in that it does not
// return an error if the view already exists. If this logic needs to be
// changed, we likely need to introduce an ErrViewExists variable and return
// that. I decided not to do that now because it would require importing the
// etcd package into the pilosa root package. The better way to do that may be
// to define those error types in the disco package instead.
func (e *Etcd) CreateView(ctx context.Context, indexName, fieldName, name string, val []byte) error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "CreateView: creating client")
	}
	defer cli.Close()

	key := schemaPrefix + indexName + "/" + fieldName + "/" + name

	// Set up Op to write view value as bytes.
	op := clientv3.OpPut(key, "")
	op.WithValueBytes(val)

	// Check for key existence, and execute Op within a transaction.
	_, err = cli.KV.Txn(ctx).
		If(clientv3util.KeyMissing(key)).
		Then(op).
		Commit()
	if err != nil {
		return errors.Wrap(err, "executing transaction")
	}

	return nil
}

func (e *Etcd) DeleteView(ctx context.Context, indexName, fieldName, name string) error {
	return e.delKey(ctx, schemaPrefix+indexName+"/"+fieldName+"/"+name, false)
}

func (e *Etcd) putKey(ctx context.Context, key, val string, opts ...clientv3.OpOption) error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "putKey: creates a new client")
	}
	defer cli.Close()

	if _, err := cli.KV.Put(ctx, key, val, opts...); err != nil {
		return errors.Wrapf(err, "putKey: Put(%s, %s)", key, val)
	}

	return nil
}

func (e *Etcd) getKeyBytes(ctx context.Context, key string) ([]byte, error) {
	cli, err := e.client()
	if err != nil {
		return nil, errors.Wrap(err, "getKeyBytes: creates a new client")
	}
	defer cli.Close()

	// Get the current value for the key.
	resp, err := cli.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// TODO: consider returning a "key does not exist" error instead of (nil, nil)
	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	return resp.Kvs[0].Value, nil
}

func (e *Etcd) getKey(ctx context.Context, key string) ([]string, [][]byte, error) {
	cli, err := e.client()
	if err != nil {
		return nil, nil, errors.Wrap(err, "getKey: creates a new client")
	}
	defer cli.Close()

	resp, err := cli.KV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), ">", -1)).
		Then(clientv3.OpGet(key, clientv3.WithPrefix())).
		Commit()
	if err != nil {
		return nil, nil, err
	}

	if !resp.Succeeded {
		return nil, nil, fmt.Errorf("key %s does not exist", key)
	}

	var (
		keys   []string
		values [][]byte
	)

	for _, r := range resp.Responses {
		for _, kv := range r.GetResponseRange().Kvs {
			keys = append(keys, string(kv.Key))
			values = append(values, kv.Value)
		}
	}

	return keys, values, nil
}

func (e *Etcd) delKey(ctx context.Context, key string, withPrefix bool) error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "delKey: creates a new client")
	}
	defer cli.Close()

	var opts []clientv3.OpOption
	if withPrefix {
		opts = append(opts, clientv3.WithPrefix())
	}

	_, err = cli.KV.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), ">", -1)).
		Then(clientv3.OpDelete(key, opts...)).
		Commit()

	return err
}

func (e *Etcd) leaseKeepAlive(ttl int64) (clientv3.LeaseID, func(context.Context, time.Duration), error) {
	cli, err := e.client()
	if err != nil {
		return 0, nil, errors.Wrap(err, "leaseKeepAlive: creates a new client")
	}
	defer cli.Close()

	leaseResp, err := cli.Grant(context.TODO(), ttl)
	if err != nil {
		return 0, nil, errors.Wrapf(err, "leaseKeepAlive: creates a new lease (TTL: %v)", ttl)
	}

	keepaliveFunc := func(ctx context.Context, tick time.Duration) {
		ticker := time.NewTicker(tick)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Printf("leaseKeepAlive: %v\n", ctx.Err())

				if cli, err := e.client(); err != nil {
					log.Printf("leaseKeepAlive: creates a new client: %v\n", err)
				} else {
					if _, err := cli.Revoke(context.TODO(), leaseResp.ID); err != nil {
						log.Printf("leaseKeepAlive: revokes the lease (ID: %x): %v\n", leaseResp.ID, err)
					}
					cli.Close()
				}
				return

			case <-ticker.C:
				if cli, err := e.client(); err != nil {
					log.Printf("leaseKeepAlive: creates a new client: %v\n", err)
				} else {
					if _, err = cli.KeepAliveOnce(ctx, leaseResp.ID); err != nil {
						log.Printf("leaseKeepAlive: renews the lease (ID: %x): %v\n", leaseResp.ID, err)
					}
					cli.Close()
				}
			}
		}
	}

	return leaseResp.ID, keepaliveFunc, nil
}

func (e *Etcd) client() (*clientv3.Client, error) {
	urls := e.e.Server.Cluster().ClientURLs()

	cli, err := clientv3.NewFromURLs(urls)
	if err != nil {
		return nil, errors.Wrapf(err, "creates a new etcd client from URLs (%v)", urls)
	}

	return cli, nil
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
func (e *Etcd) Shards(ctx context.Context, index, field string) (*roaring.Bitmap, error) {
	cli, err := e.client()
	if err != nil {
		return nil, errors.Wrap(err, "Shards: creating client")
	}
	defer cli.Close()

	return e.shards(ctx, cli, index, field)
}

func (e *Etcd) shards(ctx context.Context, cli *clientv3.Client, index, field string) (*roaring.Bitmap, error) {
	key := path.Join(shardPrefix, index, field)

	// Get the current shards for the field.
	resp, err := cli.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	bm := roaring.NewBitmap()

	if len(resp.Kvs) == 0 {
		return bm, nil
	}

	bytes := resp.Kvs[0].Value
	if err = bm.UnmarshalBinary(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshalling shards")
	}

	return bm, nil
}

// AddShards implements the Sharder interface.
func (e *Etcd) AddShards(ctx context.Context, index, field string, shards *roaring.Bitmap) (*roaring.Bitmap, error) {
	cli, err := e.client()
	if err != nil {
		return nil, errors.Wrap(err, "AddShards: creating client")
	}
	defer cli.Close()

	key := path.Join(shardPrefix, index, field)

	// This tended to add more overhead than it saved.
	// // Read shards outside of a lock just to check if shard is already included.
	// // If shard is already included, no-op.
	// if currentShards, err := e.shards(ctx, cli, index, field); err != nil {
	// 	return nil, errors.Wrap(err, "reading shards")
	// } else if currentShards.Count() == currentShards.Union(shards).Count() {
	// 	return currentShards, nil
	// }

	// Create a session to acquire a lock.
	sess, _ := concurrency.NewSession(cli)
	defer sess.Close()

	muKey := path.Join(lockPrefix, index, field)
	mu := concurrency.NewMutex(sess, muKey)

	// Acquire lock (or wait to have it).
	if err := mu.Lock(ctx); err != nil {
		return nil, errors.Wrap(err, "acquiring lock")
	}

	// Read shards within lock.
	globalShards, err := e.shards(ctx, cli, index, field)
	if err != nil {
		return nil, errors.Wrap(err, "reading shards")
	}

	// Union shard into shards.
	globalShards.UnionInPlace(shards)

	// Write shards to etcd.
	var buf bytes.Buffer
	if _, err := globalShards.WriteTo(&buf); err != nil {
		return nil, errors.Wrap(err, "writing shards to bytes buffer")
	}

	op := clientv3.OpPut(key, "")
	op.WithValueBytes(buf.Bytes())

	if _, err := cli.Do(ctx, op); err != nil {
		return nil, errors.Wrap(err, "doing op")
	}

	// Release lock.
	if err := mu.Unlock(ctx); err != nil {
		return nil, errors.Wrap(err, "releasing lock")
	}

	return globalShards, nil
}

// AddShard implements the Sharder interface.
func (e *Etcd) AddShard(ctx context.Context, index, field string, shard uint64) error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "AddShard: creating client")
	}
	defer cli.Close()

	key := path.Join(shardPrefix, index, field)

	// Read shards outside of a lock just to check if shard is already included.
	// If shard is already included, no-op.
	if shards, err := e.shards(ctx, cli, index, field); err != nil {
		return errors.Wrap(err, "reading shards")
	} else if shards.Contains(shard) {
		return nil
	}

	// According to the previous read, shard is not yet included in shards. So
	// we will acquire a distributed lock, read shards again (in case it has
	// been updated since we last read it), add shard to shards, and finally
	// write shards to etcd.

	// Create a session to acquire a lock.
	sess, _ := concurrency.NewSession(cli)
	defer sess.Close()

	muKey := path.Join(lockPrefix, index, field)
	mu := concurrency.NewMutex(sess, muKey)

	// Acquire lock (or wait to have it).
	if err := mu.Lock(ctx); err != nil {
		return errors.Wrap(err, "acquiring lock")
	}

	// Read shards again (within lock).
	shards, err := e.shards(ctx, cli, index, field)
	if err != nil {
		return errors.Wrap(err, "reading shards")
	}

	if shards.Contains(shard) {
		return nil
	}

	// Union shard into shards.
	shards.UnionInPlace(roaring.NewBitmap(shard))

	// Write shards to etcd.
	var buf bytes.Buffer
	if _, err := shards.WriteTo(&buf); err != nil {
		return errors.Wrap(err, "writing shards to bytes buffer")
	}

	op := clientv3.OpPut(key, "")
	op.WithValueBytes(buf.Bytes())

	if _, err := cli.Do(ctx, op); err != nil {
		return errors.Wrap(err, "doing op")
	}

	// Release lock.
	if err := mu.Unlock(ctx); err != nil {
		return errors.Wrap(err, "releasing lock")
	}

	return nil
}

// RemoveShard implements the Sharder interface.
func (e *Etcd) RemoveShard(ctx context.Context, index, field string, shard uint64) error {
	cli, err := e.client()
	if err != nil {
		return errors.Wrap(err, "RemoveShard: creating client")
	}
	defer cli.Close()

	key := path.Join(shardPrefix, index, field)

	// Read shards outside of a lock just to check if shard is already excluded.
	// If shard is already excluded, no-op.
	if shards, err := e.shards(ctx, cli, index, field); err != nil {
		return errors.Wrap(err, "reading shards")
	} else if !shards.Contains(shard) {
		return nil
	}

	// According to the previous read, shard is included in shards. So
	// we will acquire a distributed lock, read shards again (in case it has
	// been updated since we last read it), remove shard from shards, and finally
	// write shards to etcd.

	// Create a session to acquire a lock.
	sess, _ := concurrency.NewSession(cli)
	defer sess.Close()

	muKey := path.Join(lockPrefix, index, field)
	mu := concurrency.NewMutex(sess, muKey)

	// Acquire lock (or wait to have it).
	if err := mu.Lock(ctx); err != nil {
		return errors.Wrap(err, "acquiring lock")
	}

	// Read shards again (within lock).
	shards, err := e.shards(ctx, cli, index, field)
	if err != nil {
		return errors.Wrap(err, "reading shards")
	}

	if !shards.Contains(shard) {
		return nil
	}

	// Remove shard from shards.
	if _, err := shards.RemoveN(shard); err != nil {
		return errors.Wrap(err, "removing shard")
	}

	// If this is removing the last bit from the shards bitmap, then instead of
	// writing an empty bitmap, just delete the key.
	if shards.Count() == 0 {
		_, err := cli.Delete(ctx, key)
		return err
	}

	// Write shards to etcd.
	var buf bytes.Buffer
	if _, err := shards.WriteTo(&buf); err != nil {
		return errors.Wrap(err, "writing shards to bytes buffer")
	}

	op := clientv3.OpPut(key, "")
	op.WithValueBytes(buf.Bytes())

	if _, err := cli.Do(ctx, op); err != nil {
		return errors.Wrap(err, "doing op")
	}

	// Release lock.
	if err := mu.Unlock(ctx); err != nil {
		return errors.Wrap(err, "releasing lock")
	}

	return nil
}

// Nodes implements the Noder interface. It returns the sorted list of nodes
// based on the etcd peers.
func (e *Etcd) Nodes() []*topology.Node {
	peers := e.Peers()
	nodes := make([]*topology.Node, len(peers))
	for i, peer := range peers {
		node := &topology.Node{}

		if meta, err := e.Metadata(context.Background(), peer.ID); err != nil {
			log.Println(err, "getting metadata") // TODO: handle this with a logger
		} else if err := json.Unmarshal(meta, node); err != nil {
			log.Println(err, "unmarshaling json metadata")
		}

		node.ID = peer.ID

		nodes[i] = node
	}

	// Nodes must be sorted.
	sort.Sort(topology.ByID(nodes))

	return nodes
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
