package etcd

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/featurebasedb/featurebase/v3/disco"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TODO (twg) test coverage once we figure out how to setup external etcd in test

type ExternalEtcd struct {
	parent    *Etcd
	id        string
	peers     []*disco.Peer
	etcdHosts []string
}

func NewExternalEtcd(p *Etcd, o Options) (*ExternalEtcd, error) {
	if o.Cluster == "" {
		return nil, errors.New("cluster required")
	}
	if o.Id == "" {
		return nil, errors.New("nodeid required")
	}
	if o.EtcdHosts == "" {
		return nil, errors.New("remote etcd host required")
	}
	// %% begin sonarcloud ignore %%

	nodes := strings.Split(o.Cluster, ",") //  id=url,id2=url2,....,idn=urln
	peers := make([]*disco.Peer, len(nodes))
	for i, node := range nodes {
		parts := strings.Split(node, "=") // id=url pairs
		peers[i] = &disco.Peer{ID: parts[0], URL: parts[1]}
	}
	this := &ExternalEtcd{}
	this.parent = p
	this.peers = peers
	this.id = o.Id
	// populate parent.KnownNodes
	for _, n := range peers {
		this.parent.seeNode(n.ID)
	}
	this.parent.nodesDirty = true
	// populate parent.sortedNodes
	hosts := strings.Split(o.EtcdHosts, ",")
	this.etcdHosts = make([]string, len(hosts))
	for i := range hosts {
		this.etcdHosts[i] = hosts[i]
	}

	return this, nil
	// %% end sonarcloud ignore %%
}

func (e *ExternalEtcd) ID() string {
	return e.id
}

func (e *ExternalEtcd) Shutdown() {
}

func (e *ExternalEtcd) Peers() []*disco.Peer {
	return e.peers
}

// TODO this is a nonsense implementation right now
// will fill out when understand the issue
func (e *ExternalEtcd) IsLeader() bool {
	return e.Leader().String() == e.ID()
}

func (e *ExternalEtcd) Leader() *disco.Peer {
	// TODO (twg) NOP?
	return e.peers[0]
}

func (e *ExternalEtcd) Startup(ctx context.Context, state disco.InitialClusterState) (disco.InitialClusterState, error) {
	// TODO (twg) trye disabled
	return state, e.parent.startHeartbeatAndWatcher(ctx)
}

func (e *ExternalEtcd) NewClient() (*clientv3.Client, error) {
	return clientv3.New(
		clientv3.Config{
			DialTimeout: 10 * time.Second,
			Endpoints:   e.etcdHosts,
		},
	)
}
