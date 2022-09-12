package etcd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/logger"
	"golang.org/x/sync/errgroup"
)

// fakeCluster is a cluster of just our local etcd wrappers, without the
// rest of featurebase present. This code is expected to migrate into a
// _test.go file later, but for now it's here so we can see code coverage.
type fakeCluster struct {
	tb    testing.TB
	nodes []*Etcd
}

func (f *fakeCluster) Start() error {
	eg, ctx := errgroup.WithContext(context.Background())
	for _, node := range f.nodes {
		node := node
		eg.Go(func() error {
			_, err := node.Start(ctx)
			return err
		})
	}
	return eg.Wait()
}

func (f *fakeCluster) BringUp() error {
	eg, ctx := errgroup.WithContext(context.Background())
	for _, node := range f.nodes {
		node := node
		eg.Go(func() error {
			return node.SetState(ctx, disco.NodeStateStarted)
		})
	}
	return eg.Wait()
}

// AwaitClusterState verifies that node 0 thinks the cluster is in the
// requested state, or tells you why it failed.
func (f *fakeCluster) AwaitClusterState(expected disco.ClusterState, timeout time.Duration) (err error) {
	// You might reasonably ask why we don't use context.WithTimeout for this.
	// The answer is that "context deadline exceeded" would be ambiguous as to
	// whether it was an internal error produced by the etcd stuff, or an error
	// introduced by this function.
	now := time.Now()
	state, err := f.nodes[0].ClusterState(context.Background())
	if err != nil {
		return err
	}
	for state != expected {
		// wait a bit to see if it recovered
		time.Sleep(50 * time.Millisecond)
		state, err = f.nodes[0].ClusterState(context.Background())
		if err != nil {
			return err
		}
		if state != expected {
			nodes := f.nodes[0].Nodes()
			for _, n := range nodes {
				f.tb.Logf("  %s: %s", n.ID, n.State)
			}
		}
		elapsed := time.Since(now)
		if elapsed >= timeout {
			return fmt.Errorf("cluster did not reach state %q after %v", expected, timeout)
		}
		f.tb.Logf("cluster state after %v: %q", elapsed, state)
	}
	return nil
}

// MustAwaitClusterState verifies that node 0 thinks the cluster is in the
// requested state, or fails a test.
func (f *fakeCluster) MustAwaitClusterState(expected disco.ClusterState, timeout time.Duration) {
	err := f.AwaitClusterState(expected, timeout)
	if err != nil {
		f.tb.Fatalf("awaiting cluster state %s: %v", expected, err)
	}
}

func (f *fakeCluster) Stop() error {
	for i, node := range f.nodes {
		err := node.Close()
		if err != nil {
			return fmt.Errorf("failure closing node %d: %v", i, err)
		}
	}
	return nil
}

func (f *fakeCluster) MustNodeStates() []disco.NodeState {
	nodes := f.nodes[0].Nodes()
	states := make([]disco.NodeState, len(nodes))
	for i, node := range nodes {
		states[i] = node.State
	}
	return states
}

func (f *fakeCluster) MustClusterState() disco.ClusterState {
	state, err := f.nodes[0].ClusterState(context.TODO())
	if err != nil {
		f.tb.Fatalf("getting cluster state: %v", err)
	}
	return state
}

// Elect tries to force a leader election by identifying a leader
// and then stopping it. The cluster, if it has at least 3 members,
// should still stay running.
func (f *fakeCluster) Elect() (oldleader *Etcd, err error) {
	var leader *Etcd
	for i, node := range f.nodes {
		if node.IsLeader() {
			f.tb.Logf("leader is node %d, stopping it", i)
			leader = node
			copy(f.nodes[i:], f.nodes[i+1:])
			f.nodes = f.nodes[:len(f.nodes)-1]
		}
	}
	// close the leader
	err = leader.Close()
	if err != nil {
		return leader, err
	}
	_, err = f.nodes[0].ClusterState(context.TODO())
	return leader, err
}

// NewFakeCluster creates a fakeCluster of n nodes, providing only the
// etcd objects, not the rest of a featurebase install.
func NewFakeCluster(tb testing.TB, n int, replicas int) *fakeCluster {
	logger := logger.NewLogfLogger(tb)
	_, opts := GenEtcdConfigs(tb, n)
	fc := &fakeCluster{nodes: make([]*Etcd, n)}
	for i := range fc.nodes {
		fc.nodes[i] = NewEtcd(opts[i], logger, replicas, "foo")
	}
	fc.tb = tb
	return fc
}
