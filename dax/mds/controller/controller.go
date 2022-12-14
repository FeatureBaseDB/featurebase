// Package controller provides the core Controller struct.
package controller

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/mds/schemar"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"golang.org/x/sync/errgroup"
)

type Controller struct {
	// mu is primarily to protect against conflicting reads/writes to the nodes
	// map. The balancers map is currently never written to after
	// initialization.
	mu sync.RWMutex

	// versionStore
	versionStore dax.VersionStore

	// Schemar used by the controller to get table information. The controller
	// should NOT call Schemar methods which modify data. Schema mutations are
	// made outside of the controller (at this point that happens in MDS).
	Schemar schemar.Schemar

	// nodes is the map of nodes, by address, which have registered with the
	// controller.
	nodeService dax.NodeService

	ComputeBalancer   Balancer
	TranslateBalancer Balancer

	// Director is used to send directives to computer workers.
	Director Director

	// poller is used to notify a Poller if nodes have been added or removed.
	poller dax.AddressManager

	directiveVersion dax.DirectiveVersion

	registrationBatchTimeout time.Duration
	nodeChan                 chan *dax.Node
	stopping                 chan struct{}

	logger logger.Logger
}

var supportedRoleTypes []dax.RoleType = []dax.RoleType{
	dax.RoleTypeCompute,
	dax.RoleTypeTranslate,
}

// New returns a new instance of Controller with default values.
func New(cfg Config) *Controller {
	c := &Controller{
		Schemar: schemar.NewNopSchemar(),

		ComputeBalancer:   cfg.ComputeBalancer,
		TranslateBalancer: cfg.TranslateBalancer,

		Director: NewNopDirector(),

		poller: dax.NewNopAddressManager(),

		logger: logger.NopLogger,

		nodeChan: make(chan *dax.Node, 10),
		stopping: make(chan struct{}),
	}

	if cfg.Logger != nil {
		c.logger = cfg.Logger
	}

	switch cfg.StorageMethod {
	case "boltdb":
		if err := cfg.BoltDB.InitializeBuckets(boltdb.VersionStoreBuckets...); err != nil {
			c.logger.Panicf("initializing version store buckets: %v", err)
		}
		c.versionStore = boltdb.NewVersionStore(cfg.BoltDB, c.logger)

		if err := cfg.BoltDB.InitializeBuckets(boltdb.NodeServiceBuckets...); err != nil {
			c.logger.Panicf("initializing node service buckets: %v", err)
		}
		c.nodeService = boltdb.NewNodeService(cfg.BoltDB, c.logger)

		if err := cfg.BoltDB.InitializeBuckets(boltdb.DirectiveBuckets...); err != nil {
			c.logger.Panicf("initializing directive buckets: %v", err)
		}
		c.directiveVersion = boltdb.NewDirectiveVersion(cfg.BoltDB)
	default:
		c.logger.Panicf("storage method '%s' unsupported. (hint: try boltdb)", cfg.StorageMethod)
	}

	if cfg.Director != nil {
		c.Director = cfg.Director
	}
	if cfg.Schemar != nil {
		c.Schemar = cfg.Schemar
	}

	c.registrationBatchTimeout = cfg.RegistrationBatchTimeout

	return c
}

// Run starts the node registration goroutine.
func (c *Controller) Run() error {
	go c.nodeRegistrationRoutine(c.nodeChan, c.registrationBatchTimeout)

	return nil
}

// Stop stops the node registration routine.
func (c *Controller) Stop() {
	close(c.stopping)
}

func (c *Controller) balancerForRole(rt dax.RoleType) (Balancer, error) {
	var bal Balancer
	if rt == dax.RoleTypeCompute {
		bal = c.ComputeBalancer
	} else if rt == dax.RoleTypeTranslate {
		bal = c.TranslateBalancer
	} else {
		return nil, errors.Errorf("unknown role type: '%s'", rt)
	}
	return bal, nil
}

// RegisterNodes adds nodes to the controller's list of registered
// nodes.
func (c *Controller) RegisterNodes(ctx context.Context, nodes ...*dax.Node) error {
	c.logger.Printf("c.RegisterNodes(): %+v", nodes)
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate input.
	for _, n := range nodes {
		if n.Address == "" {
			return NewErrNodeKeyInvalid(n.Address)
		}
		if len(n.RoleTypes) == 0 {
			return NewErrRoleTypeInvalid(dax.RoleType(""))
		}
		for _, v := range n.RoleTypes {
			if !dax.RoleTypes(supportedRoleTypes).Contains(v) {
				return NewErrRoleTypeInvalid(v)
			}
		}
	}

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	// Create node if we don't already have it
	for _, n := range nodes {
		if node, _ := c.nodeService.ReadNode(ctx, n.Address); node == nil {
			if err := c.nodeService.CreateNode(ctx, n.Address, n); err != nil {
				return errors.Wrapf(err, "creating node: %s", n.Address)
			}

			// Add the node to the workerSet so that it receives a directive.
			// Even if there is currently no data for this worker (i.e. it
			// doesn't result in a diffByAddr entry below), we still want to
			// send it a "reset" directive so that in the off chance it has some
			// local data, that data gets removed.
			workerSet.Add(n.Address)
		}
	}

	// diffByAddr keeps track of the diffs that have been applied to each
	// specific address.
	diffByAddr := make(map[dax.Address]dax.WorkerDiff)

	for _, n := range nodes {
		for _, rt := range n.RoleTypes {
			balancer, err := c.balancerForRole(rt)
			if err != nil {
				return errors.Wrap(err, "getting balancer")
			}
			adiffs, err := balancer.AddWorker(ctx, n.Address)
			if err != nil {
				return errors.Wrap(err, "adding worker")
			}

			// Rebalance so existing jobs can be spread evenly across all nodes,
			// including the node being registered.
			bdiffs, err := balancer.Balance(ctx)
			if err != nil {
				return errors.Wrap(err, "balancing")
			}
			for _, diff := range append(adiffs, bdiffs...) {
				existingDiff, ok := diffByAddr[dax.Address(diff.WorkerID)]
				if !ok {
					existingDiff.WorkerID = diff.WorkerID
				}
				existingDiff.Add(diff)
				diffByAddr[dax.Address(diff.WorkerID)] = existingDiff
			}
		}
	}

	// Add any worker which has a diff to the workerSet so that it receives a
	// directive.
	for addr := range diffByAddr {
		workerSet.Add(addr)
	}

	addrs := []dax.Address{}
	for _, n := range nodes {
		addrs = append(addrs, n.Address)
	}

	// Tell the poller about the new nodes.
	if err := c.poller.AddAddresses(ctx, addrs...); err != nil {
		return NewErrInternal(err.Error())
	}

	// No need to send directives if the workerSet is empty.
	if len(workerSet) == 0 {
		return nil
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	// For the addresses which are being added, set their method to "reset".
	for i := range addressMethods {
		for j := range nodes {
			if addressMethods[i].address == nodes[j].Address {
				addressMethods[i].method = dax.DirectiveMethodReset
			}
		}
	}

	// Get the current job assignments for this worker and send that to the node
	// as a Directive.
	if err := c.sendDirectives(ctx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return nil
}

// RegisterNode adds a node to the controller's list of registered
// nodes. It makes no guarantees about when the node will actually be
// used for anything or assigned any jobs.
func (c *Controller) RegisterNode(ctx context.Context, n *dax.Node) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate input.
	if n.Address == "" {
		return NewErrNodeKeyInvalid(n.Address)
	}
	if len(n.RoleTypes) == 0 {
		return NewErrRoleTypeInvalid(dax.RoleType(""))
	}
	for _, v := range n.RoleTypes {
		if !dax.RoleTypes(supportedRoleTypes).Contains(v) {
			return NewErrRoleTypeInvalid(v)
		}
	}

	if node, _ := c.nodeService.ReadNode(ctx, n.Address); node != nil {
		return nil
	}

	c.nodeChan <- n

	return nil
}

// CheckInNode handles a "check-in" from a compute node. These come
// periodically, and if the controller already knows about the compute node, it
// can simply no-op. If, however, the controller is not aware of the node
// checking in, then that probably means that the poller has removed that node
// from its list (perhaps due to a network fault) and therefore the node needs
// to be re-registered.
func (c *Controller) CheckInNode(ctx context.Context, n *dax.Node) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// If we already know about this node, just no-op. In the future, we may
	// want this check-in payload to include things like the compute node's
	// Directive; then we could check that the compute node is actually doing
	// what we expect it to be doing. But for now, we're just checking that we
	// know about the compute node at all.
	if node, _ := c.nodeService.ReadNode(ctx, n.Address); node != nil {
		return nil
	}

	c.nodeChan <- n

	return nil
}

// DeregisterNodes removes nodes from the controller's list of registered nodes.
// It sends directives to the removed nodes, but ignores errors.
func (c *Controller) DeregisterNodes(ctx context.Context, addresses ...dax.Address) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	// diffByAddr keeps track of the diffs that have been applied to each
	// specific address.
	diffByAddr := make(map[dax.Address]dax.WorkerDiff)

	for _, address := range addresses {
		// Add the removed node to the workerSet so that it receives a
		// directive. Even if there is currently no data for the worker (i.e. it
		// doesn't result in a diffByAddr entry below), we still want to send it
		// a "reset" directive so that in the off chance it has some local data,
		// that data gets removed.
		// TODO(tlt): see below where we actually REMOVE this. We need to
		// address this confusion.
		// workerSet.Add(address)

		// Ensure the host:port is currently registered.
		n, err := c.nodeService.ReadNode(ctx, address)
		if err != nil {
			return errors.Wrapf(err, "reading the node for address: %s", address)
		}
		for _, rt := range n.RoleTypes {
			balancer, err := c.balancerForRole(rt)
			if err != nil {
				c.logger.Printf("Unsupported role type in DeregisterNode: '%s'", rt)
				// Skip any role types which aren't currently supported by a balancer.
				continue
			}
			rdiffs, err := balancer.RemoveWorker(ctx, address)
			if err != nil {
				return errors.Wrap(err, "removing worker")
			}

			// Rebalance so any jobs that were assigned to the node being deregistered
			// get assigned to another node.
			bdiffs, err := balancer.Balance(ctx)
			if err != nil {
				return errors.Wrap(err, "balancing")
			}
			// we assume that the job names are different between the
			// different role types so we don't have to track each
			// role separately which would be annoying.
			for _, diff := range append(rdiffs, bdiffs...) {
				existingDiff, ok := diffByAddr[dax.Address(diff.WorkerID)]
				if !ok {
					existingDiff.WorkerID = diff.WorkerID
				}
				existingDiff.Add(diff)
				diffByAddr[dax.Address(diff.WorkerID)] = existingDiff
			}
		}
	}

	for addr := range diffByAddr {
		workerSet.Add(addr)
	}

	// Don't send a Directive to the removed nodes after all.
	// TODO(tlt): we have to do this because otherwise the send request hangs
	// while holding a mu.Lock on Controller.
	for _, addr := range addresses {
		workerSet.Remove(addr)
	}

	for _, address := range addresses {
		if err := c.nodeService.DeleteNode(ctx, address); err != nil {
			return errors.Wrapf(err, "deleting node at address: %s", address)
		}
	}

	if err := c.poller.RemoveAddresses(ctx, addresses...); err != nil {
		return NewErrInternal(err.Error())
	}

	// No need to send Directives if nothing has ultimately changed.
	if len(workerSet) == 0 {
		return nil
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	// For the addresses which are being removed, set their method to "reset".
	for i := range addressMethods {
		for j := range addresses {
			if addressMethods[i].address == addresses[j] {
				addressMethods[i].method = dax.DirectiveMethodReset
			}
		}
	}

	// Get the current job assignments for these workers and send them to the
	// nodes as Directives.
	if err := c.sendDirectives(ctx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return nil
}

// Nodes returns the list of assigned nodes responsible for the jobs included in
// the given role. If createMissing is true, the Controller will create new jobs
// for any of which it isn't currently aware.
func (c *Controller) Nodes(ctx context.Context, role dax.Role, createMissing bool) ([]dax.AssignedNode, error) {
	nodes := []dax.AssignedNode{}
	var err error

	switch v := role.(type) {
	case *dax.ComputeRole:
		nodes, err = c.nodesCompute(ctx, v, createMissing)
		if err != nil {
			return nil, errors.Wrap(err, "getting compute nodes")
		}

	case *dax.TranslateRole:
		nodes, err = c.nodesTranslate(ctx, v, createMissing)
		if err != nil {
			return nil, errors.Wrap(err, "getting translate nodes")
		}
	}

	return nodes, nil
}

// nodesTranslate is like nodesCompute. See the comments there.
func (c *Controller) nodesTranslate(ctx context.Context, role *dax.TranslateRole, createMissing bool) ([]dax.AssignedNode, error) {
	// Try calling c.nodesTranslate as a read first. If we don't have to actually
	// create any missing partitions, then we won't have to obtain a write lock.
	translateNodes, retryAsWrite, err := c.nodesTranslateReadOrWrite(ctx, role, createMissing, false)
	if err != nil {
		return nil, errors.Wrap(err, "getting translate nodes read or write")
	}

	if retryAsWrite {
		translateNodes, _, err = c.nodesTranslateReadOrWrite(ctx, role, createMissing, retryAsWrite)
		if err != nil {
			return nil, errors.Wrap(err, "getting translate nodes read or write retry")
		}
	}

	return translateNodes, nil
}

func (c *Controller) nodesTranslateReadOrWrite(ctx context.Context, role *dax.TranslateRole, createMissing bool, asWrite bool) ([]dax.AssignedNode, bool, error) {
	if asWrite {
		c.mu.Lock()
		defer c.mu.Unlock()
	} else {
		c.mu.RLock()
		defer c.mu.RUnlock()
	}

	nodes := []dax.AssignedNode{}

	bal := c.TranslateBalancer

	//inJobs := NewStringSet()
	inJobs := dax.NewSet[dax.Job]()
	for _, p := range role.Partitions {
		partitionString := partition(role.TableKey, p).String()
		inJobs.Add(dax.Job(partitionString))
	}

	workers, err := bal.WorkersForJobs(ctx, inJobs.Sorted())
	if err != nil {
		return nil, false, errors.Wrap(err, "getting workers for jobs")
	}

	if createMissing {
		// If any provided jobs were not returned in the WorkersForJobs
		// request, then create those.
		outJobs := dax.NewSet[dax.Job]()
		for _, worker := range workers {
			for _, job := range worker.Jobs {
				outJobs.Add(job)
			}
		}

		missed := inJobs.Minus(outJobs).Sorted()

		if len(missed) > 0 {
			// If we are currently under a read lock, and we get to this point,
			// it means that we have partitions which need to be assigned (and
			// directives sent) to workers. In that case, we need to abort this
			// method run and notify the caller to rety as a write.
			if !asWrite {
				return nil, true, nil
			}

			sort.Slice(missed, func(i, j int) bool { return missed[i] < missed[j] })

			workerSet := NewAddressSet()
			for _, job := range missed {
				j, err := decodePartition(job)
				if err != nil {
					return nil, false, NewErrInternal(err.Error())
				}
				diffs, err := bal.AddJobs(ctx, j)
				if err != nil {
					return nil, false, errors.Wrap(err, "adding job")
				}
				for _, diff := range diffs {
					workerSet.Add(dax.Address(diff.WorkerID))
				}

				// Initialize the partition version to 0.
				qtid := j.table().QualifiedTableID()
				if err := c.versionStore.AddPartitions(ctx, qtid, dax.NewVersionedPartition(j.partitionNum(), 0)); err != nil {
					return nil, false, NewErrInternal(err.Error())
				}
			}

			// Convert the slice of addresses into a slice of addressMethod containing
			// the appropriate method.
			addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

			if err := c.sendDirectives(ctx, addressMethods...); err != nil {
				return nil, false, NewErrDirectiveSendFailure(err.Error())
			}

			// Re-run WorkersForJobs.
			workers, err = bal.WorkersForJobs(ctx, inJobs.Sorted())
			if err != nil {
				return nil, false, errors.Wrap(err, "getting workers for jobs")
			}
		}
	}

	for _, worker := range workers {
		// covert worker.Jobs []string to map[string][]Partition
		translateMap := make(map[dax.TableKey]dax.VersionedPartitions)
		for _, job := range worker.Jobs {
			j, err := decodePartition(job)
			if err != nil {
				return nil, false, NewErrInternal(err.Error())
			}

			// Get the partition version from the local versionStore.
			tkey := j.table()
			qtid := tkey.QualifiedTableID()
			partitionVersion, found, err := c.versionStore.PartitionVersion(ctx, qtid, j.partitionNum())
			if err != nil {
				return nil, false, err
			} else if !found {
				return nil, false, NewErrInternal("partition version not found in cache")
			}

			translateMap[tkey] = append(translateMap[tkey],
				dax.NewVersionedPartition(j.partitionNum(), partitionVersion),
			)
		}

		for table, partitions := range translateMap {
			// Sort the partitions int slice before returning it.
			sort.Sort(partitions)

			nodes = append(nodes, dax.AssignedNode{
				Address: dax.Address(worker.ID),
				Role: &dax.TranslateRole{
					TableKey:   table,
					Partitions: partitions,
				},
			})
		}
	}

	return nodes, false, nil
}

// nodesCompute tries to get the list of compute nodes under a read lock. If the
// call into c.nodesComputeReadOrWrite() comes back with `retryAsWrite = true`,
// then it gets called again but with a write lock so that sendDirective can
// happen, and the receiving compute node can get apply the latest schema,
// without encountering race conditions.
//
// Really, we shouldn't have to rely on the directive being applied within a
// mu.Lock(). Instead, if a client (or the IDK) tries to perform some action on
// a compute node (for example, ingesting data to an index/field), if that
// action fails because the schema on the compute node is not in sync, or if the
// compute node is completely unavailable, the client should ask mds for updated
// node information and keep retrying. Basically, what I'm saying is that a lot
// of the mu.Lock()s in this file can be changed back to mu.RLock()s, and the
// SendDirective() can happen asyncronously without worring about race
// conditions.
//
// The race condition happened when concurrent requests to Nodes() occurred and
// the order of events was:
// - req1 wants node for [idx, 0]
// - req2 wants node for [idx, 0]
// - (req1) [idx, 0] registered in controller to node A
//   - directive sent to node A to create index [idx]
//
// - (req2) receives: node A
// - (req2) tries to ingest data to node A [idx, 0]
// **** RACE: [idx] does not exist because (req1) directive step is not compete
func (c *Controller) nodesCompute(ctx context.Context, role *dax.ComputeRole, createMissing bool) ([]dax.AssignedNode, error) {
	if len(role.Shards) == 0 {
		return c.nodesForTableKey(ctx, role.TableKey)
	}
	// Try calling c.nodesCompute as a read first. If we don't have to actually
	// create any missing shards, then we won't have to obtain a write lock.
	computeNodes, retryAsWrite, err := c.nodesComputeReadOrWrite(ctx, role, createMissing, false)
	if err != nil {
		return nil, errors.Wrap(err, "getting compute nodes read or write")
	}

	if retryAsWrite {
		computeNodes, _, err = c.nodesComputeReadOrWrite(ctx, role, createMissing, retryAsWrite)
		if err != nil {
			return nil, errors.Wrap(err, "getting compute nodes read or write retry")
		}
	}

	return computeNodes, nil
}

func (c *Controller) nodesForTableKey(ctx context.Context, tk dax.TableKey) ([]dax.AssignedNode, error) {
	bal := c.ComputeBalancer
	workers, err := bal.WorkersForJobPrefix(ctx, string(tk))
	if err != nil {
		return nil, errors.Wrapf(err, "getting workers for table: '%s'", tk)
	}

	return c.workersToAssignedNodes(ctx, workers)

}

// nodesComputeReadOrWrite contains the logic for the c.nodesCompute() method,
// but it supports being called with either a read or write lock.
func (c *Controller) nodesComputeReadOrWrite(ctx context.Context, role *dax.ComputeRole, createMissing bool, asWrite bool) ([]dax.AssignedNode, bool, error) {
	if asWrite {
		c.mu.Lock()
		defer c.mu.Unlock()
	} else {
		c.mu.RLock()
		defer c.mu.RUnlock()
	}

	bal := c.ComputeBalancer

	inJobs := dax.NewSet[dax.Job]()
	for _, s := range role.Shards {
		shardString := shard(role.TableKey, s).String()
		inJobs.Add(dax.Job(shardString))
	}

	workers, err := bal.WorkersForJobs(ctx, inJobs.Sorted())
	if err != nil {
		return nil, false, errors.Wrap(err, "getting workers for jobs")
	}

	// figure out if any jobs in the role have no workers assigned
	outJobs := dax.NewSet[dax.Job]()
	for _, worker := range workers {
		for _, job := range worker.Jobs {
			outJobs.Add(job)
		}
	}

	missed := inJobs.Minus(outJobs).Sorted()
	if !createMissing && len(missed) > 0 {
		return nil, false, NewErrUnassignedJobs(missed)
	}

	if createMissing {
		// If any provided jobs were not returned in the WorkersForJobs
		// request, then create those.
		if len(missed) > 0 {
			// If we are currently under a read lock, and we get to this point,
			// it means that we have shards which need to be assigned (and
			// directives sent) to workers. In that case, we need to abort this
			// method run and notify the caller to rety as a write.
			if !asWrite {
				return nil, true, nil
			}

			sort.Slice(missed, func(i, j int) bool { return missed[i] < missed[j] })

			workerSet := NewAddressSet()
			for _, job := range missed {
				j, err := decodeShard(job)
				if err != nil {
					return nil, false, NewErrInternal(err.Error())
				}
				diffs, err := bal.AddJobs(ctx, j)
				if err != nil {
					return nil, false, errors.Wrap(err, "adding job")
				}
				for _, diff := range diffs {
					workerSet.Add(dax.Address(diff.WorkerID))
				}

				// Initialize the shard version to 0.
				qtid := j.table().QualifiedTableID()
				if err := c.versionStore.AddShards(ctx, qtid, dax.NewVersionedShard(j.shardNum(), 0)); err != nil {
					return nil, false, NewErrInternal(err.Error())
				}
			}

			// Convert the slice of addresses into a slice of addressMethod containing
			// the appropriate method.
			addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

			if err := c.sendDirectives(ctx, addressMethods...); err != nil {
				return nil, false, NewErrDirectiveSendFailure(err.Error())
			}

			// Re-run WorkersForJobs.
			workers, err = bal.WorkersForJobs(ctx, inJobs.Sorted())
			if err != nil {
				return nil, false, errors.Wrap(err, "getting workers for jobs")
			}
		}
	}

	nodes, err := c.workersToAssignedNodes(ctx, workers)
	return nodes, false, errors.Wrap(err, "converting to assigned nodes")
}

func (c *Controller) workersToAssignedNodes(ctx context.Context, workers []dax.WorkerInfo) ([]dax.AssignedNode, error) {
	nodes := []dax.AssignedNode{}
	for _, worker := range workers {
		// convert worker.Jobs []string to map[TableName][]Shard
		computeMap := make(map[dax.TableKey]dax.VersionedShards)
		for _, job := range worker.Jobs {
			j, err := decodeShard(job)
			if err != nil {
				return nil, NewErrInternal(err.Error())
			}

			// Get the shard version from the local versionStore.
			tkey := j.table()
			qtid := tkey.QualifiedTableID()
			shardVersion, found, err := c.versionStore.ShardVersion(ctx, qtid, j.shardNum())
			if err != nil {
				return nil, err
			} else if !found {
				return nil, NewErrInternal("shard version not found in cache")
			}

			computeMap[tkey] = append(computeMap[tkey],
				dax.NewVersionedShard(j.shardNum(), shardVersion),
			)
		}

		for table, shards := range computeMap {
			// Sort the shards uint64 slice before returning it.
			sort.Sort(shards)

			nodes = append(nodes, dax.AssignedNode{
				Address: dax.Address(worker.ID),
				Role: &dax.ComputeRole{
					TableKey: table,
					Shards:   shards,
				},
			})
		}
	}
	return nodes, nil
}

// CreateTable adds a table to the versionStore and schemar, and then sends directives
// to all affected nodes based on the change.
func (c *Controller) CreateTable(ctx context.Context, qtbl *dax.QualifiedTable) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	qtid := qtbl.QualifiedID()

	// Add the table to the versionStore.
	if err := c.versionStore.AddTable(ctx, qtid); err != nil {
		return errors.Wrapf(err, "adding table: %s", qtid)
	}

	// Add fields which have string keys to the local versionStore.
	fieldVersions := make(dax.VersionedFields, 0)
	for _, field := range qtbl.Fields {
		if !field.StringKeys() {
			continue
		}

		fieldVersions = append(fieldVersions, dax.VersionedField{
			Name:    field.Name,
			Version: 0,
		})
	}
	if len(fieldVersions) > 0 {
		if err := c.versionStore.AddFields(ctx, qtid, fieldVersions...); err != nil {
			return errors.Wrapf(err, "adding fields: %s, %v", qtid, fieldVersions)
		}
	}

	// If the table is keyed, add partitions to the balancer.
	if qtbl.StringKeys() {
		// workerSet maintains the set of workers which have a job assignment change
		// and therefore need to be sent an updated Directive.
		workerSet := NewAddressSet()

		// Generate the list of partitionsToAdd to be added.
		partitionsToAdd := make(dax.VersionedPartitions, qtbl.PartitionN)
		for partitionNum := 0; partitionNum < qtbl.PartitionN; partitionNum++ {
			partitionsToAdd[partitionNum] = dax.NewVersionedPartition(dax.PartitionNum(partitionNum), 0)
		}

		// Add partitions to versionStore. Version is intentionally set to 0
		// here as this is the initial instance of the partition.
		if err := c.versionStore.AddPartitions(ctx, qtid, partitionsToAdd...); err != nil {
			return NewErrInternal(err.Error())
		}

		stringers := make([]fmt.Stringer, 0, len(partitionsToAdd))
		for _, p := range partitionsToAdd {
			stringers = append(stringers, partition(qtbl.Key(), p))
		}

		diffs, err := c.TranslateBalancer.AddJobs(ctx, stringers...)
		if err != nil {
			return errors.Wrap(err, "adding job")
		}
		for _, diff := range diffs {
			workerSet.Add(dax.Address(diff.WorkerID))
		}

		// Convert the slice of addresses into a slice of addressMethod containing
		// the appropriate method.
		addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

		if err := c.sendDirectives(ctx, addressMethods...); err != nil {
			return NewErrDirectiveSendFailure(err.Error())
		}
	}

	// This is more FieldVersion hackery. Even if the table is not keyed, we
	// still want to manage partition 0 for the table in case any of the table's
	// fields contain string keys (we use partition 0 for field string keys for
	// now; in the future we should distribute/balance the field key translation
	// like we do shards and partitions).
	if !qtbl.StringKeys() {
		// workerSet maintains the set of workers which have a job assignment change
		// and therefore need to be sent an updated Directive.
		workerSet := NewAddressSet()

		p := dax.NewVersionedPartition(0, 0)

		// Add partition 0 to versionStore. Version is intentionally set to 0
		// here as this is the initial instance of the partition.
		if err := c.versionStore.AddPartitions(ctx, qtid, p); err != nil {
			return NewErrInternal(err.Error())
		}

		// We don't currently use the returned diff, other than to determine
		// which worker was affected, because we send the full Directive
		// every time.
		diffs, err := c.TranslateBalancer.AddJobs(ctx, partition(qtbl.Key(), p))
		if err != nil {
			return errors.Wrap(err, "adding job")
		}
		for _, diff := range diffs {
			workerSet.Add(dax.Address(diff.WorkerID))
		}

		// Convert the slice of addresses into a slice of addressMethod containing
		// the appropriate method.
		addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

		if err := c.sendDirectives(ctx, addressMethods...); err != nil {
			return NewErrDirectiveSendFailure(err.Error())
		}
	}

	return nil
}

// DropTable removes a table from the schema and sends directives to all affected
// nodes based on the change.
func (c *Controller) DropTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get the table from the schemar.
	if _, err := c.Schemar.Table(ctx, qtid); err != nil {
		return errors.Wrapf(err, "table not in schemar: %s", qtid)
	}

	// Remove the table from the versionStore.
	// Since the schemar should be the system of record for the existence of a
	// table, if the versionStore is not aware of the table, we just log it and
	// continue.
	shards, partitions, err := c.versionStore.RemoveTable(ctx, qtid)
	if err != nil {
		return errors.Wrapf(err, "removing table: %s", qtid)
	}

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	// Remove shards.
	for _, s := range shards {
		diffs, err := c.ComputeBalancer.RemoveJob(ctx, shard(qtid.Key(), s))
		if err != nil {
			return errors.Wrap(err, "removing job")
		}
		for _, diff := range diffs {
			workerSet.Add(dax.Address(diff.WorkerID))
		}
	}

	// Remove partitions.
	for _, p := range partitions {
		diffs, err := c.TranslateBalancer.RemoveJob(ctx, partition(qtid.Key(), p))
		if err != nil {
			return errors.Wrap(err, "removing job")
		}
		for _, diff := range diffs {
			workerSet.Add(dax.Address(diff.WorkerID))
		}
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	if err := c.sendDirectives(ctx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return nil
}

// Table returns a table by quaified table id.
func (c *Controller) Table(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get the table from the schemar.
	return c.Schemar.Table(ctx, qtid)
}

// Tables returns a list of tables by name.
func (c *Controller) Tables(ctx context.Context, qual dax.TableQualifier, ids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get the tables from the schemar.
	return c.Schemar.Tables(ctx, qual, ids...)
}

// AddShards registers the table/shard combinations with the controller and
// sends the necessary directive.
func (c *Controller) AddShards(ctx context.Context, qtid dax.QualifiedTableID, shards ...dax.VersionedShard) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Add shards to versionStore.
	if err := c.versionStore.AddShards(ctx, qtid, shards...); err != nil {
		return errors.Wrapf(err, "adding shards: %s, %v", qtid, shards)
	}

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	for _, s := range shards {
		// We don't currently use the returned diff, other than to determine
		// which worker was affected, because we send the full Directive every
		// time.
		diffs, err := c.ComputeBalancer.AddJobs(ctx, shard(qtid.Key(), s))
		if err != nil {
			return errors.Wrap(err, "adding job")
		}
		for _, diff := range diffs {
			workerSet.Add(dax.Address(diff.WorkerID))
		}

		// Initialize the shard version to 0.
		if err := c.versionStore.AddShards(ctx, qtid, dax.NewVersionedShard(s.Num, 0)); err != nil {
			return NewErrInternal(err.Error())
		}
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	if err := c.sendDirectives(ctx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return nil
}

// RemoveShards deregisters the table/shard combinations with the controller and
// sends the necessary directives.
func (c *Controller) RemoveShards(ctx context.Context, qtid dax.QualifiedTableID, shards ...dax.VersionedShard) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	for _, s := range shards {
		// We don't currently use the returned diff, other than to determine
		// which worker was affected, because we send the full Directive every
		// time.
		diffs, err := c.ComputeBalancer.RemoveJob(ctx, shard(qtid.Key(), s))
		if err != nil {
			return errors.Wrap(err, "removing job")
		}
		for _, diff := range diffs {
			workerSet.Add(dax.Address(diff.WorkerID))
		}
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	if err := c.sendDirectives(ctx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return nil
}

// sendDirectives sends a directive (based on the current balancer state) to
// each of the nodes provided.
func (c *Controller) sendDirectives(ctx context.Context, addrs ...addressMethod) error {
	// If nodes is empty, return early.
	if len(addrs) == 0 {
		return nil
	}

	directives, err := c.buildDirectives(ctx, addrs, c.versionStore)
	if err != nil {
		return errors.Wrap(err, "building directives")
	}

	errs := make([]error, len(directives))
	var eg errgroup.Group
	for i, dir := range directives {
		i := i
		dir := dir
		eg.Go(func() error {
			errs[i] = c.Director.SendDirective(ctx, dir)
			if dir.IsEmpty() {
				errs[i] = nil
			}
			return errs[i]
		})
	}

	err = eg.Wait()
	if err != nil {
		errCount := 0
		for _, err := range errs {
			if err != nil {
				errCount++
			}
		}
		if doWeCare(directives, errs) {
			// TODO: in this case, we should probably remove nodes
			// that didn't work and retry.
			return errors.Errorf("all directives errored: %+v", errs)
		}
	}

	return nil
}

func doWeCare(directives []*dax.Directive, errors []error) bool {
	for i, directive := range directives {
		err := errors[i]
		if err != nil && !directive.IsEmpty() {
			return true
		}
	}
	return false
}

// addressMethod is used when building a Directive to specify which
// DirectiveMethod should be applied for the given Address.
type addressMethod struct {
	address dax.Address
	method  dax.DirectiveMethod
}

// applyAddressMethod converts the slice of addrs into a slice of addressMethod
// containing the given method.
func applyAddressMethod(addrs []dax.Address, method dax.DirectiveMethod) []addressMethod {
	ams := make([]addressMethod, len(addrs))
	for i := range addrs {
		ams[i] = addressMethod{
			address: addrs[i],
			method:  method,
		}
	}

	return ams
}

// buildDirectives builds a list of directives for the given addrs (i.e. nodes)
// using information (i.e. current state) from the balancers.
func (c *Controller) buildDirectives(ctx context.Context, addrs []addressMethod, versionStore dax.VersionStore) ([]*dax.Directive, error) {
	directives := make([]*dax.Directive, len(addrs))

	for i, addressMethod := range addrs {
		dVersion, err := c.directiveVersion.Increment(ctx, 1)
		if err != nil {
			return nil, errors.Wrap(err, "incrementing directive version")
		}

		d := &dax.Directive{
			Address:        addressMethod.address,
			Method:         addressMethod.method,
			Tables:         []*dax.QualifiedTable{},
			ComputeRoles:   []dax.ComputeRole{},
			TranslateRoles: []dax.TranslateRole{},
			Version:        dVersion,
		}

		// computeMap maps a table to a list of shards for that table. We need
		// to aggregate them here because the list of jobs from WorkerState()
		// can contain a mixture of table/shards.
		computeMap := make(map[dax.TableKey][]dax.VersionedShard)

		// translateMap maps a table to a list of partitions for that table. We
		// need to aggregate them here because the list of jobs from
		// WorkerState() can contain a mixture of table/partitions.
		translateMap := make(map[dax.TableKey]dax.VersionedPartitions)

		// tableSet maintains the set of tables which have a job assignment
		// change and therefore need to be included in the Directive schema.
		tableSet := NewTableSet()

		// ownsPartition0 is the list of tables for which this node owns partition 0.
		// This is used to determine FieldVersion responsiblity.
		ownsPartition0 := make(map[dax.TableKey]struct{}, 0)

		for _, rt := range supportedRoleTypes {
			bal, err := c.balancerForRole(rt)
			if err != nil {
				return nil, errors.Wrap(err, "getting balancer")
			}
			w, err := bal.WorkerState(ctx, dax.Worker(addressMethod.address.String()))
			if err != nil {
				return nil, errors.Wrapf(err, "getting worker state: %s", addressMethod.address)
			}

			switch rt {
			case dax.RoleTypeCompute:
				for _, job := range w.Jobs {
					j, err := decodeShard(job)
					if err != nil {
						return nil, errors.Wrapf(err, "decoding shard job: %s", job)
					}

					// The Shard object decoded from the balancer doesn't
					// contain a valid version (because the balancer
					// intentionally does not store version information). Here,
					// we get the current shard version from the controller's
					// cache (i.e. versionStore) and inject that into the Shard
					// sent in the directive.
					tkey := j.table()
					qtid := tkey.QualifiedTableID()
					shardVersion, found, err := versionStore.ShardVersion(ctx, qtid, j.shardNum())
					if err != nil {
						return nil, errors.Wrapf(err, "getting shard version: %s, %d", qtid, j.shardNum())
					} else if !found {
						return nil, NewErrInternal("shard version not found in cache")
					}

					computeMap[tkey] = append(computeMap[tkey],
						dax.NewVersionedShard(j.shardNum(), shardVersion),
					)
					tableSet.Add(tkey)
				}
			case dax.RoleTypeTranslate:
				for _, job := range w.Jobs {
					j, err := decodePartition(job)
					if err != nil {
						return nil, errors.Wrapf(err, "decoding partition job: %s", job)
					}

					// This check is related to the FieldVersion logic below.
					// Basically, we need to determine if this node is
					// responsible for partition 0 for any table(s), and if so,
					// include FieldVersion in the directive for the node.
					if j.partitionNum() == 0 {
						ownsPartition0[j.table()] = struct{}{}
					}

					// The Partition object decoded from the balancer doesn't
					// contain a valid version (because the balancer
					// intentionally does not store version information). Here,
					// we get the current partition version from the
					// controller's cache (i.e. versionStore) and inject that
					// into the Partition sent in the directive.
					tkey := j.table()
					qtid := tkey.QualifiedTableID()
					partitionVersion, found, err := versionStore.PartitionVersion(ctx, qtid, j.partitionNum())
					if err != nil {
						return nil, errors.Wrapf(err, "getting partition version: %s, %d", qtid, j.partitionNum())
					} else if !found {
						return nil, NewErrInternal("partition version not found in cache")
					}

					translateMap[tkey] = append(translateMap[tkey],
						dax.NewVersionedPartition(j.partitionNum(), partitionVersion),
					)
					tableSet.Add(tkey)
				}
			}
		}

		// Convert the computeMap into a list of ComputeRole.
		for k, v := range computeMap {
			// Because these were encoded as strings in the balancer and may be
			// out of order numerically, sort them as integers.
			//sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
			sort.Sort(dax.VersionedShards(v))

			d.ComputeRoles = append(d.ComputeRoles, dax.ComputeRole{
				TableKey: k,
				Shards:   v,
			})
		}

		// Convert the translateMap into a list of TranslateRole.
		for k, v := range translateMap {
			// Because these were encoded as strings in the balancer and may be
			// out of order numerically, sort them as integers.
			sort.Sort(v)

			d.TranslateRoles = append(d.TranslateRoles, dax.TranslateRole{
				TableKey:   k,
				Partitions: v,
			})
		}

		// Add field-specific TranslateRoles to the node which is responsible
		// for partition 0. This is a bit clunkly; ideally we would handle this
		// the same way we handle shards and partitions, by maintaining a
		// distinct balancer for FieldVersions. But because the query side isn't
		// currently set up to look for field translation anywhere but on the
		// local node (or in the case of MDS, on partition 0), we're keeping
		// everything that way for now.
		for tkey := range ownsPartition0 {
			qtid := tkey.QualifiedTableID()
			table, err := c.Schemar.Table(ctx, qtid)
			if err != nil {
				return nil, errors.Wrapf(err, "getting table: %s", tkey)
			}

			fieldVersions := make(dax.VersionedFields, 0)
			for _, field := range table.Fields {
				if !field.StringKeys() {
					continue
				}

				// Skip the primary key field; it uses table translation.
				if field.IsPrimaryKey() {
					continue
				}

				fieldVersion, found, err := versionStore.FieldVersion(ctx, qtid, field.Name)
				if err != nil {
					return nil, errors.Wrapf(err, "getting field version: %s, %s", qtid, field)
				} else if !found {
					return nil, NewErrInternal("field version not found in cache")
				}

				fieldVersions = append(fieldVersions, dax.VersionedField{
					Name:    field.Name,
					Version: fieldVersion,
				})
			}

			if len(fieldVersions) == 0 {
				continue
			}

			d.TranslateRoles = append(d.TranslateRoles, dax.TranslateRole{
				TableKey: tkey,
				Fields:   fieldVersions,
			})

			tableSet.Add(tkey)
		}
		/////////////// end of FieldVersion logic //////////////////////

		if len(tableSet) > 0 {
			dTables := make([]*dax.QualifiedTable, 0)
			for qual, tblIDs := range tableSet.QualifiedSortedSlice() {
				qtbls, err := c.Schemar.Tables(ctx, qual, tblIDs...)
				if err != nil {
					return nil, errors.Wrapf(err, "getting directive tables for qual: %s", qual)
				}
				dTables = append(dTables, qtbls...)
			}
			d.Tables = dTables
		}

		// Sort ComputeRoles by table.
		sort.Slice(d.ComputeRoles, func(i, j int) bool { return d.ComputeRoles[i].TableKey < d.ComputeRoles[j].TableKey })

		// Sort TranslateRoles by table.
		sort.Slice(d.TranslateRoles, func(i, j int) bool { return d.TranslateRoles[i].TableKey < d.TranslateRoles[j].TableKey })

		directives[i] = d
	}

	return directives, nil
}

func (c *Controller) SetPoller(poller dax.AddressManager) {
	c.poller = poller
}

// InitializePoller sends the list of known nodes (to be polled) to the poller.
// This is useful in the case where MDS has restarted (or has been replaced) and
// its poller is emtpy (i.e. it doesn't know about any nodes).
func (c *Controller) InitializePoller(ctx context.Context) error {
	nodes, err := c.nodeService.Nodes(context.Background())
	if err != nil {
		return errors.Wrap(err, "initializing poller")
	}

	for _, node := range nodes {
		if err := c.poller.AddAddresses(ctx, node.Address); err != nil {
			return errors.Wrapf(err, "adding address to poller: %s", node.Address)
		}
	}

	return nil
}

// SnapshotTable snapshots a table.
func (c *Controller) SnapshotTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	shards, ok, err := c.versionStore.Shards(ctx, qtid)
	if err != nil {
		return errors.Wrap(err, "getting shards from version store")
	} else if !ok {
		return errors.New(errors.ErrUncoded, "got false back from versionStore.Shards")
	}

	for _, shard := range shards {
		if err := c.SnapshotShardData(ctx, qtid, shard.Num); err != nil {
			return errors.Wrapf(err, "snapshotting shard data: qtid: %s, shard: %d", qtid, shard.Num)
		}
	}

	partitions, ok, err := c.versionStore.Partitions(ctx, qtid)
	if err != nil {
		return errors.Wrap(err, "getting partitions from version store")
	} else if !ok {
		return errors.New(errors.ErrUncoded, "got false back from versionStore.Partitions")
	}

	for _, part := range partitions {
		if err := c.SnapshotTableKeys(ctx, qtid, part.Num); err != nil {
			return errors.Wrapf(err, "snapshotting table keys: qtid: %s, partition: %d", qtid, part.Num)
		}
	}

	fields, ok, err := c.versionStore.Fields(ctx, qtid)
	if err != nil {
		return errors.Wrap(err, "getting fields from version store")
	} else if !ok {
		return errors.New(errors.ErrUncoded, "got false back from versionStore.Fields")
	}

	for _, fld := range fields {
		if fld.Name != "_id" {
			if err := c.SnapshotFieldKeys(ctx, qtid, fld.Name); err != nil {
				return errors.Wrapf(err, "snapshotting field keys: qtid: %s, field: %s", qtid, fld.Name)
			}
		}
	}
	return nil
}

// SnapshotShardData forces the compute node responsible for the given shard to
// snapshot that shard, then increment its shard version for logs written to the
// WriteLogger.
func (c *Controller) SnapshotShardData(ctx context.Context, qtid dax.QualifiedTableID, shardNum dax.ShardNum) error {
	// Get the node responsible for the shard.
	bal := c.ComputeBalancer
	job := shard(qtid.Key(), dax.NewVersionedShard(shardNum, -1))
	workers, err := bal.WorkersForJobs(ctx, []dax.Job{dax.Job(job.String())})
	if err != nil {
		return errors.Wrapf(err, "getting workers for jobs: %s", job)
	}
	if len(workers) == 0 {
		c.logger.Printf("no worker found for shard: %s, %d", qtid, shardNum)
		return nil
	}

	addr := dax.Address(workers[0].ID)

	// Send the node a snapshot request.
	req := &dax.SnapshotShardDataRequest{
		Address:  addr,
		TableKey: qtid.Key(),
		ShardNum: shardNum,
	}

	if err := c.Director.SendSnapshotShardDataRequest(ctx, req); err != nil {
		return NewErrInternal(err.Error())
	}

	return nil
}

// SnapshotTableKeys forces the translate node responsible for the given
// partition to snapshot the table keys for that partition, then increment its
// version for logs written to the WriteLogger.
func (c *Controller) SnapshotTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partitionNum dax.PartitionNum) error {
	// Get the node responsible for the partition.
	bal := c.TranslateBalancer
	job := partition(qtid.Key(), dax.NewVersionedPartition(partitionNum, -1))
	workers, err := bal.WorkersForJobs(ctx, []dax.Job{dax.Job(job.String())})
	if err != nil {
		return errors.Wrapf(err, "getting workers for jobs: %s", job)
	}
	if len(workers) == 0 {
		c.logger.Printf("no worker found for partition: %s, %d", qtid, partitionNum)
		return nil
	}

	addr := dax.Address(workers[0].ID)

	// Send the node a snapshot request.
	req := &dax.SnapshotTableKeysRequest{
		Address:      addr,
		TableKey:     qtid.Key(),
		PartitionNum: partitionNum,
	}

	if err := c.Director.SendSnapshotTableKeysRequest(ctx, req); err != nil {
		return NewErrInternal(err.Error())
	}

	return nil
}

// SnapshotFieldKeys forces the translate node responsible for the given field
// to snapshot the keys for that field, then increment its version for logs
// written to the WriteLogger.
func (c *Controller) SnapshotFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName) error {
	// Get the node responsible for the field.
	bal := c.TranslateBalancer
	// Field translation is currently handled by partition 0.
	partitionNum := dax.PartitionNum(0)
	job := partition(qtid.Key(), dax.NewVersionedPartition(partitionNum, -1))

	workers, err := bal.WorkersForJobs(ctx, []dax.Job{dax.Job(job.String())})
	if err != nil {
		return errors.Wrapf(err, "getting workers for jobs: %s", job)
	}
	if len(workers) == 0 {
		c.logger.Printf("no worker found for partition: %s, %d", qtid, partitionNum)
		return nil
	}

	addr := dax.Address(workers[0].ID)

	// Send the node a snapshot request.
	req := &dax.SnapshotFieldKeysRequest{
		Address:  addr,
		TableKey: qtid.Key(),
		Field:    field,
	}

	if err := c.Director.SendSnapshotFieldKeysRequest(ctx, req); err != nil {
		return NewErrInternal(err.Error())
	}

	return nil
}

/////////////

func (c *Controller) ComputeNodes(ctx context.Context, qtid dax.QualifiedTableID, shards dax.ShardNums, isWrite bool) ([]dax.ComputeNode, error) {
	inRole := &dax.ComputeRole{
		TableKey: qtid.Key(),
		Shards:   dax.NewVersionedShards(shards...),
	}

	nodes, err := c.Nodes(ctx, inRole, isWrite)
	if err != nil {
		return nil, errors.Wrap(err, "getting compute nodes")
	}

	computeNodes := make([]dax.ComputeNode, 0)

	for _, node := range nodes {
		role, ok := node.Role.(*dax.ComputeRole)
		if !ok {
			// TODO: this should be impossible, but still, we could use
			// some API (HTTP?) error codes.
			return nil, NewErrInternal("not a compute node")
		}

		computeNodes = append(computeNodes, dax.ComputeNode{
			Address: node.Address,
			Table:   role.TableKey,
			Shards:  role.Shards.Nums(),
		})
	}

	return computeNodes, nil
}

func (c *Controller) TranslateNodes(ctx context.Context, qtid dax.QualifiedTableID, partitions dax.PartitionNums, isWrite bool) ([]dax.TranslateNode, error) {
	inRole := &dax.TranslateRole{
		TableKey:   qtid.Key(),
		Partitions: dax.NewVersionedPartitions(partitions...),
	}

	nodes, err := c.Nodes(ctx, inRole, isWrite)
	if err != nil {
		return nil, errors.Wrap(err, "getting translate nodes")
	}

	translateNodes := make([]dax.TranslateNode, 0)

	for _, node := range nodes {
		role, ok := node.Role.(*dax.TranslateRole)
		if !ok {
			// TODO: this should be impossible, but still, we could use
			// some API (HTTP?) error codes.
			return nil, NewErrInternal("not a translate node")
		}

		translateNodes = append(translateNodes, dax.TranslateNode{
			Address:    node.Address,
			Table:      role.TableKey,
			Partitions: role.Partitions.Nums(),
		})
	}

	return translateNodes, nil
}

func (c *Controller) IngestPartition(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum) (dax.Address, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	partitions := dax.PartitionNums{partition}

	nodes, err := c.TranslateNodes(ctx, qtid, partitions, true)
	if err != nil {
		return "", err
	}

	if l := len(nodes); l == 0 {
		return "", NewErrNoAvailableNode()
	} else if l > 1 {
		return "",
			NewErrInternal(
				fmt.Sprintf("unexpected number of nodes: %d", l))
	}

	node := nodes[0]

	// Verify that the node returned is actually responsible for the partition
	// requested.
	if node.Table != qtid.Key() {
		return "",
			NewErrInternal(
				fmt.Sprintf("table returned (%s) does not match requested (%s)", node.Table, qtid))
	} else if l := len(node.Partitions); l != 1 {
		return "",
			NewErrInternal(
				fmt.Sprintf("unexpected number of partitions returned: %d", l))
	} else if p := node.Partitions[0]; p != partition {
		return "",
			NewErrInternal(
				fmt.Sprintf("partition returned (%d) does not match requested (%d)", p, partition))
	}

	return node.Address, nil
}

////

func (c *Controller) CreateField(ctx context.Context, qtid dax.QualifiedTableID, fld *dax.Field) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	// If the field has string keys, add it to the local versionStore.
	if fld.StringKeys() {
		fieldVersion := dax.VersionedField{
			Name:    fld.Name,
			Version: 0,
		}
		if err := c.versionStore.AddFields(ctx, qtid, fieldVersion); err != nil {
			return errors.Wrapf(err, "adding fields: %s, %s", qtid, fieldVersion)
		}
	}

	// Get the worker responsible for partition 0, which handles field key
	// translation. Be sure to get the current version.
	if v, found, err := c.versionStore.PartitionVersion(ctx, qtid, 0); err != nil {
		return errors.Wrapf(err, "getting partition version: %s/0", qtid)
	} else if found {
		// Get the worker(s) responsible for partition 0.
		job := partition(qtid.Key(), dax.VersionedPartition{
			Num:     0,
			Version: v,
		}).String()
		workers, err := c.TranslateBalancer.WorkersForJobs(ctx, []dax.Job{dax.Job(job)})
		if err != nil {
			return errors.Wrapf(err, "getting workers for job: %s", job)
		}

		for _, w := range workers {
			workerSet.Add(dax.Address(w.ID))
		}
	}

	// Get the list of workers responsible for shard data for this table.
	if state, err := c.ComputeBalancer.CurrentState(ctx); err != nil {
		return errors.Wrap(err, "getting current compute state")
	} else {
		for _, worker := range state {
			for _, job := range worker.Jobs {
				if shard, err := decodeShard(job); err != nil {
					return errors.Wrapf(err, "decoding shard: %s", job)
				} else if shard.table() == qtid.Key() {
					workerSet.Add(dax.Address(worker.ID))
					break
				}
			}
		}
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	// Send a directive to any compute node responsible for this field.
	if err := c.sendDirectives(ctx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return nil
}

func (c *Controller) DropField(ctx context.Context, qtid dax.QualifiedTableID, fldName dax.FieldName) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	// If the field has string keys, remove it from the local versionStore.
	// TODO: implement RemoveField() on VersionStore interface.

	// Get the worker responsible for partition 0, which handles field key
	// translation. Be sure to get the current version.
	if v, found, err := c.versionStore.PartitionVersion(ctx, qtid, 0); err != nil {
		return errors.Wrapf(err, "getting partition version: %s/0", qtid)
	} else if found {
		// Get the worker(s) responsible for partition 0.
		job := partition(qtid.Key(), dax.VersionedPartition{
			Num:     0,
			Version: v,
		}).String()
		workers, err := c.TranslateBalancer.WorkersForJobs(ctx, []dax.Job{dax.Job(job)})
		if err != nil {
			return errors.Wrapf(err, "getting workers for job: %s", job)
		}

		for _, w := range workers {
			workerSet.Add(dax.Address(w.ID))
		}
	}

	// Get the list of workers responsible for shard data for this table.
	if state, err := c.ComputeBalancer.CurrentState(ctx); err != nil {
		return errors.Wrap(err, "getting current compute state")
	} else {
		for _, worker := range state {
			for _, job := range worker.Jobs {
				if shard, err := decodeShard(job); err != nil {
					return errors.Wrapf(err, "decoding shard: %s", job)
				} else if shard.table() == qtid.Key() {
					workerSet.Add(dax.Address(worker.ID))
					break
				}
			}
		}
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	// Send a directive to any compute node responsible for this field.
	if err := c.sendDirectives(ctx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return nil
}

//////////////////////////////////

func (c *Controller) AddAddresses(ctx context.Context, addrs ...dax.Address) error {
	return nil
}

func (c *Controller) RemoveAddresses(ctx context.Context, addrs ...dax.Address) error {
	err := c.DeregisterNodes(ctx, addrs...)
	return errors.Wrapf(err, "deregistering nodes: %s", addrs)
}

func (c *Controller) DebugNodes(ctx context.Context) ([]*dax.Node, error) {
	return c.nodeService.Nodes(ctx)
}
