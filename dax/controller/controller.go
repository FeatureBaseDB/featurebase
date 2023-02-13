// Package controller provides the core Controller struct.
package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/computer"
	"github.com/featurebasedb/featurebase/v3/dax/controller/poller"
	"github.com/featurebasedb/featurebase/v3/dax/controller/schemar"
	"github.com/featurebasedb/featurebase/v3/dax/snapshotter"
	"github.com/featurebasedb/featurebase/v3/dax/writelogger"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"golang.org/x/sync/errgroup"
)

// Ensure type implements interface.
var _ computer.Registrar = (*Controller)(nil)
var _ dax.Schemar = (*Controller)(nil)
var _ dax.NodeService = (*Controller)(nil)

type Controller struct {
	// Schemar is used by the controller to get table, and other schema,
	// information.
	Schemar  schemar.Schemar
	Balancer Balancer

	// Because we stopped using a storage method interface, and always use bolt,
	// we need to be sure to close the boltDBs that are created in controller.New()
	// whenever controller.Stop() is called. These are pointers to that DB so we can
	// close it.
	BoltDB *boltdb.DB

	Snapshotter *snapshotter.Snapshotter
	Writelogger *writelogger.Writelogger

	// Director is used to send directives to computer workers.
	Director Director

	DirectiveVersion dax.DirectiveVersion

	poller *poller.Poller

	registrationBatchTimeout time.Duration
	nodeChan                 chan *dax.Node
	snappingTurtleTimeout    time.Duration
	snapControl              chan struct{}
	stopping                 chan struct{}

	logger logger.Logger
}

var supportedRoleTypes []dax.RoleType = []dax.RoleType{
	dax.RoleTypeCompute,
	dax.RoleTypeTranslate,
}

// New returns a new instance of Controller with default values.
func New(cfg Config) *Controller {
	// Set up logger.
	var logr logger.Logger = logger.StderrLogger
	if cfg.Logger != nil {
		logr = cfg.Logger
	}

	c := &Controller{
		Schemar: schemar.NewNopSchemar(),

		Balancer: NewNopBalancer(),

		Director: NewNopDirector(),

		registrationBatchTimeout: cfg.RegistrationBatchTimeout,
		nodeChan:                 make(chan *dax.Node, 10),
		snappingTurtleTimeout:    cfg.SnappingTurtleTimeout,
		snapControl:              make(chan struct{}),

		stopping: make(chan struct{}),
		logger:   logr,
	}

	// Poller.
	pollerCfg := poller.Config{
		AddressManager: c,
		NodeService:    c,
		NodePoller:     poller.NewHTTPNodePoller(logr),
		PollInterval:   cfg.PollInterval,
		Logger:         logr,
	}
	c.poller = poller.New(pollerCfg)

	// Snapshotter.
	c.Snapshotter = snapshotter.New(cfg.SnapshotterDir, c.logger)

	// Writelogger.
	c.Writelogger = writelogger.New(cfg.WriteloggerDir, c.logger)

	return c
}

// Start starts long running subroutines.
func (c *Controller) Start() error {
	c.poller.Run()

	go c.nodeRegistrationRoutine(c.nodeChan, c.registrationBatchTimeout)
	go c.snappingTurtleRoutine(c.snappingTurtleTimeout, c.snapControl, c.logger.WithPrefix("Snapping Turtle: "))

	return nil
}

// Stop stops the node registration routine.
func (c *Controller) Stop() error {
	c.poller.Stop()

	close(c.stopping)

	return nil
}

// RegisterNodes adds nodes to the controller's list of registered
// nodes.
func (c *Controller) RegisterNodes(ctx context.Context, nodes ...*dax.Node) error {
	c.logger.Printf("c.RegisterNodes(): %s", dax.Nodes(nodes))

	tx, err := c.BoltDB.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

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

	// diffByAddr keeps track of the diffs that have been applied to each
	// specific address.
	// TODO(tlt): I don't understand why we're keeping track of the
	// dax.WorkerDiff here (as opposed to just the unique Address) because it
	// doesn't ever seem to be used.
	diffByAddr := make(map[dax.Address]dax.WorkerDiff)

	// Create node if we don't already have it
	for _, n := range nodes {
		// If the node already exists, skip it.
		if node, _ := c.Balancer.ReadNode(tx, n.Address); node != nil {
			continue
		}

		// Add the node to the workerSet so that it receives a directive.
		// Even if there is currently no data for this worker (i.e. it
		// doesn't result in a diffByAddr entry below), we still want to
		// send it a "reset" directive so that in the off chance it has some
		// local data, that data gets removed.
		workerSet.Add(n.Address)

		adiffs, err := c.Balancer.AddWorker(tx, n)
		if err != nil {
			return errors.Wrap(err, "adding worker")
		}

		for _, diff := range adiffs {
			existingDiff, ok := diffByAddr[dax.Address(diff.Address)]
			if !ok {
				existingDiff.Address = diff.Address
			}
			existingDiff.Add(diff)
			diffByAddr[dax.Address(diff.Address)] = existingDiff
		}
	}

	// Add any worker which has a diff to the workerSet so that it receives a
	// directive.
	for addr := range diffByAddr {
		workerSet.Add(addr)
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
	if err := c.sendDirectives(tx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return tx.Commit()
}

// RegisterNode adds a node to the controller's list of registered
// nodes. It makes no guarantees about when the node will actually be
// used for anything or assigned any jobs.
func (c *Controller) RegisterNode(ctx context.Context, n *dax.Node) error {
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

	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	if node, _ := c.Balancer.ReadNode(tx, n.Address); node != nil {
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
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	// If we already know about this node, just no-op. In the future, we may
	// want this check-in payload to include things like the compute node's
	// Directive; then we could check that the compute node is actually doing
	// what we expect it to be doing. But for now, we're just checking that we
	// know about the compute node at all.
	if node, _ := c.Balancer.ReadNode(tx, n.Address); node != nil {
		return nil
	}

	c.nodeChan <- n

	return nil
}

// DeregisterNodes removes nodes from the controller's list of registered nodes.
// It sends directives to the removed nodes, but ignores errors.
func (c *Controller) DeregisterNodes(ctx context.Context, addresses ...dax.Address) error {
	tx, err := c.BoltDB.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

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

		rdiffs, err := c.Balancer.RemoveWorker(tx, address)
		if err != nil {
			return errors.Wrapf(err, "removing worker: %s", address)
		}

		// we assume that the job names are different between the
		// different role types so we don't have to track each
		// role separately which would be annoying.
		for _, diff := range rdiffs {
			existingDiff, ok := diffByAddr[dax.Address(diff.Address)]
			if !ok {
				existingDiff.Address = diff.Address
			}
			existingDiff.Add(diff)
			diffByAddr[dax.Address(diff.Address)] = existingDiff
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

	// No need to send Directives if nothing has ultimately changed.
	if len(workerSet) == 0 {
		return tx.Commit()
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
	if err := c.sendDirectives(tx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return tx.Commit()
}

func (c *Controller) nodesTranslateReadOrWrite(tx dax.Transaction, role *dax.TranslateRole, qdbid dax.QualifiedDatabaseID, createMissing bool, asWrite bool) ([]dax.AssignedNode, bool, error) {
	qtid := role.TableKey.QualifiedTableID()
	roleType := dax.RoleTypeTranslate

	inJobs := dax.NewSet[dax.Job]()
	for _, p := range role.Partitions {
		partitionString := partition(role.TableKey, p).String()
		inJobs.Add(dax.Job(partitionString))
	}

	workers, err := c.Balancer.WorkersForJobs(tx, roleType, qdbid, inJobs.Sorted()...)
	if err != nil {
		return nil, false, errors.Wrap(err, "getting workers for jobs")
	}

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

	// If any provided jobs were not returned in the WorkersForJobs request,
	// then create those.
	if createMissing && len(missed) > 0 {
		// If we are currently under a read lock, and we get to this point, it
		// means that we have partitions which need to be assigned (and
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
			diffs, err := c.Balancer.AddJobs(tx, roleType, qtid, j.Job())
			if err != nil {
				return nil, false, errors.Wrap(err, "adding job")
			}
			for _, diff := range diffs {
				workerSet.Add(dax.Address(diff.Address))
			}
		}

		// Convert the slice of addresses into a slice of addressMethod
		// containing the appropriate method.
		addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

		if err := c.sendDirectives(tx, addressMethods...); err != nil {
			return nil, false, NewErrDirectiveSendFailure(err.Error())
		}

		// Re-run WorkersForJobs.
		workers, err = c.Balancer.WorkersForJobs(tx, roleType, qdbid, inJobs.Sorted()...)
		if err != nil {
			return nil, false, errors.Wrap(err, "getting workers for jobs")
		}
	}

	nodes, err := c.translateWorkersToAssignedNodes(tx, workers)
	return nodes, false, errors.Wrap(err, "converting to assigned nodes")
}

// nodesComputeReadOrWrite contains the logic for the c.nodesCompute() method,
// but it supports being called with either a read or write lock.
func (c *Controller) nodesComputeReadOrWrite(tx dax.Transaction, role *dax.ComputeRole, qdbid dax.QualifiedDatabaseID, createMissing bool, asWrite bool) ([]dax.AssignedNode, bool, error) {
	qtid := role.TableKey.QualifiedTableID()
	roleType := dax.RoleTypeCompute

	inJobs := dax.NewSet[dax.Job]()
	for _, s := range role.Shards {
		shardString := shard(role.TableKey, s).String()
		inJobs.Add(dax.Job(shardString))
	}

	workers, err := c.Balancer.WorkersForJobs(tx, roleType, qdbid, inJobs.Sorted()...)
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

	// If any provided jobs were not returned in the WorkersForJobs request,
	// then create those.
	if createMissing && len(missed) > 0 {
		// If we are currently under a read lock, and we get to this point, it
		// means that we have shards which need to be assigned (and directives
		// sent) to workers. In that case, we need to abort this method run and
		// notify the caller to rety as a write.
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
			diffs, err := c.Balancer.AddJobs(tx, roleType, qtid, j.Job())
			if err != nil {
				return nil, false, errors.Wrap(err, "adding job")
			}
			for _, diff := range diffs {
				workerSet.Add(dax.Address(diff.Address))
			}
		}

		// Convert the slice of addresses into a slice of addressMethod
		// containing the appropriate method.
		addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

		if err := c.sendDirectives(tx, addressMethods...); err != nil {
			return nil, false, NewErrDirectiveSendFailure(err.Error())
		}

		// Re-run WorkersForJobs.
		workers, err = c.Balancer.WorkersForJobs(tx, roleType, qdbid, inJobs.Sorted()...)
		if err != nil {
			return nil, false, errors.Wrap(err, "getting workers for jobs")
		}
	}

	nodes, err := c.computeWorkersToAssignedNodes(tx, workers)
	return nodes, false, errors.Wrap(err, "converting to assigned nodes")
}

func (c *Controller) computeWorkersToAssignedNodes(tx dax.Transaction, workers []dax.WorkerInfo) ([]dax.AssignedNode, error) {
	nodes := []dax.AssignedNode{}
	for _, worker := range workers {
		// convert worker.Jobs []string to map[TableName][]Shard
		computeMap := make(map[dax.TableKey]dax.ShardNums)
		for _, job := range worker.Jobs {
			j, err := decodeShard(job)
			if err != nil {
				return nil, NewErrInternal(err.Error())
			}

			computeMap[j.table()] = append(computeMap[j.table()], j.shardNum())
		}

		for table, shards := range computeMap {
			// Sort the shards uint64 slice before returning it.
			sort.Sort(shards)

			nodes = append(nodes, dax.AssignedNode{
				Address: dax.Address(worker.Address),
				Role: &dax.ComputeRole{
					TableKey: table,
					Shards:   shards,
				},
			})
		}
	}
	return nodes, nil
}

func (c *Controller) translateWorkersToAssignedNodes(tx dax.Transaction, workers []dax.WorkerInfo) ([]dax.AssignedNode, error) {
	nodes := []dax.AssignedNode{}
	for _, worker := range workers {
		// covert worker.Jobs []string to map[string][]Partition
		translateMap := make(map[dax.TableKey]dax.PartitionNums)
		for _, job := range worker.Jobs {
			j, err := decodePartition(job)
			if err != nil {
				return nil, NewErrInternal(err.Error())
			}

			translateMap[j.table()] = append(translateMap[j.table()], j.partitionNum())
		}

		for table, partitions := range translateMap {
			// Sort the partitions int slice before returning it.
			sort.Sort(partitions)

			nodes = append(nodes, dax.AssignedNode{
				Address: dax.Address(worker.Address),
				Role: &dax.TranslateRole{
					TableKey:   table,
					Partitions: partitions,
				},
			})
		}
	}
	return nodes, nil
}

// CreateDatabase adds a database to the schemar.
func (c *Controller) CreateDatabase(ctx context.Context, qdb *dax.QualifiedDatabase) error {
	tx, err := c.BoltDB.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	// Create Database ID.
	if _, err := qdb.CreateID(); err != nil {
		return errors.Wrap(err, "creating database ID")
	}

	if err := c.Schemar.CreateDatabase(tx, qdb); err != nil {
		return errors.Wrap(err, "creating database in schemar")
	}

	return tx.Commit()
}

func (c *Controller) DropDatabase(ctx context.Context, qdbid dax.QualifiedDatabaseID) error {
	tx, err := c.BoltDB.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	// Get all the tables for the database and call dropTable on each one.
	qtbls, err := c.Schemar.Tables(tx, qdbid)
	if err != nil {
		return errors.Wrapf(err, "getting tables for database: %s", qdbid)
	}

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	for _, qtbl := range qtbls {
		qtid := qtbl.QualifiedID()
		addrs, err := c.dropTable(tx, qtid)
		if err != nil {
			return errors.Wrapf(err, "dropping table: %s", qtid)
		}
		workerSet.Merge(addrs)
	}

	// Drop the database record from the schema.
	if err := c.Schemar.DropDatabase(tx, qdbid); err != nil {
		return errors.Wrap(err, "dropping database from schemar")
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	// Finally, send the directives based on the dropTable calls.
	if err := c.sendDirectives(tx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return tx.Commit()
}

// DatabaseByName returns the database for the given name.
func (c *Controller) DatabaseByName(ctx context.Context, orgID dax.OrganizationID, dbname dax.DatabaseName) (*dax.QualifiedDatabase, error) {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	qdb, err := c.Schemar.DatabaseByName(tx, orgID, dbname)
	if err != nil {
		return nil, errors.Wrap(err, "getting database by name from schemar")
	}

	return qdb, nil
}

// DatabaseByID returns the database for the given id.
func (c *Controller) DatabaseByID(ctx context.Context, qdbid dax.QualifiedDatabaseID) (*dax.QualifiedDatabase, error) {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	qdb, err := c.Schemar.DatabaseByID(tx, qdbid)
	if err != nil {
		return nil, errors.Wrap(err, "getting database by id from schemar")
	}

	return qdb, nil
}

// SetDatabaseOption sets the option on the given database.
func (c *Controller) SetDatabaseOption(ctx context.Context, qdbid dax.QualifiedDatabaseID, option string, value string) error {
	tx, err := c.BoltDB.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	if err := c.Schemar.SetDatabaseOption(tx, qdbid, option, value); err != nil {
		return errors.Wrapf(err, "setting database option: %s", option)
	}

	diffs, err := c.Balancer.BalanceDatabase(tx, qdbid)
	if err != nil {
		return errors.Wrapf(err, "balancing database: %s", qdbid)
	}

	workerSet := NewAddressSet()
	for _, diff := range diffs {
		workerSet.Add(dax.Address(diff.Address))
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)
	if err := c.sendDirectives(tx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return tx.Commit()
}

func (c *Controller) Databases(ctx context.Context, orgID dax.OrganizationID, ids ...dax.DatabaseID) ([]*dax.QualifiedDatabase, error) {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	// Get the tables from the schemar.
	return c.Schemar.Databases(tx, orgID, ids...)
}

// CreateTable adds a table to the schemar, and then sends directives to all
// affected nodes based on the change.
func (c *Controller) CreateTable(ctx context.Context, qtbl *dax.QualifiedTable) error {
	tx, err := c.BoltDB.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	// Create Table ID.
	if _, err := qtbl.CreateID(); err != nil {
		return errors.Wrap(err, "creating table ID")
	}

	// Create the table in schemar.
	if err := c.Schemar.CreateTable(tx, qtbl); err != nil {
		return errors.Wrapf(err, "creating table: %s", qtbl)
	}

	// If the table is keyed, add partitions to the balancer.
	if qtbl.StringKeys() {
		// workerSet maintains the set of workers which have a job assignment change
		// and therefore need to be sent an updated Directive.
		workerSet := NewAddressSet()

		// Generate the list of partitionsToAdd to be added.
		partitionsToAdd := make(dax.PartitionNums, qtbl.PartitionN)
		for partitionNum := 0; partitionNum < qtbl.PartitionN; partitionNum++ {
			partitionsToAdd[partitionNum] = dax.PartitionNum(partitionNum)
		}

		jobs := make([]dax.Job, 0, len(partitionsToAdd))
		for _, p := range partitionsToAdd {
			jobs = append(jobs, partition(qtbl.Key(), p).Job())
		}

		diffs, err := c.Balancer.AddJobs(tx, dax.RoleTypeTranslate, qtbl.QualifiedID(), jobs...)
		if err != nil {
			return errors.Wrap(err, "adding job")
		}
		for _, diff := range diffs {
			workerSet.Add(dax.Address(diff.Address))
		}

		// Convert the slice of addresses into a slice of addressMethod containing
		// the appropriate method.
		addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

		if err := c.sendDirectives(tx, addressMethods...); err != nil {
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

		p := dax.PartitionNum(0)

		// We don't currently use the returned diff, other than to determine
		// which worker was affected, because we send the full Directive
		// every time.
		diffs, err := c.Balancer.AddJobs(tx, dax.RoleTypeTranslate, qtbl.QualifiedID(), partition(qtbl.Key(), p).Job())
		if err != nil {
			return errors.Wrap(err, "adding job")
		}
		for _, diff := range diffs {
			workerSet.Add(dax.Address(diff.Address))
		}

		// Convert the slice of addresses into a slice of addressMethod containing
		// the appropriate method.
		addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

		if err := c.sendDirectives(tx, addressMethods...); err != nil {
			return NewErrDirectiveSendFailure(err.Error())
		}
	}

	return tx.Commit()
}

// DropTable removes a table from the schema and sends directives to all affected
// nodes based on the change.
func (c *Controller) DropTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	tx, err := c.BoltDB.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	addrs, err := c.dropTable(tx, qtid)
	if err != nil {
		return errors.Wrapf(err, "dropping table: %s", qtid)
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(addrs.SortedSlice(), dax.DirectiveMethodDiff)

	if err := c.sendDirectives(tx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return tx.Commit()
}

// dropTable removes a table from the schema and sends directives to all affected
// nodes based on the change.
func (c *Controller) dropTable(tx dax.Transaction, qtid dax.QualifiedTableID) (AddressSet, error) {
	// Get the table from the schemar.
	if _, err := c.Schemar.Table(tx, qtid); err != nil {
		return nil, errors.Wrapf(err, "table not in schemar: %s", qtid)
	}

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	diffs, err := c.Balancer.RemoveJobs(tx, dax.RoleTypeCompute, qtid)
	if err != nil {
		return nil, errors.Wrapf(err, "removing compute jobs for table: %s", qtid)
	}
	for _, diff := range diffs {
		workerSet.Add(dax.Address(diff.Address))
	}

	diffs, err = c.Balancer.RemoveJobs(tx, dax.RoleTypeTranslate, qtid)
	if err != nil {
		return nil, errors.Wrapf(err, "removing translate jobs for table: %s", qtid)
	}
	for _, diff := range diffs {
		workerSet.Add(dax.Address(diff.Address))
	}

	// Remove table from schemar.
	if err := c.Schemar.DropTable(tx, qtid); err != nil {
		return nil, errors.Wrapf(err, "dropping table from schemar: %s", qtid)
	}

	// Delete relavent table files from snapshotter and writelogger.
	if err := c.Snapshotter.DeleteTable(qtid); err != nil {
		return nil, errors.Wrap(err, "deleting from snapshotter")
	}
	if err := c.Writelogger.DeleteTable(qtid); err != nil {
		return nil, errors.Wrap(err, "deleting from writelogger")
	}

	return workerSet, nil
}

// TableByID returns a table by quaified table id.
func (c *Controller) TableByID(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	// Get the table from the schemar.
	return c.Schemar.Table(tx, qtid)
}

// Tables returns a list of tables by name.
func (c *Controller) Tables(ctx context.Context, qdbid dax.QualifiedDatabaseID, ids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	// Get the tables from the schemar.
	return c.Schemar.Tables(tx, qdbid, ids...)
}

// RemoveShards deregisters the table/shard combinations with the controller and
// sends the necessary directives.
func (c *Controller) RemoveShards(ctx context.Context, qtid dax.QualifiedTableID, shards ...dax.ShardNum) error {
	tx, err := c.BoltDB.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	for _, s := range shards {
		// We don't currently use the returned diff, other than to determine
		// which worker was affected, because we send the full Directive every
		// time.
		diffs, err := c.Balancer.RemoveJobs(tx, dax.RoleTypeCompute, qtid, shard(qtid.Key(), s).Job())
		if err != nil {
			return errors.Wrap(err, "removing job")
		}
		for _, diff := range diffs {
			workerSet.Add(dax.Address(diff.Address))
		}
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	if err := c.sendDirectives(tx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return tx.Commit()
}

// sendDirectives sends a directive (based on the current balancer state) to
// each of the nodes provided.
func (c *Controller) sendDirectives(tx dax.Transaction, addrs ...addressMethod) error {
	// If nodes is empty, return early.
	if len(addrs) == 0 {
		return nil
	}

	directives, err := c.buildDirectives(tx, addrs)
	if err != nil {
		return errors.Wrap(err, "building directives")
	}

	errs := make([]error, len(directives))
	var eg errgroup.Group
	for i, dir := range directives {
		i := i
		dir := dir
		eg.Go(func() error {
			errs[i] = c.Director.SendDirective(tx.Context(), dir)
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
func (c *Controller) buildDirectives(tx dax.Transaction, addrs []addressMethod) ([]*dax.Directive, error) {
	directives := make([]*dax.Directive, len(addrs))

	for i, addressMethod := range addrs {
		dVersion, err := c.DirectiveVersion.Increment(tx, 1)
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
		computeMap := make(map[dax.TableKey][]dax.ShardNum)

		// translateMap maps a table to a list of partitions for that table. We
		// need to aggregate them here because the list of jobs from
		// WorkerState() can contain a mixture of table/partitions.
		translateMap := make(map[dax.TableKey][]dax.PartitionNum)

		// tableSet maintains the set of tables which have a job assignment
		// change and therefore need to be included in the Directive schema.
		tableSet := NewTableSet()

		// ownsPartition0 is the list of tables for which this node owns partition 0.
		// This is used to determine FieldVersion responsiblity.
		ownsPartition0 := make(map[dax.TableKey]struct{}, 0)

		for _, rt := range supportedRoleTypes {
			w, err := c.Balancer.WorkerState(tx, rt, addressMethod.address)
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

					tkey := j.table()
					computeMap[tkey] = append(computeMap[tkey], j.shardNum())
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

					tkey := j.table()
					translateMap[tkey] = append(translateMap[tkey], j.partitionNum())
					tableSet.Add(tkey)
				}
			}
		}

		// Convert the computeMap into a list of ComputeRole.
		for k, v := range computeMap {
			// Because these were encoded as strings in the balancer and may be
			// out of order numerically, sort them as integers.
			//sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
			sort.Sort(dax.ShardNums(v))

			d.ComputeRoles = append(d.ComputeRoles, dax.ComputeRole{
				TableKey: k,
				Shards:   v,
			})
		}

		// Convert the translateMap into a list of TranslateRole.
		for k, v := range translateMap {
			// Because these were encoded as strings in the balancer and may be
			// out of order numerically, sort them as integers.
			sort.Sort(dax.PartitionNums(v))

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
		// local node (or in the case of Serverless, on partition 0), we're
		// keeping everything that way for now.
		for tkey := range ownsPartition0 {
			qtid := tkey.QualifiedTableID()
			table, err := c.Schemar.Table(tx, qtid)
			if err != nil {
				return nil, errors.Wrapf(err, "getting table: %s", tkey)
			}

			fieldNames := make([]dax.FieldName, 0)
			for _, field := range table.Fields {
				if !field.StringKeys() {
					continue
				}

				// Skip the primary key field; it uses table translation.
				if field.IsPrimaryKey() {
					continue
				}

				fieldNames = append(fieldNames, field.Name)
			}

			if len(fieldNames) == 0 {
				continue
			}

			d.TranslateRoles = append(d.TranslateRoles, dax.TranslateRole{
				TableKey: tkey,
				Fields:   fieldNames,
			})

			tableSet.Add(tkey)
		}
		/////////////// end of FieldVersion logic //////////////////////

		if len(tableSet) > 0 {
			dTables := make([]*dax.QualifiedTable, 0)
			for qdbid, tblIDs := range tableSet.QualifiedSortedSlice() {
				qtbls, err := c.Schemar.Tables(tx, qdbid, tblIDs...)
				if err != nil {
					return nil, errors.Wrapf(err, "getting directive tables for qdbid: %s", qdbid)
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

// SnapshotTable snapshots a table. It might also snapshot everything
// else... no guarantees here, only used in tests as of this writing.
func (c *Controller) SnapshotTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	c.snapControl <- struct{}{}
	return nil
}

// SnapshotShardData forces the compute node responsible for the given shard to
// snapshot that shard, then increment its shard version for logs written to the
// Writelogger.
func (c *Controller) SnapshotShardData(ctx context.Context, qtid dax.QualifiedTableID, shardNum dax.ShardNum) error {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return c.snapshotShardData(tx, qtid, shardNum)
}

func (c *Controller) snapshotShardData(tx dax.Transaction, qtid dax.QualifiedTableID, shardNum dax.ShardNum) error {
	qdbid := qtid.QualifiedDatabaseID

	// Get the node responsible for the shard.
	job := shard(qtid.Key(), shardNum).Job()
	workers, err := c.Balancer.WorkersForJobs(tx, dax.RoleTypeCompute, qdbid, job)
	if err != nil {
		return errors.Wrapf(err, "getting workers for jobs: %s", job)
	}
	if len(workers) == 0 {
		c.logger.Printf("no worker found for shard: %s, %d", qtid, shardNum)
		return nil
	}

	addr := dax.Address(workers[0].Address)

	// Send the node a snapshot request.
	req := &dax.SnapshotShardDataRequest{
		Address:  addr,
		TableKey: qtid.Key(),
		ShardNum: shardNum,
	}

	if err := c.Director.SendSnapshotShardDataRequest(tx.Context(), req); err != nil {
		return NewErrInternal(err.Error())
	}

	return nil
}

// SnapshotTableKeys forces the translate node responsible for the given
// partition to snapshot the table keys for that partition, then increment its
// version for logs written to the Writelogger.
func (c *Controller) SnapshotTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partitionNum dax.PartitionNum) error {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return c.snapshotTableKeys(tx, qtid, partitionNum)
}

func (c *Controller) snapshotTableKeys(tx dax.Transaction, qtid dax.QualifiedTableID, partitionNum dax.PartitionNum) error {
	qdbid := qtid.QualifiedDatabaseID

	// Get the node responsible for the partition.
	job := partition(qtid.Key(), partitionNum).Job()
	workers, err := c.Balancer.WorkersForJobs(tx, dax.RoleTypeTranslate, qdbid, job)
	if err != nil {
		return errors.Wrapf(err, "getting workers for jobs: %s", job)
	}
	if len(workers) == 0 {
		c.logger.Printf("no worker found for partition: %s, %d", qtid, partitionNum)
		return nil
	}

	addr := dax.Address(workers[0].Address)

	// Send the node a snapshot request.
	req := &dax.SnapshotTableKeysRequest{
		Address:      addr,
		TableKey:     qtid.Key(),
		PartitionNum: partitionNum,
	}

	if err := c.Director.SendSnapshotTableKeysRequest(tx.Context(), req); err != nil {
		return NewErrInternal(err.Error())
	}

	return nil
}

// SnapshotFieldKeys forces the translate node responsible for the given field
// to snapshot the keys for that field, then increment its version for logs
// written to the Writelogger.
func (c *Controller) SnapshotFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName) error {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return c.snapshotFieldKeys(tx, qtid, field)
}

func (c *Controller) snapshotFieldKeys(tx dax.Transaction, qtid dax.QualifiedTableID, field dax.FieldName) error {
	qdbid := qtid.QualifiedDatabaseID

	// Get the node responsible for the field.
	// Field translation is currently handled by partition 0.
	partitionNum := dax.PartitionNum(0)
	job := partition(qtid.Key(), partitionNum).Job()

	workers, err := c.Balancer.WorkersForJobs(tx, dax.RoleTypeTranslate, qdbid, job)
	if err != nil {
		return errors.Wrapf(err, "getting workers for jobs: %s", job)
	}
	if len(workers) == 0 {
		c.logger.Printf("no worker found for partition: %s, %d", qtid, partitionNum)
		return nil
	}

	addr := dax.Address(workers[0].Address)

	// Send the node a snapshot request.
	req := &dax.SnapshotFieldKeysRequest{
		Address:  addr,
		TableKey: qtid.Key(),
		Field:    field,
	}

	if err := c.Director.SendSnapshotFieldKeysRequest(tx.Context(), req); err != nil {
		return NewErrInternal(err.Error())
	}

	return nil
}

/////////////

// ComputeNodes returns the compute nodes for the given table/shards. It always
// uses a read transaction. The writable equivalent to this method is
// `IngestShard`.
func (c *Controller) ComputeNodes(ctx context.Context, qtid dax.QualifiedTableID, shards dax.ShardNums) ([]dax.ComputeNode, error) {
	role := &dax.ComputeRole{
		TableKey: qtid.Key(),
		Shards:   shards,
	}
	qdbid := qtid.QualifiedDatabaseID

	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	// If no shards are provided, get the nodes responsible for the entire
	// table.
	if len(role.Shards) == 0 {
		assignedNodes, err := c.nodesForTable(tx, dax.RoleTypeCompute, qtid)
		if err != nil {
			return nil, errors.Wrap(err, "getting nodes for table")
		}
		computeNodes, err := c.assignedToComputeNodes(assignedNodes)
		if err != nil {
			return nil, errors.Wrap(err, "converting assigned to compute nodes")
		}
		return computeNodes, nil
	}

	assignedNodes, _, err := c.nodesComputeReadOrWrite(tx, role, qdbid, false, false)
	if err != nil {
		return nil, errors.Wrap(err, "getting compute nodes read or write")
	}

	return c.assignedToComputeNodes(assignedNodes)
}

// assignedToComputeNodes converts the provided []dax.AssignedNode to
// []dax.ComputeNode. If any of the assigned nodes are not for RoleType
// "compute", an error will be returned.
func (c *Controller) assignedToComputeNodes(nodes []dax.AssignedNode) ([]dax.ComputeNode, error) {
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
			Shards:  role.Shards,
		})
	}

	return computeNodes, nil
}

// TranslateNodes returns the translate nodes for the given table/partitions. It
// always uses a read transaction. The writable equivalent to this method is
// `IngestPartition`.
func (c *Controller) TranslateNodes(ctx context.Context, qtid dax.QualifiedTableID, partitions dax.PartitionNums) ([]dax.TranslateNode, error) {
	role := &dax.TranslateRole{
		TableKey:   qtid.Key(),
		Partitions: partitions,
	}
	qdbid := qtid.QualifiedDatabaseID

	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	// If no partitions are provided, get the nodes responsible for the entire
	// table.
	if len(role.Partitions) == 0 {
		assignedNodes, err := c.nodesForTable(tx, dax.RoleTypeTranslate, qtid)
		if err != nil {
			return nil, errors.Wrap(err, "getting nodes for table")
		}
		translateNodes, err := c.assignedToTranslateNodes(assignedNodes)
		if err != nil {
			return nil, errors.Wrap(err, "converting assigned to translate nodes")
		}
		return translateNodes, nil
	}

	assignedNodes, _, err := c.nodesTranslateReadOrWrite(tx, role, qdbid, false, false)
	if err != nil {
		return nil, errors.Wrap(err, "getting translate nodes read or write")
	}

	return c.assignedToTranslateNodes(assignedNodes)
}

func (c *Controller) nodesForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) ([]dax.AssignedNode, error) {
	workers, err := c.Balancer.WorkersForTable(tx, roleType, qtid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting workers for table: '%s'", qtid)
	}
	switch roleType {
	case dax.RoleTypeCompute:
		return c.computeWorkersToAssignedNodes(tx, workers)
	case dax.RoleTypeTranslate:
		return c.translateWorkersToAssignedNodes(tx, workers)
	default:
		return nil, errors.Errorf("unsupported role type: %s", roleType)
	}
}

// assignedToTranslateNodes converts the provided []dax.AssignedNode to
// []dax.TranslateNode. If any of the assigned nodes are not for RoleType
// "translate", an error will be returned.
func (c *Controller) assignedToTranslateNodes(nodes []dax.AssignedNode) ([]dax.TranslateNode, error) {
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
			Partitions: role.Partitions,
		})
	}

	return translateNodes, nil
}

func (c *Controller) IngestPartition(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum) (dax.Address, error) {
	role := &dax.TranslateRole{
		TableKey:   qtid.Key(),
		Partitions: dax.PartitionNums{partition},
	}
	qdbid := qtid.QualifiedDatabaseID

	// Try with a read transaction first.
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return "", errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	if err := c.sanitizeQTID(tx, &qtid); err != nil {
		return "", errors.Wrap(err, "sanitizing")
	}

	// Verify that the table exists.
	if _, err := c.Schemar.Table(tx, qtid); err != nil {
		return "", err
	}

	nodes, retryAsWrite, err := c.nodesTranslateReadOrWrite(tx, role, qdbid, true, false)
	if err != nil {
		return "", errors.Wrap(err, "getting translate nodes read or write")
	}

	// If it's writable, and we couldn't find all the partitions with just a
	// read, try again with a write transaction.
	if retryAsWrite {
		tx.Rollback()

		tx, err = c.BoltDB.BeginTx(ctx, true)
		if err != nil {
			return "", errors.Wrap(err, "beginning tx")
		}
		defer tx.Rollback()

		nodes, _, err = c.nodesTranslateReadOrWrite(tx, role, qdbid, true, true)
		if err != nil {
			return "", errors.Wrap(err, "getting translate nodes read or write retry")
		}
	}

	translateNodes, err := c.assignedToTranslateNodes(nodes)
	if err != nil {
		return "", errors.Wrap(err, "converting assigned to translate nodes")
	}

	if l := len(translateNodes); l == 0 {
		return "", NewErrNoAvailableNode()
	} else if l > 1 {
		return "", NewErrInternal(
			fmt.Sprintf("unexpected number of nodes: %d", l))
	}

	node := translateNodes[0]

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

	// Only commit if the transaction is writable.
	if retryAsWrite {
		return node.Address, tx.Commit()
	}

	return node.Address, nil
}

// IngestShard handles an ingest shard request.
func (c *Controller) IngestShard(ctx context.Context, qtid dax.QualifiedTableID, shrdNum dax.ShardNum) (dax.Address, error) {
	role := &dax.ComputeRole{
		TableKey: qtid.Key(),
		Shards:   dax.ShardNums{shrdNum},
	}
	qdbid := qtid.QualifiedDatabaseID

	// Try with a read transaction first.
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return "", errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	if err := c.sanitizeQTID(tx, &qtid); err != nil {
		return "", errors.Wrap(err, "sanitizing")
	}

	// Verify that the table exists.
	if _, err := c.Schemar.Table(tx, qtid); err != nil {
		return "", err
	}

	nodes, retryAsWrite, err := c.nodesComputeReadOrWrite(tx, role, qdbid, true, false)
	if err != nil {
		return "", errors.Wrap(err, "getting compute nodes read or write")
	}

	// If it's writable, and we couldn't find all the partitions with just a
	// read, try again with a write transaction.
	if retryAsWrite {
		tx.Rollback()

		tx, err = c.BoltDB.BeginTx(ctx, true)
		if err != nil {
			return "", errors.Wrap(err, "beginning tx")
		}
		defer tx.Rollback()

		nodes, _, err = c.nodesComputeReadOrWrite(tx, role, qdbid, true, true)
		if err != nil {
			return "", errors.Wrap(err, "getting compute nodes read or write retry")
		}
	}

	computeNodes, err := c.assignedToComputeNodes(nodes)
	if err != nil {
		return "", errors.Wrap(err, "converting assigned to compute nodes")
	}

	if l := len(computeNodes); l == 0 {
		return "", NewErrNoAvailableNode()
	} else if l > 1 {
		return "", NewErrInternal(
			fmt.Sprintf("unexpected number of nodes: %d", l))
	}

	node := computeNodes[0]

	// Verify that the node returned is actually responsible for the shard
	// requested.
	if node.Table != qtid.Key() {
		return "", NewErrInternal(
			fmt.Sprintf("table returned (%s) does not match requested (%s)", node.Table, qtid))
	} else if l := len(node.Shards); l != 1 {
		return "", NewErrInternal(
			fmt.Sprintf("unexpected number of shards returned: %d", l))
	} else if s := node.Shards[0]; s != shrdNum {
		return "", NewErrInternal(
			fmt.Sprintf("shard returned (%d) does not match requested (%d)", s, shrdNum))
	}

	// Only commit if the transaction is writable.
	if retryAsWrite {
		return node.Address, tx.Commit()
	}

	return node.Address, nil
}

////

func (c *Controller) CreateField(ctx context.Context, qtid dax.QualifiedTableID, fld *dax.Field) error {
	tx, err := c.BoltDB.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	if err := c.sanitizeQTID(tx, &qtid); err != nil {
		return errors.Wrap(err, "sanitizing")
	}

	// Create the field in schemar.
	if err := c.Schemar.CreateField(tx, qtid, fld); err != nil {
		return errors.Wrapf(err, "creating field: %s, %s", qtid, fld)
	}

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	qdbid := qtid.QualifiedDatabaseID

	// Get the worker(s) responsible for partition 0.
	job := partition(qtid.Key(), 0).Job()
	workers, err := c.Balancer.WorkersForJobs(tx, dax.RoleTypeTranslate, qdbid, job)
	if err != nil {
		return errors.Wrapf(err, "getting workers for job: %s", job)
	}

	for _, w := range workers {
		workerSet.Add(dax.Address(w.Address))
	}

	// Get the list of workers responsible for shard data for this table.
	if state, err := c.Balancer.CurrentState(tx, dax.RoleTypeCompute, qdbid); err != nil {
		return errors.Wrap(err, "getting current compute state")
	} else {
		for _, worker := range state {
			for _, job := range worker.Jobs {
				if shard, err := decodeShard(job); err != nil {
					return errors.Wrapf(err, "decoding shard: %s", job)
				} else if shard.table() == qtid.Key() {
					workerSet.Add(dax.Address(worker.Address))
					break
				}
			}
		}
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	// Send a directive to any compute node responsible for this field.
	if err := c.sendDirectives(tx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return tx.Commit()
}

func (c *Controller) DropField(ctx context.Context, qtid dax.QualifiedTableID, fldName dax.FieldName) error {
	tx, err := c.BoltDB.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	if err := c.sanitizeQTID(tx, &qtid); err != nil {
		return errors.Wrap(err, "sanitizing")
	}

	// Drop the field from schemar.
	if err := c.Schemar.DropField(tx, qtid, fldName); err != nil {
		return errors.Wrapf(err, "dropping field: %s, %s", qtid, fldName)
	}

	// workerSet maintains the set of workers which have a job assignment change
	// and therefore need to be sent an updated Directive.
	workerSet := NewAddressSet()

	qdbid := qtid.QualifiedDatabaseID

	// Get the worker(s) responsible for partition 0.
	job := partition(qtid.Key(), 0).Job()
	workers, err := c.Balancer.WorkersForJobs(tx, dax.RoleTypeTranslate, qdbid, job)
	if err != nil {
		return errors.Wrapf(err, "getting workers for job: %s", job)
	}

	for _, w := range workers {
		workerSet.Add(dax.Address(w.Address))
	}

	// Get the list of workers responsible for shard data for this table.
	if state, err := c.Balancer.CurrentState(tx, dax.RoleTypeCompute, qdbid); err != nil {
		return errors.Wrap(err, "getting current compute state")
	} else {
		for _, worker := range state {
			for _, job := range worker.Jobs {
				if shard, err := decodeShard(job); err != nil {
					return errors.Wrapf(err, "decoding shard: %s", job)
				} else if shard.table() == qtid.Key() {
					workerSet.Add(dax.Address(worker.Address))
					break
				}
			}
		}
	}

	// Convert the slice of addresses into a slice of addressMethod containing
	// the appropriate method.
	addressMethods := applyAddressMethod(workerSet.SortedSlice(), dax.DirectiveMethodDiff)

	// Send a directive to any compute node responsible for this field.
	if err := c.sendDirectives(tx, addressMethods...); err != nil {
		return NewErrDirectiveSendFailure(err.Error())
	}

	return tx.Commit()
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
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return c.Balancer.Nodes(tx)
}

// sanitizeQTID populates Table.ID (by looking up the table, by name, in
// schemar) for a given table having only a Name value, but no ID.
func (c *Controller) sanitizeQTID(tx dax.Transaction, qtid *dax.QualifiedTableID) error {
	if qtid.ID == "" {
		nqtid, err := c.Schemar.TableID(tx, qtid.QualifiedDatabaseID, qtid.Name)
		if err != nil {
			return errors.Wrap(err, "getting table ID")
		}
		qtid.ID = nqtid.ID
	}
	return nil
}

// TableByName gets the full table by name.
func (c *Controller) TableByName(ctx context.Context, qdbid dax.QualifiedDatabaseID, name dax.TableName) (*dax.QualifiedTable, error) {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	qtid, err := c.Schemar.TableID(tx, qdbid, name)
	if err != nil {
		return nil, errors.Wrap(err, "getting table id")
	}

	// Get the table from the schemar.
	return c.Schemar.Table(tx, qtid)
}

// TableID returns the table id by table name.
// TODO(tlt): try to phase this out in favor of TableByName().
func (c *Controller) TableID(ctx context.Context, qdbid dax.QualifiedDatabaseID, name dax.TableName) (dax.QualifiedTableID, error) {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return dax.QualifiedTableID{}, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return c.Schemar.TableID(tx, qdbid, name)
}

// NodeService

func (c *Controller) CreateNode(context.Context, dax.Address, *dax.Node) error {
	return errors.Errorf("Controller.CreateNode() not implemented")
}
func (c *Controller) ReadNode(context.Context, dax.Address) (*dax.Node, error) {
	return nil, errors.Errorf("Controller.ReadNode() not implemented")
}
func (c *Controller) DeleteNode(context.Context, dax.Address) error {
	return errors.Errorf("Controller.DeleteNode() not implemented")
}
func (c *Controller) Nodes(ctx context.Context) ([]*dax.Node, error) {
	tx, err := c.BoltDB.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return c.Balancer.Nodes(tx)
}
