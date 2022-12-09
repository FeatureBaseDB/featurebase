// Package mds provides the overall interface to Metadata Services.
package mds

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/computer"
	"github.com/featurebasedb/featurebase/v3/dax/mds/controller"
	naiveboltdb "github.com/featurebasedb/featurebase/v3/dax/mds/controller/naive/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/mds/poller"
	"github.com/featurebasedb/featurebase/v3/dax/mds/schemar"
	schemarboltdb "github.com/featurebasedb/featurebase/v3/dax/mds/schemar/boltdb"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

type Config struct {
	// Controller
	Director controller.Director `toml:"-"`
	// RegistrationBatchTimeout is the time that the controller will
	// wait after a node registers itself to see if any more nodes
	// will register before sending out directives to all nodes which
	// have been registered.
	RegistrationBatchTimeout time.Duration `toml:"registration-batch-timeout"`

	// Poller
	PollInterval time.Duration `toml:"poll-interval"`

	// Storage
	StorageMethod string `toml:"-"`
	DataDir       string `toml:"-"`

	// Logger
	Logger logger.Logger `toml:"-"`
}

// Ensure type implements interface.
var _ computer.Registrar = (*MDS)(nil)

// MDS provides public MDS methods for an MDS service.
type MDS struct {
	mu sync.RWMutex

	controller *controller.Controller
	poller     *poller.Poller
	schemar    schemar.Schemar

	// Because we stopped using a storage method interface, and always use bolt,
	// we need to be sure to close the boltDBs that are created in mds.New()
	// whenever mds.Close() is called. These are pointers to those DBs so we can
	// close them.
	schemarDB    *boltdb.DB
	controllerDB *boltdb.DB

	logger logger.Logger
}

// New returns a new instance of MDS.
func New(cfg Config) *MDS {
	// Set up logger.
	var logr logger.Logger = logger.StderrLogger
	if cfg.Logger != nil {
		logr = cfg.Logger
	}

	// Storage methods.
	if cfg.StorageMethod != "boltdb" && cfg.StorageMethod != "" {
		logr.Printf("storagemethod %s not supported, try 'boltdb'", cfg.StorageMethod)
	}

	cfg.StorageMethod = "boltdb"

	if cfg.DataDir == "" {
		dir, err := os.MkdirTemp("", "mds_*")
		if err != nil {
			logr.Printf("Making temp dir for MDS storage: %v", err)
			os.Exit(1)
		}
		cfg.DataDir = dir
		logr.Warnf("no DataDir given (like '/path/to/directory') using temp dir at '%s'", cfg.DataDir)
	}

	schemarDB, err := boltdb.NewSvcBolt(cfg.DataDir, "schemar", schemarboltdb.SchemarBuckets...)
	if err != nil {
		logr.Printf("Error creating schemar db: %v", err)
		os.Exit(1)
	}

	schemar := schemarboltdb.NewSchemar(schemarDB, logr)

	controllerDB, err := boltdb.NewSvcBolt(cfg.DataDir, "balancer", naiveboltdb.NaiveBalancerBuckets...)
	if err != nil {
		logr.Printf(errors.Wrap(err, "creating balancer bolt").Error())
		os.Exit(1)
	}

	controllerCfg := controller.Config{
		Director:          cfg.Director,
		Schemar:           schemar,
		ComputeBalancer:   naiveboltdb.NewBalancer("compute", controllerDB, logr),
		TranslateBalancer: naiveboltdb.NewBalancer("translate", controllerDB, logr),

		RegistrationBatchTimeout: cfg.RegistrationBatchTimeout,

		StorageMethod: cfg.StorageMethod,
		// just reusing this bolt for internal controller svcs
		// rn... ultimately controller shouldn't know what bolt is at
		// all
		BoltDB: controllerDB,

		Logger: logr,
	}
	controller := controller.New(controllerCfg)

	pollerCfg := poller.Config{
		AddressManager: controller,
		NodePoller:     poller.NewHTTPNodePoller(logr),
		PollInterval:   cfg.PollInterval,
		Logger:         logr,
	}
	poller := poller.New(pollerCfg)

	// The controller needs to tell the poller about nodes which have been
	// added/removed.
	// TODO: this feels hacky. We need an elegant way to register interface
	// implementations across services without an explicit Set method like this.
	controller.SetPoller(poller)

	return &MDS{
		controller: controller,
		poller:     poller,
		schemar:    schemar,

		schemarDB:    schemarDB,
		controllerDB: controllerDB,

		logger: logr,
	}
}

////////////////////////////////////////////////////
// mds specific endpoints
////////////////////////////////////////////////////

// Start starts MDS services, such as the Poller.
func (m *MDS) Start() error {
	// Initialize the poller (in the case where this MDS instance has restarted
	// or is a replacement). Then start the poller.
	if err := m.controller.InitializePoller(context.Background()); err != nil {
		return errors.Wrap(err, "initializing the poller")
	}
	m.poller.Run()

	return m.controller.Run()
}

// Stop stops MDS services, such as the Poller and the controller's node
// registration routine.
func (m *MDS) Stop() error {
	m.poller.Stop()
	m.controller.Stop()

	if m.schemarDB != nil {
		m.schemarDB.Close()
	}
	if m.controllerDB != nil {
		m.controllerDB.Close()
	}

	return nil
}

// sanitizeQTID populates Table.ID (by looking up the table, by name, in
// schemar) for a given table having only a Name value, but no ID.
func (m *MDS) sanitizeQTID(ctx context.Context, qtid *dax.QualifiedTableID) error {
	if qtid.ID == "" {
		nqtid, err := m.schemar.TableID(ctx, qtid.TableQualifier, qtid.Name)
		if err != nil {
			return errors.Wrap(err, "getting table ID")
		}
		qtid.ID = nqtid.ID
	}
	return nil
}

// CreateTable handles a create table request.
func (m *MDS) CreateTable(ctx context.Context, qtbl *dax.QualifiedTable) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create Table ID.
	if _, err := qtbl.CreateID(); err != nil {
		return errors.Wrap(err, "creating table ID")
	}

	// Create the table in schemar.
	if err := m.schemar.CreateTable(ctx, qtbl); err != nil {
		return errors.Wrapf(err, "creating table: %s", qtbl)
	}

	// TODO: if error here, we should probably roll-back the
	// schemar.CreateTable() request.

	// Add the table to the controller.
	return m.controller.CreateTable(ctx, qtbl)
}

// DropTable handles a drop table request. // TODO(jaffee) how do we
// reason about consistency here? What if controller DropTable
// succeeds, but schemar fails?
func (m *MDS) DropTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return errors.Wrap(err, "sanitizing")
	}

	if err := m.controller.DropTable(ctx, qtid); err != nil {
		return errors.Wrapf(err, "dropping table: %s", qtid)
	}

	return m.schemar.DropTable(ctx, qtid)
}

type CreateFieldRequest struct {
	Table dax.TableName
	Field *dax.Field
}

// CreateField handles a create Field request.
func (m *MDS) CreateField(ctx context.Context, qtid dax.QualifiedTableID, fld *dax.Field) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return errors.Wrap(err, "sanitizing")
	}

	// Create the field in schemar.
	if err := m.schemar.CreateField(ctx, qtid, fld); err != nil {
		return errors.Wrapf(err, "creating field: %s, %s", qtid, fld)
	}

	// Add the table to the controller.
	return m.controller.CreateField(ctx, qtid, fld)
}

// DropField handles a drop Field request.
func (m *MDS) DropField(ctx context.Context, qtid dax.QualifiedTableID, fldName dax.FieldName) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return errors.Wrap(err, "sanitizing")
	}

	// Drop the field from schemar.
	if err := m.schemar.DropField(ctx, qtid, fldName); err != nil {
		return errors.Wrapf(err, "dropping field: %s, %s", qtid, fldName)
	}

	// Drop the field from the controller.
	return m.controller.DropField(ctx, qtid, fldName)
}

type DropFieldResponse struct{}

// Table handles a table request.
func (m *MDS) Table(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return nil, errors.Wrap(err, "sanitizing")
	}

	return m.schemar.Table(ctx, qtid)
}

// Tables handles a tables request.
func (m *MDS) Tables(ctx context.Context, qual dax.TableQualifier, ids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.schemar.Tables(ctx, qual, ids...)
}

// TableID handles a table id (i.e. by name) request.
func (m *MDS) TableID(ctx context.Context, qual dax.TableQualifier, name dax.TableName) (dax.QualifiedTableID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.schemar.TableID(ctx, qual, name)
}

// IngestPartition handles an ingest partition request.
func (m *MDS) IngestPartition(ctx context.Context, qtid dax.QualifiedTableID, partnNum dax.PartitionNum) (dax.Address, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return "", errors.Wrap(err, "sanitizing")
	}

	// Verify that the table exists.
	if _, err := m.schemar.Table(ctx, qtid); err != nil {
		return "", err
	}

	partitions := dax.PartitionNums{partnNum}

	nodes, err := m.controller.TranslateNodes(ctx, qtid, partitions, true)
	if err != nil {
		return "", err
	}

	if l := len(nodes); l == 0 {
		return "", controller.NewErrNoAvailableNode()
	} else if l > 1 {
		return "", controller.NewErrInternal(
			fmt.Sprintf("unexpected number of nodes: %d", l))
	}

	node := nodes[0]

	// Verify that the node returned is actually responsible for the partition
	// requested.
	if node.Table != qtid.Key() {
		return "", controller.NewErrInternal(
			fmt.Sprintf("table returned (%s) does not match requested (%s)", node.Table, qtid))
	} else if l := len(node.Partitions); l != 1 {
		return "", controller.NewErrInternal(
			fmt.Sprintf("unexpected number of partitions returned: %d", l))
	} else if p := node.Partitions[0]; p != partnNum {
		return "", controller.NewErrInternal(
			fmt.Sprintf("partition returned (%d) does not match requested (%d)", p, partnNum))
	}

	return node.Address, nil
}

// IngestShard handles an ingest shard request.
func (m *MDS) IngestShard(ctx context.Context, qtid dax.QualifiedTableID, shrdNum dax.ShardNum) (dax.Address, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return "", errors.Wrap(err, "sanitizing")
	}

	// Verify that the table exists.
	if _, err := m.schemar.Table(ctx, qtid); err != nil {
		return "", err
	}

	shards := dax.ShardNums{shrdNum}

	nodes, err := m.controller.ComputeNodes(ctx, qtid, shards, true)
	if err != nil {
		return "", err
	}

	if l := len(nodes); l == 0 {
		return "", controller.NewErrNoAvailableNode()
	} else if l > 1 {
		return "", controller.NewErrInternal(
			fmt.Sprintf("unexpected number of nodes: %d", l))
	}

	node := nodes[0]

	// Verify that the node returned is actually responsible for the shard
	// requested.
	if node.Table != qtid.Key() {
		return "", controller.NewErrInternal(
			fmt.Sprintf("table returned (%s) does not match requested (%s)", node.Table, qtid))
	} else if l := len(node.Shards); l != 1 {
		return "", controller.NewErrInternal(
			fmt.Sprintf("unexpected number of shards returned: %d", l))
	} else if s := node.Shards[0]; s != shrdNum {
		return "", controller.NewErrInternal(
			fmt.Sprintf("shard returned (%d) does not match requested (%d)", s, shrdNum))
	}

	return node.Address, nil
}

// SnapshotTable handles a snapshot table request.
func (m *MDS) SnapshotTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return errors.Wrap(err, "sanitizing")
	}

	return m.controller.SnapshotTable(ctx, qtid)
}

// SnapshotShardData handles a snapshot shard request.
func (m *MDS) SnapshotShardData(ctx context.Context, qtid dax.QualifiedTableID, shardNum dax.ShardNum) error {
	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return errors.Wrap(err, "sanitizing")
	}

	return m.controller.SnapshotShardData(ctx, qtid, shardNum)
}

// SnapshotTableKeys handles a snapshot table/keys request.
func (m *MDS) SnapshotTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partitionNum dax.PartitionNum) error {
	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return errors.Wrap(err, "sanitizing")
	}

	return m.controller.SnapshotTableKeys(ctx, qtid, partitionNum)
}

// SnapshotFieldKeys handles a snapshot field/keys request.
func (m *MDS) SnapshotFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, fldName dax.FieldName) error {
	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return errors.Wrap(err, "sanitizing")
	}

	return m.controller.SnapshotFieldKeys(ctx, qtid, fldName)
}

////////////////////////////////////////////////////
// controller specific endpoints
// These are just pass-throughs for now.
////////////////////////////////////////////////////

// RegisterNode handles a node registration request. It does not
// synchronously do much of anything, but the node will eventually
// probably get a directive... unless the MDS crashes or something in
// which case the fact that this endpoint was ever called will be lost
// to time.
func (m *MDS) RegisterNode(ctx context.Context, node *dax.Node) error {
	return m.controller.RegisterNode(ctx, node)
}

// CheckInNode handles a node check-in request. If MDS is not aware of the node,
// it will be sent through the RegisterNode process.
func (m *MDS) CheckInNode(ctx context.Context, node *dax.Node) error {
	return m.controller.CheckInNode(ctx, node)
}

// RegisterNodes immediately registers the given nodes and sends out
// new directives synchronously, bypassing the wait time of the
// RegisterNode endpoint.
func (m *MDS) RegisterNodes(ctx context.Context, nodes ...*dax.Node) error {
	return m.controller.RegisterNodes(ctx, nodes...)
}

// DeregisterNodes handles a request to deregister multiple nodes at once.
func (m *MDS) DeregisterNodes(ctx context.Context, addrs ...dax.Address) error {
	return m.controller.DeregisterNodes(ctx, addrs...)
}

// ComputeNodes gets the compute nodes responsible for the table/shards
// specified in the ComputeNodeRequest.
func (m *MDS) ComputeNodes(ctx context.Context, qtid dax.QualifiedTableID, shardNums ...dax.ShardNum) ([]controller.ComputeNode, error) {
	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return nil, errors.Wrap(err, "sanitizing")
	}

	return m.controller.ComputeNodes(ctx, qtid, shardNums, false)
}

func (m *MDS) DebugNodes(ctx context.Context) ([]*dax.Node, error) {
	return m.controller.DebugNodes(ctx)
}

// TranslateNodes gets the translate nodes responsible for the table/partitions
// specified in the TranslateNodeRequest.
func (m *MDS) TranslateNodes(ctx context.Context, qtid dax.QualifiedTableID, partitionNums ...dax.PartitionNum) ([]controller.TranslateNode, error) {
	if err := m.sanitizeQTID(ctx, &qtid); err != nil {
		return nil, errors.Wrap(err, "sanitizing")
	}

	return m.controller.TranslateNodes(ctx, qtid, partitionNums, false)
}
