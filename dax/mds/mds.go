// Package mds provides the overall interface to Metadata Services.
package mds

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/computer"
	"github.com/featurebasedb/featurebase/v3/dax/mds/controller"
	balancerboltdb "github.com/featurebasedb/featurebase/v3/dax/mds/controller/balancer/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/mds/poller"
	"github.com/featurebasedb/featurebase/v3/dax/mds/schemar"
	schemarboltdb "github.com/featurebasedb/featurebase/v3/dax/mds/schemar/boltdb"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

type Config struct {
	// TODO(jaffee) director on config is a bit weird? Shouldn't this be set up internally?
	Director controller.Director `toml:"-"`
	// RegistrationBatchTimeout is the time that the controller will
	// wait after a node registers itself to see if any more nodes
	// will register before sending out directives to all nodes which
	// have been registered.
	RegistrationBatchTimeout time.Duration `toml:"registration-batch-timeout"`

	SnappingTurtleTimeout time.Duration

	// Poller
	PollInterval time.Duration `toml:"poll-interval"`

	// Storage
	StorageMethod string `toml:"-"`
	DataDir       string `toml:"-"`

	SnapshotterDir string `toml:"snapshotter-dir"`
	WriteloggerDir string `toml:"writelogger-dir"`

	// Logger
	Logger logger.Logger `toml:"-"`
}

// Ensure type implements interface.
var _ computer.Registrar = (*MDS)(nil)
var _ dax.Schemar = (*MDS)(nil)

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

	buckets := append(schemarboltdb.SchemarBuckets, balancerboltdb.BalancerBuckets...)
	controllerDB, err := boltdb.NewSvcBolt(cfg.DataDir, "controller", buckets...)
	if err != nil {
		logr.Printf(errors.Wrap(err, "creating controller bolt").Error())
		os.Exit(1)
	}

	schemar := schemarboltdb.NewSchemar(controllerDB, logr)

	controllerCfg := controller.Config{
		Director: cfg.Director,
		Schemar:  schemar,

		Balancer: balancerboltdb.NewBalancer(controllerDB, schemar, logr),

		RegistrationBatchTimeout: cfg.RegistrationBatchTimeout,
		SnappingTurtleTimeout:    cfg.SnappingTurtleTimeout,
		SnapshotterDir:           cfg.SnapshotterDir,
		WriteloggerDir:           cfg.WriteloggerDir,

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

	if m.controllerDB != nil {
		m.controllerDB.Close()
	}

	return nil
}

// CreateDatabase handles a create table request.
func (m *MDS) CreateDatabase(ctx context.Context, qdb *dax.QualifiedDatabase) error {
	return m.controller.CreateDatabase(ctx, qdb)
}

func (m *MDS) DropDatabase(ctx context.Context, qdbid dax.QualifiedDatabaseID) error {
	return m.controller.DropDatabase(ctx, qdbid)
}

func (m *MDS) DatabaseByName(ctx context.Context, orgID dax.OrganizationID, dbname dax.DatabaseName) (*dax.QualifiedDatabase, error) {
	return m.controller.DatabaseByName(ctx, orgID, dbname)
}

func (m *MDS) DatabaseByID(ctx context.Context, qdbid dax.QualifiedDatabaseID) (*dax.QualifiedDatabase, error) {
	return m.controller.DatabaseByID(ctx, qdbid)
}

func (m *MDS) Databases(ctx context.Context, orgID dax.OrganizationID, ids ...dax.DatabaseID) ([]*dax.QualifiedDatabase, error) {
	return m.controller.Databases(ctx, orgID, ids...)
}

// CreateTable handles a create table request.
func (m *MDS) CreateTable(ctx context.Context, qtbl *dax.QualifiedTable) error {
	return m.controller.CreateTable(ctx, qtbl)
}

// DropTable handles a drop table request. // TODO(jaffee) how do we
// reason about consistency here? What if controller DropTable
// succeeds, but schemar fails?
func (m *MDS) DropTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	return m.controller.DropTable(ctx, qtid)
}

// CreateField handles a create Field request.
func (m *MDS) CreateField(ctx context.Context, qtid dax.QualifiedTableID, fld *dax.Field) error {
	return m.controller.CreateField(ctx, qtid, fld)
}

// DropField handles a drop Field request.
func (m *MDS) DropField(ctx context.Context, qtid dax.QualifiedTableID, fldName dax.FieldName) error {
	return m.controller.DropField(ctx, qtid, fldName)
}

// TableByID handles a table request.
func (m *MDS) TableByID(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	return m.controller.TableByID(ctx, qtid)
}

// Tables handles a tables request.
func (m *MDS) Tables(ctx context.Context, qdbid dax.QualifiedDatabaseID, ids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	return m.controller.Tables(ctx, qdbid, ids...)
}

// TableByName handles a table id (i.e. by name) request.
func (m *MDS) TableByName(ctx context.Context, qdbid dax.QualifiedDatabaseID, name dax.TableName) (*dax.QualifiedTable, error) {
	return m.controller.TableByName(ctx, qdbid, name)
}

// IngestPartition handles an ingest partition request.
func (m *MDS) IngestPartition(ctx context.Context, qtid dax.QualifiedTableID, partnNum dax.PartitionNum) (dax.Address, error) {
	return m.controller.IngestPartition(ctx, qtid, partnNum)
}

// IngestShard handles an ingest shard request.
func (m *MDS) IngestShard(ctx context.Context, qtid dax.QualifiedTableID, shrdNum dax.ShardNum) (dax.Address, error) {
	return m.controller.IngestShard(ctx, qtid, shrdNum)
}

// SnapshotTable handles a snapshot table request.
func (m *MDS) SnapshotTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	return m.controller.SnapshotTable(ctx, qtid)
}

// SnapshotShardData handles a snapshot shard request.
func (m *MDS) SnapshotShardData(ctx context.Context, qtid dax.QualifiedTableID, shardNum dax.ShardNum) error {
	return m.controller.SnapshotShardData(ctx, qtid, shardNum)
}

// SnapshotTableKeys handles a snapshot table/keys request.
func (m *MDS) SnapshotTableKeys(ctx context.Context, qtid dax.QualifiedTableID, partitionNum dax.PartitionNum) error {
	return m.controller.SnapshotTableKeys(ctx, qtid, partitionNum)
}

// SnapshotFieldKeys handles a snapshot field/keys request.
func (m *MDS) SnapshotFieldKeys(ctx context.Context, qtid dax.QualifiedTableID, fldName dax.FieldName) error {
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
func (m *MDS) ComputeNodes(ctx context.Context, qtid dax.QualifiedTableID, shardNums ...dax.ShardNum) ([]dax.ComputeNode, error) {
	return m.controller.ComputeNodes(ctx, qtid, shardNums)
}

func (m *MDS) DebugNodes(ctx context.Context) ([]*dax.Node, error) {
	return m.controller.DebugNodes(ctx)
}

// TranslateNodes gets the translate nodes responsible for the table/partitions
// specified in the TranslateNodeRequest.
func (m *MDS) TranslateNodes(ctx context.Context, qtid dax.QualifiedTableID, partitionNums ...dax.PartitionNum) ([]dax.TranslateNode, error) {
	return m.controller.TranslateNodes(ctx, qtid, partitionNums)
}
