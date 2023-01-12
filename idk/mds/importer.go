package mds

import (
	"context"
	"sync"
	"time"

	featurebase "github.com/molecula/featurebase/v3"
	featurebaseclient "github.com/molecula/featurebase/v3/client"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/mds/controller/partitioner"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/pkg/errors"
)

// Ensure type implements interface.
var _ featurebase.Importer = &importer{}

// importer
type importer struct {
	noder   dax.Noder
	schemar dax.Schemar

	mu   sync.Mutex
	qual dax.TableQualifier
	tbl  *dax.Table
}

func NewImporter(noder dax.Noder, schemar dax.Schemar, qual dax.TableQualifier, tbl *dax.Table) *importer {
	return &importer{
		noder:   noder,
		schemar: schemar,
		qual:    qual,
		tbl:     tbl,
	}
}

// fbClient currently returns a new FeatureBase client (for the address) for
// every call to this method. We could cache these connections in a map (keyed
// on address) to avoid creating a new client for an address that we already
// have a client for.
func (m *importer) fbClient(address dax.Address) (*featurebaseclient.Client, error) {
	// Set up a FeatureBase client with address.
	return featurebaseclient.NewClient(address.HostPort(),
		featurebaseclient.OptClientRetries(2),
		featurebaseclient.OptClientTotalPoolSize(1000),
		featurebaseclient.OptClientPoolSizePerRoute(400),
		featurebaseclient.OptClientPathPrefix(address.Path()),
		//featurebaseclient.OptClientStatsClient(m.stats),
	)
}

func (m *importer) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*featurebase.Transaction, error) {
	return &featurebase.Transaction{
		ID: "not-used",
	}, nil
}

func (m *importer) FinishTransaction(ctx context.Context, id string) (*featurebase.Transaction, error) {
	return nil, nil
}

func (m *importer) CreateTableKeys(ctx context.Context, tid dax.TableID, keys ...string) (map[string]uint64, error) {
	qtbl, err := m.getQtbl(ctx, tid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting qtbl")
	}

	out := make(map[string]uint64)

	partitioner := partitioner.NewPartitioner()

	// Get the partition for each key (map[int][]string).
	partitions := partitioner.PartitionsForKeys(qtbl.Key(), qtbl.PartitionN, keys...)

	// TODO: we can be more efficient here by calling IngestPartitions() with
	// all the partitions at once, then getting the distinct list of addresses
	// and looping over that instead.
	for partition, ks := range partitions {
		address, err := m.noder.IngestPartition(context.Background(), qtbl.QualifiedID(), partition)
		if err != nil {
			return nil, errors.Wrapf(err, "calling ingest-partition on table: %s, partition: %d", qtbl, partition)
		}

		fbClient, err := m.fbClient(address)
		if err != nil {
			return nil, errors.Wrap(err, "getting featurebase client")
		}

		cidx := featurebaseclient.QTableToClientIndex(qtbl)
		stringToIDMap, err := fbClient.CreateIndexKeys(cidx, ks...)
		if err != nil {
			return nil, errors.Wrapf(err, "creating index keys for partition: %d", partition)
		}

		for str, id := range stringToIDMap {
			out[str] = id
		}
	}

	return out, nil
}

func (m *importer) CreateFieldKeys(ctx context.Context, tid dax.TableID, fname dax.FieldName, keys ...string) (map[string]uint64, error) {
	qtbl, err := m.getQtbl(ctx, tid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting qtbl")
	}

	// For now, we are going to direct all field key translation to the same
	// node handling index key translation for the table, partition 0.
	// TODO: we should be able to partition field key translation on fieldName.
	// If we do that, we might also consider whether we want to support a
	// different partitionN for field translation.
	partition := dax.PartitionNum(0)

	address, err := m.noder.IngestPartition(context.Background(), qtbl.QualifiedID(), partition)
	if err != nil {
		return nil, errors.Wrapf(err, "calling ingest-partition on table: %s, partition: %d", qtbl, partition)
	}

	// Set up a FeatureBase client with address.
	fbClient, err := m.fbClient(address)
	if err != nil {
		return nil, errors.Wrap(err, "getting featurebase client")
	}

	cfld, err := featurebaseclient.TableFieldToClientField(qtbl, fname)
	if err != nil {
		return nil, errors.Wrap(err, "converting fieldinfo to client field")
	}

	return fbClient.CreateFieldKeys(cfld, keys...)
}

func (m *importer) ImportRoaringBitmap(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, views map[string]*roaring.Bitmap, clear bool) error {
	qtbl, err := m.getQtbl(ctx, tid)
	if err != nil {
		return errors.Wrapf(err, "getting qtbl")
	}

	address, err := m.noder.IngestShard(context.Background(), qtbl.QualifiedID(), dax.ShardNum(shard))
	if err != nil {
		return errors.Wrap(err, "calling ingest-shard")
	}

	// Set up a FeatureBase client with address.
	fbClient, err := m.fbClient(address)
	if err != nil {
		return errors.Wrap(err, "getting featurebase client")
	}

	cfld, err := featurebaseclient.TableFieldToClientField(qtbl, fld.Name)
	if err != nil {
		return errors.Wrap(err, "converting fieldinfo to client field")
	}

	return fbClient.ImportRoaringBitmap(cfld, shard, views, clear)
}

func (m *importer) ImportRoaringShard(ctx context.Context, tid dax.TableID, shard uint64, request *featurebase.ImportRoaringShardRequest) error {
	qtbl, err := m.getQtbl(ctx, tid)
	if err != nil {
		return errors.Wrapf(err, "getting qtbl")
	}

	address, err := m.noder.IngestShard(context.Background(), qtbl.QualifiedID(), dax.ShardNum(shard))
	if err != nil {
		return errors.Wrap(err, "calling ingest-shard")
	}

	// Set up a FeatureBase client with address.
	fbClient, err := m.fbClient(address)
	if err != nil {
		return errors.Wrap(err, "getting featurebase client")
	}

	return fbClient.ImportRoaringShard(string(qtbl.Key()), shard, request)
}

func (m *importer) EncodeImportValues(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error) {
	qtbl, err := m.getQtbl(ctx, tid)
	if err != nil {
		return "", nil, errors.Wrapf(err, "getting qtbl")
	}

	address, err := m.noder.IngestShard(context.Background(), qtbl.QualifiedID(), dax.ShardNum(shard))
	if err != nil {
		return "", nil, errors.Wrap(err, "calling ingest-shard")
	}

	// Set up a FeatureBase client with address.
	fbClient, err := m.fbClient(address)
	if err != nil {
		return "", nil, errors.Wrap(err, "getting featurebase client")
	}

	cfld, err := featurebaseclient.TableFieldToClientField(qtbl, fld.Name)
	if err != nil {
		return "", nil, errors.Wrap(err, "converting fieldinfo to client field")
	}

	return fbClient.EncodeImportValues(cfld, shard, vals, ids, clear)
}

func (m *importer) EncodeImport(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error) {
	qtbl, err := m.getQtbl(ctx, tid)
	if err != nil {
		return "", nil, errors.Wrapf(err, "getting qtbl")
	}

	address, err := m.noder.IngestShard(context.Background(), qtbl.QualifiedID(), dax.ShardNum(shard))
	if err != nil {
		return "", nil, errors.Wrap(err, "calling ingest-shard")
	}

	// Set up a FeatureBase client with address.
	fbClient, err := m.fbClient(address)
	if err != nil {
		return "", nil, errors.Wrap(err, "getting featurebase client")
	}

	cfld, err := featurebaseclient.TableFieldToClientField(qtbl, fld.Name)
	if err != nil {
		return "", nil, errors.Wrap(err, "converting fieldinfo to client field")
	}

	return fbClient.EncodeImport(cfld, shard, vals, ids, clear)
}

func (m *importer) DoImport(ctx context.Context, tid dax.TableID, fld *dax.Field, shard uint64, path string, data []byte) error {
	qtbl, err := m.getQtbl(ctx, tid)
	if err != nil {
		return errors.Wrapf(err, "getting qtbl")
	}

	address, err := m.noder.IngestShard(context.Background(), qtbl.QualifiedID(), dax.ShardNum(shard))
	if err != nil {
		return errors.Wrap(err, "calling ingest-shard")
	}

	// Set up a FeatureBase client with address.
	fbClient, err := m.fbClient(address)
	if err != nil {
		return errors.Wrap(err, "getting featurebase client")
	}

	return fbClient.DoImport(string(qtbl.Key()), shard, path, data)
}

// getQtbl takes a table (TableKey) and sets the local m.qtbl value. When we
// originally set up this type, it was only used by IDK, and the table was known
// at the beginning of the process, so it could be set on this import. But
// later, we used this importer in the Queryer, and it gets set up prior to
// parsing the table out of sql; which means we don't know what the table is
// yet. So this method allows us to use the table which is passed into each
// method to determine the table. We look it up from mds schema once and save it
// in m.qtbl for any further method calls.
func (m *importer) getQtbl(ctx context.Context, tid dax.TableID) (*dax.QualifiedTable, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.tbl != nil {
		return dax.NewQualifiedTable(m.qual, m.tbl), nil
	}

	qtid := dax.NewQualifiedTableID(m.qual, tid)

	qtbl, err := m.schemar.TableByID(ctx, qtid)
	if err != nil {
		return nil, errors.Wrap(err, "getting table")
	}

	m.tbl = &qtbl.Table

	return qtbl, nil
}
