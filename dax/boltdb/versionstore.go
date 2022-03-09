package boltdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/inmem"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/molecula/featurebase/v3/logger"
)

var (
	bucketTables    = Bucket("versionStoreTables")
	bucketShards    = Bucket("versionStoreShards")
	bucketTableKeys = Bucket("versionStoreTableKeys")
	bucketFieldKeys = Bucket("versionStoreFieldKeys")
)

// VersionStoreBuckets defines the buckets used by this package. It can be
// called during setup to create the buckets ahead of time.
var VersionStoreBuckets []Bucket = []Bucket{
	bucketTables,
	bucketShards,
	bucketTableKeys,
	bucketFieldKeys,
}

// Ensure type implements interface.
var _ dax.VersionStore = (*VersionStore)(nil)

// VersionStore manages all version info for shard, table keys, and field keys.
type VersionStore struct {
	db *DB

	logger logger.Logger
}

// NewVersionStore returns a new instance of VersionStore with default values.
func NewVersionStore(db *DB, logger logger.Logger) *VersionStore {
	return &VersionStore{
		db:     db,
		logger: logger,
	}
}

func (s *VersionStore) AddTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "getting transaction")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketTables)
	if bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketTables)
	}

	if val := bkt.Get(tableKey(qtid)); val != nil {
		return dax.NewErrTableIDExists(qtid)
	}

	// The assumption is that we may store information about the table (other
	// than just the fact that it exists). So for now, the value is an empty
	// JSON object.
	val := []byte("{}")

	if err := bkt.Put(tableKey(qtid), val); err != nil {
		return errors.Wrap(err, "putting table")
	}

	// Add the table to the "table index" of the other buckets.
	//
	// Shards
	if bkt := tx.Bucket(bucketShards); bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketShards)
	} else if err := bkt.Put(tableKey(qtid), val); err != nil {
		return errors.Wrap(err, "putting table into shards")
	}

	// TableKeys.
	if bkt := tx.Bucket(bucketTableKeys); bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketTableKeys)
	} else if err := bkt.Put(tableKey(qtid), val); err != nil {
		return errors.Wrap(err, "putting table into table keys")
	}

	// FieldKeys.
	if bkt := tx.Bucket(bucketFieldKeys); bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketFieldKeys)
	} else if err := bkt.Put(tableKey(qtid), val); err != nil {
		return errors.Wrap(err, "putting table into field keys")
	}

	return tx.Commit()
}

func (s *VersionStore) RemoveTable(ctx context.Context, qtid dax.QualifiedTableID) (dax.Shards, dax.Partitions, error) {
	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	// Get the shards and partitions before deleting by table.
	shards, err := s.getShards(ctx, tx, qtid)
	if err != nil {
		return nil, nil, err
	}

	partitions, err := s.getPartitions(ctx, tx, qtid)
	if err != nil {
		return nil, nil, err
	}

	if err := removeTable(ctx, tx, qtid); err != nil {
		return nil, nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}

	return shards, partitions, nil
}

func removeTable(ctx context.Context, tx *Tx, qtid dax.QualifiedTableID) error {
	// Tables.
	if bkt := tx.Bucket(bucketTables); bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketTables)
	} else if err := bkt.Delete(tableKey(qtid)); err != nil {
		return errors.Wrap(err, "deleting table")
	}

	// Shards.
	if bkt := tx.Bucket(bucketShards); bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketShards)
	} else if err := bkt.Delete(tableKey(qtid)); err != nil {
		return errors.Wrap(err, "deleting table in shards")
	} else if err := deleteByPrefix(tx, bucketShards, []byte(fmt.Sprintf(prefixFmtShards, qtid.OrganizationID, qtid.DatabaseID, qtid.ID))); err != nil {
		return errors.Wrap(err, "deleting shards for table")
	}

	// TableKeys.
	if bkt := tx.Bucket(bucketTableKeys); bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketTableKeys)
	} else if err := bkt.Delete(tableKey(qtid)); err != nil {
		return errors.Wrap(err, "deleting table in table keys")
	} else if err := deleteByPrefix(tx, bucketTableKeys, []byte(fmt.Sprintf(prefixFmtTableKeys, qtid.OrganizationID, qtid.DatabaseID, qtid.ID))); err != nil {
		return errors.Wrap(err, "deleting table keys for table")
	}

	// FieldKeys.
	if bkt := tx.Bucket(bucketFieldKeys); bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketFieldKeys)
	} else if err := bkt.Delete(tableKey(qtid)); err != nil {
		return errors.Wrap(err, "deleting table in field keys")
	} else if err := deleteByPrefix(tx, bucketFieldKeys, []byte(fmt.Sprintf(prefixFmtFieldKeys, qtid.OrganizationID, qtid.DatabaseID, qtid.ID))); err != nil {
		return errors.Wrap(err, "deleting field keys for table")
	}

	return nil
}

func deleteByPrefix(tx *Tx, bucket Bucket, prefix []byte) error {
	bkt := tx.Bucket(bucket)
	cursor := bkt.Cursor()

	// Deleting keys within the for loop seems to cause Next() to skip the next
	// matching key because the Delete() call pops the item and effectively
	// moves the cursor forward. Then calling Next() skips the item that was
	// being pointed to after the delete. So, we're going to make a list of keys
	// to delete, and then delete them outside of the cursor logic.
	var keysToDelete [][]byte
	for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
		keysToDelete = append(keysToDelete, k)
	}

	for _, k := range keysToDelete {
		if err := bkt.Delete(k); err != nil {
			return errors.Wrapf(err, "deleting key: %s", k)
		}
	}

	return nil
}

func (s *VersionStore) AddShards(ctx context.Context, qtid dax.QualifiedTableID, shards ...dax.Shard) error {
	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "getting transaction")
	}
	defer tx.Rollback()

	for _, shard := range shards {
		if err := createShard(ctx, tx, qtid, shard); err != nil {
			return errors.Wrap(err, "creating shard")
		}
	}

	return tx.Commit()
}

func createShard(ctx context.Context, tx *Tx, qtid dax.QualifiedTableID, shard dax.Shard) error {
	// TODO: validate data more formally
	if shard.Version < 0 {
		return errors.New(errors.ErrUncoded, fmt.Sprintf("invalid shard version: %d", shard.Version))
	}

	bkt := tx.Bucket(bucketShards)
	if bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketShards)
	}

	// Ensure the table exists.
	if val := bkt.Get(tableKey(qtid)); val == nil {
		return dax.NewErrTableIDDoesNotExist(qtid)
	}

	vsn := make([]byte, 8)
	binary.LittleEndian.PutUint64(vsn, uint64(shard.Version))

	return bkt.Put(shardKey(qtid, shard.Num), vsn)
}

func (s *VersionStore) Shards(ctx context.Context, qtid dax.QualifiedTableID) (dax.Shards, bool, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, false, errors.Wrap(err, "getting tx")
	}
	defer tx.Rollback()

	shards, err := s.getShards(ctx, tx, qtid)
	if err != nil {
		return nil, false, errors.Wrap(err, "getting shards")
	}

	return shards, true, nil
}

func (s *VersionStore) getShards(ctx context.Context, tx *Tx, qtid dax.QualifiedTableID) (dax.Shards, error) {
	c := tx.Bucket(bucketShards).Cursor()

	// Deserialize rows into Shard objects.
	shards := make(dax.Shards, 0)

	prefix := []byte(fmt.Sprintf(prefixFmtShards, qtid.OrganizationID, qtid.DatabaseID, qtid.ID))
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if v == nil {
			s.logger.Printf("nil value for key: %s", k)
			continue
		}

		var shard dax.Shard

		shardNum, err := keyShardNum(k)
		if err != nil {
			return nil, errors.Wrapf(err, "getting shardNum from key: %v", k)
		}

		shard.Num = shardNum
		shard.Version = int(binary.LittleEndian.Uint64(v))

		shards = append(shards, shard)
	}

	return shards, nil
}

// ShardVersion return the current version for the given table/shardNum.
// If a version is not being tracked, it returns a bool value of false.
func (s *VersionStore) ShardVersion(ctx context.Context, qtid dax.QualifiedTableID, shardNum dax.ShardNum) (int, bool, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return -1, false, err
	}
	defer tx.Rollback()

	return getShardVersion(ctx, tx, qtid, shardNum)
}

func getShardVersion(ctx context.Context, tx *Tx, qtid dax.QualifiedTableID, shardNum dax.ShardNum) (int, bool, error) {
	version := -1

	bkt := tx.Bucket(bucketShards)
	if bkt == nil {
		return version, false, errors.Errorf(ErrFmtBucketNotFound, bucketShards)
	}

	b := bkt.Get(shardKey(qtid, shardNum))
	if b == nil {
		return version, false, nil
	}
	version = int(binary.LittleEndian.Uint64(b))

	return version, true, nil
}

func (s *VersionStore) ShardTables(ctx context.Context, qual dax.TableQualifier) (dax.TableIDs, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return s.getTableIDs(ctx, tx, qual, bucketShards)
}

func (s *VersionStore) getTableIDs(ctx context.Context, tx *Tx, qual dax.TableQualifier, bucket Bucket) (dax.TableIDs, error) {
	c := tx.Bucket(bucket).Cursor()

	// Deserialize rows into Tables objects.
	tableIDs := make(dax.TableIDs, 0)

	prefix := []byte(fmt.Sprintf(prefixFmtTables, qual.OrganizationID, qual.DatabaseID))
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if v == nil {
			s.logger.Printf("nil value for key: %s", k)
			continue
		}

		var tableID dax.TableID

		tableID, err := keyTableID(k)
		if err != nil {
			return nil, errors.Wrapf(err, "getting table name from key: %v", k)
		}

		tableIDs = append(tableIDs, tableID)
	}

	return tableIDs, nil
}

func (s *VersionStore) bucketTables(ctx context.Context, bucket Bucket) ([]dax.QualifiedTableID, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	c := tx.Bucket(bucket).Cursor()

	// Deserialize rows into Tables objects.
	qtids := make([]dax.QualifiedTableID, 0)

	prefix := []byte(prefixTables)
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if v == nil {
			s.logger.Printf("nil value for key: %s", k)
			continue
		}

		qtid, err := keyQualifiedTableID(k)
		if err != nil {
			return nil, errors.Wrapf(err, "getting qualified table id from key: %v", k)
		}

		qtids = append(qtids, qtid)
	}

	return qtids, nil
}

// AddPartitions adds new partitions to be managed by VersionStore. It returns
// the number of partitions added or an error.
func (s *VersionStore) AddPartitions(ctx context.Context, qtid dax.QualifiedTableID, partitions ...dax.Partition) error {
	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "getting transaction")
	}
	defer tx.Rollback()

	for _, partition := range partitions {
		if err := createPartition(ctx, tx, qtid, partition); err != nil {
			return errors.Wrap(err, "creating partition")
		}
	}

	return tx.Commit()
}

func createPartition(ctx context.Context, tx *Tx, qtid dax.QualifiedTableID, partition dax.Partition) error {
	// TODO: validate data more formally
	if partition.Version < 0 {
		return errors.New(errors.ErrUncoded, fmt.Sprintf("invalid partition version: %d", partition.Version))
	}

	bkt := tx.Bucket(bucketTableKeys)
	if bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketTableKeys)
	}

	// Ensure the table exists.
	if val := bkt.Get(tableKey(qtid)); val == nil {
		return dax.NewErrTableIDDoesNotExist(qtid)
	}

	vsn := make([]byte, 8)
	binary.LittleEndian.PutUint64(vsn, uint64(partition.Version))

	return bkt.Put(partitionKey(qtid, partition.Num), vsn)
}

func (s *VersionStore) Partitions(ctx context.Context, qtid dax.QualifiedTableID) (dax.Partitions, bool, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, false, errors.Wrap(err, "getting tx")
	}
	defer tx.Rollback()

	partitions, err := s.getPartitions(ctx, tx, qtid)
	if err != nil {
		return nil, false, errors.Wrap(err, "getting partitions")
	}

	return partitions, true, nil
}

func (s *VersionStore) getPartitions(ctx context.Context, tx *Tx, qtid dax.QualifiedTableID) (dax.Partitions, error) {
	c := tx.Bucket(bucketTableKeys).Cursor()

	// Deserialize rows into Partition objects.
	partitions := make(dax.Partitions, 0)

	prefix := []byte(fmt.Sprintf(prefixFmtTableKeys, qtid.OrganizationID, qtid.DatabaseID, qtid.ID))
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if v == nil {
			s.logger.Printf("nil value for key: %s", k)
			continue
		}

		var partition dax.Partition

		partitionNum, err := keyPartitionNum(k)
		if err != nil {
			return nil, errors.Wrapf(err, "getting partitionNum from key: %v", k)
		}

		partition.Num = partitionNum
		partition.Version = int(binary.LittleEndian.Uint64(v))

		partitions = append(partitions, partition)
	}

	return partitions, nil
}

func (s *VersionStore) PartitionVersion(ctx context.Context, qtid dax.QualifiedTableID, partitionNum dax.PartitionNum) (int, bool, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return -1, false, err
	}
	defer tx.Rollback()

	return getPartitionVersion(ctx, tx, qtid, partitionNum)
}

func getPartitionVersion(ctx context.Context, tx *Tx, qtid dax.QualifiedTableID, partitionNum dax.PartitionNum) (int, bool, error) {
	version := -1

	bkt := tx.Bucket(bucketTableKeys)
	if bkt == nil {
		return version, false, errors.Errorf(ErrFmtBucketNotFound, bucketTableKeys)
	}

	b := bkt.Get(partitionKey(qtid, partitionNum))
	if b == nil {
		return version, false, nil
	}
	version = int(binary.LittleEndian.Uint64(b))

	return version, true, nil
}

func (s *VersionStore) PartitionTables(ctx context.Context, qual dax.TableQualifier) (dax.TableIDs, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return s.getTableIDs(ctx, tx, qual, bucketTableKeys)
}

// AddFields adds new fields to be managed by VersionStore. It returns the
// number of fields added or an error.
func (s *VersionStore) AddFields(ctx context.Context, qtid dax.QualifiedTableID, fields ...dax.FieldVersion) error {
	tx, err := s.db.BeginTx(ctx, true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, field := range fields {
		if err := createFieldVersion(ctx, tx, qtid, field); err != nil {
			return errors.Wrap(err, "creating field version")
		}
	}

	return tx.Commit()
}

func createFieldVersion(ctx context.Context, tx *Tx, qtid dax.QualifiedTableID, field dax.FieldVersion) error {
	// TODO: validate data more formally
	if field.Version < 0 {
		return errors.New(errors.ErrUncoded, fmt.Sprintf("invalid field version: %d", field.Version))
	}

	bkt := tx.Bucket(bucketFieldKeys)
	if bkt == nil {
		return errors.Errorf(ErrFmtBucketNotFound, bucketFieldKeys)
	}

	// Ensure the table exists.
	if val := bkt.Get(tableKey(qtid)); val == nil {
		return dax.NewErrTableIDDoesNotExist(qtid)
	}

	vsn := make([]byte, 8)
	binary.LittleEndian.PutUint64(vsn, uint64(field.Version))

	return bkt.Put(fieldKey(qtid, field.Name), vsn)
}

func (s *VersionStore) Fields(ctx context.Context, qtid dax.QualifiedTableID) (dax.FieldVersions, bool, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, false, errors.Wrap(err, "getting tx")
	}
	defer tx.Rollback()

	fields, err := s.getFields(ctx, tx, qtid)
	if err != nil {
		return nil, false, errors.Wrap(err, "getting fields")
	}

	return fields, true, nil
}

func (s *VersionStore) getFields(ctx context.Context, tx *Tx, qtid dax.QualifiedTableID) (dax.FieldVersions, error) {
	c := tx.Bucket(bucketFieldKeys).Cursor()

	// Deserialize rows into FieldVersion objects.
	fieldVersions := make(dax.FieldVersions, 0)

	prefix := []byte(fmt.Sprintf(prefixFmtFieldKeys, qtid.OrganizationID, qtid.DatabaseID, qtid.ID))
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if v == nil {
			s.logger.Printf("nil value for key: %s", k)
			continue
		}

		var fieldVersion dax.FieldVersion

		fieldName, err := keyFieldName(k)
		if err != nil {
			return nil, errors.Wrapf(err, "getting partitionNum from key: %v", k)
		}

		fieldVersion.Name = fieldName
		fieldVersion.Version = int(binary.LittleEndian.Uint64(v))

		fieldVersions = append(fieldVersions, fieldVersion)
	}

	return fieldVersions, nil
}

func (s *VersionStore) FieldVersion(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName) (int, bool, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return -1, false, err
	}
	defer tx.Rollback()

	return getFieldVersion(ctx, tx, qtid, field)
}

func getFieldVersion(ctx context.Context, tx *Tx, qtid dax.QualifiedTableID, field dax.FieldName) (int, bool, error) {
	version := -1

	bkt := tx.Bucket(bucketFieldKeys)
	if bkt == nil {
		return version, false, errors.Errorf(ErrFmtBucketNotFound, bucketFieldKeys)
	}

	b := bkt.Get(fieldKey(qtid, field))
	if b == nil {
		return version, false, nil
	}
	version = int(binary.LittleEndian.Uint64(b))

	return version, true, nil
}

func (s *VersionStore) FieldTables(ctx context.Context, qual dax.TableQualifier) (dax.TableIDs, error) {
	tx, err := s.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	return s.getTableIDs(ctx, tx, qual, bucketFieldKeys)
}

// Copy returns an in-memory copy of VersionStore.
func (s *VersionStore) Copy(ctx context.Context) (dax.VersionStore, error) {
	new := inmem.NewVersionStore()

	// shards.
	qtids, err := s.bucketTables(ctx, bucketShards)
	if err != nil {
		return nil, errors.Wrap(err, "getting shard tables")
	}
	for _, qtid := range qtids {
		shards, found, err := s.Shards(ctx, qtid)
		if err != nil {
			return nil, errors.Wrap(err, "getting shards")
		} else if !found {
			continue
		}
		_ = new.AddTable(ctx, qtid)
		new.AddShards(ctx, qtid, shards...)
	}

	// tableKeys.
	qtids, err = s.bucketTables(ctx, bucketTableKeys)
	if err != nil {
		return nil, errors.Wrap(err, "getting table key tables")
	}
	for _, qtid := range qtids {
		partitions, found, err := s.Partitions(ctx, qtid)
		if err != nil {
			return nil, errors.Wrap(err, "getting partitions")
		} else if !found {
			continue
		}
		_ = new.AddTable(ctx, qtid)
		new.AddPartitions(ctx, qtid, partitions...)
	}

	// fieldKeys.
	qtids, err = s.bucketTables(ctx, bucketFieldKeys)
	if err != nil {
		return nil, errors.Wrap(err, "getting field key tables")
	}
	for _, qtid := range qtids {
		fields, found, err := s.Fields(ctx, qtid)
		if err != nil {
			return nil, errors.Wrap(err, "getting fields")
		} else if !found {
			continue
		}
		_ = new.AddTable(ctx, qtid)
		new.AddFields(ctx, qtid, fields...)
	}

	return new, nil
}

/////////////////////////////////////////////////////////

const (
	prefixShards    = "shards/"
	prefixFmtShards = prefixShards + "%s/%s/%s/"

	prefixTableKeys    = "tablekeys/"
	prefixFmtTableKeys = prefixTableKeys + "%s/%s/%s/"

	prefixFieldKeys    = "fieldkeys/"
	prefixFmtFieldKeys = prefixFieldKeys + "%s/%s/%s/"

	prefixTables    = "tables/"
	prefixFmtTables = prefixTables + "%s/%s/"
)

// tableKey returns a key based on table name.
func tableKey(qtid dax.QualifiedTableID) []byte {
	qual := qtid.TableQualifier
	key := fmt.Sprintf(prefixFmtTables+"%s", qual.OrganizationID, qual.DatabaseID, qtid.ID)
	return []byte(key)
}

// keyTableID gets the table ID out of the key.
func keyTableID(key []byte) (dax.TableID, error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 4 {
		return "", errors.New(errors.ErrUncoded, "table key format expected: `tables/orgID/dbID/tableID`")
	}

	return dax.TableID(parts[3]), nil
}

// keyQualifiedTableID gets the qualified table ID out of the key.
func keyQualifiedTableID(key []byte) (dax.QualifiedTableID, error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 4 {
		return dax.QualifiedTableID{}, errors.New(errors.ErrUncoded, "table key format expected: `tables/orgID/dbID/tableID`")
	}

	return dax.NewQualifiedTableID(
		dax.NewTableQualifier(dax.OrganizationID(parts[1]), dax.DatabaseID(parts[2])),
		dax.TableID(parts[3]),
	), nil
}

// shardKey returns a key based on table and shard.
func shardKey(qtid dax.QualifiedTableID, shard dax.ShardNum) []byte {
	key := fmt.Sprintf(prefixFmtShards+"%d", qtid.OrganizationID, qtid.DatabaseID, qtid.ID, shard)
	return []byte(key)
}

// keyShardNum gets the shardNum out of the key.
func keyShardNum(key []byte) (dax.ShardNum, error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 5 {
		return 0, errors.New(errors.ErrUncoded, "shard key format expected: `shards/orgID/dbID/table/shard`")
	}

	intVar, err := strconv.Atoi(parts[4])
	if err != nil {
		return 0, errors.Wrapf(err, "converting string to shardNum: %s", parts[4])
	}

	return dax.ShardNum(intVar), nil
}

// partitionKey returns a key based on table and partition.
func partitionKey(qtid dax.QualifiedTableID, partition dax.PartitionNum) []byte {
	key := fmt.Sprintf(prefixFmtTableKeys+"%d", qtid.OrganizationID, qtid.DatabaseID, qtid.ID, partition)
	return []byte(key)
}

// keyPartitionNum gets the partitionNum out of the key.
func keyPartitionNum(key []byte) (dax.PartitionNum, error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 5 {
		return 0, errors.New(errors.ErrUncoded, "partition key format expected: `tablekeys/orgID/dbID/table/partition`")
	}

	intVar, err := strconv.Atoi(parts[4])
	if err != nil {
		return 0, errors.Wrapf(err, "converting string to partitionNum: %s", parts[4])
	}

	return dax.PartitionNum(intVar), nil
}

// fieldKey returns a key based on table and field.
func fieldKey(qtid dax.QualifiedTableID, field dax.FieldName) []byte {
	key := fmt.Sprintf(prefixFmtFieldKeys+"%s", qtid.OrganizationID, qtid.DatabaseID, qtid.ID, field)
	return []byte(key)
}

// keyFieldName gets the fieldName out of the key.
func keyFieldName(key []byte) (dax.FieldName, error) {
	parts := strings.Split(string(key), "/")
	if len(parts) != 5 {
		return "", errors.New(errors.ErrUncoded, "field key format expected: `fieldkeys/orgID/dbID/table/field`")
	}

	return dax.FieldName(parts[4]), nil
}
