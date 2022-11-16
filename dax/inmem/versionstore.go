package inmem

import (
	"context"
	"sort"
	"sync"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/errors"
)

// Ensure type implements interface.
var _ dax.VersionStore = (*VersionStore)(nil)

// VersionStore manages all version info for shard, table keys, and field keys.
type VersionStore struct {
	mu sync.RWMutex

	// shards is a map of all shards, by table, by shard number, known to
	// contain data.
	shards map[dax.TableQualifierKey]map[dax.TableID]map[dax.ShardNum]dax.VersionedShard

	// tableKeys is a map of all partitions, by table, by partition number,
	// known to contain key data.
	tableKeys map[dax.TableQualifierKey]map[dax.TableID]map[dax.PartitionNum]int

	// fieldKeys is a map of all fields, by table, known to contain key data.
	fieldKeys map[dax.TableQualifierKey]map[dax.TableID]map[dax.FieldName]int
}

// NewVersionStore returns a new instance of VersionStore with default values.
func NewVersionStore() *VersionStore {
	return &VersionStore{
		shards:    make(map[dax.TableQualifierKey]map[dax.TableID]map[dax.ShardNum]dax.VersionedShard),
		tableKeys: make(map[dax.TableQualifierKey]map[dax.TableID]map[dax.PartitionNum]int),
		fieldKeys: make(map[dax.TableQualifierKey]map[dax.TableID]map[dax.FieldName]int),
	}
}

// AddTable adds a table to be managed by VersionStore.
func (s *VersionStore) AddTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// This check is clunky; three maps contain the table, but we only check for
	// existence in one of them. It also seems weird to check all three, because
	// if we get in a state where one of the maps doesn't contain a table that
	// the other maps do contain, the state of the data is in question.
	if _, found := s.shards[qtid.TableQualifier.Key()][qtid.ID]; found {
		return dax.NewErrTableIDExists(qtid)
	}

	// Initialize the maps in case VersionStore wasn't created with NewVersionStore().
	if s.shards == nil {
		s.shards = make(map[dax.TableQualifierKey]map[dax.TableID]map[dax.ShardNum]dax.VersionedShard)
	}
	if s.tableKeys == nil {
		s.tableKeys = make(map[dax.TableQualifierKey]map[dax.TableID]map[dax.PartitionNum]int)
	}
	if s.fieldKeys == nil {
		s.fieldKeys = make(map[dax.TableQualifierKey]map[dax.TableID]map[dax.FieldName]int)
	}

	// shards.
	if _, ok := s.shards[qtid.TableQualifier.Key()]; !ok {
		s.shards[qtid.TableQualifier.Key()] = make(map[dax.TableID]map[dax.ShardNum]dax.VersionedShard, 0)
	}
	if _, ok := s.shards[qtid.TableQualifier.Key()][qtid.ID]; !ok {
		s.shards[qtid.TableQualifier.Key()][qtid.ID] = make(map[dax.ShardNum]dax.VersionedShard, 0)
	}

	// tableKeys.
	if _, ok := s.tableKeys[qtid.TableQualifier.Key()]; !ok {
		s.tableKeys[qtid.TableQualifier.Key()] = make(map[dax.TableID]map[dax.PartitionNum]int, 0)
	}
	if _, ok := s.tableKeys[qtid.TableQualifier.Key()][qtid.ID]; !ok {
		s.tableKeys[qtid.TableQualifier.Key()][qtid.ID] = make(map[dax.PartitionNum]int, 0)
	}

	// fieldKeys.
	if _, ok := s.fieldKeys[qtid.TableQualifier.Key()]; !ok {
		s.fieldKeys[qtid.TableQualifier.Key()] = make(map[dax.TableID]map[dax.FieldName]int, 0)
	}
	if _, ok := s.fieldKeys[qtid.TableQualifier.Key()][qtid.ID]; !ok {
		s.fieldKeys[qtid.TableQualifier.Key()][qtid.ID] = make(map[dax.FieldName]int, 0)
	}

	return nil
}

// RemoveTable removes the given table. An error will be returned if the table
// does not exist.
func (s *VersionStore) RemoveTable(ctx context.Context, qtid dax.QualifiedTableID) (dax.VersionedShards, dax.VersionedPartitions, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var foundTable bool
	var shards dax.VersionedShards
	var partitions dax.VersionedPartitions
	var err error

	// Remove shards for table.
	if s.shards != nil {
		if _, ok := s.shards[qtid.TableQualifier.Key()][qtid.ID]; ok {
			foundTable = true

			// Get the shards to return before deleting from map.
			shards, _, err = s.shardSlice(qtid)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "getting shard slice: %s", qtid)
			}

			// Remove the shards.
			delete(s.shards[qtid.TableQualifier.Key()], qtid.ID)
		}
	}

	// Remove tableKeys for table.
	if s.tableKeys != nil {
		if _, ok := s.tableKeys[qtid.TableQualifier.Key()][qtid.ID]; ok {
			foundTable = true

			// Get the partitions to return before deleting from map.
			partitions, _, err = s.partitionSlice(qtid)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "getting partition slice: %s", qtid)
			}

			// Remove the tableKeys.
			delete(s.tableKeys[qtid.TableQualifier.Key()], qtid.ID)
		}
	}

	// Remove fieldKeys for table.
	if s.fieldKeys != nil {
		if _, ok := s.fieldKeys[qtid.TableQualifier.Key()][qtid.ID]; ok {
			foundTable = true

			// Remove the fieldKeys.
			delete(s.fieldKeys[qtid.TableQualifier.Key()], qtid.ID)
		}
	}

	if !foundTable {
		return nil, nil, dax.NewErrTableIDDoesNotExist(qtid)
	}

	return shards, partitions, nil
}

// AddShards adds new shards to be managed by VersionStore. It returns the
// number of shards added or an error.
func (s *VersionStore) AddShards(ctx context.Context, qtid dax.QualifiedTableID, shards ...dax.VersionedShard) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sh, ok := s.shards[qtid.TableQualifier.Key()][qtid.ID]
	if !ok {
		return dax.NewErrTableIDDoesNotExist(qtid)
	}

	var n int

	for _, shard := range shards {
		if _, ok := sh[shard.Num]; !ok {
			n++ // TODO: this isn't considering a shard that exists, but the version changes.
		}
		sh[shard.Num] = shard
	}

	return nil
}

// Shards returns the list of shards available for the give table. It returns
// false if the table does not exist.
func (s *VersionStore) Shards(ctx context.Context, qtid dax.QualifiedTableID) (dax.VersionedShards, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.shardSlice(qtid)
}

// shardSlice is an unprotected version of Shards().
func (s *VersionStore) shardSlice(qtid dax.QualifiedTableID) (dax.VersionedShards, bool, error) {
	if s.shards == nil {
		return nil, false, nil
	}

	if shardNumMap, ok := s.shards[qtid.TableQualifier.Key()][qtid.ID]; ok {
		rtn := make(dax.VersionedShards, 0, len(shardNumMap))
		for _, shard := range shardNumMap {
			rtn = append(rtn, shard)
		}
		sort.Sort(rtn)
		return rtn, true, nil
	}

	return nil, false, nil
}

// ShardVersion return the current version for the given table/shardNum.
// If a version is not being tracked, it returns a bool value of false.
func (s *VersionStore) ShardVersion(ctx context.Context, qtid dax.QualifiedTableID, shardNum dax.ShardNum) (int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.shards[qtid.TableQualifier.Key()][qtid.ID]
	if !ok {
		return -1, false, nil
	}

	v, ok := t[shardNum]
	if !ok {
		return -1, false, nil
	}
	return v.Version, true, nil
}

func (s *VersionStore) ShardTables(ctx context.Context, qual dax.TableQualifier) (dax.TableIDs, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	qual.Key()

	tableIDs := make(dax.TableIDs, 0, len(s.shards[qual.Key()]))

	for tableID := range s.shards[qual.Key()] {
		tableIDs = append(tableIDs, tableID)
	}

	return tableIDs, nil
}

// AddPartitions adds new partitions to be managed by VersionStore. It returns
// the number of partitions added or an error.
func (s *VersionStore) AddPartitions(ctx context.Context, qtid dax.QualifiedTableID, partitions ...dax.VersionedPartition) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tk, ok := s.tableKeys[qtid.TableQualifier.Key()][qtid.ID]
	if !ok {
		return dax.NewErrTableIDDoesNotExist(qtid)
	}

	for _, partition := range partitions {
		tk[partition.Num] = partition.Version
	}

	return nil
}

// Partitions returns the list of partitions available for the give table. It
// returns false if the table does not exist.
func (s *VersionStore) Partitions(ctx context.Context, qtid dax.QualifiedTableID) (dax.VersionedPartitions, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.partitionSlice(qtid)
}

// partitionSlice is an unprotected version of Partitions().
func (s *VersionStore) partitionSlice(qtid dax.QualifiedTableID) (dax.VersionedPartitions, bool, error) {
	if s.tableKeys == nil {
		return nil, false, nil
	}

	if partitionNumMap, ok := s.tableKeys[qtid.TableQualifier.Key()][qtid.ID]; ok {
		rtn := make(dax.VersionedPartitions, 0, len(partitionNumMap))
		for partitionNum, version := range partitionNumMap {
			rtn = append(rtn, dax.NewVersionedPartition(partitionNum, version))
		}
		sort.Sort(rtn)
		return rtn, true, nil
	}

	return nil, false, nil
}

// PartitionVersion return the current version for the given table/partitionNum.
// If a version is not being tracked, it returns a bool value of false.
func (s *VersionStore) PartitionVersion(ctx context.Context, qtid dax.QualifiedTableID, partitionNum dax.PartitionNum) (int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.tableKeys[qtid.TableQualifier.Key()][qtid.ID]
	if !ok {
		return -1, false, nil
	}

	v, ok := t[partitionNum]
	if !ok {
		return -1, false, nil
	}
	return v, true, nil
}

func (s *VersionStore) PartitionTables(ctx context.Context, qual dax.TableQualifier) (dax.TableIDs, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tableIDs := make(dax.TableIDs, 0, len(s.tableKeys[qual.Key()]))

	for tableName := range s.tableKeys[qual.Key()] {
		tableIDs = append(tableIDs, tableName)
	}

	return tableIDs, nil
}

// AddFields adds new fields to be managed by VersionStore. It returns the
// number of fields added or an error.
func (s *VersionStore) AddFields(ctx context.Context, qtid dax.QualifiedTableID, fields ...dax.VersionedField) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	fk, ok := s.fieldKeys[qtid.TableQualifier.Key()][qtid.ID]
	if !ok {
		return dax.NewErrTableIDDoesNotExist(qtid)
	}

	for _, field := range fields {
		fk[field.Name] = field.Version
	}

	return nil
}

// Fields returns the list of fields available for the give table. It returns
// false if the table does not exist.
func (s *VersionStore) Fields(ctx context.Context, qtid dax.QualifiedTableID) (dax.VersionedFields, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.fieldSlice(qtid)
}

// fieldSlice is an unprotected version of Fields().
func (s *VersionStore) fieldSlice(qtid dax.QualifiedTableID) (dax.VersionedFields, bool, error) {
	if s.fieldKeys == nil {
		return nil, false, nil
	}

	if fieldNameMap, ok := s.fieldKeys[qtid.TableQualifier.Key()][qtid.ID]; ok {
		rtn := make(dax.VersionedFields, 0, len(fieldNameMap))
		for fieldName, version := range fieldNameMap {
			rtn = append(rtn, dax.NewVersionedField(fieldName, version))
		}
		sort.Sort(rtn)
		return rtn, true, nil
	}

	return nil, false, nil
}

// FieldVersion return the current version for the given table/field.
// If a version is not being tracked, it returns a bool value of false.
func (s *VersionStore) FieldVersion(ctx context.Context, qtid dax.QualifiedTableID, field dax.FieldName) (int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.fieldKeys[qtid.TableQualifier.Key()][qtid.ID]
	if !ok {
		return -1, false, nil
	}

	v, ok := t[field]
	if !ok {
		return -1, false, nil
	}
	return v, true, nil
}

func (s *VersionStore) FieldTables(ctx context.Context, qual dax.TableQualifier) (dax.TableIDs, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tableIDs := make(dax.TableIDs, 0, len(s.fieldKeys[qual.Key()]))

	for tableID := range s.fieldKeys[qual.Key()] {
		tableIDs = append(tableIDs, tableID)
	}

	return tableIDs, nil
}

// Copy returns a new copy of VersionStore.
func (s *VersionStore) Copy(ctx context.Context) (dax.VersionStore, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	new := NewVersionStore()

	// shards.
	for qkey, tableIDs := range s.shards {
		for tableID, shards := range tableIDs {
			qual := dax.NewTableQualifier(qkey.OrganizationID(), qkey.DatabaseID())
			qtid := dax.NewQualifiedTableID(qual, tableID)
			_ = new.AddTable(ctx, qtid)
			for shardNum, shard := range shards {
				new.shards[qual.Key()][tableID][shardNum] = shard
			}
		}
	}

	// tableKeys.
	for qkey, tableIDs := range s.tableKeys {
		for tableID, partitions := range tableIDs {
			qual := dax.NewTableQualifier(qkey.OrganizationID(), qkey.DatabaseID())
			qtid := dax.NewQualifiedTableID(qual, tableID)
			_ = new.AddTable(ctx, qtid)
			for partitionNum, version := range partitions {
				new.tableKeys[qual.Key()][tableID][partitionNum] = version
			}
		}
	}

	// fieldKeys.
	for qkey, tableIDs := range s.fieldKeys {
		for tableID, fields := range tableIDs {
			qual := dax.NewTableQualifier(qkey.OrganizationID(), qkey.DatabaseID())
			qtid := dax.NewQualifiedTableID(qual, tableID)
			_ = new.AddTable(ctx, qtid)
			for field, version := range fields {
				new.fieldKeys[qual.Key()][tableID][field] = version
			}
		}
	}

	return new, nil
}
