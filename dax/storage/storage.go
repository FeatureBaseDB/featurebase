package storage

import (
	"fmt"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/computer"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// ResourceManager holds all the various Resources each of which is
// specific to a particular shard, table key partition or field, but
// all of which use the same underlying snapshotter and writelogger.
type ResourceManager struct {
	Snapshotter computer.SnapshotService
	Writelogger computer.WritelogService
	Logger      logger.Logger

	mu                sync.Mutex
	shardResources    map[shardK]*Resource
	tableKeyResources map[tableKeyK]*Resource
	fieldKeyResources map[fieldKeyK]*Resource
}

func NewResourceManager(s computer.SnapshotService, w computer.WritelogService, l logger.Logger) *ResourceManager {
	return &ResourceManager{
		Snapshotter: s,
		Writelogger: w,
		Logger:      l,

		shardResources:    make(map[shardK]*Resource),
		tableKeyResources: make(map[tableKeyK]*Resource),
		fieldKeyResources: make(map[fieldKeyK]*Resource),
	}
}

// compound map keys

type shardK struct {
	qtid      dax.QualifiedTableID
	partition dax.PartitionNum
	shard     dax.ShardNum
}

type tableKeyK struct {
	qtid      dax.QualifiedTableID
	partition dax.PartitionNum
}

type fieldKeyK struct {
	qtid  dax.QualifiedTableID
	field dax.FieldName
}

func (mm *ResourceManager) GetShardResource(qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum) *Resource {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	key := shardK{qtid: qtid, partition: partition, shard: shard}
	if m, ok := mm.shardResources[key]; ok {
		return m
	}
	mm.shardResources[key] = (&Resource{
		snapshotter: mm.Snapshotter,
		writelogger: mm.Writelogger,
		bucket:      partitionBucket(qtid.Key(), partition),
		key:         shardKey(shard),
		log:         mm.Logger,
	}).initialize()

	return mm.shardResources[key]
}

func (mm *ResourceManager) RemoveShardResource(qtid dax.QualifiedTableID, partition dax.PartitionNum, shard dax.ShardNum) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	key := shardK{qtid: qtid, partition: partition, shard: shard}
	if m, ok := mm.shardResources[key]; ok {
		err := m.Unlock()
		if err != nil {
			mm.Logger.Printf("unlocking shard resource during removal: %v", err)
		}
		delete(mm.shardResources, key)
	}
}

func (mm *ResourceManager) GetTableKeyResource(qtid dax.QualifiedTableID, partition dax.PartitionNum) *Resource {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	key := tableKeyK{qtid: qtid, partition: partition}
	if m, ok := mm.tableKeyResources[key]; ok {
		return m
	}
	mm.tableKeyResources[key] = (&Resource{
		snapshotter: mm.Snapshotter,
		writelogger: mm.Writelogger,
		bucket:      partitionBucket(qtid.Key(), partition),
		key:         keysFileName,
		log:         mm.Logger,
	}).initialize()
	return mm.tableKeyResources[key]
}

func (mm *ResourceManager) RemoveTableKeyResource(qtid dax.QualifiedTableID, partition dax.PartitionNum) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	key := tableKeyK{qtid: qtid, partition: partition}
	if m, ok := mm.tableKeyResources[key]; ok {
		err := m.Unlock()
		if err != nil {
			mm.Logger.Printf("unlocking table key resource during removal: %v", err)
		}
		delete(mm.tableKeyResources, key)
	}
}

func (mm *ResourceManager) GetFieldKeyResource(qtid dax.QualifiedTableID, field dax.FieldName) *Resource {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	key := fieldKeyK{qtid: qtid, field: field}
	if m, ok := mm.fieldKeyResources[key]; ok {
		return m
	}
	mm.fieldKeyResources[key] = (&Resource{
		snapshotter: mm.Snapshotter,
		writelogger: mm.Writelogger,
		bucket:      fieldBucket(qtid.Key(), field),
		key:         keysFileName,
		log:         mm.Logger,
	}).initialize()
	return mm.fieldKeyResources[key]
}

func (mm *ResourceManager) RemoveFieldKeyResource(qtid dax.QualifiedTableID, field dax.FieldName) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	key := fieldKeyK{qtid: qtid, field: field}
	if m, ok := mm.fieldKeyResources[key]; ok {
		err := m.Unlock()
		if err != nil {
			mm.Logger.Printf("unlocking field key resource during removal: %v", err)
		}
		delete(mm.fieldKeyResources, key)
	}
}

// RemoveAll unlocks and deletes all resources held within this
// ResourceManager. To be clear, the underlying resources are *not*
// deleted, they are simply no longer under this resource manager's
// purview. If there's a problem removing any resource, we'll save the
// error, but keep going... this is only called on shutdown, so we do
// our best to unlock everything.
func (mm *ResourceManager) RemoveAll() error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	errList := make([]error, 0)
	for k, resource := range mm.shardResources {
		err := resource.Unlock()
		if err != nil && !strings.Contains(err.Error(), "resource was not locked") {
			errList = append(errList, err)
		}
		delete(mm.shardResources, k)
	}
	for k, resource := range mm.tableKeyResources {
		err := resource.Unlock()
		if err != nil && !strings.Contains(err.Error(), "resource was not locked") {
			errList = append(errList, err)
		}
		delete(mm.tableKeyResources, k)
	}
	for k, resource := range mm.fieldKeyResources {
		err := resource.Unlock()
		if err != nil && !strings.Contains(err.Error(), "resource was not locked") {
			errList = append(errList, err)
		}
		delete(mm.fieldKeyResources, k)
	}
	if len(errList) > 0 {
		return errors.Errorf("%v", errList)
	}
	return nil
}

// RemoveTable, unlocks and removes all resources related to the given
// table from this ResourceManager. The underlying files are not
// deleted. (If the table is being dropped, deleting the files is
// under the purview of metadata services). In this case (in contrast
// to RemoveAll), we'll stop on any error and return it. Not sure it
// actually matters in either of these cases.
func (mm *ResourceManager) RemoveTable(qtid dax.QualifiedTableID) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	for k, resource := range mm.shardResources {
		if k.qtid == qtid {
			if err := resource.Unlock(); err != nil {
				return errors.Wrap(err, "unlocking shard")
			}
		}
	}
	for k, resource := range mm.tableKeyResources {
		if k.qtid == qtid {
			if err := resource.Unlock(); err != nil {
				return errors.Wrap(err, "unlocking shard")
			}
		}
	}
	for k, resource := range mm.fieldKeyResources {
		if k.qtid == qtid {
			if err := resource.Unlock(); err != nil {
				return errors.Wrap(err, "deleting field key resource %s")
			}
		}
	}
	return nil
}

// Resource wraps the snapshotter and writelogger to maintain messy
// state between calls. Resource is *not* threadsafe, care should be
// taken that concurrent calls are not made to Resource methods. The
// exception being that Snapshot and Append are safe to call
// concurrently.
type Resource struct {
	snapshotter computer.SnapshotService
	writelogger computer.WritelogService
	bucket      string
	key         string

	log logger.Logger

	loadWLsPastVersion int
	latestWLVersion    int
	lastWLPos          int

	locked bool

	dirty bool
}

func (m *Resource) initialize() *Resource {
	m.loadWLsPastVersion = -2
	m.latestWLVersion = -1
	m.lastWLPos = -1
	return m
}

// IsLocked checks to see if this particular instance of the resource
// believes it holds the lock. It does not look at the state of
// underlying storage to verify the lock.
func (m *Resource) IsLocked() bool {
	return m.locked
}

// LoadLatestSnapshot finds the most recent snapshot for this resource
// and returns a ReadCloser for that snapshot data. If there is no
// snapshot for this resource it returns nil, nil.
func (m *Resource) LoadLatestSnapshot() (data io.ReadCloser, err error) {
	snaps, err := m.snapshotter.List(m.bucket, m.key)
	if err != nil {
		return nil, errors.Wrap(err, "listing snapshots")
	}
	m.log.Debugf("LoadLatestSnapshot %s/%s: list: %v", m.bucket, m.key, snaps)
	m.lastWLPos = 0

	if len(snaps) == 0 {
		m.loadWLsPastVersion = -1
		return nil, nil
	}
	// assuming snapshots come back in sorted order
	latest := snaps[len(snaps)-1]
	m.loadWLsPastVersion = latest.Version

	// TODO(jaffee): whatever is using the snapshot may discover that
	// it is corrupted/incomplete. We don't want to separately check
	// the checksum in here because then we'd have to read the whole
	// snapshot twice. Need a way to catch the checksum error and tell
	// Resource to mark that version as bad and remove it, then try
	// LoadLatestSnapshot again.
	return m.snapshotter.Read(m.bucket, m.key, latest.Version)
}

// // Potential future methods to support getting older versions. SnapInfo would have timestamp information as well.
//
// ListSnapshots() []SnapInfo
// LoadSnapshot(version int) (data io.ReadCloser, err error)

// LoadWriteLog can be called after LoadLatestSnapshot. It loads any
// writelog data which has been written since the latest
// snapshot. Subsequent calls to LoadWriteLog will only return new
// data that hasn't previously been returned from LoadWriteLog. If
// there is no writelog, it returns nil, nil.
func (m *Resource) LoadWriteLog() (data io.ReadCloser, err error) {
	if m.loadWLsPastVersion == -2 {
		return nil, errors.New(errors.ErrUncoded, "LoadWriteLog called in inconsistent state, can't tell what version to load from")
	}
	wLogs, err := m.writelogger.List(m.bucket, m.key)
	if err != nil {
		return nil, errors.Wrap(err, "listing write logs")
	}

	m.log.Debugf("LoadWriteLog %s/%s: list: %v", m.bucket, m.key, wLogs)

	versions := make([]int, 0, len(wLogs))
	for _, log := range wLogs {
		if log.Version > m.loadWLsPastVersion {
			versions = append(versions, log.Version)
		}
	}

	if len(versions) > 1 {
		// TODO(jaffee) This can happen if there's a failure writing a
		// snapshot. Need to implement a MultiReadCloser or similar
		// that wraps all the latest write logs into one ReadCloser.
		// It should only wrap the last one in a trackingReader.
		return nil, errors.New(dax.ErrUnimplemented, "UNIMPLEMENTED: multiple write log versions ahead of latest snapshot.")
	}

	if len(versions) == 0 {
		m.log.Debugf("LoadWriteLog: no logs after snapshot: %d on %s", m.loadWLsPastVersion, path.Join(m.bucket, m.key))
		m.latestWLVersion = m.loadWLsPastVersion + 1
		return nil, nil
	}

	if m.locked && m.latestWLVersion != versions[0] {
		return nil, errors.New(errors.ErrUncoded, "write log version gone since locking")
	}
	m.latestWLVersion = versions[0]
	m.dirty = true

	r, err := m.writelogger.LogReaderFrom(m.bucket, m.key, versions[0], m.lastWLPos)
	if err != nil {
		return nil, errors.Wrap(err, "getting writelog")
	}
	return &trackingReader{
		r: r,
		update: func(n int, err error) {
			m.lastWLPos += n
		},
	}, nil
}

// Lock acquires an advisory lock for this resource which grants
// us exclusive access to write to it.  The normal pattern is to
// call:
//
// 1. LoadLatestSnapshot
// 2. LoadWriteLog
// 3. Lock
// 4. LoadWriteLog
//
// The second call to LoadWriteLog is necessary in case any writes
// occurred between the last load and acquiring the lock. Once the
// lock is acquired it should not be possible for any more writes
// to occur. Lock will error if (a) we fail to acquire the lock or
// (b) the state of the snapshot store for this resource is not
// identical to what is was before the lock was acquired. Case (b)
// means that quite a lot has happened in between LoadWriteLog and
// Lock, and we should probably just die and start over.
func (m *Resource) Lock() error {
	m.log.Debugf("Lock %s/%s", m.bucket, m.key)
	// lock is sort of arbitrarily on the write log interface
	if err := m.writelogger.Lock(m.bucket, m.key); err != nil {
		return errors.Wrap(err, "acquiring lock")
	}
	m.locked = true
	return nil
}

// Append appends the msg to the write log. It will fail if we
// haven't properly loaded and gotten a lock for the resource
// we're writing to.
func (m *Resource) Append(msg []byte) error {
	m.log.Debugf("Append %s/%s", m.bucket, m.key)
	if m.latestWLVersion < 0 {
		return errors.New(errors.ErrUncoded, "can't call append before loading and locking write log")
	}
	m.dirty = true
	return m.writelogger.AppendMessage(m.bucket, m.key, m.latestWLVersion, msg)
}

// IncrementWLVersion should be called during snapshotting with a
// write Tx held on the local resource. This ensures that any writes
// which completed prior to the snapshot are in the prior WL and any
// that complete after the snapshot are in the incremented WL. If
// there have been no writes since the latest snapshot, this returns
// false and does nothing. In this case, Snapshot should *not* be
// called.
func (m *Resource) IncrementWLVersion() (bool, error) {
	if !m.dirty {
		return false, nil
	}
	m.log.Debugf("IncrementWLVersion %s/%s", m.bucket, m.key)
	m.latestWLVersion++
	m.lastWLPos = -1
	m.loadWLsPastVersion = -1
	m.dirty = false
	return true, nil
}

// Snapshot takes a ReadCloser which has the contents of the resource
// being tracked at a particular point in time and writes them to the
// Snapshot Store. Upon a successful write it will truncate any write
// logs which are now incorporated into the snapshot. Do not call
// until after calling IncrementWLVersion, and only if that method
// returns "true".
func (m *Resource) Snapshot(rc io.ReadCloser) error {
	m.log.Debugf("Snapshot %s/%s", m.bucket, m.key)
	// latestWLVersion has already been incremented at this point, so
	// we write that version minus 1.
	err := m.snapshotter.Write(m.bucket, m.key, m.latestWLVersion-1, rc)
	if err != nil {
		return errors.Wrap(err, "writing snapshot")
	}
	err = m.writelogger.DeleteLog(m.bucket, m.key, m.latestWLVersion-1)
	return errors.Wrap(err, "deleting old write log")
}

// SnapshotTo is Snapshot's ugly stepsister supporting the weirdness
// of reading from translate stores who we're hoping to off in the
// next season.
func (m *Resource) SnapshotTo(wt io.WriterTo) error {
	m.log.Debugf("SnapshotTo %s/%s", m.bucket, m.key)
	err := m.snapshotter.WriteTo(m.bucket, m.key, m.latestWLVersion-1, wt)
	if err != nil {
		return errors.Wrap(err, "writing snapshot SnapshotTo")
	}
	err = m.writelogger.DeleteLog(m.bucket, m.key, m.latestWLVersion-1)
	return errors.Wrap(err, "deleting old write log snapshotTo")
}

// Unlock releases the lock. This should be called if control of
// the underlying resource is being transitioned to another
// node. Ideally it's also called if the process crashes (e.g. via
// a defer), but an implementation based on filesystem locks
// should have those removed by the operating system when the
// process exits anyway.
func (m *Resource) Unlock() error {
	m.log.Debugf("Unlock %s/%s", m.bucket, m.key)
	if !m.locked {
		return errors.New(errors.ErrUncoded, "resource was not locked")
	}
	if err := m.writelogger.Unlock(m.bucket, m.key); err != nil {
		return errors.Wrap(err, "unlocking")
	}
	m.locked = false
	return nil
}

const (
	keysFileName = "keys"
)

func partitionBucket(table dax.TableKey, partition dax.PartitionNum) string {
	return path.Join(string(table), "partition", fmt.Sprintf("%d", partition))
}

func shardKey(shard dax.ShardNum) string {
	return path.Join("shard", fmt.Sprintf("%d", shard))
}

func fieldBucket(table dax.TableKey, field dax.FieldName) string {
	return path.Join(string(table), "field", string(field))
}
