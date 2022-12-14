// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"
	"io"
	"log"
	"sync"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/computer"
	"github.com/molecula/featurebase/v3/dax/storage"
	"github.com/molecula/featurebase/v3/disco"
	"github.com/pkg/errors"
)

// ApplyDirective applies a Directive received, from the Controller, at the
// /directive endpoint.
func (api *API) ApplyDirective(ctx context.Context, d *dax.Directive) error {
	// Get the current directive for comparison.
	previousDirective := api.holder.Directive()

	// Check that incoming version is newer.
	// Note: 0 is an invalid Directive version. This decision was made because
	// previousDirective is not a pointer to a directive, but a concrete
	// Directive. Which means we can't check for nil, and by default it has a
	// version of 0. So in order to ensure the version has increased, we need to
	// require that incoming directive versions are greater than 0.
	if d.Version == 0 {
		return errors.Errorf("directive version cannot be 0")
	} else if previousDirective.Version >= d.Version {
		return errors.Errorf("directive version mismatch, got %d, but already have %d", d.Version, previousDirective.Version)
	}

	// Handle the operations based on the directive method.
	switch d.Method {
	case dax.DirectiveMethodDiff:
		// pass: normal operation

	case dax.DirectiveMethodReset:
		// Delete all tables.
		if err := api.deleteAllIndexes(ctx); err != nil {
			return errors.Wrap(err, "deleting all indexes")
		}
		if err := api.serverlessStorage.RemoveAll(); err != nil {
			return errors.Wrap(err, "removing all managers")
		}

		// Set previousDirective to empty so the diff handles everything as new.
		previousDirective = dax.Directive{}

	case dax.DirectiveMethodSnapshot:
		// TODO(tlt): this was the existing logic, but we should really diff the
		// directive and ensure that overwriting the value in the cache doesn't
		// have a negative effect.
		api.holder.SetDirective(d)
		return nil

	default:
		return errors.Errorf("invalid directive method: %s", d.Method)
	}

	// Cache this directive as the latest applied. There is functionality within
	// the "enactDirective" stage of ApplyDirective which validates against this
	// cached Directive, so it's important that it be set before calling
	// enactDirective(). An example: when loading partition data from the
	// WriteLogger, there are validations to ensure that the partition being
	// loaded is meant to be handled by this node; that validation is done
	// against the cached Directive.
	// TODO(tlt): despite what this comment says, this logic is not sound; we
	// shouldn't be setting the directive until enactiveDirective() succeeds.
	api.holder.SetDirective(d)
	defer api.holder.SetDirectiveApplied(true)

	return api.enactDirective(ctx, &previousDirective, d)
}

// deleteAllIndexes deletes all indexes handled by this node.
func (api *API) deleteAllIndexes(ctx context.Context) error {
	indexes, err := api.Schema(ctx, false)
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}

	for i := range indexes {
		if err := api.DeleteIndex(ctx, indexes[i].Name); err != nil {
			return errors.Wrapf(err, "deleting index: %s", indexes[i].Name)
		}
	}

	return nil
}

// directiveJobType allows us to switch on jobType in the directiveWorker in
// order to use a single worker pool for all job types (as opposed to having a
// separate worker pool for each job type).
type directiveJobType interface {
	// We have this method just to prevent *any* struct from implementing this
	// interface automatically. But, interestingly enough, we don't actually
	// have to have this method on the implementation because we embed the
	// interface.
	isJobType() bool
}

type directiveJobTableKeys struct {
	directiveJobType
	idx       *Index
	tkey      dax.TableKey
	partition dax.PartitionNum
}

type directiveJobFieldKeys struct {
	directiveJobType
	tkey  dax.TableKey
	field dax.FieldName
}

type directiveJobShards struct {
	directiveJobType
	tkey  dax.TableKey
	shard dax.ShardNum
}

// directiveWorker is a worker in a worker pool which handles portions of a
// directive. Multiple instances of directiveWorker run in goroutines in order
// to load data from snapshotter and writelogger concurrently. Note: unlike the
// api.ingestWorkerPool, of which one pool is always running, the
// directiveWorker pool is only running during the life of the
// api.ApplyDirective call. Technically, this means that multiple
// directiveWorker pools could be active at the same time, but we should never
// be running more than once instance of ApplyDirective concurrently.
func (api *API) directiveWorker(ctx context.Context, jobs <-chan directiveJobType, errs chan<- error) {
	for j := range jobs {
		switch job := j.(type) {
		case directiveJobTableKeys:
			if err := api.loadTableKeys(ctx, job.idx, job.tkey, job.partition); err != nil {
				errs <- errors.Wrapf(err, "loading table keys: %s, %s", job.tkey, job.partition)
			}
		case directiveJobFieldKeys:
			if err := api.loadFieldKeys(ctx, job.tkey, job.field); err != nil {
				errs <- errors.Wrapf(err, "loading field keys: %s, %s", job.tkey, job.field)
			}
		case directiveJobShards:
			if err := api.loadShard(ctx, job.tkey, job.shard); err != nil {
				errs <- errors.Wrapf(err, "loading shard: %s, %s", job.tkey, job.shard)
			}
		default:
			errs <- errors.Errorf("unsupported job type: %T %[1]v", job)
		}

		select {
		case <-ctx.Done():
			return
		default:
			// continue pulling jobs off the channel
		}
	}
}

func (api *API) enactDirective(ctx context.Context, fromD, toD *dax.Directive) error {
	// enactTables is called before the jobs that run in the worker pool because
	// it probably makes sense to apply the schema before trying to load data
	// concurrently.
	if err := api.enactTables(ctx, fromD, toD); err != nil {
		return errors.Wrap(err, "enactTables")
	}

	// The following types use a shared pool of workers to run each
	// directiveJobType.

	var wg sync.WaitGroup

	// open job channel
	jobs := make(chan directiveJobType, api.directiveWorkerPoolSize)
	errs := make(chan error)
	done := make(chan struct{})

	// Spin up n workers in goroutines that pull jobs from the jobs channel.
	for i := 0; i < api.directiveWorkerPoolSize; i++ {
		wg.Add(1)
		go func() {
			api.directiveWorker(ctx, jobs, errs)
			defer wg.Done()
		}()
	}

	// Wait for the WaitGroup counter to reach 0. When it has, indicate that
	// we're done processing all jobs by closing the done channel.
	go func() {
		wg.Wait()
		close(done)
	}()

	// Run through all the "enact" methods. These push jobs onto the jobs
	// channel. Once all the jobs have been queued to the channel, we close the
	// jobs channel. This allows the directiveWorkers to exit out of the
	// function, which will then decrement the WaitGroup counter.
	go func() {
		api.pushJobsTableKeys(ctx, jobs, fromD, toD)
		api.pushJobsFieldKeys(ctx, jobs, fromD, toD)
		api.pushJobsShards(ctx, jobs, fromD, toD)
		close(jobs)
	}()

	// Keep running until we get an error or until the done channel is closed.
	// Note: the code is written such that only non-nil errors are pushed to the
	// errs channel.
	for {
		select {
		case err := <-errs:
			return err
		case <-done:
			return nil
		}
	}
}

func (api *API) enactTables(ctx context.Context, fromD, toD *dax.Directive) error {
	currentIndexes := api.holder.Indexes()

	// Make a list of indexes that currently exist (from).
	from := make(dax.TableKeys, 0, len(currentIndexes))
	for _, idx := range currentIndexes {
		qtid, err := dax.QualifiedTableIDFromKey(idx.Name())
		if err != nil {
			return errors.Wrap(err, "converting index name to qualified table id")
		}
		from = append(from, qtid.Key())
	}

	// TODO sanity check holder against fromD. We're getting existing
	// indexes from holder, but in theory fromD should be
	// identical. If we have an error in our directive-caching logic
	// (it has happened before (just now, in fact!) and we'd be
	// foolish to think it won't happen again), or we have schema
	// mutations that are not going through the directive path, we
	// could potentially catch them here.

	// Make a list of tables that are in the directive (to) along with a map of
	// tableKey to table (m).
	m := make(map[dax.TableKey]*dax.QualifiedTable, len(toD.Tables))
	to := make(dax.TableKeys, 0, len(toD.Tables))
	for _, t := range toD.Tables {
		m[t.Key()] = t
		to = append(to, t.Key())
	}

	sc := newSliceComparer(from, to)

	// Remove all indexes that are no longer part of the directive.
	for _, tkey := range sc.removed() {
		idx := string(tkey)
		if err := api.holder.deleteIndex(idx); err != nil {
			return errors.Wrapf(err, "deleting index: %s", tkey)
		}
	}

	// Put partitions into a map by table.
	partitionMap := toD.TranslatePartitionsMap()

	// Add all indexes that weren't previously (but now are) a part of the
	// directive.
	for _, tkey := range sc.added() {
		if qtbl, found := m[tkey]; !found {
			return errors.Errorf("table '%s' was not in map", tkey)
		} else if err := api.createTableAndFields(qtbl, partitionMap[tkey]); err != nil {
			return err
		}
	}

	// Check fields on all indexes present in both from and to.
	for _, tkey := range sc.same() {
		if err := api.enactFieldsForTable(ctx, tkey, fromD, toD); err != nil {
			return errors.Wrapf(err, "enacting fields for table: '%s'", tkey)
		}
	}

	return nil
}

func (api *API) enactFieldsForTable(ctx context.Context, tkey dax.TableKey, fromD, toD *dax.Directive) error {
	qtid := tkey.QualifiedTableID()

	fromT, err := fromD.Table(qtid)
	if err != nil {
		return errors.Wrap(err, "getting from table")
	}
	toT, err := toD.Table(qtid)
	if err != nil {
		return errors.Wrap(err, "getting to table")
	}

	// Get the index for tkey.
	idx := api.holder.Index(string(tkey))
	if idx == nil {
		return errors.Errorf("index not found: %s", tkey)
	}

	sc := newSliceComparer(fromT.FieldNames(), toT.FieldNames())

	// Add fields new to toT.
	for _, fldName := range sc.added() {
		if field, found := toT.Field(fldName); !found {
			return dax.NewErrFieldDoesNotExist(fldName)
		} else if err := createField(idx, field); err != nil {
			return errors.Wrapf(err, "creating field: %s/%s", tkey, fldName)
		}
	}

	// Remove fields which don't exist in toT.
	for _, fldName := range sc.removed() {
		if err := api.DeleteField(ctx, string(tkey), string(fldName)); err != nil {
			return errors.Wrapf(err, "deleting field: %s/%s", tkey, fldName)
		}
	}

	// // Update any field options which have changed for existing fields.
	// for _, fldName := range sc.same() {
	// 	// handle changed field options??
	// }

	return nil
}

func (api *API) pushJobsTableKeys(ctx context.Context, jobs chan<- directiveJobType, fromD, toD *dax.Directive) {
	toPartitionsMap := toD.TranslatePartitionsMap()

	// Get the diff between from/to directive.partitions.
	partComp := newPartitionsComparer(fromD.TranslatePartitionsMap(), toPartitionsMap)

	// Loop over the partition map and load from WriteLogger.
	for tkey, partitions := range partComp.added() {
		// Get index in order to find the translate stores (by partition) for
		// the table.
		idx := api.holder.Index(string(tkey))
		if idx == nil {
			log.Printf("index not found in holder: %s", tkey)
			continue
		}

		// Update the cached version of translate partitions that we keep on the
		// Index.
		idx.SetTranslatePartitions(toPartitionsMap[tkey])

		for _, partition := range partitions {
			jobs <- directiveJobTableKeys{
				idx:       idx,
				tkey:      tkey,
				partition: partition.Num,
			}
		}
	}
}

func (api *API) loadTableKeys(ctx context.Context, idx *Index, tkey dax.TableKey, partition dax.PartitionNum) error {
	qtid := tkey.QualifiedTableID()

	mgr := api.serverlessStorage.GetTableKeyManager(qtid, partition)
	if mgr.IsLocked() {
		api.logger().Debugf("skipping loadTableKeys (already held) %s %d", tkey, partition)
		return nil
	}

	// load latest snapshot
	rc, err := mgr.LoadLatestSnapshot()
	if err != nil {
		return errors.Wrap(err, "loading table key snapshot")
	}
	if rc != nil {
		defer rc.Close()
		if err := api.TranslateIndexDB(ctx, string(tkey), int(partition), rc); err != nil {
			return errors.Wrap(err, "restoring table keys")
		}

	}

	// define write log loading in a function since we have to do it
	// before and after locking
	loadWriteLog := func() error {
		writelog, err := mgr.LoadWriteLog()
		if err != nil {
			return errors.Wrap(err, "getting write log reader for table keys")
		}
		if writelog == nil {
			return nil
		}
		reader := storage.NewTableKeyReader(qtid, partition, writelog)
		defer reader.Close()
		store := idx.TranslateStore(int(partition))
		for msg, err := reader.Read(); err != io.EOF; msg, err = reader.Read() {
			if err != nil {
				return errors.Wrap(err, "reading from log reader")
			}
			for key, id := range msg.StringToID {
				if err := store.ForceSet(id, key); err != nil {
					return errors.Wrapf(err, "forcing set id, key: %d, %s", id, key)
				}
			}
		}
		return nil
	}
	// 1st write log load
	if err := loadWriteLog(); err != nil {
		return err
	}

	// acquire lock on this partition's keys
	if err := mgr.Lock(); err != nil {
		return errors.Wrap(err, "locking table key partition")
	}

	// reload writelog in case of changes between last load and
	// lock. The manager object takes care of only loading new data.
	return loadWriteLog()
}

func (api *API) pushJobsFieldKeys(ctx context.Context, jobs chan<- directiveJobType, fromD, toD *dax.Directive) {
	// Get the diff between from/to directive.fields.
	fieldComp := newFieldsComparer(fromD.TranslateFieldsMap(), toD.TranslateFieldsMap())

	// Loop over the field map and load from WriteLogger.
	for tkey, fields := range fieldComp.added() {
		for _, field := range fields {
			jobs <- directiveJobFieldKeys{
				tkey:  tkey,
				field: field.Name,
			}
		}
	}
}

func (api *API) loadFieldKeys(ctx context.Context, tkey dax.TableKey, field dax.FieldName) error {
	qtid := tkey.QualifiedTableID()

	mgr := api.serverlessStorage.GetFieldKeyManager(qtid, field)
	if mgr.IsLocked() {
		api.logger().Debugf("skipping loadFieldKeys (already held) %s %s", tkey, field)
		return nil
	}

	// load latest snapshot
	rc, err := mgr.LoadLatestSnapshot()
	if err != nil {
		return errors.Wrap(err, "loading field key snapshot")
	}
	if rc != nil {
		defer rc.Close()
		if err := api.TranslateFieldDB(ctx, string(tkey), string(field), rc); err != nil {
			return errors.Wrap(err, "restoring field keys")
		}
	}

	// define write log loading in a function since we have to do it
	// before and after locking
	loadWriteLog := func() error {
		writelog, err := mgr.LoadWriteLog()
		if err != nil {
			return errors.Wrap(err, "getting write log reader for field keys")
		}
		if writelog == nil {
			return nil
		}
		reader := storage.NewFieldKeyReader(qtid, field, writelog)
		defer reader.Close()
		// Get field in order to find the translate store.
		fld := api.holder.Field(string(tkey), string(field))
		if fld == nil {
			log.Printf("field not found in holder: %s", field)
			return nil
		}
		store := fld.TranslateStore()

		for msg, err := reader.Read(); err != io.EOF; msg, err = reader.Read() {
			if err != nil {
				return errors.Wrap(err, "reading from log reader")
			}
			for key, id := range msg.StringToID {
				if err := store.ForceSet(id, key); err != nil {
					return errors.Wrapf(err, "forcing set id, key: %d, %s", id, key)
				}
			}
		}
		return nil
	}
	// 1st write log load
	if err := loadWriteLog(); err != nil {
		return err
	}

	// acquire lock on this partition's keys
	if err := mgr.Lock(); err != nil {
		return errors.Wrap(err, "locking field key partition")
	}

	// reload writelog in case of changes between last load and
	// lock. The manager object takes care of only loading new data.
	return loadWriteLog()
}

func (api *API) pushJobsShards(ctx context.Context, jobs chan<- directiveJobType, fromD, toD *dax.Directive) {
	// Put shards into a map by table.
	shardMap := toD.ComputeShardsMap()

	// Get the diff between from/to directive shards.
	shardComp := newShardsComparer(fromD.ComputeShardsMap(), shardMap)

	// Loop over the shard map and load from WriteLogger.
	for tkey, shards := range shardComp.added() {
		for _, shard := range shards {
			jobs <- directiveJobShards{
				tkey:  tkey,
				shard: shard.Num,
			}
		}
	}
}

func (api *API) loadShard(ctx context.Context, tkey dax.TableKey, shard dax.ShardNum) error {
	qtid := tkey.QualifiedTableID()

	partition := dax.PartitionNum(disco.ShardToShardPartition(string(tkey), uint64(shard), disco.DefaultPartitionN))

	mgr := api.serverlessStorage.GetShardManager(qtid, partition, shard)
	if mgr.IsLocked() {
		api.logger().Debugf("skipping loadShard (already held) %s %d", tkey, shard)
		return nil
	}

	rc, err := mgr.LoadLatestSnapshot()
	if err != nil {
		return errors.Wrap(err, "reading latest snapshot for shard")
	}
	if rc != nil {
		defer rc.Close()
		if err := api.RestoreShard(ctx, string(tkey), uint64(shard), rc); err != nil {
			return errors.Wrap(err, "restoring shard data")
		}
	}

	// define write log loading in a func because we do it twice.
	loadWriteLog := func() error {
		writelog, err := mgr.LoadWriteLog()
		if err != nil {
			return errors.Wrap(err, "")
		}
		if writelog == nil {
			return nil
		}

		reader := storage.NewShardReader(qtid, partition, shard, writelog)
		defer reader.Close()
		for logMsg, err := reader.Read(); err != io.EOF; logMsg, err = reader.Read() {
			if err != nil {
				return errors.Wrap(err, "reading from log reader")
			}

			switch msg := logMsg.(type) {
			case *computer.ImportRoaringMessage:
				req := &ImportRoaringRequest{
					Clear:           msg.Clear,
					Action:          msg.Action,
					Block:           msg.Block,
					Views:           msg.Views,
					UpdateExistence: msg.UpdateExistence,
					SuppressLog:     true,
				}
				if err := api.ImportRoaring(ctx, msg.Table, msg.Field, msg.Shard, true, req); err != nil {
					return errors.Wrapf(err, "import roaring, table: %s, field: %s, shard: %d", msg.Table, msg.Field, msg.Shard)
				}

			case *computer.ImportMessage:
				req := &ImportRequest{
					Index:      msg.Table,
					Field:      msg.Field,
					Shard:      msg.Shard,
					RowIDs:     msg.RowIDs,
					ColumnIDs:  msg.ColumnIDs,
					RowKeys:    msg.RowKeys,
					ColumnKeys: msg.ColumnKeys,
					Timestamps: msg.Timestamps,
					Clear:      msg.Clear,
				}

				qcx := api.Txf().NewQcx()
				defer qcx.Abort()

				opts := []ImportOption{
					OptImportOptionsClear(msg.Clear),
					OptImportOptionsIgnoreKeyCheck(msg.IgnoreKeyCheck),
					OptImportOptionsPresorted(msg.Presorted),
					OptImportOptionsSuppressLog(true),
				}
				if err := api.Import(ctx, qcx, req, opts...); err != nil {
					return errors.Wrapf(err, "import, table: %s, field: %s, shard: %d", msg.Table, msg.Field, msg.Shard)
				}

			case *computer.ImportValueMessage:
				req := &ImportValueRequest{
					Index:           msg.Table,
					Field:           msg.Field,
					Shard:           msg.Shard,
					ColumnIDs:       msg.ColumnIDs,
					ColumnKeys:      msg.ColumnKeys,
					Values:          msg.Values,
					FloatValues:     msg.FloatValues,
					TimestampValues: msg.TimestampValues,
					StringValues:    msg.StringValues,
					Clear:           msg.Clear,
				}

				qcx := api.Txf().NewQcx()
				defer qcx.Abort()

				opts := []ImportOption{
					OptImportOptionsClear(msg.Clear),
					OptImportOptionsIgnoreKeyCheck(msg.IgnoreKeyCheck),
					OptImportOptionsPresorted(msg.Presorted),
					OptImportOptionsSuppressLog(true),
				}
				if err := api.ImportValue(ctx, qcx, req, opts...); err != nil {
					return errors.Wrapf(err, "import value, table: %s, field: %s, shard: %d", msg.Table, msg.Field, msg.Shard)
				}
			case *computer.ImportRoaringShardMessage:
				req := &ImportRoaringShardRequest{
					Remote:      true,
					Views:       make([]RoaringUpdate, len(msg.Views)),
					SuppressLog: true,
				}
				for i, view := range msg.Views {
					req.Views[i] = RoaringUpdate{
						Field:        view.Field,
						View:         view.View,
						Clear:        view.Clear,
						Set:          view.Set,
						ClearRecords: view.ClearRecords,
					}
				}
				if err := api.ImportRoaringShard(ctx, msg.Table, msg.Shard, req); err != nil {
					return errors.Wrapf(err, "import roaring shard table: %s, shard: %d", msg.Table, msg.Shard)
				}
			}
		}
		return nil
	}
	// 1st write log load
	if err := loadWriteLog(); err != nil {
		return err
	}

	// acquire lock on this partition's keys
	if err := mgr.Lock(); err != nil {
		return errors.Wrap(err, "locking field key partition")
	}

	// reload writelog in case of changes between last load and
	// lock. The manager object takes care of only loading new data.
	return loadWriteLog()
}

//////////////////////////////////////////////////////////////

// sliceComparer is used to compare the differences between two slices of comparables.
type sliceComparer[K comparable] struct {
	from []K
	to   []K
}

func newSliceComparer[K comparable](from []K, to []K) *sliceComparer[K] {
	return &sliceComparer[K]{
		from: from,
		to:   to,
	}
}

// added returns the items which are present in `to` but not in `from`.
func (s *sliceComparer[K]) added() []K {
	return thingsAdded(s.from, s.to)
}

// removed returns the items which are present in `from` but not in `to`.
func (s *sliceComparer[K]) removed() []K {
	return thingsAdded(s.to, s.from)
}

// same returns the items which are in both `to` and `from`.
func (s *sliceComparer[K]) same() []K {
	var same []K
	for _, fromThing := range s.from {
		for _, toThing := range s.to {
			if fromThing == toThing {
				same = append(same, fromThing)
				break
			}
		}
	}
	return same
}

// thingsAdded returns the comparable things which are present in `to` but not
// in `from`.
func thingsAdded[K comparable](from []K, to []K) []K {
	var added []K
	for i := range to {
		var found bool
		for j := range from {
			if from[j] == to[i] {
				found = true
				break
			}
		}
		if !found {
			added = append(added, to[i])
		}
	}
	return added
}

// partitionsComparer is used to compare the differences between two maps of
// table:[]partition.
type partitionsComparer struct {
	from map[dax.TableKey]dax.VersionedPartitions
	to   map[dax.TableKey]dax.VersionedPartitions
}

func newPartitionsComparer(from map[dax.TableKey]dax.VersionedPartitions, to map[dax.TableKey]dax.VersionedPartitions) *partitionsComparer {
	return &partitionsComparer{
		from: from,
		to:   to,
	}
}

// added returns the partitions which are present in `to` but not in `from`. The
// results remain in the format of a map of table:[]partition.
func (p *partitionsComparer) added() map[dax.TableKey]dax.VersionedPartitions {
	return partitionsAdded(p.from, p.to)
}

// removed returns the partitions which are present in `from` but not in `to`.
// The results remain in the format of a map of table:[]partition.
func (p *partitionsComparer) removed() map[dax.TableKey]dax.VersionedPartitions {
	return partitionsAdded(p.to, p.from)
}

// partitionsAdded returns the partitions which are present in `to` but not in `from`.
func partitionsAdded(from map[dax.TableKey]dax.VersionedPartitions, to map[dax.TableKey]dax.VersionedPartitions) map[dax.TableKey]dax.VersionedPartitions {
	if from == nil {
		return to
	}

	added := make(map[dax.TableKey]dax.VersionedPartitions)
	for tt, tps := range to {
		fps, found := from[tt]
		if !found {
			added[tt] = tps
			continue
		}

		addedPartitions := dax.VersionedPartitions{}
		for i := range tps {
			var found bool
			for j := range fps {
				if fps[j] == tps[i] {
					found = true
					break
				}
			}
			if !found {
				addedPartitions = append(addedPartitions, tps[i])
			}
		}

		if len(addedPartitions) > 0 {
			added[tt] = addedPartitions
		}
	}
	return added
}

// fieldsComparer is used to compare the differences between two maps of
// table:[]fieldVersion.
type fieldsComparer struct {
	from map[dax.TableKey]dax.VersionedFields
	to   map[dax.TableKey]dax.VersionedFields
}

func newFieldsComparer(from map[dax.TableKey]dax.VersionedFields, to map[dax.TableKey]dax.VersionedFields) *fieldsComparer {
	return &fieldsComparer{
		from: from,
		to:   to,
	}
}

// added returns the fields which are present in `to` but not in `from`. The
// results remain in the format of a map of table:[]field.
func (f *fieldsComparer) added() map[dax.TableKey]dax.VersionedFields {
	return fieldsAdded(f.from, f.to)
}

// removed returns the fields which are present in `from` but not in `to`.
// The results remain in the format of a map of table:[]field.
func (f *fieldsComparer) removed() map[dax.TableKey]dax.VersionedFields {
	return fieldsAdded(f.to, f.from)
}

// fieldsAdded returns the fields which are present in `to` but not in `from`.
func fieldsAdded(from map[dax.TableKey]dax.VersionedFields, to map[dax.TableKey]dax.VersionedFields) map[dax.TableKey]dax.VersionedFields {
	if from == nil {
		return to
	}

	added := make(map[dax.TableKey]dax.VersionedFields)
	for tt, tps := range to {
		fps, found := from[tt]
		if !found {
			added[tt] = tps
			continue
		}

		addedFieldVersions := dax.VersionedFields{}
		for i := range tps {
			var found bool
			for j := range fps {
				if fps[j] == tps[i] {
					found = true
					break
				}
			}
			if !found {
				addedFieldVersions = append(addedFieldVersions, tps[i])
			}
		}

		if len(addedFieldVersions) > 0 {
			added[tt] = addedFieldVersions
		}
	}
	return added
}

// shardsComparer is used to compare the differences between two maps of
// table:[]shardV.
type shardsComparer struct {
	from map[dax.TableKey]dax.VersionedShards
	to   map[dax.TableKey]dax.VersionedShards
}

func newShardsComparer(from map[dax.TableKey]dax.VersionedShards, to map[dax.TableKey]dax.VersionedShards) *shardsComparer {
	return &shardsComparer{
		from: from,
		to:   to,
	}
}

// added returns the shards which are present in `to` but not in `from`. The
// results remain in the format of a map of table:[]shard.
func (s *shardsComparer) added() map[dax.TableKey]dax.VersionedShards {
	return shardsAdded(s.from, s.to)
}

// removed returns the shards which are present in `from` but not in `to`. The
// results remain in the format of a map of table:[]shard.
func (s *shardsComparer) removed() map[dax.TableKey]dax.VersionedShards {
	return shardsAdded(s.to, s.from)
}

// shardsAdded returns the shards which are present in `to` but not in `from`.
func shardsAdded(from map[dax.TableKey]dax.VersionedShards, to map[dax.TableKey]dax.VersionedShards) map[dax.TableKey]dax.VersionedShards {
	if from == nil {
		return to
	}

	added := make(map[dax.TableKey]dax.VersionedShards)
	for tt, tss := range to {
		fss, found := from[tt]
		if !found {
			added[tt] = tss
			continue
		}

		addedShards := dax.VersionedShards{}
		for i := range tss {
			var found bool
			for j := range fss {
				if fss[j] == tss[i] {
					found = true
					break
				}
			}
			if !found {
				addedShards = append(addedShards, tss[i])
			}
		}

		if len(addedShards) > 0 {
			added[tt] = addedShards
		}
	}
	return added
}

// createTableAndFields creates the FeatureBase Tables and Fields provided in
// the dax.Directive format.
func (api *API) createTableAndFields(tbl *dax.QualifiedTable, partitions dax.VersionedPartitions) error {
	cim := &CreateIndexMessage{
		Index:     string(tbl.Key()),
		CreatedAt: 0,
		Meta: IndexOptions{
			Keys:           tbl.StringKeys(),
			TrackExistence: true,
		},
	}

	// Create the index in etcd as the system of record.
	if err := api.holder.persistIndex(context.Background(), cim); err != nil {
		return errors.Wrap(err, "persisting index")
	}

	idx, err := api.holder.createIndexWithPartitions(cim, partitions)
	if err != nil {
		return errors.Wrapf(err, "adding index: %s", tbl.Name)
	}

	// Add the fields
	for _, fld := range tbl.Fields {
		if fld.IsPrimaryKey() {
			continue
		}
		if err := createField(idx, fld); err != nil {
			return errors.Wrapf(err, "creating field: %s", fld.Name)
		}
	}

	return nil
}

// createField creates a FeatureBase Field in the provided FeatureBase Index
// based on the provided field's type.
func createField(idx *Index, fld *dax.Field) error {
	opts, err := FieldOptionsFromField(fld)
	if err != nil {
		return errors.Wrapf(err, "creating field options from field: %s", fld.Name)
	}

	if _, err := idx.CreateField(string(fld.Name), "", opts...); err != nil {
		return errors.Wrapf(err, "creating field on index: %s", fld.Name)
	}
	return nil
}
