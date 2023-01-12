// Package boltdb contains the boltdb implementation of the Balancer interface.
package boltdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	"github.com/featurebasedb/featurebase/v3/dax/mds/controller"
	"github.com/featurebasedb/featurebase/v3/dax/mds/controller/naive"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

var (
	bucketNaiveBalancer = boltdb.Bucket("naiveBalancer")
)

// NaiveBalancerBuckets defines the buckets used by this package. It can be
// called during setup to create the buckets ahead of time.
var NaiveBalancerBuckets []boltdb.Bucket = []boltdb.Bucket{
	bucketNaiveBalancer,
}

// NewBalancer returns a new instance of controller.Balancer.
func NewBalancer(name string, db *boltdb.DB, logger logger.Logger) controller.Balancer {
	fjs := newFreeJobService(db)
	wjs := newWorkerJobService(db, logger)

	return naive.New(name, fjs, wjs, logger)
}

// Ensure type implements interface.
var _ naive.WorkerJobService = (*workerJobService)(nil)

type workerJobService struct {
	db     *boltdb.DB
	logger logger.Logger
}

func newWorkerJobService(db *boltdb.DB, logger logger.Logger) *workerJobService {
	return &workerJobService{
		db:     db,
		logger: logger,
	}
}

func (w *workerJobService) WorkersJobs(ctx context.Context, balancerName string) ([]dax.WorkerInfo, error) {
	tx, err := w.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "getting tx")
	}
	defer tx.Rollback()

	workerInfos, err := getWorkerInfos(ctx, tx, balancerName)
	if err != nil {
		return nil, errors.Wrapf(err, "getting worker infos: %s", balancerName)
	}

	return workerInfos, nil
}

func (w *workerJobService) WorkerCount(ctx context.Context, balancerName string) (int, error) {
	tx, err := w.db.BeginTx(ctx, false)
	if err != nil {
		return 0, errors.Wrap(err, "getting tx")
	}
	defer tx.Rollback()

	workers, err := w.getWorkers(ctx, tx, balancerName)
	if err != nil {
		return 0, errors.Wrapf(err, "getting workers: %s", balancerName)
	}

	return len(workers), nil
}

func (w *workerJobService) ListWorkers(ctx context.Context, balancerName string) (dax.Workers, error) {
	tx, err := w.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	workers, err := w.getWorkers(ctx, tx, balancerName)
	if err != nil {
		return nil, errors.Wrapf(err, "getting workers: %s", balancerName)
	}

	return workers, nil
}

func (w *workerJobService) getWorkers(ctx context.Context, tx *boltdb.Tx, balancerName string) (dax.Workers, error) {
	c := tx.Bucket(bucketNaiveBalancer).Cursor()

	// Deserialize rows into Worker objects.
	workers := make(dax.Workers, 0)

	prefix := []byte(fmt.Sprintf(prefixFmtWorkers, balancerName))
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if v == nil {
			w.logger.Printf("nil value for key: %s", k)
			continue
		}

		worker, err := keyWorker(k)
		if err != nil {
			return nil, errors.Wrapf(err, "getting worker from key: %s", k)
		}

		workers = append(workers, worker)
	}

	return workers, nil
}

func getWorkerInfos(ctx context.Context, tx *boltdb.Tx, balancerName string) (dax.WorkerInfos, error) {
	c := tx.Bucket(bucketNaiveBalancer).Cursor()

	// Deserialize rows into WorkerInfo objects.
	workerInfos := make(dax.WorkerInfos, 0)

	prefix := []byte(fmt.Sprintf(prefixFmtWorkers, balancerName))
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		worker, err := keyWorker(k)
		if err != nil {
			return nil, errors.Wrapf(err, "getting worker from key: %s", k)
		}

		jobs := dax.NewSet[dax.Job]()
		if v != nil {
			jobs, err = decodeJobSet(v)
			if err != nil {
				return nil, errors.Wrap(err, "decoding job set")
			}
		}

		workerInfo := dax.WorkerInfo{
			ID:   worker,
			Jobs: jobs.Sorted(),
		}

		workerInfos = append(workerInfos, workerInfo)
	}

	return workerInfos, nil
}

func (w *workerJobService) WorkerExists(ctx context.Context, balancerName string, worker dax.Worker) (bool, error) {
	tx, err := w.db.BeginTx(ctx, false)
	if err != nil {
		return false, errors.Wrapf(err, "getting tx: %s", balancerName)
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return false, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	wrkr := bkt.Get(workerKey(balancerName, worker))

	return wrkr != nil, nil
}

func (w *workerJobService) CreateWorker(ctx context.Context, balancerName string, worker dax.Worker) error {
	tx, err := w.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "getting transaction")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	// If this worker already exists, don't do anything.
	wrkr := bkt.Get(workerKey(balancerName, worker))
	if wrkr != nil {
		return nil
	}

	val := []byte("[]")
	if err := bkt.Put(workerKey(balancerName, worker), val); err != nil {
		return errors.Wrap(err, "putting worker")
	}

	return tx.Commit()
}

func (w *workerJobService) DeleteWorker(ctx context.Context, balancerName string, worker dax.Worker) error {
	tx, err := w.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	if err := bkt.Delete(workerKey(balancerName, worker)); err != nil {
		return errors.Wrapf(err, "deleting node key: %s", workerKey(balancerName, worker))
	}

	return tx.Commit()
}

func (w *workerJobService) CreateJobs(ctx context.Context, balancerName string, worker dax.Worker, jobs ...dax.Job) error {
	tx, err := w.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	jobset := dax.NewSet[dax.Job]()

	// get worker
	wrkr := bkt.Get(workerKey(balancerName, worker))
	if wrkr != nil {
		jobset, err = decodeJobSet(wrkr)
		if err != nil {
			return errors.Wrap(err, "decoding job set")
		}
	}

	for _, job := range jobs {
		jobset.Add(job)
	}
	val, err := encodeJobSet(jobset)
	if err != nil {
		return errors.Wrap(err, "encoding job set")
	}

	if err := bkt.Put(workerKey(balancerName, worker), val); err != nil {
		return errors.Wrap(err, "putting worker")
	}

	return tx.Commit()
}

func (w *workerJobService) DeleteJob(ctx context.Context, balancerName string, worker dax.Worker, job dax.Job) error {
	tx, err := w.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	// get worker
	wrkr := bkt.Get(workerKey(balancerName, worker))
	if wrkr == nil {
		return nil
	}

	jobset, err := decodeJobSet(wrkr)
	if err != nil {
		return errors.Wrap(err, "decoding job set")
	}
	if !jobset.Contains(job) {
		return nil
	}

	jobset.Remove(job)
	val, err := encodeJobSet(jobset)
	if err != nil {
		return errors.Wrap(err, "encoding job set")
	}

	if err := bkt.Put(workerKey(balancerName, worker), val); err != nil {
		return errors.Wrap(err, "putting worker")
	}

	return tx.Commit()
}

func (w *workerJobService) DeleteJobs(ctx context.Context, balancerName, prefix string) (naive.InternalDiffs, error) {
	tx, err := w.db.BeginTx(ctx, true)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	workers, err := w.getWorkers(ctx, tx, balancerName)
	if err != nil {
		return nil, errors.Wrap(err, "getting workers")
	}

	idiffs := naive.NewInternalDiffs()
	for _, worker := range workers {
		// get worker
		wrkr := bkt.Get(workerKey(balancerName, worker))
		if wrkr == nil {
			panic("didn't find worker that should... definitely exist")
		}
		jobset, err := decodeJobSet(wrkr)
		if err != nil {
			return nil, errors.Wrap(err, "decoding job set")
		}

		jobs := jobset.RemovePrefix(prefix)
		for _, job := range jobs {
			idiffs.Removed(worker, job)
		}
		val, err := encodeJobSet(jobset)
		if err != nil {
			return nil, errors.Wrap(err, "encoding job set")
		}

		if err := bkt.Put(workerKey(balancerName, worker), val); err != nil {
			return nil, errors.Wrap(err, "putting worker")
		}

	}

	return idiffs, tx.Commit()
}

func (w *workerJobService) ListJobs(ctx context.Context, balancerName string, worker dax.Worker) (dax.Jobs, error) {
	tx, err := w.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	jobset := dax.NewSet[dax.Job]()

	// get worker
	wrkr := bkt.Get(workerKey(balancerName, worker))
	if wrkr != nil {
		jobset, err = decodeJobSet(wrkr)
		if err != nil {
			return nil, errors.Wrap(err, "decoding job set")
		}
	}

	return jobset.Sorted(), nil
}

func (w *workerJobService) JobCounts(ctx context.Context, balancerName string, workers ...dax.Worker) (map[dax.Worker]int, error) {
	tx, err := w.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrapf(err, "getting tx: %s", balancerName)
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	m := make(map[dax.Worker]int)

	for _, worker := range workers {
		jobset := dax.NewSet[dax.Job]()

		// get worker
		wrkr := bkt.Get(workerKey(balancerName, worker))
		if wrkr != nil {
			jobset, err = decodeJobSet(wrkr)
			if err != nil {
				return nil, errors.Wrap(err, "decoding job set")
			}
		}

		m[worker] = len(jobset)
	}

	return m, nil
}

// encodeJobSet encode the jobSet into a JSON array of strings.
func encodeJobSet(jobSet dax.Set[dax.Job]) ([]byte, error) {
	arr := jobSet.Sorted()
	b, err := json.Marshal(arr)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling json")
	}
	return b, nil
}

// decodeJobSet decode the string (a JSON array of strings) into jobSet.
func decodeJobSet(v []byte) (dax.Set[dax.Job], error) {
	var arr []string
	err := json.Unmarshal(v, &arr)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling json")
	}

	js := dax.NewSet[dax.Job]()
	for _, s := range arr {
		js.Add(dax.Job(s))
	}

	return js, nil
}

// Ensure type implements interface.
var _ naive.FreeJobService = (*freeJobService)(nil)

type freeJobService struct {
	db *boltdb.DB
}

func newFreeJobService(db *boltdb.DB) *freeJobService {
	return &freeJobService{
		db: db,
	}
}

func (f *freeJobService) CreateFreeJobs(ctx context.Context, balancerName string, jobs ...dax.Job) error {
	return f.MergeFreeJobs(ctx, balancerName, jobs)
}

func (f *freeJobService) DeleteFreeJob(ctx context.Context, balancerName string, job dax.Job) error {
	tx, err := f.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	// get free jobs
	fjs := bkt.Get(freeJobKey(balancerName))
	if fjs == nil {
		return nil
	}

	jobset, err := decodeJobSet(fjs)
	if err != nil {
		return errors.Wrap(err, "decoding job set")
	}
	if !jobset.Contains(job) {
		return nil
	}

	jobset.Remove(job)
	val, err := encodeJobSet(jobset)
	if err != nil {
		return errors.Wrap(err, "encoding job set")
	}

	if err := bkt.Put(freeJobKey(balancerName), val); err != nil {
		return errors.Wrap(err, "putting free job")
	}

	return tx.Commit()
}

func (f *freeJobService) DeleteFreeJobs(ctx context.Context, balancerName, prefix string) error {
	tx, err := f.db.BeginTx(ctx, true)
	if err != nil {
		return errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	// get free jobs
	fjs := bkt.Get(freeJobKey(balancerName))
	if fjs == nil {
		return nil
	}

	jobset, err := decodeJobSet(fjs)
	if err != nil {
		return errors.Wrap(err, "decoding job set")
	}

	jobset.RemovePrefix(prefix)
	val, err := encodeJobSet(jobset)
	if err != nil {
		return errors.Wrap(err, "encoding job set")
	}

	if err := bkt.Put(freeJobKey(balancerName), val); err != nil {
		return errors.Wrap(err, "putting free job")
	}

	return tx.Commit()
}

func (f *freeJobService) ListFreeJobs(ctx context.Context, balancerName string) (dax.Jobs, error) {
	tx, err := f.db.BeginTx(ctx, false)
	if err != nil {
		return nil, errors.Wrap(err, "beginning tx")
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	jobset := dax.NewSet[dax.Job]()

	// get free jobs
	fjs := bkt.Get(freeJobKey(balancerName))
	if fjs != nil {
		jobset, err = decodeJobSet(fjs)
		if err != nil {
			return nil, errors.Wrap(err, "decoding job set")
		}
	}

	return jobset.Sorted(), nil
}

func (f *freeJobService) MergeFreeJobs(ctx context.Context, balancerName string, jobs dax.Jobs) error {
	tx, err := f.db.BeginTx(ctx, true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bkt := tx.Bucket(bucketNaiveBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketNaiveBalancer)
	}

	jobset := dax.NewSet[dax.Job]()

	// get free jobs
	fjs := bkt.Get(freeJobKey(balancerName))
	if fjs != nil {
		jobset, err = decodeJobSet(fjs)
		if err != nil {
			return errors.Wrap(err, "decoding job set")
		}
	}

	for _, j := range jobs {
		jobset.Add(j)
	}
	val, err := encodeJobSet(jobset)
	if err != nil {
		return errors.Wrap(err, "encoding job set")
	}

	if err := bkt.Put(freeJobKey(balancerName), val); err != nil {
		return errors.Wrap(err, "putting free job")
	}

	return tx.Commit()
}

//////////////////////////////////////////////////////

const (
	prefixFmtWorkers  = "workers/%s/" // %s - balancerName
	prefixFmtFreeJobs = "freejobs/%s" // %s - balancerName
)

// workerKey returns a key based on worker.
func workerKey(bal string, worker dax.Worker) []byte {
	key := fmt.Sprintf(prefixFmtWorkers+"%s", bal, worker)
	return []byte(key)
}

// keyWorker gets the worker out of the key.
func keyWorker(key []byte) (dax.Worker, error) {
	parts := strings.SplitN(string(key), "/", 3)
	if len(parts) != 3 {
		return "", errors.New(errors.ErrUncoded, "worker key format expected: `workers/balancer/worker`")
	}

	return dax.Worker(parts[2]), nil
}

// freeJobKey returns a key for all freeJobs.
func freeJobKey(bal string) []byte {
	key := fmt.Sprintf(prefixFmtFreeJobs, bal)
	return []byte(key)
}
