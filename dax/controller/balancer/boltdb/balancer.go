// Package boltdb contains the boltdb implementation of the Balancer interface.
package boltdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/boltdb"
	balancer "github.com/featurebasedb/featurebase/v3/dax/controller/balancer"
	"github.com/featurebasedb/featurebase/v3/dax/controller/schemar"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

var (
	bucketBalancer = boltdb.Bucket("balancer")
)

// BalancerBuckets defines the buckets used by this package. It can be
// called during setup to create the buckets ahead of time.
var BalancerBuckets []boltdb.Bucket = []boltdb.Bucket{
	bucketBalancer,
}

// NewBalancer returns a new instance of controller.Balancer.
func NewBalancer(db *boltdb.DB, schemar schemar.Schemar, logger logger.Logger) *balancer.Balancer {
	fjs := newFreeJobService(db)
	wjs := newWorkerJobService(db, logger)
	fws := newFreeWorkerService(db)
	ns := NewNodeService(db, logger)

	return balancer.New(ns, fjs, wjs, fws, schemar, logger)
}

// Ensure type implements interface.
var _ balancer.WorkerJobService = (*workerJobService)(nil)

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

func (w *workerJobService) WorkersJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) ([]dax.WorkerInfo, error) {
	workerInfos, err := w.getWorkerInfos(tx, roleType, qdbid)
	if err != nil {
		return nil, errors.Wrapf(err, "getting worker infos: %s", roleType)
	}

	return workerInfos, nil
}

func (w *workerJobService) WorkerCount(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (int, error) {
	workers, err := w.getWorkers(tx, roleType, qdbid)
	if err != nil {
		return 0, errors.Wrapf(err, "getting workers: %s", roleType)
	}

	return len(workers), nil
}

func (w *workerJobService) ListWorkers(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (dax.Addresses, error) {
	return w.getWorkers(tx, roleType, qdbid)
}

func (w *workerJobService) getWorkers(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (dax.Addresses, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	c := txx.Bucket(bucketBalancer).Cursor()

	// Deserialize rows into Worker objects.
	addrs := make(dax.Addresses, 0)

	prefix := []byte(fmt.Sprintf(prefixFmtWorkersDB, roleType, qdbid.Key()))
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if v == nil {
			w.logger.Printf("nil value for key: %s", k)
			continue
		}

		addr, err := keyWorker(k)
		if err != nil {
			return nil, errors.Wrapf(err, "getting worker from key: %s", k)
		}

		addrs = append(addrs, addr)
	}

	return addrs, nil
}

func (w *workerJobService) getWorkerInfos(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (dax.WorkerInfos, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	c := txx.Bucket(bucketBalancer).Cursor()

	// Deserialize rows into WorkerInfo objects.
	workerInfos := make(dax.WorkerInfos, 0)

	var prefix []byte
	empty := dax.QualifiedDatabaseID{}
	if roleType == "" && qdbid == empty {
		prefix = []byte("workers/role/")
	} else {
		prefix = []byte(fmt.Sprintf(prefixFmtWorkersDB, roleType, qdbid.Key()))
	}
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		addr, err := keyWorker(k)
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
			Address: addr,
			Jobs:    jobs.Sorted(),
		}

		workerInfos = append(workerInfos, workerInfo)
	}

	return workerInfos, nil
}

func (w *workerJobService) CreateWorker(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	// If this worker already exists, don't do anything.
	wrkr := bkt.Get(workerDBKey(roleType, qdbid, addr))
	if wrkr != nil {
		return nil
	}

	val := []byte("[]")
	if err := bkt.Put(workerDBKey(roleType, qdbid, addr), val); err != nil {
		return errors.Wrapf(err, "putting db worker: %s, %s", qdbid, addr)
	}

	if err := bkt.Put(workerAssignedKey(addr), []byte(qdbid.Key())); err != nil {
		return errors.Wrapf(err, "putting assigned worker: %s, %s", qdbid, addr)
	}

	return nil
}

func (w *workerJobService) DeleteWorker(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	if err := bkt.Delete(workerDBKey(roleType, qdbid, addr)); err != nil {
		return errors.Wrapf(err, "deleting node key: %s", workerDBKey(roleType, qdbid, addr))
	}

	if err := bkt.Delete(workerAssignedKey(addr)); err != nil {
		return errors.Wrapf(err, "deleting assigned worker: %s", workerAssignedKey(addr))
	}

	return nil
}

func (w *workerJobService) CreateJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address, jobs ...dax.Job) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	jobset := dax.NewSet[dax.Job]()
	var err error

	// get worker
	wrkr := bkt.Get(workerDBKey(roleType, qdbid, addr))
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

	if err := bkt.Put(workerDBKey(roleType, qdbid, addr), val); err != nil {
		return errors.Wrap(err, "putting worker")
	}

	return nil
}

func (w *workerJobService) DeleteJob(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address, job dax.Job) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	// get worker
	wrkr := bkt.Get(workerDBKey(roleType, qdbid, addr))
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

	if err := bkt.Put(workerDBKey(roleType, qdbid, addr), val); err != nil {
		return errors.Wrap(err, "putting worker")
	}

	return nil
}

func (w *workerJobService) DeleteJobsForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) (balancer.InternalDiffs, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	qdbid := qtid.QualifiedDatabaseID
	prefix := string(qtid.Key())

	workers, err := w.getWorkers(tx, roleType, qdbid)
	if err != nil {
		return nil, errors.Wrap(err, "getting workers")
	}

	idiffs := balancer.NewInternalDiffs()
	for _, worker := range workers {
		// get worker
		wrkr := bkt.Get(workerDBKey(roleType, qdbid, worker))
		if wrkr == nil {
			panic("didn't find worker that should... definitely exist")
		}
		jobset, err := decodeJobSet(wrkr)
		if err != nil {
			return nil, errors.Wrap(err, "decoding job set")
		}

		jobs := jobset.RemoveByPrefix(prefix)
		for _, job := range jobs {
			idiffs.Removed(worker, job)
		}
		val, err := encodeJobSet(jobset)
		if err != nil {
			return nil, errors.Wrap(err, "encoding job set")
		}

		if err := bkt.Put(workerDBKey(roleType, qdbid, worker), val); err != nil {
			return nil, errors.Wrap(err, "putting worker")
		}

	}

	return idiffs, nil
}

func (w *workerJobService) ListJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) (dax.Jobs, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	jobset := dax.NewSet[dax.Job]()
	var err error

	// get worker
	wrkr := bkt.Get(workerDBKey(roleType, qdbid, addr))
	if wrkr != nil {
		jobset, err = decodeJobSet(wrkr)
		if err != nil {
			return nil, errors.Wrap(err, "decoding job set")
		}
	}

	return jobset.Sorted(), nil
}

func (w *workerJobService) JobCounts(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addrs ...dax.Address) (map[dax.Address]int, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	m := make(map[dax.Address]int)

	for _, addr := range addrs {
		jobset := dax.NewSet[dax.Job]()
		var err error

		// get worker
		wrkr := bkt.Get(workerDBKey(roleType, qdbid, addr))
		if wrkr != nil {
			jobset, err = decodeJobSet(wrkr)
			if err != nil {
				return nil, errors.Wrap(err, "decoding job set")
			}
		}

		m[addr] = len(jobset)
	}

	return m, nil
}

func (w *workerJobService) DatabaseForWorker(tx dax.Transaction, addr dax.Address) dax.DatabaseKey {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return "" // TODO(tlt): return error here?
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return ""
	}

	wrkr := bkt.Get(workerAssignedKey(addr))

	return dax.DatabaseKey(wrkr)
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

// encodeWorkerSet encode the workerSet into a JSON array of strings.
func encodeWorkerSet(workerSet dax.Set[dax.Address]) ([]byte, error) {
	arr := workerSet.Sorted()
	b, err := json.Marshal(arr)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling json")
	}
	return b, nil
}

// decodeWorkerSet decode the string (a JSON array of strings) into workerSet.
func decodeWorkerSet(v []byte) (dax.Set[dax.Address], error) {
	var arr []string
	err := json.Unmarshal(v, &arr)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling json")
	}

	ws := dax.NewSet[dax.Address]()
	for _, s := range arr {
		ws.Add(dax.Address(s))
	}

	return ws, nil
}

// Ensure type implements interface.
var _ balancer.FreeJobService = (*freeJobService)(nil)

type freeJobService struct {
	db *boltdb.DB
}

func newFreeJobService(db *boltdb.DB) *freeJobService {
	return &freeJobService{
		db: db,
	}
}

func (f *freeJobService) CreateJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, jobs ...dax.Job) error {
	return f.MergeJobs(tx, roleType, qdbid, jobs)
}

func (f *freeJobService) DeleteJob(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, job dax.Job) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	// get free jobs
	fjs := bkt.Get(freeJobKey(roleType, qdbid))
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

	if err := bkt.Put(freeJobKey(roleType, qdbid), val); err != nil {
		return errors.Wrap(err, "putting free job")
	}

	return nil
}

func (f *freeJobService) DeleteJobsForTable(tx dax.Transaction, roleType dax.RoleType, qtid dax.QualifiedTableID) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	qdbid := qtid.QualifiedDatabaseID
	prefix := string(qtid.Key())

	// get free jobs
	fjs := bkt.Get(freeJobKey(roleType, qdbid))
	if fjs == nil {
		return nil
	}

	jobset, err := decodeJobSet(fjs)
	if err != nil {
		return errors.Wrap(err, "decoding job set")
	}

	jobset.RemoveByPrefix(prefix)
	val, err := encodeJobSet(jobset)
	if err != nil {
		return errors.Wrap(err, "encoding job set")
	}

	if err := bkt.Put(freeJobKey(roleType, qdbid), val); err != nil {
		return errors.Wrap(err, "putting free job")
	}

	return nil
}

func (f *freeJobService) ListJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) (dax.Jobs, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	jobset := dax.NewSet[dax.Job]()
	var err error

	// get free jobs
	fjs := bkt.Get(freeJobKey(roleType, qdbid))
	if fjs != nil {
		jobset, err = decodeJobSet(fjs)
		if err != nil {
			return nil, errors.Wrap(err, "decoding job set")
		}
	}

	return jobset.Sorted(), nil
}

func (f *freeJobService) MergeJobs(tx dax.Transaction, roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, jobs dax.Jobs) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	jobset := dax.NewSet[dax.Job]()
	var err error

	// get free jobs
	fjs := bkt.Get(freeJobKey(roleType, qdbid))
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

	if err := bkt.Put(freeJobKey(roleType, qdbid), val); err != nil {
		return errors.Wrap(err, "putting free job")
	}

	return nil
}

//////////////////////////////////////////////////////

// Ensure type implements interface.
var _ balancer.FreeWorkerService = (*freeWorkerService)(nil)

type freeWorkerService struct {
	db *boltdb.DB
}

func newFreeWorkerService(db *boltdb.DB) *freeWorkerService {
	return &freeWorkerService{
		db: db,
	}
}

func (f *freeWorkerService) AddWorkers(tx dax.Transaction, roleType dax.RoleType, addres ...dax.Address) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	workerset := dax.NewSet[dax.Address]()
	var err error

	// get free workers
	fws := bkt.Get(freeWorkerKey(roleType))
	if fws != nil {
		workerset, err = decodeWorkerSet(fws)
		if err != nil {
			return errors.Wrap(err, "decoding worker set")
		}
	}

	for _, w := range addres {
		workerset.Add(w)
	}
	val, err := encodeWorkerSet(workerset)
	if err != nil {
		return errors.Wrap(err, "encoding worker set")
	}

	if err := bkt.Put(freeWorkerKey(roleType), val); err != nil {
		return errors.Wrap(err, "putting free worker")
	}

	return nil
}

func (f *freeWorkerService) RemoveWorker(tx dax.Transaction, roleType dax.RoleType, addr dax.Address) error {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	workers, err := f.ListWorkers(tx, roleType)
	if err != nil {
		return errors.Wrap(err, "listing free workers")
	}

	// Create a workerset containing the free workers which remain after
	// removing num workers.
	workerset := dax.NewSet[dax.Address]()
	for _, w := range workers {
		workerset.Add(w)
	}

	if !workerset.Contains(addr) {
		return nil
	}

	// Remove the worker.
	workerset.Remove(addr)

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	val, err := encodeWorkerSet(workerset)
	if err != nil {
		return errors.Wrap(err, "encoding worker set")
	}

	if err := bkt.Put(freeWorkerKey(roleType), val); err != nil {
		return errors.Wrap(err, "putting free worker")
	}

	return nil
}

func (f *freeWorkerService) PopWorkers(tx dax.Transaction, roleType dax.RoleType, num int) ([]dax.Address, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	workers, err := f.ListWorkers(tx, roleType)
	if err != nil {
		return nil, errors.Wrap(err, "listing free workers")
	}

	if len(workers) < num {
		return nil, errors.Errorf("not enough free workers to pop: wanted %d, have: %d", num, len(workers))
	}

	// Get num workers from the list.
	workersToAssign := workers[0:num]

	// Create a workerset containing the free workers which remain after
	// removing num workers.
	workerset := dax.NewSet[dax.Address]()
	for _, worker := range workers[num:] {
		workerset.Add(worker)
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	val, err := encodeWorkerSet(workerset)
	if err != nil {
		return nil, errors.Wrap(err, "encoding worker set")
	}

	if err := bkt.Put(freeWorkerKey(roleType), val); err != nil {
		return nil, errors.Wrap(err, "putting free worker")
	}

	return workersToAssign, nil
}

func (f *freeWorkerService) ListWorkers(tx dax.Transaction, roleType dax.RoleType) (dax.Addresses, error) {
	txx, ok := tx.(*boltdb.Tx)
	if !ok {
		return nil, dax.NewErrInvalidTransaction("*boltdb.Tx")
	}

	bkt := txx.Bucket(bucketBalancer)
	if bkt == nil {
		return nil, errors.Errorf(boltdb.ErrFmtBucketNotFound, bucketBalancer)
	}

	workerset := dax.NewSet[dax.Address]()
	var err error

	// get free workers
	fws := bkt.Get(freeWorkerKey(roleType))
	if fws != nil {
		workerset, err = decodeWorkerSet(fws)
		if err != nil {
			return nil, errors.Wrap(err, "decoding worker set")
		}
	}

	return workerset.Sorted(), nil
}

//////////////////////////////////////////////////////

const (
	prefixFmtWorkersDB       = "workers/role/%s/db/%s/" // %s - role, dbKey
	prefixFmtWorkersAssigned = "workers/assigned/"

	prefixFmtFreeJobs    = "freejobs/role/%s/db/%s" // %s - role, dbKey
	prefixFmtFreeWorkers = "freeworkers/role/%s"    // %s - role
)

// workerDBKey returns a key based on worker.
//
// Format: workers/role/[role]/db/[dbKey]/[worker] = [job1, job2, ...]
func workerDBKey(roleType dax.RoleType, qdbid dax.QualifiedDatabaseID, addr dax.Address) []byte {
	key := fmt.Sprintf(prefixFmtWorkersDB+"%s", roleType, qdbid.Key(), addr)
	return []byte(key)
}

// workerAssignedKey returns a key based on worker.
//
// Format: workers/assigned/[worker] = dbKey
func workerAssignedKey(addr dax.Address) []byte {
	key := fmt.Sprintf(prefixFmtWorkersAssigned+"%s", addr)
	return []byte(key)
}

// keyWorker gets the worker out of the key.
func keyWorker(key []byte) (dax.Address, error) {
	parts := strings.SplitN(string(key), "/", 6)
	if len(parts) != 6 {
		return "", errors.New(errors.ErrUncoded, "worker key format expected: `workers/role/[role]/db/[db]/worker`")
	}

	return dax.Address(parts[5]), nil
}

// freeJobKey returns a key for all freeJobs.
//
// Format: freejobs/role/[role]/db/[dbKey] = [job1, job2, ...]
func freeJobKey(roleType dax.RoleType, qdbid dax.QualifiedDatabaseID) []byte {
	key := fmt.Sprintf(prefixFmtFreeJobs, roleType, qdbid.Key())
	return []byte(key)
}

// freeWorkerKey returns a key for all freeWorkers.
//
// Format: freeworkers/role/[role] = [worker1, worker2, ...]
func freeWorkerKey(roleType dax.RoleType) []byte {
	key := fmt.Sprintf(prefixFmtFreeWorkers, roleType)
	return []byte(key)
}
