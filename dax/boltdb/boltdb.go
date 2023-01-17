// Package boltdb contains the boltdb implementations of the DAX interfaces.
package boltdb

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/molecula/featurebase/v3/errors"
	bolt "go.etcd.io/bbolt"
)

const (
	ErrFmtBucketNotFound = "boltdb: bucket '%s' not found"
)

type Bucket []byte

// DB represents the database connection.
type DB struct {
	db     *bolt.DB
	ctx    context.Context // background context
	cancel func()          // cancel background context

	// Datasource name.
	DSN string

	// Destination for events to be published.
	// EventService wtf.EventService

	// Returns the current time. Defaults to time.Now().
	// Can be mocked for tests.
	Now func() time.Time

	filePath string

	// bucketQueue contains a list of buckets to create upon Open.
	bucketQueue []Bucket
}

// NewDB returns a new instance of DB associated with the given datasource name.
func NewDB(dsn string) *DB {
	db := &DB{
		DSN: dsn,
		Now: time.Now,

		//EventService: wtf.NopEventService(),
	}
	db.ctx, db.cancel = context.WithCancel(context.Background())
	return db
}

// NewSvcBolt gets, opens, and creates buckets for a boltDB for a
// particular named service (the data file will be named after the
// service).
func NewSvcBolt(dir, svc string, buckets ...Bucket) (*DB, error) {
	dir = strings.TrimPrefix(dir, "file:")
	filename := filepath.Join(dir, svc+".boltdb")
	db := NewDB("file:" + filename)
	db.RegisterBuckets(buckets...)
	err := db.Open()
	return db, errors.Wrap(err, "opening")
}

// path returns the file path to the boltdb database file.
func (db *DB) path() (string, error) {
	if !strings.HasPrefix(db.DSN, "file:") {
		return "", errors.New(errors.ErrUncoded, "boltdb package only supports a DSN beginning with `file:`")
	}

	return db.DSN[5:], nil
}

// RegisterBuckets queues up the buckets to be created when the database is
// first opened.
func (db *DB) RegisterBuckets(buckets ...Bucket) {
	db.bucketQueue = append(db.bucketQueue, buckets...)
}

// InitializeBuckets creates the given buckets if they do not already exist.
func (db *DB) InitializeBuckets(buckets ...Bucket) (err error) {
	return db.db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range buckets {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return errors.Wrapf(err, "creating bucket: %s", bucket)
			}
		}
		return nil
	})
}

// Open opens the database connection.
func (db *DB) Open() (err error) {
	path, err := db.path()
	if err != nil {
		return errors.Wrap(err, "getting path from DSN")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		return errors.Wrapf(err, "mkdir %s", filepath.Dir(path))
	} else if db.db, err = bolt.Open(path, 0666, &bolt.Options{Timeout: 1 * time.Second}); err != nil {
		return errors.Wrapf(err, "open file: %s", err)
	}

	// cache the path in db.filePath.
	db.filePath = path

	if err := db.InitializeBuckets(db.bucketQueue...); err != nil {
		return errors.Wrap(err, "initializing buckets")
	}

	// Reset the bucketQueue.
	db.bucketQueue = make([]Bucket, 0)

	return nil
}

// Close closes the database connection.
func (db *DB) Close() (err error) {
	return db.db.Close()
}

// BeginTx starts a transaction and returns a wrapper Tx type. This type
// provides a reference to the database and a fixed timestamp at the start of
// the transaction. The timestamp allows us to mock time during tests as well.
// The wrapper also contains the context.
func (db *DB) BeginTx(ctx context.Context, writable bool) (*Tx, error) {
	tx, err := db.db.Begin(writable)
	if err != nil {
		return nil, err
	}

	// Return wrapper Tx that includes the transaction start time.
	return &Tx{
		Tx:  tx,
		ctx: ctx,
		db:  db,
		now: db.Now().UTC().Truncate(time.Second),
	}, nil
}

// Tx wraps the SQL Tx object to provide a timestamp at the start of the transaction.
type Tx struct {
	*bolt.Tx
	ctx context.Context
	db  *DB
	now time.Time
}

func (tx *Tx) Context() context.Context {
	return tx.ctx
}

func (db *DB) Path() string {
	return db.filePath
}
