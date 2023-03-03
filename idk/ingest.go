package idk

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pilosacore "github.com/featurebasedb/featurebase/v3"
	pilosagrpc "github.com/featurebasedb/featurebase/v3/api/client"
	pilosabatch "github.com/featurebasedb/featurebase/v3/batch"
	pilosaclient "github.com/featurebasedb/featurebase/v3/client"
	client_types "github.com/featurebasedb/featurebase/v3/client/types"
	"github.com/featurebasedb/featurebase/v3/dax"
	controllerclient "github.com/featurebasedb/featurebase/v3/dax/controller/client"
	"github.com/featurebasedb/featurebase/v3/idk/serverless"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/pql"
	proto "github.com/featurebasedb/featurebase/v3/proto"
	"github.com/felixge/fgprof"
	"github.com/go-avro/avro"
	"github.com/pkg/errors"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prom2json"
	"golang.org/x/sync/errgroup"
)

type contextKey int

const contextKeyToken contextKey = iota

const (
	Exists           = "-exists"
	ErrCommittingIDs = "committing IDs for batch"
)

var (
	ErrNoDirective = fmt.Errorf("no delete directive in this record")
)

// TODO Jaeger

// Main holds all config for general ingest
type Main struct {
	PilosaHosts              []string      `short:"p" help:"Alias for --featurebase-hosts. Will be deprecated in the next major release."`
	FeaturebaseHosts         []string      `short:"" help:"Comma separated list of host:port pairs for FeatureBase."`
	PilosaGRPCHosts          []string      `short:"" help:"Alias for --featurebase-grpc-hosts. Will be deprecated in the next major release."`
	FeaturebaseGRPCHosts     []string      `short:"" help:"Comma separated list of host:port pairs for FeatureBase's GRPC endpoint. Used by Kafka delete consumer."`
	BatchSize                int           `short:"b" help:"Number of records to read before indexing all of them at once. Generally, larger means better throughput and more memory usage. 1,048,576 might be a good number."`
	KeyTranslateBatchSize    int           `help:"Maximum number of keys to translate at a time."`
	BatchMaxStaleness        time.Duration `short:"" help:"Maximum length of time that the oldest record in a batch can exist before flushing the batch. Note that this can potentially stack with timeouts waiting for the source."`
	Index                    string        `short:"i" help:"Name of FeatureBase index."`
	LogPath                  string        `short:"l" help:"Log file to write to. Empty means stderr."`
	PrimaryKeyFields         []string      `short:"r" help:"Data field(s) which make up the primary key for a record. These will be concatenated and translated to a FeatureBase ID. If empty, record key translation will not be used."`
	IDField                  string        `short:"d" help:"Field which contains the integer column ID. May not be used in conjunction with primary-key-fields. If both are empty, auto-generated IDs will be used."`
	AutoGenerate             bool          `short:"a" help:"Automatically generate IDs."`
	ExternalGenerate         bool          `short:"" help:"Use FeatureBase's ID generation (must be set alongside auto-generate)."`
	IDAllocKeyPrefix         string        `short:"" help:"A prefix for ID allocator keys when using FeatureBase's ID generation (must be different for each concurrent ingester)."`
	Concurrency              int           `short:"c" help:"Number of concurrent sources and indexing routines to launch. Concurrency is not supported for molecula-consumer-sql. Concurrency for molecula-consumer-csv only works when providing multiple files and does not support '--auto-generate'"`
	CacheLength              uint64        `short:""  help:"Number of batches of ID mappings to cache."`
	PackBools                string        `short:"k" help:"If non-empty, boolean fields will be packed into two set fields—one with this name, and one with <name>-exists."`
	Verbose                  bool          `short:"v" help:"Enable verbose logging."`
	Delete                   bool          `help:"If true, delete records rather than write them." flag:"-"`
	Pprof                    string        `short:"o" help:"host:port on which to listen for pprof"`
	Stats                    string        `short:"s" help:"host:port on which to host metrics"`
	ExpSplitBatchMode        bool          `short:"x" help:"Tell featurebase client to build bitmaps locally over many batches and import them at the end. Experimental. Does not support int or mutex fields. Don't use this unless you know what you're doing."`
	AssumeEmptyPilosa        bool          `short:"u" help:"Alias for --assume-empty-featurebase. Will be deprecated in the next major release."`
	AssumeEmptyFeaturebase   bool          `short:"" help:"Setting this means that you're doing an initial bulk ingest which assumes that data does not need to be cleared/unset in FeatureBase. There are various performance enhancements that can be made in this case. For example, for booleans if a false value comes in, we'll just set the bit in the bools-exists field... we won't clear it in the bools field."`
	WriteCSV                 string        `short:"" help:"Write data we're ingesting to a CSV file with the given name."`
	Namespace                string        `flag:"-"`
	DeleteIndex              bool          `short:"" help:"Delete index specified by --index (if it already exists) before starting ingest - NOTE: this will delete any data already imported into this index, use with caution."`
	DryRun                   bool          `short:"" help:"Dry run - just flag parsing."`
	TrackProgress            bool          `help:"Periodically print status updates on how many records have been sourced." short:""`
	OffsetMode               bool          `short:"" help:"Set offset-mode based Autogenerated IDs, for use with a data-source that is offset-based (must be set alongside auto-generate and external-generate)."`
	LookupDBDSN              string        `flag:"lookup-db-dsn" help:"Connection string for connecting to Lookup database."`
	LookupBatchSize          int           `help:"Number of records to batch before writing them to Lookup database."`
	AuthToken                string        `flag:"auth-token" help:"Authentication Token for FeatureBase"`
	CommitTimeout            time.Duration `help:"Maximum time before canceling commit."`
	AllowIntOutOfRange       bool          `help:"Allow ingest to continue when it encounters out of range integers in IntFields. (default false)"`
	AllowDecimalOutOfRange   bool          `help:"Allow ingest to continue when it encounters out of range decimals in DecimalFields. (default false)"`
	AllowTimestampOutOfRange bool          `help:"Allow ingest to continue when it encounters out of range timestamps in TimestampFields. (default false)"`
	SkipBadRows              int           `help:"If you fail to process the first n rows without processing one successfully, fail."`

	UseShardTransactionalEndpoint bool `flag:"use-shard-transactional-endpoint" help:"Use alternate import endpoint that ingests data for all fields in a shard in a single atomic request. No negative performance impact and better consistency. Recommended."`

	ControllerAddress string             `short:"" help:"Controller address."`
	OrganizationID    dax.OrganizationID `short:"" help:"auto-assigned organization ID"`
	DatabaseID        dax.DatabaseID     `short:"" help:"auto-assigned database ID"`
	TableName         dax.TableName      `short:"" help:"human friendly table name"`

	csvWriter *csv.Writer
	csvFile   *os.File
	// TODO implement the auto-generated IDs... hopefully using Pilosa to manage it.
	TLS TLSConfig

	// NewSource must be set by the user of Main before calling
	// Main.Run. Main.Run will call this function "Concurrency" times. It
	// is the job of this function to ensure that the concurrent
	// sources which are started partition work appropriately. This is
	// typically set up (by convention) in the Source's package in
	// cmd.go
	NewSource func() (Source, error) `flag:"-"`

	lookupClient *PostgresClient

	client     *pilosaclient.Client
	index      *pilosaclient.Index
	grpcClient *pilosagrpc.GRPCClient

	NewImporterFn func() pilosacore.Importer `flag:"-"`

	SchemaManager SchemaManager       `flag:"-"`
	Qtbl          *dax.QualifiedTable `flag:"-"`

	newNexter func(c int) (IDAllocator, error)
	ra        RangeAllocator

	metricsServer *http.Server

	log logger.Logger

	progress *ProgressTracker

	// Metrics
	importDuration time.Duration

	// Future flags are used to represent features or functionality which is not
	// yet the default behavior, but will be in a future release.
	Future struct {
		// Rename, if true, will interact with a service called FeatureBase
		// instead of Pilosa.
		Rename bool `help:"Interact with FeatureBase instead of Pilosa."`
	}
	MaxMsgs uint64 `short:"m" help:"Number of messages to consume from Kafka before stopping. Useful for testing when you don't want to run indefinitely."`
}

// msgCounter is a thread safe counter which counts the number of
// messages fully ingested/committed from all concurrent sources. The
// ingest loop will exit with no error once MaxMsgs have been ingested
// across all sources sharing the counter.
type msgCounter struct {
	count   uint64
	MaxMsgs uint64
}

func (l *msgCounter) Increment(n uint64) uint64 {
	return atomic.AddUint64(&l.count, n)
}

func (l *msgCounter) IsDone() bool {
	val := atomic.LoadUint64(&l.count)
	return val >= l.MaxMsgs
}

func (l *msgCounter) Value() uint64 {
	return atomic.LoadUint64(&l.count)
}

type ConfluentCommand struct {
	KafkaSecurityProtocol string   `help:"Protocol used to communicate with brokers (security.protocol)"`
	KafkaBootstrapServers []string `help:"Comma separated list of host:port pairs for Kafka."`

	KafkaSslCaLocation                      string `help:"File or directory path to CA certificate(s) for verifying the broker's key(ssl.ca.location)"`
	KafkaSslCertificateLocation             string `help:"Path to client's public key (PEM) used for authentication(ssl.certificate.location)"`
	KafkaSslKeyLocation                     string `help:"Path to client's private key (PEM) used for authentication(ssl.key.location)."`
	KafkaSslKeyPassword                     string `help:"Private key passphrase (for use with ssl.key.location and set_ssl_cert()(ssl.key.password))"`
	KafkaSslEndpointIdentificationAlgorithm string `help:"The endpoint identification algorithm used by clients to validate server host name (ssl.endpoint.identification.algorithm) "`
	KafkaEnableSslCertificateVerification   bool   `help:"(enable.ssl.certificate.verification)"`
	KafkaSocketTimeoutMs                    int    `help:"(socket.timeout.ms)"`
	KafkaSocketKeepaliveEnable              string `help:"The (socket.keepalive.enable) kafka consumer configuration"`

	KafkaClientId        string `help:"(client.id)"`
	KafkaDebug           string `help:"The (debug) kafka consumer configuration. A comma-separated list of debug contexts to enable. Detailed Consumer: consumer,cgrp,topic,fetch. Set to 'all' for most verbose option."`
	KafkaMaxPollInterval string `help:"The (max.poll.interval.ms) kafka consumer configuration. The max time the consumer can go without polling the broker. Consumer exits after this timeout."`
	KafkaSessionTimeout  string `help:"The (session.timeout.ms) kafka consumer configuration. The max time the consumer can go without sending a heartbeat to the broker"`
	KafkaGroupInstanceId string `help:"The (group.instance.id) kafka consumer configuration."`

	KafkaSaslUsername  string `help:"SASL authentication username (sasl.username)"`
	KafkaSaslPassword  string `help:"SASL authentication password (sasl.password)"`
	KafkaSaslMechanism string `help:"SASL mechanism to use for authentication.(sasl.mechanism)"`

	SchemaRegistryUsername string `help:"authenticaion key provided by confluent for schema registry."`
	SchemaRegistryPassword string `help:"authenticaion secret provided by confluent for schema registry."`
	SchemaRegistryURL      string `short:"g" help:"Location of Confluent Schema Registry. Must start with 'https://' if you want to use TLS."`

	KafkaConfiguration string `help:"json configuration for confluents ConfigMap will be applied first EXPERIMENTAL"`
}

func init() {
	http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
}

func (m *Main) PilosaClient() *pilosaclient.Client { return m.client }
func (m *Main) Log() logger.Logger                 { return m.log }
func (m *Main) SetLog(log logger.Logger)           { m.log = log }

func NewMain() *Main {
	fmt.Fprintf(os.Stderr, "Molecula Consumer %s, build time %s\n", Version, BuildTime)

	return &Main{
		PilosaHosts:      []string{"localhost:10101"},
		PilosaGRPCHosts:  []string{"localhost:20101"},
		BatchSize:        1, // definitely increase this to achieve any amount of performance
		Concurrency:      1,
		CacheLength:      64,
		PackBools:        "bools",
		Namespace:        "ingester", // this is now ignored and hardcoded in metrics.go
		IDAllocKeyPrefix: "ingest",

		UseShardTransactionalEndpoint: os.Getenv("IDK_DEFAULT_SHARD_TRANSACTIONAL") != "",

		Pprof: "localhost:6062",
		Stats: "localhost:9093",

		SchemaManager: NopSchemaManager,

		log: logger.NewStandardLogger(os.Stderr),
	}
}

func (m *Main) Rename() {
	if m.Future.Rename {
		m.PilosaHosts = m.FeaturebaseHosts
		m.PilosaGRPCHosts = m.FeaturebaseGRPCHosts
		m.AssumeEmptyPilosa = m.AssumeEmptyFeaturebase
	}
}

func (m *Main) Run() (err error) {
	onFinishRun, err := m.Setup()
	if err != nil {
		return errors.Wrap(err, "setting up")
	}
	defer onFinishRun()
	err = m.run()
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

func (m *Main) run() error {
	m.log.Debugf("Ingest Config: %+v", m)

	eg := errgroup.Group{}

	if m.MaxMsgs == 0 {
		m.MaxMsgs = math.MaxUint64
	}
	l := &msgCounter{MaxMsgs: m.MaxMsgs}

	if m.Delete {
		err := m.runDeleter(l)
		if err != nil {
			return err
		}
	} else {
		for c := 0; c < m.Concurrency; c++ {
			c := c
			eg.Go(func() error {
				err := m.runIngester(c, l)
				if err != nil && err != io.EOF {
					return err
				}
				return nil
			})
		}
	}
	return errors.Wrap(eg.Wait(), "idk.Main.Run")
}

func (m *Main) clone() (*Main, error) {
	var index *pilosaclient.Index

	schema, err := m.SchemaManager.Schema()
	if err != nil {
		return nil, err
	}

	index = schema.Index(m.Index)

	// use a copy (schema race condition issues)
	mClone := *m
	mClone.index = index

	return &mClone, nil
}

func (m *Main) runIngester(c int, l *msgCounter) error {
	m.log.Printf("start ingester %d", c)
	// TODO: actually implement cancellation and graceful shutdown
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKeyToken, m.AuthToken))
	defer cancel()

	// get source
	source, err := m.NewSource()
	if err != nil {
		return errors.Wrap(err, "getting source")
	}
	defer func() {
		err = source.Close()
		if err != nil {
			if m.log != nil {
				m.log.Errorf("Closing source: %v", err)
			}
		}
	}()

	// get nexter
	nexter, err := m.newNexter(c)
	if err != nil {
		return errors.Wrap(err, "getting nexter")
	}

	mc, err := m.clone()
	if err != nil {
		return errors.Wrap(err, "cloning *Main before ingest")
	}

	err = mc.ingest(ctx, source, nexter, c, l)
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

func (m *Main) ingest(ctx context.Context, source Source, nexter IDAllocator, sourceInstance int, limitCounter *msgCounter) error {
	defer func() {
		m.log.Printf("metrics: import=%s\n", time.Duration(atomic.LoadInt64((*int64)(&m.importDuration))))
	}()
	var batch pilosabatch.RecordBatch
	var recordizers []Recordizer
	var prevRec Record
	var row *pilosabatch.Row
	var errorCounter int // keeps track of consecuitive errors across records
	var anyRecordSuccessful bool
	if m.progress != nil {
		source = m.progress.Track(source)
	}
	if limitCounter.IsDone() {
		return io.EOF
	}
initialFetch:
	rec, err := source.Record()
	switch err {
	case nil:
		// Fetch the schema for the first time.
		err = ErrSchemaChange
	case ErrFlush:
		// There is nothing to flush yet.
		// This needs a record before the main loop can start.
		goto initialFetch
	}
	var csvSlice []string
	batchStart := true

	var lookupBatcher LookupBatcher = &NopLookupBatcher{}
	var lookupWriteIdxs []int
	var lookupRow []interface{}
	if m.lookupClient != nil {
		lookupFieldNames := selectLookupFieldNames(source)
		if err := m.lookupClient.Setup(lookupFieldNames); err != nil {
			return errors.Wrap(err, "initializing lookup database")
		}
		lookupBatcher = NewPostgresUpsertBatcher(
			m.lookupClient,
			m.LookupBatchSize,
		)
		lookupRow = make([]interface{}, len(lookupFieldNames)+1) // re-use. +1 for id column
	}
	recordCounter := 0
	next := func() {
		if limitCounter.IsDone() {
			return
		}
		rec, err = source.Record()
	}
	for ; !limitCounter.IsDone(); next() {
		recordCounter++
		if err == ErrFlush {
			if batch != nil && batch.Len() > 0 {
				batchLen := batch.Len()
				if err := m.importBatch(batch); err != nil {
					return errors.Wrap(err, "importing batch after timeout")
				}

				if err := m.commitRecord(ctx, prevRec, limitCounter, uint64(batchLen)); err != nil {
					return errors.Wrap(err, "committing after timeout")
				}

				if nexter != nil {
					err := nexter.Commit(ctx)
					if err != nil {
						return errors.Wrap(err, ErrCommittingIDs)
					}
				}
				m.log.Printf("imported batch after timeout")
			}
			batchStart = true

			if lookupBatcher.Len() > 0 {
				if err := lookupBatcher.Import(); err != nil {
					return errors.Wrap(err, "importing lookup batch after timeout")
				}
			}
			continue
		}
		if err != nil {
			// finish previous batch if this is not the first
			if batch != nil && batch.Len() > 0 {
				batchLen := batch.Len()
				ierr := m.importBatch(batch)
				if ierr != nil {
					return errors.Wrapf(ierr, "importing after error getting record: %v", err)
				} else if !errors.Is(err, io.EOF) {
					if v, ok := source.(Metadata); ok {
						m.log.Printf("imported batch of previous records (%v) after err: %v", v.SchemaMetadata(), err)
					} else {
						m.log.Printf("imported batch of previous records after err: %v", err)
					}
				}
				ierr = m.commitRecord(ctx, prevRec, limitCounter, uint64(batchLen))
				if ierr != nil {
					return errors.Wrapf(ierr, "committing after error getting record: %v", err)
				}
				if nexter != nil {
					ierr := nexter.Commit(ctx)
					if ierr != nil {
						return errors.Wrap(ierr, ErrCommittingIDs)
					}
				}
				m.log.Printf("1 records processed %v-> (%v)", sourceInstance, recordCounter)
			}
			batchStart = true
			if lookupBatcher.Len() > 0 {
				if err := lookupBatcher.Import(); err != nil {
					return errors.Wrap(err, "importing lookup batch after error")
				}
			}

			if err == ErrSchemaChange {
				schema := source.Schema()
				if v, ok := source.(Metadata); ok {
					m.log.Printf("new schema - subject: %#v; version: %d; schema: %#v",
						v.SchemaSubject(), v.SchemaVersion(), v.SchemaSchema())
				} else {
					m.log.Printf("new schema: %#v", schema)
				}
				recordizers, batch, row, lookupWriteIdxs, err = m.batchFromSchema(schema)
				if err != nil {
					return errors.Wrap(err, "batchFromSchema")
				}
				CounterIngesterSchemaChanges.Inc()
				csvSlice = make([]string, len(schema))
				if m.csvWriter != nil {
					for i := range schema {
						csvSlice[i] = schema[i].DestName()
					}
					err := m.csvWriter.Write(csvSlice)
					if err != nil {
						return errors.Wrap(err, "writing header to CSV file")
					}
				}

				// check lookup fields
				// TODO if we want to support a changing schema while doing
				// lookup imports, need to adjust the lookupBatcher's schema here.
				if len(lookupWriteIdxs) > 0 && m.LookupDBDSN == "" {
					return errors.New("must set --lookup-db-dsn when using lookup field types")
				}
			} else {
				break
			}
		}
		data := rec.Data()

		if m.csvWriter != nil {
			for i, item := range data {
				var cerr error
				csvSlice[i], cerr = toString(item)
				if cerr != nil {
					return errors.Wrap(cerr, "toString")
				}
			}
			err := m.csvWriter.Write(csvSlice)
			if err != nil {
				return errors.Wrap(err, "writing to CSV file")
			}
		}

		rowHasError := false
		for _, rdz := range recordizers {
			err = rdz(data, row)
			if err != nil {
				err = errors.Wrap(err, "recordizing")
				// excludes 'out of range' errors so that ingest can continue importing the rest
				// of the records
				if !m.allowError(err) {
					rowHasError = true
					// must return error and exit idk when SkipBadRows is not defined (or set to 0)
					if m.SkipBadRows == 0 {
						return err
					} else {
						// will handle this error based on errorCounter in the later if block
						m.Log().Errorf("Bad record: +%v, reason: %v\n", row, err)
						break
					}
				}
				m.Log().Errorf(err.Error())
			}
		}

		if !anyRecordSuccessful && rowHasError {
			// We cannot allow a certain number of consecutive errors in the beginning of ingest.
			errorCounter++
			if errorCounter > m.SkipBadRows {
				return errors.Wrapf(err, "consecutive bad records exceeded limit, errorCounter: %d\n", errorCounter) // wraps the current recordizer error
			}
			err = nil // already logged the recordizer error, no need to propagate the err
		} else {
			anyRecordSuccessful = true // after this is set true, we will skip any bad rows in the future
			err = nil
		}

		if nexter != nil { // add ID if no id field specified
			if batchStart {
				rerr := nexter.Reserve(ctx, uint64(m.BatchSize))
				if rerr != nil {
					return errors.Wrap(rerr, "reserving IDs for batch")
				}
				batchStart = false
			}
			id, err := nexter.Next(ctx, rec)
			if err != nil {
				var esync ErrIDOffsetDesync
				if errors.As(err, &esync) {
					// if we get an esync error, it means that the current
					// source's offset is behind and some other client has
					// already processed the same message and used the ID,
					// Therefore, ideally this client should fast-forward its
					// offset to catch up.
					// For the time being though, we'll skip adding this record
					// to the batch and simply log the error.
					// This is under the assumption that the other client is already/will be
					// fully responsible for ingesting messages for that group. Also,
					// implementing offset fast-forward will require extending a source's
					// API which we should keep as minimal as possible
					m.log.Printf(esync.Error())
					continue
				}
				return errors.Wrap(err, "getting next ID")
			}
			row.ID = id
		}

		// skip bad rows only
		if !rowHasError {
			err = batch.Add(*row)
			CounterIngesterRowsAdded.Inc()
		}

		if err == pilosabatch.ErrBatchNowFull || err == pilosabatch.ErrBatchNowStale {
			batchLen := batch.Len()
			err = m.importBatch(batch)
			if err != nil {
				return errors.Wrap(err, "importing batch")
			}
			err = m.commitRecord(ctx, rec, limitCounter, uint64(batchLen))
			if err != nil {
				return errors.Wrap(err, "committing record")
			}
			m.log.Printf("records processed %v-> (%v)", sourceInstance, recordCounter)
			if nexter != nil {
				err = nexter.Commit(ctx)
				if err != nil {
					return errors.Wrap(err, ErrCommittingIDs)
				}
			}
			batchStart = true
		} else if err != nil {
			return errors.Wrap(err, "adding to batch")
		}
		prevRec = rec

		// handle lookup write
		// NOTE this assumes the ID is an integer, and the data is a string
		if len(lookupWriteIdxs) > 0 {
			skipLookupRow := false
			switch rowIdent := row.ID.(type) {
			case uint64:
				lookupRow[0] = rowIdent
				// rowIdent is the ID sent to pilosa, also generated by pilosa when ExternalGenerate is enabled.
			default:
				return errors.New("lookup writes only support uint64 ids")
			}

			for targetIdx, sourceIdx := range lookupWriteIdxs {
				// NOTE when lookupWrites schema can change, need to resize lookupRow
				if data[sourceIdx] == nil {
					if len(lookupRow) > 2 {
						// Schema contains 2+ lookup fields, and we may
						// have received a record with only SOME of them.
						// PostgreUpsertBatcher.Import can't handle heterogeneous
						// rows, so this is an error. When we need to do this,
						// batch import logic must be updated, for example:
						// - Add should accept a map columnName:value
						// - Import should execute several batch insert queries,
						//   each generated from a homogeneous slice of rows.
						return errors.Errorf("lookup field missing (id=%v, data=%v)", lookupRow[0], data)
					}
					// When we have only one lookup field, instead of worrying about
					// hetergeneous rows, just skip when missing.
					skipLookupRow = true
					lookupRow[targetIdx+1] = nil
				} else {
					lookupRow[targetIdx+1] = data[sourceIdx].(string)
				}
			}

			if !skipLookupRow {
				if err := lookupBatcher.AddFullRow(lookupRow); err != nil {
					return errors.Wrap(err, "adding to lookup batch")
				}
			}

			// Zero the row. I'm not sure if this is necessary, but it might
			// convert any surprise data integrity issues into NPEs
			// we can't miss.
			for n := range lookupRow {
				lookupRow[n] = nil
			}

		}
	}

	defer func() {
		if m.lookupClient != nil {
			if cerr := m.lookupClient.Close(); cerr != nil {
				panic(cerr)
			}
		}
	}()

	// if the limitCounter is done, we don't care if there was an
	// error getting a record... we've ingested enough. Specifically
	// added this because we were getting an ErrFlush that we didn't
	// care about in a test.
	if limitCounter.IsDone() {
		return nil
	}
	if !errors.Is(err, io.EOF) {
		return errors.Wrap(err, "getting record")
	}
	if batch == nil || err == io.EOF {
		return io.EOF // this means that there were 0 records
	}
	return errors.Wrap(batch.Flush(), "final fragment import")

}

func (m *Main) Setup() (onFinishRun func(), err error) {
	if err := m.validate(); err != nil {
		return nil, errors.Wrap(err, "validating configuration")
	}

	if m.DeleteIndex {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("Delete index '%s' and all data already imported into it (enter '%[1]s' to delete)? ", m.Index)
		text, _ := reader.ReadString('\n')
		if strings.TrimSpace(text) == strings.TrimSpace(m.Index) {
			err := m.SchemaManager.DeleteIndex(m.index)
			if err != nil {
				return nil, errors.Wrap(err, "deleting index")
			}
		} else {
			return nil, errors.New("index delete not confirmed")
		}
	}

	// setup logging
	var f *logger.FileWriter
	var logOut io.Writer = os.Stderr
	if m.LogPath != "" {
		f, err = logger.NewFileWriter(m.LogPath)
		if err != nil {
			return nil, errors.Wrap(err, "opening log file")
		}
		logOut = f
	}
	if m.Verbose {
		m.log = logger.NewVerboseLogger(logOut)
	} else {
		m.log = logger.NewStandardLogger(logOut)
	}

	if m.LogPath != "" {
		sighup := make(chan os.Signal, 1)
		signal.Notify(sighup, syscall.SIGHUP)
		go func() {
			for {
				// duplicate stderr onto log file
				err := dup(int(f.Fd()), int(os.Stderr.Fd()))
				if err != nil {
					m.log.Printf("syscall dup: %s\n", err.Error())
				}

				// reopen log file on SIGHUP
				<-sighup
				err = f.Reopen()
				if err != nil {
					m.log.Printf("reopen: %s\n", err.Error())
				}
			}
		}()
	}

	// start profiling endpoint
	//
	// TODO do this the same way we do stats, potentially sharing a
	// listener and mux, and shutting down properly when Run exits.
	if m.Pprof != "" {
		go func() {
			runtime.SetBlockProfileRate(10000000) // 1 sample per 10 ms
			runtime.SetMutexProfileFraction(100)  // 1% sampling
			m.log.Printf("Listening for /debug/pprof/ and /debug/fgprof on '%s'", m.Pprof)
			m.log.Printf("%v", http.ListenAndServe(m.Pprof, nil))
		}()
	}

	if err := m.setupStats(); err != nil {
		m.log.Printf("setting up stats: %v", err)
	}

	// set up Pilosa client
	tlsConfig, err := m.setupClient()
	if err != nil {
		return nil, errors.Wrap(err, "setting up client")
	}

	if m.AuthToken != "" {
		m.AuthToken = "Bearer " + m.AuthToken     // Gets added to context
		m.SchemaManager.SetAuthToken(m.AuthToken) // Gets used for calls made from the client
	}

	schema, err := m.SchemaManager.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}

	// validate ID generation specification
	if schema.HasIndex(m.Index) {
		if schema.Index(m.Index).Opts().Keys() && m.IDField != "" {
			return nil, errors.New("ID field not allowed for existing index with keys enabled")
		} else if schema.Index(m.Index).Opts().Keys() && m.AutoGenerate {
			return nil, errors.New("Autogenerated IDs not allowed for existing index with keys enabled")
		} else if !schema.Index(m.Index).Opts().Keys() && len(m.PrimaryKeyFields) != 0 {
			return nil, errors.New("PrimaryKeyFields not allowed for existing index with keys disabled")
		}
	}

	keyTranslation := len(m.PrimaryKeyFields) > 0
	m.index = schema.Index(m.Index, pilosaclient.OptIndexKeys(keyTranslation))
	if err := m.SchemaManager.SyncIndex(m.index); err != nil {
		return nil, errors.Wrap(err, "syncing index")
	}

	if m.AutoGenerate {
		shardWidth := m.index.ShardWidth()
		if shardWidth == 0 {
			shardWidth = pilosacore.ShardWidth
		}
		m.ra = NewLocalRangeAllocator(shardWidth)
	}

	if m.WriteCSV != "" {
		m.csvFile, err = os.Create(m.WriteCSV)
		if err != nil {
			return nil, errors.Wrap(err, "trying to create write-csv file")
		}
		m.csvWriter = csv.NewWriter(m.csvFile)
	}

	if m.Delete {
		grpcClient, err := pilosagrpc.NewGRPCClient(m.PilosaGRPCHosts, tlsConfig, m.log)
		if err != nil {
			return nil, errors.Wrap(err, "creating featurebase client")
		}
		m.grpcClient = grpcClient
		if m.Concurrency > 1 {
			return nil, errors.New("delete consumers does not support concurrency > 1")
		}
	}

	if m.LookupDBDSN != "" {
		pg, err := m.NewLookupClient()
		if err != nil {
			return nil, errors.Wrap(err, "creating lookup client")
		}
		m.lookupClient = pg
	}

	if m.TrackProgress {
		m.progress = &ProgressTracker{}
	}

	// Set up progress tracking.
	if m.progress != nil {
		startTime := time.Now()
		var wg sync.WaitGroup
		defer wg.Wait()
		doneCh := make(chan struct{})
		defer func() { close(doneCh) }()
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Set up a timer to check progress every 10 seconds.
			tick := time.NewTicker(10 * time.Second)
			defer tick.Stop()

			prev := uint64(0)
			stalled := true
			for {
				progress := m.progress.Check()
				switch {
				case progress != prev:
					// Forward progress continues.
					m.log.Printf("sourced %d records (%.2f records/minute)", progress, float64(progress)/time.Since(startTime).Minutes())
					stalled = false
				case stalled:
					// We already told the user that it is stalled.
				default:
					// This is the start of a stall.
					// No records have been sourced in the past 5 seconds.
					m.log.Printf("record sourcing stalled")
					stalled = true
				}
				prev = progress

				select {
				case <-tick.C:
				case <-doneCh:
					// Generate a final status update.
					m.log.Printf("sourced %d records in %s", m.progress.Check(), time.Since(startTime))
					return
				}
			}
		}()
	}

	// sort of like NewSource but for getting an IDAllocator
	// to use
	m.newNexter = func(c int) (IDAllocator, error) {
		var nexter IDAllocator
		if m.AutoGenerate {
			if m.ExternalGenerate {
				// Basically we need pilosa client to. . . not do what it usually does.
				// So just find the URI of the primary and use a regular HTTP client.
				httpClient := http.DefaultClient
				if m.TLS.CertificatePath != "" {
					httpClient = GetHTTPClient(tlsConfig)
				}

				coord, err := m.findPrimary()
				if err != nil {
					return nil, errors.Wrap(err, "finding primary node to setup idAllocator")
				}
				idManager := &pilosaIDManager{
					hcli: httpClient,
					url:  *coord,
				}

				// check if Offset mode
				if m.OffsetMode {
					nexter = newOffsetModeNexter(idManager, m.Index, uint64(m.BatchSize))

				} else { // Normal mode, default

					// under normal mode, we have to provide the key that idk
					// will use for reserve/commit
					key := fmt.Sprintf("%s-%d", m.IDAllocKeyPrefix, c)
					nexter = newNormalModeNexter(
						idManager,
						m.Index,
						key,
					)
				}
			} else {
				var err error
				nexter, err = NewRangeNexter(m.ra)
				if err != nil {
					return nil, errors.Wrap(err, "getting range nexter")
				}
			}
		}
		return nexter, nil
	}
	onFinishRun = func() {
		if m.csvWriter != nil {
			m.csvWriter.Flush()
			err := m.csvWriter.Error()
			if err != nil {
				m.log.Printf("flushing CSV: %v", err)
			}
		}
		if m.csvFile != nil {
			err := m.csvFile.Close()
			if err != nil {
				m.log.Printf("closing CSV file: %v", err)
			}
		}
		if m.metricsServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := m.metricsServer.Shutdown(ctx)
			if err != nil {
				m.log.Printf("shutting down metrics server: %v", err)
			}
		}
	}

	return onFinishRun, nil
}

func GetHTTPClient(t *tls.Config) *http.Client {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: time.Second * 20,
		}).Dial,
	}
	if t != nil {
		transport.TLSClientConfig = t
	}
	return &http.Client{Transport: transport}
}

// commitRecord commits rec. Sets the commit timeout on the context, if enabled.
func (m *Main) commitRecord(ctx context.Context, rec Record, limitCounter *msgCounter, numRecords uint64) error {
	if m.CommitTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, m.CommitTimeout)
		defer cancel()
	}
	err := rec.Commit(ctx)
	if err != nil {
		return errors.Wrap(err, "committing")
	}
	limitCounter.Increment(numRecords)
	CounterCommittedRecords.Add(float64(numRecords))
	return nil
}

// NewLookupClient returns an instance of a lookupClient. This represents
// a somewhat generic interface to a separate data store; currently the
// only implementation uses Postgres. This is also used for testing.
func (m *Main) NewLookupClient() (*PostgresClient, error) {
	return NewPostgresClient(m.LookupDBDSN, m.Index, m.log)
}

func (m *Main) setupClient() (*tls.Config, error) {
	var tlsConfig *tls.Config
	var err error
	var opts = []pilosaclient.ClientOption{}
	if m.TLS.CertificatePath != "" {
		tlsConfig, err = GetTLSConfig(&m.TLS, m.Log())
		if err != nil {
			return nil, errors.Wrap(err, "getting TLS config")
		}
		opts = append(opts, pilosaclient.OptClientTLSConfig(tlsConfig))
	} else {
		opts = append(opts,
			pilosaclient.OptClientRetries(2),
			pilosaclient.OptClientTotalPoolSize(1000),
			pilosaclient.OptClientPoolSizePerRoute(400),
		)
	}

	if m.useController() {
		// We should only have one "pilosa host" here. Get the path from that
		// and use it as the client path prefix.
		var prefix string
		for i := range m.PilosaHosts {
			addr := dax.Address(m.PilosaHosts[i])
			prefix = addr.Path()
			m.PilosaHosts = []string{addr.HostPort()}
			break
		}
		opts = append(opts, pilosaclient.OptClientPathPrefix(prefix))
	}

	m.client, err = pilosaclient.NewClient(m.PilosaHosts, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "getting featurebase client")
	}

	// Set up the SchemaManager
	if m.useController() {
		ctx := context.Background()

		// Controller doesn't auto-create a table based on IDK ingest; the table
		// must already exist.
		controllerClient := controllerclient.New(dax.Address(m.ControllerAddress), m.log)
		qdbid := dax.NewQualifiedDatabaseID(m.OrganizationID, m.DatabaseID)
		qtid, err := controllerClient.TableID(ctx, qdbid, m.TableName)
		if err != nil {
			return nil, errors.Wrapf(err, "getting table id: qual: %s, table name: %s", qdbid, m.TableName)
		}
		qtbl, err := controllerClient.Table(ctx, qtid)
		if err != nil {
			return nil, errors.Wrapf(err, "getting table id: qtid: %s", qtid)
		}

		m.Qtbl = qtbl
		m.Index = string(qtbl.Key())
		m.SchemaManager = serverless.NewSchemaManager(dax.Address(m.ControllerAddress), qdbid, m.log)

		m.NewImporterFn = func() pilosacore.Importer {
			return serverless.NewImporter(controllerClient, qtbl.Qualifier(), &qtbl.Table)
		}
	} else {
		m.SchemaManager = m.client
	}

	return tlsConfig, nil
}

func (m *Main) setupStats() error {
	mux := http.NewServeMux()
	promHandler := promhttp.InstrumentMetricHandler(prom.DefaultRegisterer, promhttp.HandlerFor(prom.DefaultGatherer, promhttp.HandlerOpts{}))
	mux.Handle("/metrics", promHandler)
	mux.Handle("/metrics.json", metricsJSONHandler{metricsURI: "http://" + m.Stats + "/metrics"})
	m.metricsServer = &http.Server{Addr: m.Stats, Handler: mux}
	ln, err := net.Listen("tcp", m.Stats)
	if err != nil {
		return errors.Wrapf(err, "listen for metrics on '%s'", m.Stats)
	}

	go func() {
		m.log.Printf("Serving Prometheus metrics with namespace \"%s\" at %v/metrics\n", m.Namespace, m.Stats)
		err = m.metricsServer.Serve(ln)
		if err != http.ErrServerClosed {
			m.log.Printf("serve metrics on '%s': %v", m.Stats, err)
		}
	}()
	return nil
}

type metricsJSONHandler struct {
	metricsURI string
}

func (h metricsJSONHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	transport := http.DefaultTransport.(*http.Transport).Clone()

	mfChan := make(chan *dto.MetricFamily, 60)
	errChan := make(chan error)
	go func() {
		err := prom2json.FetchMetricFamilies(h.metricsURI, mfChan, transport)
		errChan <- err
	}()

	metrics := []*prom2json.Family{}
	for mf := range mfChan {
		metrics = append(metrics, prom2json.NewFamily(mf))
	}
	err := <-errChan
	if err != nil {
		http.Error(w, "ERROR: fetching metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(w).Encode(metrics)
	if err != nil {
		http.Error(w, "ERROR: json write error: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

// runDeleter pulls records from the source associated with a consumer
// and deletes data from featurebase based on the format of that record
// it is currently mainly used with and tested on the kafka / avro consumer
// built on IDK
func (m *Main) runDeleter(limitCounter *msgCounter) error {
	m, err := m.clone()
	if err != nil {
		return errors.Wrap(err, "cloning *Main before delete")
	}
	m.log.Printf("starting the delete consumer...")
	source, err := m.NewSource()
	if err != nil {
		return errors.Wrap(err, "getting source")
	}
	defer func() {
		err = source.Close()
		if err != nil {
			if m.log != nil {
				m.log.Printf("error on close %v", err)
			}
		}

	}()

	if m.progress != nil {
		source = m.progress.Track(source)
	}

	var recordizers []Recordizer
	var row *pilosabatch.Row
	rec, err := source.Record()
	if err == nil {
		err = ErrSchemaChange // always need to fetch the schema the first time
	}

	client := m.PilosaClient()
	index := m.index

	// Pull records one by one from kafka
	for ; !limitCounter.IsDone(); rec, err = source.Record() {
		if err == ErrFlush {
			continue
		} else if err != nil && err != ErrSchemaChange {
			return errors.Wrap(err, "getting record")
		}

		bq := index.BatchQuery()
		recSchema := rec.Schema()
		avroRecord := false
		deleteType := ""
		switch recSchema.(type) {
		case avro.Schema:
			// record was encoded using avro
			// in runDeleter, that avro.RecordSchema should have a delete property
			// if it doesn't, it defaults to "fields" which runs older logic
			// (i.e. this new code won't break old integrations)
			deleteProp, ok := recSchema.(avro.Schema).Prop("delete")
			if !ok || deleteProp == nil {
				deleteType = "fields"
			} else {
				deleteType, ok = deleteProp.(string)
				if !ok {
					return errors.Errorf("delete property of avro delete record should be a string")
				}
			}
			avroRecord = true
		}

		if !avroRecord || deleteType == "fields" {
			// records without avro schemas or records with avro schemas
			// with deletes property of "fields" uses historical logic
			m.log.Debugf("deleter record: %v\n", rec)
			if err != nil {
				if err == ErrSchemaChange {
					schema := source.Schema()
					recordizers, _, row, _, err = m.batchFromSchema(schema)
					if err != nil {
						return errors.Wrap(err, "batchFromSchema")
					}
				} else {
					// shouldn't be able to get here
					break
				}
			}
			data := rec.Data()
			m.log.Debugf("deleter data: %+v %+v %+v", data, data[0], data[1])
			for _, rdz := range recordizers {
				err = rdz(data, row)
				if err != nil {
					return errors.Wrap(err, "recordizing")
				}
			}

			var columnIDs []uint64
			var columnKeys []string
			switch rowIdent := row.ID.(type) {
			case int64:
				columnIDs = []uint64{uint64(rowIdent)}
			case uint64:
				columnIDs = []uint64{rowIdent}
			case string:
				columnKeys = []string{rowIdent}
			case []byte:
				columnKeys = []string{string(rowIdent)}
			default:
				return errors.Errorf("recordizing primary key, got type: %T", row.ID)
			}

			// TODO: sentinel value in field list to delete entire record
			directives, ok := row.Values[len(row.Values)-1].([]string)
			if !ok {
				if row.Values[len(row.Values)-1] == nil {
					continue
				}
				return errors.Errorf("directives should be a string slice but got: %+v of %[1]T", row.Values[len(row.Values)-1])
			}
			if len(directives) == 0 {
				continue
			}

			// TODO: `directives` is not necessarily equivalent to a list of fieldNames
			rr, err := inspect(m.grpcClient, m.Index, columnIDs, columnKeys, directives)
			if err != nil {
				return errors.Wrap(err, "retrieving values for delete")
			}

			trns, err := m.SchemaManager.StartTransaction("", time.Minute, false, time.Hour)
			if err != nil {
				return errors.Wrap(err, "starting transaction")
			}
			for _, directive := range directives {
				var recordID interface{} = row.ID
				if idbytes, ok := recordID.([]byte); ok {
					recordID = string(idbytes)
				}
				var fieldName string
				if nameval := strings.SplitN(directive, "|", 2); len(nameval) == 2 {
					// special handling for packed bools — may generalize this in future
					fieldName = nameval[0]
					value := nameval[1]
					if m.PackBools == "" || fieldName != m.PackBools {
						return errors.Errorf("unsupported directive '%s' field name must be equal to packed bools field: '%s'", directive, m.PackBools)
					}
					boolsField := index.Field(m.PackBools)
					boolsExists := index.Field(m.PackBools + Exists)
					_, err := client.Query(index.BatchQuery(
						boolsField.Clear(value, recordID),
						boolsExists.Clear(value, recordID),
					))
					if err != nil {
						return errors.Wrap(err, "clearing bools")
					}
					CounterDeleterRowsAdded.With(prom.Labels{"type": "packed-bool"}).Inc()
					continue
				} else {
					fieldName = directive
				}

				// get field, refreshing schema if needed
				field, ok := index.Fields()[fieldName]
				if !ok {
					schema, err := m.SchemaManager.Schema()
					if err != nil {
						return errors.Wrap(err, "unknown field, getting new schema")
					}
					index = schema.Index(m.Index)
					field, ok = index.Fields()[fieldName]
					if !ok {
						return errors.Errorf("field '%s' not found", fieldName)
					}
				}

				val, err := rr.Val(fieldName)
				if err != nil {
					return errors.Wrap(err, "getting value from inspect")
				}

				switch field.Options().Type() {
				case pilosaclient.FieldTypeDefault, pilosaclient.FieldTypeSet:
					if field.Options().Keys() {
						valStrs, ok := val.([]string)
						if !ok {
							return errors.Errorf("unexpected value type for set field with keys, not []string but %T", val)
						}
						for _, valS := range valStrs {
							bq.Add(field.Clear(valS, recordID))
						}
					} else {
						valIDs, ok := val.([]uint64)
						if !ok {
							return errors.Errorf("unexpected value type for set field, not []uint64 but %T", val)
						}
						for _, valID := range valIDs {
							bq.Add(field.Clear(valID, recordID))
						}
					}
					_, err := client.Query(bq)
					if err != nil {
						return errors.Wrap(err, "clearing set")
					}
					CounterDeleterRowsAdded.With(prom.Labels{"type": "set"}).Inc()
				case pilosaclient.FieldTypeMutex:
					if val == "" {
						continue
					}
					_, err := client.Query(index.BatchQuery(
						field.Clear(val, recordID),
					))
					if err != nil {
						return errors.Wrap(err, "clearing mutex")
					}
					CounterDeleterRowsAdded.With(prom.Labels{"type": "mutex"}).Inc()
				case pilosaclient.FieldTypeBool:
					_, err := client.Query(index.BatchQuery(
						field.Clear(0, recordID),
						field.Clear(1, recordID),
					))
					if err != nil {
						return errors.Wrap(err, "clearing bool")
					}
					CounterDeleterRowsAdded.With(prom.Labels{"type": "bool"}).Inc()
				case pilosaclient.FieldTypeInt:
					_, err := client.Query(field.Clear(0, recordID))
					if err != nil {
						return errors.Wrap(err, "clearing int")
					}
					CounterDeleterRowsAdded.With(prom.Labels{"type": "int"}).Inc()
				case pilosaclient.FieldTypeDecimal:
					_, err := client.Query(field.Clear(0, recordID))
					if err != nil {
						return errors.Wrap(err, "clearing decimal")
					}
					CounterDeleterRowsAdded.With(prom.Labels{"type": "decimal"}).Inc()
				case pilosaclient.FieldTypeTime:
					return errors.Errorf("deletion on time fields unimplemented")
				default:
					return errors.Errorf("unhandled field type %s", field.Options().Type())
				}
			}
			if len(directives) == 0 {
				continue
			}
			_, err = m.SchemaManager.FinishTransaction(trns.ID)
			if err != nil {
				return errors.Wrap(err, "finishing transaction")
			}
		} else {
			// here we have an record encoded by avro and it's delete type is
			// "values", "records", or some unacceptable input
			_, ok := recSchema.(*avro.RecordSchema)
			if !ok {
				return errors.Errorf("got data of type %T but wanted avro.RecordSchema", recSchema)
			}

			// map values to fields or _id
			avroFields := recSchema.(*avro.RecordSchema).Fields
			var recordID interface{}
			fieldValues := make(map[string]interface{})
			for i, value := range rec.Data() {
				name := avroFields[i].Name
				if name == "_id" {
					recordID = value
				} else {
					fieldValues[name] = value
				}
			}
			switch deleteType {
			case "values":
				// find featurebase field based on avro / record field name
				indexFields := index.Fields()
				for key, value := range fieldValues {
					field, ok := indexFields[key]
					if !ok {
						return errors.Errorf("unable to find field %s in index %s", key, index.Name())
					}
					if value == nil {
						// don't delete anything for this field if value is null
						continue
					}
					switch fType := field.Options().Type(); fType {
					case pilosaclient.FieldTypeSet, pilosaclient.FieldTypeMutex:
						if key == m.PackBools {
							// value should be list of bools to clear if avro field name
							// is equal the name of the packed bools field
							if arrayValue, err := toStringArray(value); err == nil {
								boolsField := index.Field(m.PackBools)
								boolsExists := index.Field(m.PackBools + Exists)
								for _, v := range arrayValue {
									m.log.Debugf("clearing %s and %s for %s bool field", m.PackBools, m.PackBools+Exists, v)
									bq.Add(boolsField.Clear(v, recordID))
									bq.Add(boolsExists.Clear(v, recordID))
								}
							} else {
								return errors.Errorf("packed bools field %s should be a list of boolean values to delete", field.Name())
							}
						} else {
							// not packed bools, check for string vs ID field keys
							switch keys := field.Options().Keys(); keys {
							case true:
								if singleValue, err := toString(value); err == nil {
									bq.Add(field.Clear(singleValue, recordID))
								} else if arrayValue, err := toString(value); err == nil {
									for _, v := range arrayValue {
										bq.Add(field.Clear(string(v), recordID))
									}
								} else {
									return errors.Errorf("value of keyed %s field %s should be a string or array of strings but was %T", fType, field.Name(), value)
								}
							case false:
								if singleValue, err := toUint64(value); err == nil {
									bq.Add(field.Clear(singleValue, recordID))
								} else if arrayValue, err := toUint64Array(value); err == nil {
									for _, v := range arrayValue {
										bq.Add(field.Clear(v, recordID))
									}
								} else {
									return errors.Errorf("value of non keyed %s field %s should be an int or array of ints but was %T", fType, field.Name(), value)
								}
							default:
								return errors.Errorf("set field %s should have keys true or false", field.Name())
							}
						}
					case pilosaclient.FieldTypeInt, pilosaclient.FieldTypeDecimal, pilosaclient.FieldTypeTimestamp:
						if boolVal, ok := value.(bool); ok {
							if boolVal {
								bq.Add(field.Clear(0, recordID))
							}
						} else {
							return errors.Errorf("%s fields should have a boolean value set to rue if value is to be deleted, false otherwise", fType)
						}
					case pilosaclient.FieldTypeBool:
						if boolVal, ok := value.(bool); ok {
							if boolVal {
								bq.Add(field.Clear(0, recordID))
								bq.Add(field.Clear(1, recordID))
							}
						} else {
							return errors.Errorf("%s fields should have a boolean value set to rue if value is to be deleted, false otherwise", fType)
						}
					default:
						// pilosa.FieldTypeTime is the only other field type at time of coding
						return errors.Errorf("unable to handle values from fields with type: %s", fType)
					}
				}
				resp, err := client.Query(bq, nil)
				if err != nil || resp.Success != true {
					return errors.Wrap(err, "error deleting values")
				}
			case "records":
				// deleting a record
				var rawQueries []string
				if fieldValues["keys"] != nil {
					// if keys set, delete list of record keys
					keysAsStrings, err := toStringArray(fieldValues["keys"])
					if err != nil {
						return errors.Errorf("unable to convert 'keys' value to an array of strings")
					}
					columnKeys := "'" + strings.Join(keysAsStrings, "','") + "'"
					rawQueries = append(rawQueries, fmt.Sprintf("Delete(ConstRow(columns=[%s]))", columnKeys))

				}
				if fieldValues["ids"] != nil {
					// if ids set, delete list of record IDs
					idsAsInts, err := toUint64Array(fieldValues["ids"])
					if err != nil {
						return errors.Errorf("unable to convert 'ids' value to an array of int64s")
					}
					keysAsStrings := make([]string, len(idsAsInts))
					for i, v := range idsAsInts {
						keysAsStrings[i] = fmt.Sprint(v)
					}
					columnKeys := strings.Join(keysAsStrings, ",")
					rawQueries = append(rawQueries, fmt.Sprintf("Delete(ConstRow(columns=[%s]))", columnKeys))
				}
				if fieldValues["filter"] != nil {
					// if filter set, use it as filter in delete query
					filter, ok := fieldValues["filter"].(string)
					if !ok {
						return errors.Errorf("unable to convert fitler into string")
					}
					rawQueries = append(rawQueries, fmt.Sprintf("Delete(%s)", filter))
				}
				if len(rawQueries) == 0 {
					m.log.Infof("delete record doesn't contain any delete queries: confirm 'keys', 'ids', or 'filter' key has a value")
				}
				for _, query := range rawQueries {
					baseQuery := index.RawQuery(query)
					bq.Add(baseQuery)
				}

				_, err := client.Query(bq, nil)
				if err != nil {
					return errors.Errorf("running delete query: %s", err)
				}
			default:
				return errors.Errorf("unable to process delete where record is avro encoded & the delete property is not empty, 'records', 'fields', or 'values'")
			}
		}
		err = m.commitRecord(context.Background(), rec, limitCounter, 1)
		if err != nil {
			return errors.Wrap(err, "committing record")
		}
		if limitCounter.IsDone() {
			return nil
		}

	}

	if !errors.Is(err, io.EOF) {
		return errors.Wrap(err, "deleter getting record")
	}
	return nil
}

type inspectRecord proto.RowResponse

func (rr *inspectRecord) Val(field string) (interface{}, error) {
	for i, columnInfo := range rr.Headers {
		cr := rr.Columns[i]
		if columnInfo.Name == field {
			switch typ := columnInfo.GetDatatype(); typ {
			case "[]uint64":
				return cr.GetUint64ArrayVal().GetVals(), nil
			case "[]string":
				return cr.GetStringArrayVal().GetVals(), nil
			case "bool":
				return cr.GetBoolVal(), nil
			case "float64":
				return cr.GetFloat64Val(), nil
			case "decimal":
				return cr.GetDecimalVal(), nil
			case "int64":
				return cr.GetInt64Val(), nil
			case "uint64":
				return cr.GetUint64Val(), nil
			case "string":
				return cr.GetStringVal(), nil
			default:
				return nil, errors.Errorf("unhandled column info data type: '%s'", typ)
			}
		}
	}
	return nil, errors.Errorf("field '%s' not found in inspect record", field)
}

func inspect(grpcClient *pilosagrpc.GRPCClient, index string, columnIDs []uint64, columnKeys []string, fieldFilters []string) (*inspectRecord, error) {
	stream, err := grpcClient.Inspect(context.Background(), index, columnIDs, columnKeys, "", fieldFilters, 1, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "calling inspect")
	}
	row, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil, err
	}
	if err != nil {
		return nil, errors.Wrapf(err, "receiving row")
	}
	return (*inspectRecord)(row), nil
}

func (m *Main) findPrimary() (*url.URL, error) {
	status, err := m.SchemaManager.Status()
	if err != nil {
		return nil, errors.Wrap(err, "looking up cluster status")
	}
	for _, n := range status.Nodes {
		if !n.IsPrimary {
			continue
		}

		uri := n.URI.URI()
		url, err := url.Parse(uri.Normalize())
		if err != nil {
			return nil, errors.Wrap(err, "parsing normalized primary node address")
		}
		return url, nil
	}

	return nil, errors.New("primary node not found")
}

// importBatch executes batch.Import() and saves its timing.
func (m *Main) importBatch(batch pilosabatch.RecordBatch) error {
	t := time.Now()
	err := batch.Import()
	elapsed := time.Since(t)
	atomic.AddInt64((*int64)(&m.importDuration), int64(elapsed))
	return err
}

type Recordizer func(rawRec []interface{}, rec *pilosabatch.Row) error

func (m *Main) batchFromSchema(schema []Field) ([]Recordizer, pilosabatch.RecordBatch, *pilosabatch.Row, []int, error) {
	// Before attempting to do anything, check for duplicates in the schema.
	{
		dedup := make(map[string]struct{})
		for _, f := range schema {
			name := f.DestName()
			if name == "" {
				continue
			}
			if _, dup := dedup[name]; dup {
				return nil, nil, nil, nil, errors.Errorf("found duplicate field %q", name)
			}
			dedup[name] = struct{}{}
		}
	}

	// From the schema, and the configuration stored on Main, we need
	// to create a []pilosacore.Field and a []Recordizer processing
	// functions which take a []interface{} which conforms to the
	// schema, and converts it to a record which conforms to the
	// []pilosacore.Field.
	//
	// The relevant config options on Main are:
	// 1. PrimaryKeyFields, IDField, AutoGenerate
	// 2. PackBools
	// 3. BatchSize (gets passed directly to the batch)
	//
	// For PrimaryKeyFields and IDField there is some complexity.
	// There are 3 top level options: one, the other, or neither
	// (auto-generated IDs).
	//
	// 1. PrimarKeyFields - the main question here is whether—in
	// addition to combining these and translating them to column
	// ID—we should index them separately. I think the answer by
	// default should be yes.
	//
	// 2. IDField — this is pretty easy. Use the integer value as the
	// column ID. Do not index it separately by default.
	//
	// 3. Autogenerate IDs. Ideally using a RangeAllocator per
	// concurrent goroutine. OK, let's assume that if we set row.ID to
	// nil, the auto generation can happen inside the Batch.
	recordizers := make([]Recordizer, 0)

	// lookupWriteIdxs contains a slice of integers representing the
	// slice indexes, in the record, that should be imported into the
	// lookup data store.
	lookupWriteIdxs := make([]int, 0)

	var rz Recordizer
	skips := make(map[int]struct{})
	var err error

	// primary key stuff
	if len(m.PrimaryKeyFields) != 0 {
		rz, skips, err = getPrimaryKeyRecordizer(schema, m.PrimaryKeyFields)
		if err != nil {
			return nil, nil, nil, nil, errors.Wrap(err, "getting primary key recordizer")
		}
	} else if m.IDField != "" {
		for fieldIndex, field := range schema {
			if field.DestName() == m.IDField {
				if _, ok := field.(IDField); !ok {
					if _, ok := field.(IntField); !ok {
						return nil, nil, nil, nil, errors.Errorf("specified column id field %s is not an IDField or an IntField: %T", m.IDField, field)
					}
				}
				fieldIndex := fieldIndex
				rz = func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
					id, err := field.PilosafyVal(rawRec[fieldIndex])
					if err != nil {
						return errors.Wrapf(err, "converting %+v to ID", rawRec[fieldIndex])
					}
					if uid, ok := id.(uint64); ok {
						rec.ID = uid
					} else if iid, ok := id.(int64); ok {
						if iid < 0 {
							return errors.Errorf("can't use negative value %v as ID", iid)
						}
						rec.ID = uint64(iid)
					} else {
						return errors.Errorf("can't convert %v of %[1]T to uint64 for use as ID", id)
					}
					return nil
				}
				skips[fieldIndex] = struct{}{}
				break
			}
		}
		if rz == nil {
			return nil, nil, nil, nil, errors.Errorf("ID field %s not found", m.IDField)
		}
	} else if m.AutoGenerate {
		m.log.Debugf("getting no recordizer because we're autogenerating IDs")
	}
	if rz != nil {
		recordizers = append(recordizers, rz)
	}

	// set up bool fields
	var boolField, boolFieldExists *pilosaclient.Field

	// We only need to set up the packed bool fields if the schema
	// contains at least one bool field.
	if m.PackBools != "" && Fields(schema).ContainsBool() {
		// See if boolField and boolFieldExists already exist in Pilosa.
		// If they do, ensure that their options are compatible with what we
		// expect of a packed bool field.
		if m.index.HasField(m.PackBools) {
			pBoolField := m.index.Field(m.PackBools)
			if err := m.checkFieldCompatibility(pBoolField, nil, m.PackBools); err != nil {
				return nil, nil, nil, nil, errors.Wrap(err, "checking packed bool field compatibility")
			}
		}
		packBoolsExistsFld := m.PackBools + Exists
		if m.index.HasField(packBoolsExistsFld) {
			pBoolFieldExists := m.index.Field(packBoolsExistsFld)
			if err := m.checkFieldCompatibility(pBoolFieldExists, nil, packBoolsExistsFld); err != nil {
				return nil, nil, nil, nil, errors.Wrap(err, "checking packed bool exists field compatibility")
			}
		}
		boolField = m.index.Field(m.PackBools, pilosaclient.OptFieldTypeSet(pilosaclient.CacheTypeRanked, pilosacore.DefaultCacheSize), pilosaclient.OptFieldKeys(true))
		boolFieldExists = m.index.Field(m.PackBools+Exists, pilosaclient.OptFieldTypeSet(pilosaclient.CacheTypeRanked, pilosacore.DefaultCacheSize), pilosaclient.OptFieldKeys(true))
	}

	fields := make([]*pilosaclient.Field, 0, len(schema))
	existingFields := m.index.Fields()
	for i, idkField := range schema {
		// we redefine these inside the loop since we're
		// capturing them in closures
		i := i
		idkField := idkField

		// see if we previously decided to skip this field of the raw
		// record.
		if _, ok := skips[i]; ok {
			continue
		} else if _, ok := idkField.(IgnoreField); ok {
			continue
		}

		// Validate the field.
		if err := m.validateField(idkField); err != nil {
			return nil, nil, nil, nil, errors.Wrap(err, "validating field")
		}

		// Handle records where pilosa already has the field
		// TODO: we should really unify this with the logic
		// below so we aren't creating recordizers in two
		// different places
		_, isBool := idkField.(BoolField)
		_, isSIBK := idkField.(SignedIntBoolKeyField)
		pilosaField, ok := existingFields[idkField.DestName()]
		if !isSIBK && (m.PackBools == "" || !isBool) && ok {
			// Validate that Pilosa's existing field matches the
			// type and options of the IDK field.
			if err := m.checkFieldCompatibility(pilosaField, idkField, ""); err != nil {
				return nil, nil, nil, nil, errors.Wrap(err, "checking field compatibility")
			}

			fields = append(fields, pilosaField)
			valIdx := len(fields) - 1
			// TODO may need to have more sophisticated recordizer by type at some point
			switch idkField.(type) {
			case RecordTimeField:
				recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {

					tyme, err := idkField.PilosafyVal(rawRec[i])
					if err != nil {
						return errors.Wrap(err, "converting recordtimefield")
					}

					rec.Time.Set(tyme.(time.Time))
					return nil
				})
			case IntField, DecimalField, TimestampField:
				recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
					switch rawRec[i].(type) {
					case DeleteSentinel:
						rec.Clears[valIdx] = uint64(0)
					default:
						rec.Values[valIdx], err = idkField.PilosafyVal(rawRec[i])
					}
					return errors.Wrapf(err, "converting field %d:%+v, val:%+v", i, idkField, rawRec[i])
				})
			case IDField, StringField:
				hasMutex := HasMutex(idkField)
				recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
					switch rawRec[i].(type) {
					case DeleteSentinel:
						if hasMutex { //need to clear the mutex
							rec.Clears[valIdx] = nil //? maybe
						}
						// else { //TODO(twg) set fields not supported

						// }
					default:
						rec.Values[valIdx], err = idkField.PilosafyVal(rawRec[i])
					}
					return errors.Wrapf(err, "converting field %d:%+v, val:%+v", i, idkField, rawRec[i])
				})
			case BoolField:
				recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
					switch rawRec[i].(type) {
					case DeleteSentinel:
						rec.Values[valIdx] = nil
					default:
						rec.Values[valIdx], err = idkField.PilosafyVal(rawRec[i])
					}
					return errors.Wrapf(err, "converting field %d:%+v, val:%+v", i, idkField, rawRec[i])
				})
			default:
				recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
					rec.Values[valIdx], err = idkField.PilosafyVal(rawRec[i])
					return errors.Wrapf(err, "converting field %d:%+v, val:%+v", i, idkField, rawRec[i])
				})
			}
			continue
		}

		// now handle this field if it was not already found in pilosa
		switch fld := idkField.(type) {
		case RecordTimeField:
			recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
				tyme, err := idkField.PilosafyVal(rawRec[i])
				if err != nil {
					return errors.Wrap(err, "converting recordtimefield")
				} else if tyme == nil {
					return nil
				}
				rec.Time.Set(tyme.(time.Time))
				return nil
			})
		case StringField, IDField, StringArrayField, IDArrayField:
			opts := []pilosaclient.FieldOption{CacheConfigOf(fld).setOption()}
			hasMutex := false
			if HasMutex(fld) {
				hasMutex = true
				opts = append(opts, CacheConfigOf(fld).mutexOption())
			} else if q := QuantumOf(fld); q != "" {
				opts = append(opts, pilosaclient.OptFieldTypeTime(client_types.TimeQuantum(q)))

				ttl, err := TTLOf(fld)
				if err != nil {
					return nil, nil, nil, nil, err
				} else {
					opts = append(opts, pilosaclient.OptFieldTTL(ttl))
				}
			}

			_, ok1 := fld.(StringArrayField)
			if _, ok2 := fld.(StringField); ok1 || ok2 {
				opts = append(opts, pilosaclient.OptFieldKeys(true))
			}
			fields = append(fields, m.index.Field(fld.DestName(), opts...))
			valIdx := len(fields) - 1

			recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
				switch rawRec[i].(type) {
				case DeleteSentinel:
					if hasMutex { //need to clear the mutex
						rec.Clears[valIdx] = nil
					}
				default:
					rec.Values[valIdx], err = idkField.PilosafyVal(rawRec[i])
				}
				return errors.Wrapf(err, "converting field %d:%+v, val:%+v", i, idkField, rawRec[i])
			})

		case LookupTextField:
			lookupWriteIdxs = append(lookupWriteIdxs, i)

		case BoolField:
			if m.PackBools == "" {
				fields = append(fields, m.index.Field(fld.DestName(), pilosaclient.OptFieldTypeBool()))
				valIdx := len(fields) - 1
				recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
					switch rawRec[i].(type) {
					case DeleteSentinel:
						rec.Values[valIdx] = nil
					default:
						val, err := idkField.PilosafyVal(rawRec[i])
						if err != nil {
							return errors.Wrapf(err, "booling '%v' of %[1]T", val)
						}
						if b, ok := val.(bool); ok {
							rec.Values[valIdx] = b
						}
					}
					return errors.Wrapf(err, "converting field %d:%+v, val:%+v", i, idkField, rawRec[i])
				})
			} else {
				fields = append(fields, boolField, boolFieldExists)
				fieldIdx := len(fields) - 2
				recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
					switch rawRec[i].(type) {
					case DeleteSentinel:
						rec.Clears[fieldIdx] = idkField.DestName() // clear bools bit for this field name
					default:
						val, err := idkField.PilosafyVal(rawRec[i])
						if err != nil {
							return errors.Wrapf(err, "booling '%v' of %[1]T", val)
						}
						if b, ok := val.(bool); ok {
							rec.Values[fieldIdx+1] = idkField.DestName() // set bools-exists bit for this field name
							if b {
								rec.Values[fieldIdx] = idkField.DestName() // set bools bit for this field name
							} else if !m.AssumeEmptyPilosa {
								rec.Clears[fieldIdx] = idkField.DestName() // clear bools bit for this field name
							}
						}
					}
					return nil
				})
				continue
			}
		case IntField:
			var opts []pilosaclient.FieldOption
			switch {
			case fld.Max != nil && fld.Min != nil:
				opts = append(opts, pilosaclient.OptFieldTypeInt(*fld.Min, *fld.Max))
			case fld.Max == nil && fld.Min != nil:
				opts = append(opts, pilosaclient.OptFieldTypeInt(*fld.Min))
			case fld.Max != nil && fld.Min == nil:
				return nil, nil, nil, nil, errors.New("cannot create an integer field with only an upper bound")
			default:
				// No bounds contstraints.
				opts = append(opts, pilosaclient.OptFieldTypeInt())
			}
			if fld.ForeignIndex != "" {
				opts = append(opts, pilosaclient.OptFieldForeignIndex(fld.ForeignIndex))
			}
			fields = append(fields, m.index.Field(fld.DestName(), opts...))
			valIdx := len(fields) - 1
			recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
				switch rawRec[i].(type) {
				case DeleteSentinel:
					rec.Clears[valIdx] = uint64(0)
				default:
					rec.Values[valIdx], err = idkField.PilosafyVal(rawRec[i])
				}
				return errors.Wrapf(err, "converting field %s, val:%+v", idkField.DestName(), rawRec[i])
			})
		case DecimalField:
			fields = append(fields, m.index.Field(fld.DestName(), pilosaclient.OptFieldTypeDecimal(fld.Scale)))
			valIdx := len(fields) - 1
			recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
				switch rawRec[i].(type) {
				case DeleteSentinel:
					rec.Clears[valIdx] = uint64(0)
				default:
					rec.Values[valIdx], err = idkField.PilosafyVal(rawRec[i])
				}
				return errors.Wrapf(err, "converting field %d:%+v, val:%+v", i, idkField, rawRec[i])
			})
		case TimestampField:
			fields = append(fields, m.index.Field(fld.DestName(), pilosaclient.OptFieldTypeTimestamp(fld.epoch(), string(fld.granularity()))))
			valIdx := len(fields) - 1
			recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
				switch rawRec[i].(type) {
				case DeleteSentinel:
					rec.Clears[valIdx] = uint64(0)
				default:
					rec.Values[valIdx], err = idkField.PilosafyVal(rawRec[i])
				}
				return errors.Wrapf(err, "converting field %d:%+v, val:%+v", i, idkField, rawRec[i])
			})
		case DateIntField:
			fields = append(fields, m.index.Field(fld.DestName(), pilosaclient.OptFieldTypeInt()))
			valIdx := len(fields) - 1
			recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
				rec.Values[valIdx], err = idkField.PilosafyVal(rawRec[i])
				return errors.Wrapf(err, "converting field %d:%+v, val:%+v", i, idkField, rawRec[i])
			})
		case SignedIntBoolKeyField:
			name := fld.DestName()
			fields = append(fields,
				m.index.Field(name, pilosaclient.OptFieldTypeSet(pilosaclient.CacheTypeRanked, pilosacore.DefaultCacheSize)),
				m.index.Field(name+Exists, pilosaclient.OptFieldTypeSet(pilosaclient.CacheTypeRanked, pilosacore.DefaultCacheSize)),
			)
			valIdx := len(fields) - 2
			recordizers = append(recordizers, func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
				val, err := idkField.PilosafyVal(rawRec[i])
				if val == nil && err == nil {
					rec.Values[valIdx] = nil
					rec.Values[valIdx+1] = nil
					return nil
				}
				rowID, ok := val.(int64)
				if !ok {
					return errors.Errorf("SignedIntBoolKey value needs to be int64, got %+v of %[1]T", val)
				}

				if rowID > 0 {
					rec.Values[valIdx] = uint64(rowID)
				} else {
					rowID = -rowID
					if !m.AssumeEmptyPilosa {
						rec.Clears[valIdx] = uint64(rowID)
					}
				}
				rec.Values[valIdx+1] = uint64(rowID)

				return errors.Wrapf(err, "converting field %d:%+v, val:%+v", i, idkField, rawRec[i])
			})

		default:
			return nil, nil, nil, nil, errors.Errorf("unknown schema field type %T %[1]v", idkField)
		}
	}

	err = m.SchemaManager.SyncIndex(m.index)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "syncing index")
	}

	// Now we need to get the schema back from the server and rewrite
	// our local fields to make sure we have all the computed options
	// (like 'base' on int fields).
	sSchema, err := m.SchemaManager.Schema()
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "fetching final schema")
	}

	// The table name we use to look up in the index in the schema returned from
	// SchemaManager.Schema() differs depending on whether this is Serverless
	// supported or not (i.e. whether it has a table qualifier).
	tblName := m.index.Name()
	if m.useController() {
		tblName = string(m.Qtbl.Key())
	}

	for i, fld := range fields {
		fields[i] = sSchema.Index(tblName).Field(fld.Name())
	}

	batch, err := m.newBatch(fields)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "creating batch")
	}
	row := &pilosabatch.Row{
		Values: make([]interface{}, len(fields)),
		Clears: make(map[int]interface{}),
	}
	return recordizers, batch, row, lookupWriteIdxs, nil
}

func (m *Main) newBatch(fields []*pilosaclient.Field) (pilosabatch.RecordBatch, error) {
	opts := []pilosabatch.BatchOption{
		pilosabatch.OptLogger(m.log),
		pilosabatch.OptCacheMaxAge(m.CacheLength),
		pilosabatch.OptSplitBatchMode(m.ExpSplitBatchMode),
		pilosabatch.OptMaxStaleness(m.BatchMaxStaleness),
		pilosabatch.OptKeyTranslateBatchSize(m.KeyTranslateBatchSize),
		pilosabatch.OptUseShardTransactionalEndpoint(m.UseShardTransactionalEndpoint),
	}
	var importer pilosacore.Importer
	if m.useController() {
		importer = m.NewImporterFn()
	} else {
		sapi := pilosaclient.NewSchemaAPI(m.client)
		importer = pilosaclient.NewImporter(m.client, sapi)
	}
	opts = append(opts, pilosabatch.OptImporter(importer))

	ii := pilosaclient.FromClientIndex(m.index)
	tbl := pilosacore.IndexInfoToTable(ii)

	return pilosabatch.NewBatch(importer, m.BatchSize, tbl, pilosaclient.FromClientFields(fields), opts...)
}

// validateField ensures that the field is configured correctly.
func (m *Main) validateField(fld Field) error {
	if sfld, ok := fld.(StringField); ok && sfld.Mutex {
		if sfld.Quantum != "" {
			return errors.Errorf("can't specify a time quantum on a string mutex field: '%s'", fld.Name())
		}
		if sfld.TTL != "0s" && sfld.TTL != "" {
			return errors.Errorf("can't specify a TTL on a string mutex field: '%s'", fld.Name())
		}
	}
	if sfld, ok := fld.(IDField); ok && sfld.Mutex {
		if sfld.Quantum != "" {
			return errors.Errorf("can't specify a time quantum on a id mutex field: '%s'", fld.Name())
		}
		if sfld.TTL != "0s" && sfld.TTL != "" {
			return errors.Errorf("can't specify a TTL on a id mutex field: '%s'", fld.Name())
		}
	}
	return nil
}

// checkFieldCompatibility ensures that the IDK field is compatible
// with the existing (i.e. same name) Pilosa field. If it is compatible
// but has some minor differences (a different cache size, for example),
// then it will log the difference and return with no error. If it is
// obviously incompatible then it returns an error.
func (m *Main) checkFieldCompatibility(pFld *pilosaclient.Field, iFld Field, packFld string) error {
	pFldOpts := pFld.Options()

	// Populate an anonymous struct (which tracks *pilosaclient.FieldOptions)
	// based on what we know about iFld. Then use that to compare
	// against pFldOpts.
	iFldOpts := struct {
		fieldType    pilosaclient.FieldType
		timeQuantum  client_types.TimeQuantum
		cacheType    pilosaclient.CacheType
		cacheSize    int
		min          pql.Decimal
		max          pql.Decimal
		scale        int64
		keys         bool
		foreignIndex string
		timeUnit     string
		TTL          time.Duration
	}{}

	// name holds the field name, used in log/error messages.
	var name string

	// If we passed an argument indicating that this is a packed bool field
	// (packBool = true), then instead of using iFld to build iFldOpts, we
	// create iFldOpts with the options we expect from the packed bool field.
	if packFld != "" {
		name = packFld
		iFldOpts.fieldType = pilosaclient.FieldTypeSet
		iFldOpts.cacheType = pilosaclient.CacheTypeRanked
		iFldOpts.cacheSize = pilosacore.DefaultCacheSize
		iFldOpts.keys = true
	} else {
		name = iFld.DestName()
		switch fld := iFld.(type) {
		case StringField, IDField, StringArrayField, IDArrayField:
			iFldOpts.fieldType = pilosaclient.FieldTypeSet
			cc := CacheConfigOf(fld)
			iFldOpts.cacheType = cc.CacheType
			iFldOpts.cacheSize = cc.CacheSize

			if HasMutex(fld) {
				iFldOpts.fieldType = pilosaclient.FieldTypeMutex
				iFldOpts.cacheType = pilosaclient.CacheTypeRanked
				iFldOpts.cacheSize = pilosacore.DefaultCacheSize
			} else if q := QuantumOf(fld); q != "" {
				iFldOpts.fieldType = pilosaclient.FieldTypeTime
				iFldOpts.timeQuantum = client_types.TimeQuantum(q)

				ttl, err := TTLOf(fld)
				if err != nil {
					return err
				} else {
					iFldOpts.TTL = ttl
				}
			}

			_, ok1 := fld.(StringArrayField)
			if _, ok2 := fld.(StringField); ok1 || ok2 {
				iFldOpts.keys = true
			}
		case BoolField:
			if m.PackBools != "" {
				// The compatibility check for packed bool fields
				// is handled when packBool = true.
				return nil
			}
			iFldOpts.fieldType = pilosaclient.FieldTypeBool
		case IntField:
			iFldOpts.fieldType = pilosaclient.FieldTypeInt
			iFldOpts.foreignIndex = fld.ForeignIndex
			// Default min/max to pFld min/max. In the case
			// where fld min/max are not specified we won't
			// error on compatibility with pFld because
			// the values will be the same.
			min := pFldOpts.Min()
			max := pFldOpts.Max()
			if fld.Min != nil {
				min = pql.NewDecimal(*fld.Min, 0)
				if fld.Max != nil {
					max = pql.NewDecimal(*fld.Max, 0)
				}
			}
			iFldOpts.min = min
			iFldOpts.max = max
		case DecimalField:
			iFldOpts.fieldType = pilosaclient.FieldTypeDecimal
			iFldOpts.scale = fld.Scale
			// Default min/max to pFld min/max. In the case
			// where fld min/max are not specified we won't
			// error on compatibility with pFld because
			// the values will be the same.
			min := pFldOpts.Min()
			max := pFldOpts.Max()
			iFldOpts.min = min
			iFldOpts.max = max
		case TimestampField:
			iFldOpts.fieldType = pilosaclient.FieldTypeTimestamp
			iFldOpts.timeUnit = pFldOpts.TimeUnit()
			iFldOpts.min = pFldOpts.Min()
			iFldOpts.max = pFldOpts.Max()
		case DateIntField:
			iFldOpts.fieldType = pilosaclient.FieldTypeInt
			iFldOpts.min = pFldOpts.Min()
			iFldOpts.max = pFldOpts.Max()
		case SignedIntBoolKeyField:
			// TODO: we actually need to compare 2 fields
			iFldOpts.fieldType = pilosaclient.FieldTypeSet
			iFldOpts.cacheType = pilosaclient.CacheTypeRanked
			iFldOpts.cacheSize = pilosacore.DefaultCacheSize
		default:
			return errors.Errorf("can't check compatibility of unknown field: %s (%T)", iFld.Name(), iFld)
		}
	}

	// Keep a list of errors and a list of logs in order to
	// display all logs and all errors at once.
	var errs []string
	var logs []string

	// compare the two field option structs
	if i, p := iFldOpts.fieldType, pFldOpts.Type(); i != p {
		errs = append(errs, fmt.Sprintf("idk field type %s is incompatible with featurebase field type %s: %s", i, p, name))
	}
	if i, p := iFldOpts.timeQuantum, pFldOpts.TimeQuantum(); i != p {
		logs = append(logs, fmt.Sprintf("idk time quantum '%s' differs from featurebase time quantum '%s': %s", i, p, name))
	}
	if i, p := iFldOpts.TTL, pFldOpts.TTL(); i != p {
		logs = append(logs, fmt.Sprintf("idk TTL '%s' differs from featurebase TTL '%s': %s", i, p, name))
	}
	if i, p := iFldOpts.cacheType, pFldOpts.CacheType(); i != p {
		logs = append(logs, fmt.Sprintf("idk cache type '%s' differs from featurebase cache type '%s': %s", i, p, name))
	}
	if i, p := iFldOpts.cacheSize, pFldOpts.CacheSize(); i != p {
		logs = append(logs, fmt.Sprintf("idk cache size %d differs from featurebase cache size %d: %s", i, p, name))
	}
	// We compare the decimal strings for min/max because the values for two
	// pql.Decimal structs can differ, but they still represent the same value.
	if i, p := iFldOpts.min, pFldOpts.Min(); i.String() != p.String() {
		errs = append(errs, fmt.Sprintf("idk min %s differs from featurebase min %s: %s", i, p, name))
	}
	if i, p := iFldOpts.max, pFldOpts.Max(); i.String() != p.String() {
		errs = append(errs, fmt.Sprintf("idk max %s differs from featurebase max %s: %s", i, p, name))
	}
	if i, p := iFldOpts.scale, pFldOpts.Scale(); i != p {
		errs = append(errs, fmt.Sprintf("idk scale %d differs from featurebase scale %d: %s", i, p, name))
	}
	if i, p := iFldOpts.keys, pFldOpts.Keys(); i != p {
		errs = append(errs, fmt.Sprintf("idk keys %v differs from featurebase keys %v: %s", i, p, name))
	}
	if i, p := iFldOpts.foreignIndex, pFldOpts.ForeignIndex(); i != p {
		errs = append(errs, fmt.Sprintf("idk foreign index '%s' differs from featurebase foreign index '%s': %s", i, p, name))
	}
	if i, p := iFldOpts.timeUnit, pFldOpts.TimeUnit(); i != p {
		errs = append(errs, fmt.Sprintf("idk field type %s is incompatible with featurebase timeunit %s: %s", i, p, name))
	}

	// Print any log-level incompatibilities.
	if len(logs) > 0 {
		for i := range logs {
			m.log.Printf(logs[i])
		}
	}

	// Return any error-level incompatibilities.
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

// getPrimaryKeyRecordizer returns a Recordizer function which
// extracts the primary key fields from a record, combines them, and
// sets the ID on the record. If pkFields is a single field, and that
// field is of type string, we'll return it in skipFields, because we
// won't want to index it separately.
func getPrimaryKeyRecordizer(schema []Field, pkFields []string) (recordizer Recordizer, skipFields map[int]struct{}, err error) {
	if len(schema) == 0 {
		return nil, nil, errors.New("can't call getPrimaryKeyRecordizer with empty schema")
	}
	if len(pkFields) == 0 {
		return nil, nil, errors.New("can't call getPrimaryKeyRecordizer with empty pkFields")
	}
	fieldIndices := make([]int, 0, len(pkFields))
	for pkIndex, pk := range pkFields {
		pk = strings.TrimSpace(pk)
		for fieldIndex, field := range schema {
			if pk == field.DestName() {
				switch field.(type) {
				case StringArrayField:
					return nil, nil, errors.Errorf("field %s cannot be a primary key field because it is a StringArray field.", pk)
				}
				fieldIndices = append(fieldIndices, fieldIndex)
				break
			}
		}
		if len(fieldIndices) != pkIndex+1 {
			return nil, nil, errors.Errorf("no field with primary key field name %s found. fields: %+v", pk, schema)
		}
	}
	if len(pkFields) == 1 {
		if _, ok := schema[fieldIndices[0]].(StringField); ok {
			skipFields = make(map[int]struct{}, 1)
			skipFields[fieldIndices[0]] = struct{}{}
		}
	}
	recordizer = func(rawRec []interface{}, rec *pilosabatch.Row) (err error) {
		// first, special case for performance when there is a single
		// primary key field and it is a byte slice already.
		if len(fieldIndices) == 1 {
			switch recID := rawRec[fieldIndices[0]].(type) {
			case []byte, string:
				rec.ID = recID
				return nil
			}
		}

		idbytes, ok := rec.ID.([]byte)
		if ok {
			idbytes = idbytes[:0]
		} else {
			idbytes = make([]byte, 0)
		}
		buf := bytes.NewBuffer(idbytes) // TODO does the buffer escape to heap?

		for i, fieldIdx := range fieldIndices {
			if i != 0 {
				err := buf.WriteByte('|')
				if err != nil {
					return errors.Wrap(err, "writing separator")
				}
			}
			switch val := rawRec[fieldIdx].(type) {
			case []byte:
				_, err := buf.Write(val)
				if err != nil {
					return errors.Wrapf(err, "writing byte slice to buffer")
				}
			case nil:
				return errors.New("data for primary key should not be nil or missing")
			default:
				_, err := fmt.Fprintf(buf, "%v", val)
				if err != nil {
					return errors.Wrapf(err, "encoding primary key val:'%v' type: %[1]T", val)
				}
			}

		}
		rec.ID = buf.Bytes()
		return nil
	}
	return recordizer, skipFields, nil
}

func (m *Main) validate() error {
	IDSpecCount := 0
	if len(m.PrimaryKeyFields) != 0 {
		IDSpecCount++
	}
	if m.IDField != "" {
		IDSpecCount++
	}
	if m.AutoGenerate {
		IDSpecCount++
	}
	if IDSpecCount != 1 {
		return errors.New("must set exactly one of --primary-key-field <fieldnames>, --id-field <fieldname>, --auto-generate")
	}

	if m.useController() {
		if m.OrganizationID == "" {
			return errors.New("must set an organization with --featurebase.org-id")
		} else if m.DatabaseID == "" {
			return errors.New("must set a database with --featurebase.db-id")
		} else if m.TableName == "" {
			return errors.New("must set a table with --featurebase.table-name")
		}
	} else if m.Index == "" {
		return errors.New("must set an index with --pilosa.index")
	}

	if m.NewSource == nil {
		return errors.New("must set a NewSource function on IDK ingester")
	}
	return nil
}

// allowError checks if value errors of specific types are explicitly allowed by the user
func (m *Main) allowError(err error) bool {
	switch {
	case m.AllowIntOutOfRange && strings.Contains(err.Error(), ErrIntOutOfRange.Error()):
		return true
	case m.AllowDecimalOutOfRange && strings.Contains(err.Error(), ErrDecimalOutOfRange.Error()):
		return true
	case m.AllowTimestampOutOfRange && strings.Contains(err.Error(), ErrTimestampOutOfRange.Error()):
		return true
	default:
		return false
	}
}

func (m *Main) useController() bool {
	return m.ControllerAddress != ""
}
