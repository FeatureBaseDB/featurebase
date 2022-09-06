package kinesis

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/featurebasedb/featurebase/v3/idk/internal"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/pkg/errors"
)

const (
	defaultGetRecordsBatchSize                 = 10000
	defaultGetRecordsQueriesPerSecondAtTip     = 1
	defaultGetRecordsQueriesPerSecondBehindTip = 5

	getRecordsErrorBackoff     = 10 * time.Second
	waitForActiveStreamBackoff = 5 * time.Second
	waitForClosedShardBackoff  = 1 * time.Second

	ShardStatusActive  = "ACTIVE"
	ShardStatusPending = "PENDING"
	ShardStatusClosed  = "CLOSED"
)

// ShardRecord wraps a kinesis record including metadata about its shard and an IDK-local index
type ShardRecord struct {
	ShardID string
	Index   uint64
	*kinesis.Record
}

type StreamReader struct {
	recordsChan chan ShardRecord
	StreamReaderConfig
	offsets *StreamOffsets
	// keep track of what shards have been consumed to the end
	shardStatus      sync.Map
	stopCh           chan struct{}
	processShardsMtx sync.Mutex
}

type StreamReaderConfig struct {
	log                                 logger.Logger
	streamName                          string
	offsetsPath                         string
	kinesisClient                       kinesisiface.KinesisAPI
	s3client                            s3iface.S3API
	getRecordsBatchSize                 int
	getRecordsQueriesPerSecondAtTip     int
	getRecordsQueriesPerSecondBehindTip int
}

func (r *StreamReader) Close() {
	r.log.Infof("Shutting down stream reader")
	close(r.stopCh)
}

func (r *StreamReader) getShardIterator(shardID string) *string {
	r.log.Infof("Getting iterator for shard %s", shardID)
	shardIteratorInput := &kinesis.GetShardIteratorInput{
		// Shard Id is provided when making put record(s) request.
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String(kinesis.ShardIteratorTypeTrimHorizon),
		StreamName:        aws.String(r.streamName),
	}

	if shardOffset, ok := r.offsets.Load(shardID); ok {
		r.log.Infof("Shard %s will begin reading from sequence number %s", shardID, shardOffset.SequenceNumber)
		shardIteratorInput.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber)
		shardIteratorInput.StartingSequenceNumber = &shardOffset.SequenceNumber
	}

	// retrieve iterator
	iteratorOutput, err := r.kinesisClient.GetShardIterator(shardIteratorInput)
	if err != nil {
		panic(errors.Wrap(err, "Failed to get shard iterator"))
	}
	return iteratorOutput.ShardIterator
}

// loadRecordIndex reads the shard record index from the offsets file
// or returns a pointer initialized with a value of 0 if not found
func (r *StreamReader) loadRecordIndex(shardID string) *uint64 {
	var idx uint64
	if shardOffset, ok := r.offsets.Load(shardID); ok {
		atomic.StoreUint64(&idx, shardOffset.Index)
	}
	return &idx
}

type shardReader struct {
	*StreamReader
	shardID       string
	shardIterator *string
	recordIdx     *uint64
	atTip         bool
	limiter       *rate.Limiter
}

func (r *StreamReader) newShardReader(shardID string) *shardReader {
	return &shardReader{
		StreamReader:  r,
		shardID:       shardID,
		shardIterator: r.getShardIterator(shardID),
		recordIdx:     r.loadRecordIndex(shardID),
		atTip:         false,
		limiter:       rate.NewLimiter(rate.Limit(r.getRecordsQueriesPerSecondBehindTip), 1),
	}
}

// Start consumes records from a Kinesis shard indefinitely until the shard
// has been closed, at which point this method will return.
func (r *shardReader) start() {
	r.log.Infof("Starting to get records for shard %s", r.shardID)
	defer r.log.Infof("Stopped reading from shard %s", r.shardID)

	for {
		select {
		case <-r.stopCh:
			return
		default:
			if err := r.getRecords(); err != nil {
				r.log.Errorf("Backing off due to Kinesis GetRecords error: %v\n", err)
				time.Sleep(getRecordsErrorBackoff)
			}
		}
		// mark the shard as closed, so we can reflect its status in the offsets file once
		// all records have been committed
		if r.shardIterator == nil {
			r.log.Infof("Shard %s has been closed", r.shardID)
			return
		}
	}
}

func (r *shardReader) getRecords() error {
	if err := r.limiter.Wait(context.Background()); err != nil {
		r.log.Warnf("Unexpected error waiting for shard reader limiter: %v", err)
	}
	// get records use shard iterator for making request
	records, err := r.kinesisClient.GetRecords(&kinesis.GetRecordsInput{
		Limit:         aws.Int64(int64(r.getRecordsBatchSize)),
		ShardIterator: r.shardIterator,
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case kinesis.ErrCodeExpiredIteratorException:
				r.log.Warnf("Shard iterator expired for shard %s", r.shardID)
				// obtain a new shard iterator from Kinesis
				r.shardIterator = r.getShardIterator(r.shardID)
				// retry calling GetRecords with new iterator
				return nil
			}
		}
		return err
	}

	if len(records.Records) > 0 {
		r.log.Debugf("Read %d records from shard %v", len(records.Records), r.shardID)
		for _, d := range records.Records {
			r.recordsChan <- ShardRecord{ShardID: r.shardID, Index: atomic.AddUint64(r.recordIdx, 1), Record: d}
		}
	}

	r.shardIterator = records.NextShardIterator
	if records.NextShardIterator == nil {
		return nil
	}

	if *records.MillisBehindLatest == 0 { // at tip of queue
		if !r.atTip {
			r.log.Debugf("Caught up with tip of shard %s", r.shardID)
			// we are caught up and can reduce the polling rate
			r.limiter.SetLimit(rate.Limit(r.getRecordsQueriesPerSecondAtTip))
			r.atTip = true
		}
	} else { // behind tip of queue
		if r.atTip {
			r.log.Debugf("Fell behind tip of shard %s", r.shardID)
			r.limiter.SetLimit(rate.Limit(r.getRecordsQueriesPerSecondBehindTip))
			r.atTip = false
		}
	}
	return nil
}

func ReadOffsets(cfg StreamReaderConfig) (*StreamOffsets, error) {
	offsetsBytes, err := internal.ReadFileOrURL(cfg.offsetsPath, cfg.s3client)
	if err != nil {
		if err == internal.FileOrURLNotFound {
			return &StreamOffsets{
				StreamName: cfg.streamName,
				Shards:     make(map[string]*ShardOffset),
			}, nil
		}
		return nil, errors.Wrap(err, "failed to read path")
	}

	var offsets StreamOffsets
	if err := json.Unmarshal(offsetsBytes, &offsets); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal offsets")
	}
	cfg.log.Infof("Successfully read offsets")
	return &offsets, err
}

func (r *StreamReader) isShardClosed(shardID string) bool {
	// unknown shards (nil entry in status map) are expired and effectively closed
	if status, found := r.shardStatus.Load(shardID); found && status != ShardStatusClosed {
		return false
	}
	return true
}

// CanReadFromShard returns true when both parent and adjacent shards have been closed
// if they exist
func (r *StreamReader) canReadFromShard(shard *kinesis.Shard) bool {
	return (shard.ParentShardId == nil || r.isShardClosed(*shard.ParentShardId)) &&
		(shard.AdjacentParentShardId == nil || r.isShardClosed(*shard.AdjacentParentShardId))
}

func (r *StreamReader) startShardReader(shard *kinesis.Shard) {
	for !r.canReadFromShard(shard) {
		r.log.Debugf("Waiting for parent shards of %s to be closed", *shard.ShardId)
		time.Sleep(waitForClosedShardBackoff)
	}
	r.shardStatus.Store(*shard.ShardId, ShardStatusActive)
	reader := r.newShardReader(*shard.ShardId)
	// Start returns when the shard has been closed
	reader.start()
	r.shardStatus.Store(*shard.ShardId, ShardStatusClosed)

	select {
	case <-r.stopCh:
		return
	default:
		r.log.Infof("Reprocessing shards due to close event")
		if err := r.ProcessShards(); err != nil {
			// cannot recover
			panic("failed to reprocess shards after a close event")
		}
	}
}

func (r *StreamReader) ProcessShards() error {
	r.log.Infof("Processing shards for stream %s", r.streamName)
	r.processShardsMtx.Lock()
	defer r.processShardsMtx.Unlock()

	for {
		streamOut, err := r.kinesisClient.DescribeStream(
			&kinesis.DescribeStreamInput{StreamName: aws.String(r.streamName)})

		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case kinesis.ErrCodeResourceNotFoundException:
					return errors.Wrap(err, "kinesis.DescribeStream stream not found")
				}
			}
			r.log.Errorf("kinesis.DescribeStream failed with error %v", err)
			return errors.Wrap(err, "calling kinesis.DescribeStream")
		}

		if *streamOut.StreamDescription.StreamStatus == kinesis.StreamStatusActive {
			shards := streamOut.StreamDescription.Shards
			for _, shard := range shards {
				shardID := *shard.ShardId
				// process the new shard if we haven't seen it before, and set its status to pending
				if _, exists := r.shardStatus.LoadOrStore(shardID, ShardStatusPending); !exists {
					r.log.Infof("Found pending shard %s", *shard.ShardId)
					go r.startShardReader(shard)
				}
			}
			return nil
		}

		r.log.Infof("Waiting for stream %s to become active", r.streamName)
		time.Sleep(waitForActiveStreamBackoff)
	}
}

func NewStreamReader(cfg StreamReaderConfig) (*StreamReader, error) {
	// initialize optional config if not set
	if cfg.getRecordsBatchSize == 0 {
		cfg.getRecordsBatchSize = defaultGetRecordsBatchSize
	}
	if cfg.getRecordsQueriesPerSecondAtTip == 0 {
		cfg.getRecordsQueriesPerSecondAtTip = defaultGetRecordsQueriesPerSecondAtTip
	}
	if cfg.getRecordsQueriesPerSecondBehindTip == 0 {
		cfg.getRecordsQueriesPerSecondBehindTip = defaultGetRecordsQueriesPerSecondBehindTip
	}

	offsets, err := ReadOffsets(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read offsets")
	}

	return &StreamReader{
		recordsChan:        make(chan ShardRecord, 100),
		StreamReaderConfig: cfg,
		offsets:            offsets,
		shardStatus:        sync.Map{},
		stopCh:             make(chan struct{}),
	}, nil
}

type ShardOffset struct {
	ArrivalTime    string `json:"arrival_time"`
	CommittedTime  string `json:"committed_time"`
	Index          uint64 `json:"index"`
	SequenceNumber string `json:"sequence_number"`
}

type StreamOffsets struct {
	// synchronize updates/reads to the Shards map
	sync.RWMutex
	StreamName string                  `json:"stream_name"`
	Shards     map[string]*ShardOffset `json:"shards"`
}

// Load returns the ShardOffset for the shard in a thread safe manner
func (o *StreamOffsets) Load(shardID string) (*ShardOffset, bool) {
	o.RLock()
	defer o.RUnlock()
	v, ok := o.Shards[shardID]
	return v, ok
}

func (r *StreamReader) Start() error {
	if err := r.ProcessShards(); err != nil {
		return errors.Wrap(err, "failed to process shards")
	}
	return nil
}

func (r *StreamReader) FetchMessage(ctx context.Context) (ShardRecord, error) {
	if err := ctx.Err(); err != nil {
		return ShardRecord{}, err
	}

	select {
	case record, ok := <-r.recordsChan:
		if !ok {
			return ShardRecord{}, io.EOF
		}
		return record, nil

	case <-ctx.Done():
		return ShardRecord{}, ctx.Err()
	}
}

func (r *StreamReader) CommitMessages(ctx context.Context, msgs ...ShardRecord) error {
	r.log.Debugf("Commiting %d messages", len(msgs))
	now := time.Now().UTC().Format(time.RFC3339)

	r.offsets.Lock()
	for _, record := range msgs {
		r.offsets.Shards[record.ShardID] = &ShardOffset{
			ArrivalTime:    (*record.ApproximateArrivalTimestamp).UTC().Format(time.RFC3339),
			CommittedTime:  now,
			Index:          atomic.LoadUint64(&record.Index),
			SequenceNumber: *record.SequenceNumber,
		}
	}
	r.offsets.Unlock()

	offsetsBytes, err := json.Marshal(r.offsets)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal offsets")
	}
	return internal.WriteFileOrURL(r.offsetsPath, offsetsBytes, r.s3client)
}
