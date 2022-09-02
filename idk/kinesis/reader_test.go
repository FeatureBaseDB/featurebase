package kinesis

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/molecula/featurebase/v3/idk/idktest/mocks"
	"github.com/molecula/featurebase/v3/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	s3mock      = &mocks.S3API{}
	kinesisMock = &mocks.KinesisAPI{}
)

func newMockedStreamReader(t *testing.T, offsetsPath string) *StreamReader {
	cfg := StreamReaderConfig{
		log:           logger.NopLogger,
		streamName:    "teststream",
		offsetsPath:   offsetsPath,
		kinesisClient: kinesisMock,
		s3client:      s3mock,
	}
	reader, err := NewStreamReader(cfg)
	if err != nil {
		t.Fatalf("Unexpected error creating stream reader: %v", err)
	}

	reader.getRecordsBatchSize = 2
	reader.getRecordsQueriesPerSecondBehindTip = 1
	reader.getRecordsQueriesPerSecondAtTip = 1
	return reader
}

func TestNewKinesisReaderWithMissingOffsetsFile(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := StreamReaderConfig{
		log:           logger.NopLogger,
		streamName:    "teststream",
		offsetsPath:   fmt.Sprintf("%s/missing", tmpDir),
		kinesisClient: kinesisMock,
		s3client:      s3mock,
	}
	reader, err := NewStreamReader(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	assert.NotNil(t, reader.offsets)
	assert.Empty(t, reader.offsets.Shards)
}

func TestNewKinesisReaderWithInvalidOffsetsFile(t *testing.T) {
	cfg := StreamReaderConfig{
		log:           logger.NopLogger,
		streamName:    "teststream",
		offsetsPath:   "./testdata/invalid_offsets.json",
		kinesisClient: kinesisMock,
		s3client:      s3mock,
	}
	reader, err := NewStreamReader(cfg)
	assert.Error(t, err)
	assert.Nil(t, reader)
}

func TestNewKinesisReaderWithExistingOffsets(t *testing.T) {
	tmpDir := t.TempDir()
	offsets := fmt.Sprintf("%s/offsets.json", tmpDir)
	if err := copyFile(offsets, "./testdata/offsets.json"); err != nil {
		t.Fatal("failed to copy offsets to temp directory")
	}

	cfg := StreamReaderConfig{
		log:           logger.NopLogger,
		streamName:    "teststream",
		offsetsPath:   offsets,
		kinesisClient: kinesisMock,
		s3client:      s3mock,
	}
	reader, err := NewStreamReader(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	shardOffset, found := reader.offsets.Load("shardId-000000000000")
	assert.True(t, found)
	assert.NotNil(t, shardOffset)
	assert.Equal(t, "49626031806177636477557067721327867885693021124781146130", shardOffset.SequenceNumber)
	assert.EqualValues(t, shardOffset.Index, 1)

	shardOffset, found = reader.offsets.Load("shardId-000000000001")
	assert.True(t, found)
	assert.NotNil(t, shardOffset)
	assert.Equal(t, "49626031806199937222755598499219162069555906398413389858", shardOffset.SequenceNumber)
	assert.EqualValues(t, shardOffset.Index, 2)
}

func shardName(shardIDx int) string {
	return fmt.Sprintf("shardId-00000000000%d", shardIDx)
}

func shardIterator(shardIDx int) string {
	return fmt.Sprintf("%s-%d", shardName(shardIDx), time.Now().UnixNano())
}

func TestStreamReaderStartFetchCommitWithoutOffsets(t *testing.T) {
	tmpDir := t.TempDir()
	reader := newMockedStreamReader(t, fmt.Sprintf("%s/offsets.json", tmpDir))

	describeStreamOut := &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamStatus: aws.String(kinesis.StreamStatusActive),
		},
	}

	const testShardCount = 2
	for i := 0; i < testShardCount; i++ {
		describeStreamOut.StreamDescription.Shards = append(describeStreamOut.StreamDescription.Shards,
			&kinesis.Shard{ShardId: aws.String(shardName(i))})
	}

	kinesisMock.On("DescribeStream",
		mock.MatchedBy(func(input *kinesis.DescribeStreamInput) bool {
			return *input.StreamName == reader.streamName
		}),
	).Return(describeStreamOut, nil)

	for i := 0; i < testShardCount; i++ {
		c := i
		getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String(shardIterator(c))}
		kinesisMock.On("GetShardIterator",
			mock.MatchedBy(func(input *kinesis.GetShardIteratorInput) bool {
				return *input.StreamName == reader.streamName && *input.ShardId == shardName(c) &&
					*input.ShardIteratorType == kinesis.ShardIteratorTypeTrimHorizon
			}),
		).Return(getShardIteratorOutput, nil)

		getRecordsOutput := &kinesis.GetRecordsOutput{
			Records: []*kinesis.Record{
				{SequenceNumber: aws.String("1"), Data: []byte("deadbeef"), ApproximateArrivalTimestamp: aws.Time(time.Now().UTC())},
			},
			MillisBehindLatest: aws.Int64(100),
			NextShardIterator:  aws.String(shardIterator(c)),
		}
		kinesisMock.On("GetRecords",
			mock.MatchedBy(func(input *kinesis.GetRecordsInput) bool {
				return strings.HasPrefix(*input.ShardIterator, shardName(c))
			}),
		).Return(getRecordsOutput, nil)
	}

	err := reader.Start()
	assert.Nil(t, err)

	var records []ShardRecord
	for i := 0; i < testShardCount; i++ {
		rec, err := reader.FetchMessage(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, rec.SequenceNumber, aws.String("1"))
		records = append(records, rec)
	}
	assert.Empty(t, reader.offsets.Shards)

	err = reader.CommitMessages(context.Background(), records...)
	assert.Nil(t, err)

	assert.Len(t, reader.offsets.Shards, 2)
	for i := 0; i < testShardCount; i++ {
		shardOffset, exists := reader.offsets.Load(shardName(i))
		assert.True(t, exists)
		assert.EqualValues(t, shardOffset.Index, 1)
	}
	defer reader.Close()
}

func copyFile(dst, src string) error {
	to, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer to.Close()

	from, err := os.Open(src)
	if err != nil {
		return err
	}
	defer from.Close()

	_, err = io.Copy(to, from)
	if err != nil {
		return err
	}
	return nil
}

func TestStreamReaderStartFetchCommitFromExistingOffsets(t *testing.T) {
	tmpDir := t.TempDir()
	offsets := fmt.Sprintf("%s/offsets.json", tmpDir)
	if err := copyFile(offsets, "./testdata/offsets.json"); err != nil {
		t.Fatal("failed to copy offsets to temp directory")
	}

	reader := newMockedStreamReader(t, offsets)

	describeStreamOut := &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamStatus: aws.String(kinesis.StreamStatusActive),
		},
	}

	const testShardCount = 2
	for i := 0; i < testShardCount; i++ {
		describeStreamOut.StreamDescription.Shards = append(describeStreamOut.StreamDescription.Shards,
			&kinesis.Shard{ShardId: aws.String(shardName(i))})
	}

	kinesisMock.On("DescribeStream",
		mock.MatchedBy(func(input *kinesis.DescribeStreamInput) bool {
			return *input.StreamName == reader.streamName
		}),
	).Return(describeStreamOut, nil)

	for i := 0; i < testShardCount; i++ {
		c := i
		getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String(shardIterator(c))}
		kinesisMock.On("GetShardIterator",
			mock.MatchedBy(func(input *kinesis.GetShardIteratorInput) bool {
				return *input.StreamName == reader.streamName && *input.ShardId == shardName(c) &&
					*input.ShardIteratorType == kinesis.ShardIteratorTypeAfterSequenceNumber
			}),
		).Return(getShardIteratorOutput, nil)

		getRecordsOutput := &kinesis.GetRecordsOutput{
			Records: []*kinesis.Record{
				{SequenceNumber: aws.String("1"), Data: []byte("deadbeef"), ApproximateArrivalTimestamp: aws.Time(time.Now().UTC())},
			},
			MillisBehindLatest: aws.Int64(100),
			NextShardIterator:  aws.String(shardIterator(c)),
		}
		kinesisMock.On("GetRecords",
			mock.MatchedBy(func(input *kinesis.GetRecordsInput) bool {
				return strings.HasPrefix(*input.ShardIterator, shardName(c))
			}),
		).Return(getRecordsOutput, nil)
	}

	err := reader.Start()
	assert.Nil(t, err)

	var records []ShardRecord
	for i := 0; i < testShardCount; i++ {
		rec, err := reader.FetchMessage(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, rec.SequenceNumber, aws.String("1"))
		records = append(records, rec)
	}
	assert.Len(t, reader.offsets.Shards, 2)
	for i := 0; i < testShardCount; i++ {
		shardOffset, exists := reader.offsets.Load(shardName(i))
		assert.True(t, exists)
		assert.EqualValues(t, shardOffset.Index, i+1)
	}

	err = reader.CommitMessages(context.Background(), records...)
	assert.Nil(t, err)

	assert.Len(t, reader.offsets.Shards, 2)
	for i := 0; i < testShardCount; i++ {
		shardOffset, exists := reader.offsets.Load(shardName(i))
		assert.True(t, exists)
		assert.EqualValues(t, shardOffset.Index, i+2)
	}
	defer reader.Close()
}

func TestStreamOrderlyReadsAfterResharding(t *testing.T) {
	s3mock = &mocks.S3API{}
	kinesisMock = &mocks.KinesisAPI{}

	tmpDir := t.TempDir()

	reader := newMockedStreamReader(t, fmt.Sprintf("%s/offsets.json", tmpDir))
	defer reader.Close()

	describeStreamOut := &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamStatus: aws.String(kinesis.StreamStatusActive),
		},
	}
	describeStreamOut.StreamDescription.Shards = []*kinesis.Shard{{ShardId: aws.String(shardName(0))}}
	kinesisMock.On("DescribeStream",
		mock.MatchedBy(func(input *kinesis.DescribeStreamInput) bool {
			return *input.StreamName == reader.streamName
		}),
	).Return(describeStreamOut, nil).Once()

	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String(shardIterator(0))}
	kinesisMock.On("GetShardIterator",
		mock.MatchedBy(func(input *kinesis.GetShardIteratorInput) bool {
			return *input.StreamName == reader.streamName && *input.ShardId == shardName(0) &&
				*input.ShardIteratorType == kinesis.ShardIteratorTypeTrimHorizon
		}),
	).Return(getShardIteratorOutput, nil).Once()

	const recordsBeforeClose = 2
	for i := 1; i <= recordsBeforeClose; i++ {
		c := strconv.Itoa(i)
		getRecordsOutput := &kinesis.GetRecordsOutput{
			Records: []*kinesis.Record{
				{SequenceNumber: aws.String(c), Data: []byte(c), ApproximateArrivalTimestamp: aws.Time(time.Now().UTC())},
			},
			MillisBehindLatest: aws.Int64(100),
			NextShardIterator:  aws.String(shardIterator(0)),
		}
		// last record signals the shard is closed
		if i == recordsBeforeClose {
			getRecordsOutput.NextShardIterator = nil
		}
		kinesisMock.On("GetRecords",
			mock.MatchedBy(func(input *kinesis.GetRecordsInput) bool {
				return strings.HasPrefix(*input.ShardIterator, shardName(0))
			}),
		).After(time.Second).Return(getRecordsOutput, nil).Once()
	}

	getRecordsOutput := &kinesis.GetRecordsOutput{
		Records: []*kinesis.Record{
			{SequenceNumber: aws.String("1"), Data: []byte("1"), ApproximateArrivalTimestamp: aws.Time(time.Now().UTC())},
		},
		MillisBehindLatest: aws.Int64(100),
		NextShardIterator:  aws.String(shardIterator(0)),
	}
	kinesisMock.On("GetRecords",
		mock.MatchedBy(func(input *kinesis.GetRecordsInput) bool {
			return strings.HasPrefix(*input.ShardIterator, shardName(0))
		}),
	).After(time.Second).Return(getRecordsOutput, nil).Once()

	describeCalls := 2
	for i := 1; i <= describeCalls; i++ {
		// stream goes into Updating status
		describeStreamOut = &kinesis.DescribeStreamOutput{
			StreamDescription: &kinesis.StreamDescription{
				StreamStatus: aws.String(kinesis.StreamStatusUpdating),
				Shards:       []*kinesis.Shard{{ShardId: aws.String(shardName(0))}},
			},
		}
		// last call transitions stream to active and adds child shard
		if i == describeCalls {
			describeStreamOut.StreamDescription.StreamStatus = aws.String(kinesis.StreamStatusActive)
			describeStreamOut.StreamDescription.Shards = append(describeStreamOut.StreamDescription.Shards,
				&kinesis.Shard{ShardId: aws.String(shardName(1)), ParentShardId: aws.String(shardName(0))})
		}

		kinesisMock.On("DescribeStream",
			mock.MatchedBy(func(input *kinesis.DescribeStreamInput) bool {
				return *input.StreamName == reader.streamName
			}),
		).Return(describeStreamOut, nil).Once()
	}

	getShardIteratorOutput = &kinesis.GetShardIteratorOutput{ShardIterator: aws.String(shardIterator(1))}
	kinesisMock.On("GetShardIterator",
		mock.MatchedBy(func(input *kinesis.GetShardIteratorInput) bool {
			return *input.StreamName == reader.streamName && *input.ShardId == shardName(1) &&
				*input.ShardIteratorType == kinesis.ShardIteratorTypeTrimHorizon
		}),
	).Return(getShardIteratorOutput, nil).Once()

	getRecordsOutput = &kinesis.GetRecordsOutput{
		Records: []*kinesis.Record{
			{SequenceNumber: aws.String("1"), Data: []byte("1"), ApproximateArrivalTimestamp: aws.Time(time.Now().UTC())},
		},
		MillisBehindLatest: aws.Int64(0),
		NextShardIterator:  aws.String(shardIterator(1)),
	}
	kinesisMock.On("GetRecords",
		mock.MatchedBy(func(input *kinesis.GetRecordsInput) bool {
			return strings.HasPrefix(*input.ShardIterator, shardName(1))
		}),
	).Return(getRecordsOutput, nil).Once()

	err := reader.Start()
	assert.Nil(t, err)

	for i := 1; i <= recordsBeforeClose; i++ {
		rec, err := reader.FetchMessage(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, strconv.Itoa(i), *rec.SequenceNumber)
		assert.Equal(t, rec.ShardID, shardName(0))
	}

	rec, err := reader.FetchMessage(context.Background())
	assert.Nil(t, err)
	assert.Equal(t, "1", *rec.SequenceNumber)
	assert.Equal(t, rec.ShardID, shardName(1))

}
