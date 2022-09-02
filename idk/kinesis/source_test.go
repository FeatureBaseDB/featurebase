package kinesis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSource(t *testing.T) {
	tmpDir := t.TempDir()
	src := NewSource()
	assert.NotNil(t, src)

	const testStreamName = "teststream"

	src.kinesisClient = kinesisMock
	src.s3client = s3mock
	src.StreamName = testStreamName
	src.Header = "./testdata/header.json"
	src.OffsetsPath = fmt.Sprintf("%s/offsets.json", tmpDir)

	describeStreamOut := &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamStatus: aws.String(kinesis.StreamStatusActive),
			Shards:       []*kinesis.Shard{{ShardId: aws.String("shard0")}},
		},
	}
	kinesisMock.On("DescribeStream",
		mock.MatchedBy(func(input *kinesis.DescribeStreamInput) bool {
			return *input.StreamName == testStreamName
		}),
	).Return(describeStreamOut, nil)

	getShardIteratorOutput := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("iter")}
	kinesisMock.On("GetShardIterator",
		mock.MatchedBy(func(input *kinesis.GetShardIteratorInput) bool {
			return *input.StreamName == testStreamName && *input.ShardId == "shard0"
		}),
	).Return(getShardIteratorOutput, nil)

	getRecordsOutput := &kinesis.GetRecordsOutput{
		Records: []*kinesis.Record{
			{SequenceNumber: aws.String("1"), Data: []byte("{ \"language\": 0, \"project_id\": 2 }"), ApproximateArrivalTimestamp: aws.Time(time.Now().UTC())},
		},
		MillisBehindLatest: aws.Int64(100),
		NextShardIterator:  nil,
	}
	kinesisMock.On("GetRecords",
		mock.MatchedBy(func(input *kinesis.GetRecordsInput) bool {
			return true
		}),
	).Return(getRecordsOutput, nil)

	err := src.Open()
	assert.NoError(t, err)

	rec, err := src.Record()
	assert.NoError(t, err)
	assert.NotNil(t, rec, err)
	assert.NotNil(t, rec.Data())

	err = rec.Commit(context.Background())
	assert.NoError(t, err)

	defer src.Close()
}

func TestInitAWS(t *testing.T) {
	src := NewSource()
	err := src.initAWS()
	assert.NoError(t, err)
}
