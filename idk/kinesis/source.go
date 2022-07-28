package kinesis

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/internal"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"
)

// Source implements the idk.Source interface using kafka as a data
// source. It is not threadsafe! Due to the way Kafka clients work, to
// achieve concurrency, create multiple Sources.
type Source struct {
	Log        logger.Logger
	Timeout    time.Duration
	Header     string
	AWSRegion  string
	AWSProfile string

	AllowMissingFields bool

	StreamName  string
	OffsetsPath string

	schema []idk.Field
	paths  idk.PathTable

	session *session.Session
	reader  *StreamReader

	s3client      s3iface.S3API
	kinesisClient kinesisiface.KinesisAPI

	spoolBase uint64
	spool     []ShardRecord
}

// NewSource gets a new Source
func NewSource() *Source {
	src := &Source{
		StreamName: "example",
		Log:        logger.NopLogger,
	}

	return src
}

// Record returns the value of the next kinesis message. The same Record
// object may be used by successive calls to Record, so it should not
// be retained.
func (s *Source) Record() (idk.Record, error) {
	msg, err := s.fetch()
	switch err {
	case nil:
	case io.EOF:
		return nil, io.EOF
	case context.DeadlineExceeded:
		return nil, idk.ErrFlush
	default:
		return nil, errors.Wrap(err, "failed to fetch record from Kinesis")
	}

	data, err := s.decodeMessage(msg.Data)
	if err != nil {
		return nil, errors.Wrap(err, "decoding message")
	}

	s.spool = append(s.spool, msg)

	return &Record{
		src:            s,
		shardID:        msg.ShardID,
		sequenceNumber: *msg.SequenceNumber,
		idx:            s.spoolBase + uint64(len(s.spool)),
		data:           data,
	}, err
}

func (s *Source) fetch() (ShardRecord, error) {
	ctx := context.Background()
	if s.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.Timeout)
		defer cancel()
	}

	return s.reader.FetchMessage(ctx)
}

func (s *Source) decodeMessage(buf []byte) ([]interface{}, error) {
	message := map[string]interface{}{}
	err := json.Unmarshal(buf, &message)
	if err != nil {
		jsonError, ok := err.(*json.SyntaxError)
		if ok {
			return nil, errors.Wrapf(err, "unmarshaling kafka message at character offset %v: %s", jsonError.Offset, string(buf))
		} else {
			return nil, errors.Wrapf(err, "unmarshaling kafka message at unknown character offset: %s", string(buf))
		}
	}

	return s.paths.Lookup(message, s.AllowMissingFields)
}

func (s *Source) Schema() []idk.Field {
	return s.schema
}

type Record struct {
	src            *Source
	shardID        string
	sequenceNumber string
	idx            uint64
	data           []interface{}
}

func (r *Record) StreamOffset() (string, uint64) {
	return fmt.Sprintf("%s:%s", r.src.StreamName, r.shardID), r.idx
}

var _ idk.OffsetStreamRecord = &Record{}

func (r *Record) Commit(ctx context.Context) error {
	idx, base := r.idx, r.src.spoolBase
	if idx < base {
		return errors.New("cannot commit a record that has already been committed")
	}

	section, remaining := r.src.spool[:idx-base], r.src.spool[idx-base:]
	err := r.src.reader.CommitMessages(ctx, section...)
	if err != nil {
		return errors.Wrap(err, "failed to commit messages")
	}

	r.src.spool = remaining
	r.src.spoolBase = idx

	return nil
}

func (r *Record) Data() []interface{} {
	return r.data
}

func (s *Source) initAWS() error {
	s.Log.Infof("Initializing AWS session")
	config := &aws.Config{
		// retry on ephemeral AWS errors
		Retryer: client.DefaultRetryer{NumMaxRetries: 10},
	}
	if len(s.AWSProfile) > 0 {
		s.Log.Infof("Overriding default AWS profile %s", s.AWSProfile)
		config.Credentials = credentials.NewSharedCredentials("", s.AWSProfile)
	}

	if len(s.AWSRegion) > 0 {
		s.Log.Infof("Overriding default AWS region: %s", s.AWSRegion)
		config.Region = aws.String(s.AWSRegion)
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return errors.Wrap(err, "creating AWS session")
	}
	s.session = sess
	s.s3client = s3.New(sess)
	s.kinesisClient = kinesis.New(sess)
	return nil
}

// Open initializes the Kinesis source.
func (s *Source) Open() error {
	// allow mocking of AWS dependencies in unit tests
	if s.s3client == nil || s.kinesisClient == nil {
		if err := s.initAWS(); err != nil {
			return err
		}
	}

	if len(s.Header) == 0 {
		return errors.New("missing required header file")
	}

	if len(s.OffsetsPath) == 0 {
		return errors.New("missing required offsets-path")
	}

	{
		headerData, err := internal.ReadFileOrURL(s.Header, s.s3client)
		if err != nil {
			return errors.Wrap(err, "reading header file")
		}

		schema, paths, err := idk.ParseHeader(headerData)
		if err != nil {
			return errors.Wrap(err, "processing header")
		}
		s.schema = schema
		s.paths = paths
	}

	if len(s.StreamName) == 0 {
		return errors.New("Missing required stream name parameter")
	}

	var err error
	s.reader, err = NewStreamReader(StreamReaderConfig{
		log:           s.Log,
		streamName:    s.StreamName,
		offsetsPath:   s.OffsetsPath,
		kinesisClient: s.kinesisClient,
		s3client:      s.s3client,
	})
	if err != nil {
		return errors.Wrap(err, "failed to start stream reader")
	}

	if err := s.reader.Start(); err != nil {
		return errors.Wrap(err, "failed to start stream reader")
	}
	return err
}

// Close closes the underlying Kinesis consumer.
func (s *Source) Close() error {
	s.reader.Close()
	return nil
}
