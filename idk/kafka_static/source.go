package kafka_static

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/internal"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"
	segmentio "github.com/segmentio/kafka-go"
)

// Source implements the idk.Source interface using kafka as a data
// source. It is not threadsafe! Due to the way Kafka clients work, to
// achieve concurrency, create multiple Sources.
type Source struct {
	Hosts    []string
	Topics   []string
	Group    string
	TLS      idk.TLSConfig
	Log      logger.Logger
	Timeout  time.Duration
	SkipOld  bool
	Header   string
	S3Region string

	AllowMissingFields bool

	schema []idk.Field
	paths  idk.PathTable

	reader internal.KafkaReader

	spoolBase uint64
	spool     []segmentio.Message
}

// NewSource gets a new Source
func NewSource() *Source {
	src := &Source{
		Hosts:  []string{"localhost:9092"},
		Topics: []string{"test"},
		Group:  "group0",
		Log:    logger.NopLogger,
	}

	return src
}

// Record returns the value of the next kafka message. The same Record
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
		return nil, errors.Wrap(err, "failed to fetch record from Kafka")
	}

	data, err := s.decodeMessage(msg.Value)
	if err != nil {
		return nil, errors.Wrap(err, "decoding with schema registry")
	}

	s.spool = append(s.spool, msg)

	return &Record{
		src:       s,
		topic:     msg.Topic,
		partition: msg.Partition,
		offset:    msg.Offset,
		idx:       s.spoolBase + uint64(len(s.spool)),
		data:      data,
	}, err
}

func (s *Source) fetch() (segmentio.Message, error) {
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
	src       *Source
	topic     string
	partition int
	offset    int64
	idx       uint64
	data      []interface{}
}

func (r *Record) StreamOffset() (string, uint64) {
	return r.topic + ":" + strconv.Itoa(r.partition), uint64(r.offset)
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

// Open initializes the kafka source.
func (s *Source) Open() error {
	if len(s.Header) == 0 {
		return errors.New("needs header specification file")
	}

	{
		headerData, err := s.readFileOrURL(s.Header)
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

	// init (custom) config, enable errors and notifications
	config := segmentio.ReaderConfig{
		Brokers:     s.Hosts,
		GroupID:     s.Group,
		Logger:      segmentio.LoggerFunc(s.Log.Debugf),
		ErrorLogger: s.Log,
	}
	if s.SkipOld {
		config.StartOffset = segmentio.LastOffset
	}

	if s.TLS.CertificatePath != "" {
		tlsConfig, err := idk.GetTLSConfig(&s.TLS, s.Log)
		if err != nil {
			return errors.Wrap(err, "getting TLS config")
		}

		config.Dialer = &segmentio.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
			TLS:       tlsConfig,
		}
	}

	readers := make(map[string]internal.KafkaReader, len(s.Topics))
	for _, topic := range s.Topics {
		config := config
		config.Topic = topic
		readers[topic] = internal.RetryReader{segmentio.NewReader(config)} //nolint:govet
	}

	// Throw the readers into a blender.
	s.reader = internal.BlendKafka(readers)

	// TODO: dump stats

	return nil
}

// Close closes the underlying kafka consumer.
func (s *Source) Close() error {
	err := s.reader.Close()
	return errors.Wrap(err, "closing kafka consumer")
}

func (s *Source) readFileOrURL(name string) ([]byte, error) {
	var content []byte
	var err error
	if strings.HasPrefix(name, "s3://") {
		u, err := url.Parse(name)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing S3 URL %v", name)
		}
		bucket := u.Host
		key := u.Path[1:] // strip leading slash

		config := &aws.Config{}
		if s.S3Region != "" {
			config.Region = aws.String(s.S3Region)
			// else, NewSession will use the default region.
		}
		sess, err := session.NewSession(config)
		if err != nil {
			return nil, errors.Wrap(err, "creating S3 session")
		}

		s3client := s3.New(sess)

		result, err := s3client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, errors.Wrapf(err, "fetching S3 object %v", name)
		}

		buf := new(bytes.Buffer)
		bytesRead, err := buf.ReadFrom(result.Body)
		if err != nil {
			return nil, errors.Wrapf(err, "reading S3 object %v", name)
		}
		s.Log.Printf("read %d bytes from %s\n", bytesRead, name)
		content = buf.Bytes()
	} else {
		content, err = ioutil.ReadFile(name)
		if err != nil {
			return nil, errors.Wrapf(err, "reading file %v", name)
		}
	}
	return content, nil
}
