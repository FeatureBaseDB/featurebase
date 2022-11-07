package kafka

import (
	"context"
	"encoding/binary"
	"os"
	"time"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/common"
	"github.com/featurebasedb/featurebase/v3/idk/kafka/csrc"
	"github.com/featurebasedb/featurebase/v3/logger"
	liavro "github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
)

type PutCmd struct {
	idk.ConfluentCommand `flag:"!embed"`
	Topic                string `help:"Kafka topic to post to."`

	Schema     string
	SchemaFile string
	Subject    string

	Data string

	DryRun    bool                 `help:"Dry run - just flag parsing."`
	configMap *confluent.ConfigMap `flag:"-"`

	log               logger.Logger
	NumPartitions     int
	ReplicationFactor int
}

func (p *PutCmd) Log() logger.Logger { return p.log }

func NewPutCmd() (*PutCmd, error) {
	var err error
	p := PutCmd{}
	p.ConfluentCommand = idk.ConfluentCommand{}

	p.KafkaBootstrapServers = []string{"localhost:9092"}
	p.SchemaRegistryURL = "localhost:8081"
	p.Topic = "defaulttopic"
	p.Schema = `{"type": "record","namespace": "c.e","name": "F","fields": [{"name":"id","type":"long"},{"name":"a","type": "boolean"},{"name": "b", "type": "float", "scale": 2}]}`
	p.SchemaFile = ""
	p.Subject = "test"
	p.Data = `{"id": 1, "a": true, "b": 1.43}`
	p.log = logger.NewVerboseLogger(os.Stderr)

	return &p, err
}

func (p *PutCmd) Run() (err error) {
	p.configMap, err = common.SetupConfluent(&p.ConfluentCommand)
	if err != nil {
		return errors.Wrap(err, "setting up confluent")
	}
	var auth *csrc.BasicAuth
	if p.SchemaRegistryUsername != "" {
		auth = &csrc.BasicAuth{
			KafkaSchemaApiKey:    p.SchemaRegistryUsername,
			KafkaSchemaApiSecret: p.SchemaRegistryPassword,
		}
	}
	client := csrc.NewClient(p.SchemaRegistryURL, nil, auth)

	schemaStr, err := p.getSchema()
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}

	resp, err := client.PostSubjects(p.Subject, schemaStr)
	if err != nil {
		return errors.Wrap(err, "posting schema")
	}
	p.log.Printf("Posted schema ID: %d\n", resp.ID)

	licodec, err := liavro.NewCodec(schemaStr)
	if err != nil {
		return errors.Wrap(err, "li decoding schema")
	}

	rec, _, err := licodec.NativeFromTextual([]byte(p.Data))
	if err != nil {
		return errors.Wrap(err, "decoding data")
	}

	p.log.Debugf("Decoded: %v\n", rec)

	data, err := endcodeAvro(int(resp.ID), licodec, rec.(map[string]interface{}))
	if err != nil {
		return errors.Wrap(err, "encoding data")
	}

	// Create Producer instance
	pr, err := confluent.NewProducer(p.configMap)
	if err != nil {
		return errors.Wrap(err, "failed to create producer")
	}
	defer pr.Close()

	err = CreateKafkaTopic(context.Background(), p.Topic, pr, p.NumPartitions, p.ReplicationFactor)
	if err != nil {
		return errors.Wrap(err, "creating topic")
	}

	delivery_chan := make(chan confluent.Event, 10)

	err = pr.Produce(&confluent.Message{
		TopicPartition: confluent.TopicPartition{Topic: &p.Topic, Partition: confluent.PartitionAny},
		Value:          data,
	}, delivery_chan)

	e := <-delivery_chan
	m := e.(*confluent.Message)

	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	}
	close(delivery_chan)

	if err != nil {
		return errors.Wrap(err, "failed to write message to kafka")
	}

	return nil
}

func CreateKafkaTopic(ctx context.Context, topic string, p *confluent.Producer, numPartitions int, replicationFactor int) error {
	a, err := confluent.NewAdminClientFromProducer(p)
	if err != nil {
		return errors.Wrap(err, "failed to create new admin client from producer")
	}
	defer a.Close()
	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Create topics on cluster.
	// Set Admin options to wait up to 60s for the operation to finish on the remote cluster
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		return errors.Wrap(err, "ParseDuration(60s)")
	}
	// TODO: maybe add delete and retention period to config
	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]confluent.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		}},
		// Admin options
		confluent.SetAdminOperationTimeout(maxDur))
	if err != nil {
		return errors.Wrap(err, "admin client request error")
	}
	for _, result := range results {
		if result.Error.Code() != confluent.ErrNoError && result.Error.Code() != confluent.ErrTopicAlreadyExists {
			return errors.Wrap(errors.New(result.Error.Error()), "failed to create topic")
		}
	}
	return nil
}

func (p *PutCmd) getSchema() (string, error) {
	if p.Schema == "" && p.SchemaFile == "" {
		return "", errors.New("need a string schema or schema file")
	}
	if p.Schema != "" {
		return p.Schema, nil
	}
	bytes, err := os.ReadFile(p.SchemaFile)
	if err != nil {
		return "", errors.Wrap(err, "reading schema file")
	}
	return string(bytes), nil
}

func endcodeAvro(schemaID int, schema *liavro.Codec, record map[string]interface{}) ([]byte, error) {
	buf := make([]byte, 5, 1000)
	buf[0] = 0
	binary.BigEndian.PutUint32(buf[1:], uint32(schemaID))
	buf, err := schema.BinaryFromNative(buf, record)
	if err != nil {
		return nil, errors.Errorf("encoding %v: %v", record, err)
	}

	return buf, nil
}
