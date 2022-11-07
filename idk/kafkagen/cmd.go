package kafkagen

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/common"
	"github.com/featurebasedb/featurebase/v3/idk/kafka/csrc"
	liavro "github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
)

type Main struct {
	idk.ConfluentCommand `flag:"!embed"`
	SchemaFile           string
	Subject              string
	Topic                string
	DryRun               bool
	configMap            *confluent.ConfigMap
}

func NewMain() (*Main, error) {
	var err error
	m := &Main{ConfluentCommand: idk.ConfluentCommand{}}
	m.SchemaFile = "bigschema.json"
	m.Subject = "bigschema"
	m.SchemaRegistryURL = "localhost:8081"
	m.KafkaBootstrapServers = []string{"localhost:9092"}
	m.Topic = "defaulttopic"
	m.configMap, err = common.SetupConfluent(&m.ConfluentCommand)
	return m, err
}

func (m *Main) Run() (err error) {
	if m.DryRun {
		log.Printf("%+v\n", m)
		return nil
	}

	licodec, err := decodeSchema(m.SchemaFile)
	if err != nil {
		return errors.Wrap(err, "decoding schema")
	}
	schemaID, err := m.postSchema(m.SchemaFile, m.Subject)
	if err != nil {
		return errors.Wrap(err, "psting schema")
	}

	fields := []string{"abc", "db", "user_id", "all_users", "has_deleted_date", "central_group", "custom_audiences", "desktop_boolean", "desktop_frequency", "desktop_recency", "product_boolean_historical_forestry_cravings_or_bugles", "ddd_category_total_current_rhinocerous_checking", "ddd_category_total_current_rhinocerous_thedog_cheetah", "survey1234", "days_since_last_logon", "elephant_added_for_account"}

	// make a bunch of data and insert it
	records := [][]interface{}{
		{"2", "1", 159, map[string]interface{}{"boolean": true}, map[string]interface{}{"boolean": false}, map[string]interface{}{"string": "cgr"}, map[string]interface{}{"array": []string{"a", "b"}}, nil, map[string]interface{}{"int": 7}, nil, nil, map[string]interface{}{"float": 5.4}, nil, map[string]interface{}{"org.test.survey1234": "yes"}, map[string]interface{}{"float": 8.0}, nil},
	}

	// kakfa.SetupSasl(configMap *confluent.ConfigMap, SASLConfig SaslConfig) (err error) {

	p, err := confluent.NewProducer(m.configMap)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer p.Close()

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		cnt := 0
		for e := range p.Events() {
			switch ev := e.(type) {
			case *confluent.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				}
				cnt += 1
				if cnt%10000 == 0 {
					fmt.Printf("Delivered %v messages to topic %v\n", cnt, m.TopicPartition.Topic)
				}
				if cnt == len(records) {
					return
				}
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()

	for _, vals := range records {
		rec, err := makeRecord(fields, vals)
		if err != nil {
			return errors.Wrap(err, "making record")
		}
		err = m.putRecordKafka(p, schemaID, licodec, "akey", rec)
		if err != nil {
			return errors.Wrap(err, "putting record")
		}
	}
	<-doneChan // wait till all messages are acked
	p.Flush(10 * 1000)

	return nil
}

func (m *Main) putRecordKafka(p *confluent.Producer, schemaID int, schema *liavro.Codec, key string, record map[string]interface{}) error {
	buf := make([]byte, 5, 1000)
	buf[0] = 0
	binary.BigEndian.PutUint32(buf[1:], uint32(schemaID))
	buf, err := schema.BinaryFromNative(buf, record)
	if err != nil {
		return err
	}
	p.ProduceChannel() <- &confluent.Message{
		TopicPartition: confluent.TopicPartition{Topic: &m.Topic, Partition: confluent.PartitionAny},
		Key:            []byte(key),
		Value:          buf,
	}
	return nil
}

func readSchema(filename string) (string, error) {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func decodeSchema(filename string) (*liavro.Codec, error) {
	s, err := readSchema(filename)
	if err != nil {
		return nil, errors.Wrap(err, "reading schema")
	}
	codec, err := liavro.NewCodec(s)
	if err != nil {
		return nil, err
	}
	return codec, nil
}

func (m *Main) postSchema(schemaFile, subj string) (schemaID int, err error) {
	var auth *csrc.BasicAuth
	if m.SchemaRegistryUsername != "" {
		auth = &csrc.BasicAuth{
			KafkaSchemaApiKey:    m.SchemaRegistryUsername,
			KafkaSchemaApiSecret: m.SchemaRegistryPassword,
		}
	}
	schemaClient := csrc.NewClient("http://"+m.SchemaRegistryURL, nil, auth)
	schemaStr, err := readSchema(schemaFile)
	if err != nil {
		return 0, errors.Wrap(err, "reading schema file")
	}
	resp, err := schemaClient.PostSubjects(subj, schemaStr)
	if err != nil {
		return 0, errors.Wrap(err, "posting schema")
	}
	return resp.ID, nil
}

func makeRecord(fields []string, vals []interface{}) (map[string]interface{}, error) {
	if len(fields) != len(vals) {
		return nil, errors.Errorf("have %d fields and %d vals", len(fields), len(vals))
	}
	ret := make(map[string]interface{})
	for i, field := range fields {
		ret[field] = vals[i]
	}
	return ret, nil
}
