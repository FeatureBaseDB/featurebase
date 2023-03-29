package internal

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/featurebasedb/featurebase/v3/errors"
)

func ProduceMessages(topic string, bootstrapServers string, messages [][]byte) error {

	// create producer, defer closing
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return errors.Errorf("creating producer: %s", err)
	}
	defer p.Close()

	// create admin client needed to create topic, defer closing
	ac, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return errors.Errorf("creating admin client: %s", err)
	}
	defer ac.Close()

	// create a topic
	var ts = []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}
	ac.CreateTopics(context.Background(), ts, nil)
	// caller must delete if needed

	for _, message := range messages {
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          message,
		}, nil)
		if err != nil {
			return errors.Errorf("producing messages: %s", err)
		}
		p.Flush(3000)
	}

	return nil

}

func DeleteTopic(topic string, bootstrapServers string) error {
	// create admin client needed to create topic, defer closing
	ac, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return errors.Errorf("creating admin client: %s", err)
	}
	defer ac.Close()

	// delete a topic, catch any errors
	results, err := ac.DeleteTopics(context.Background(), []string{topic}, nil)
	if err != nil {
		return errors.Errorf("deleting topic: %s", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return errors.Errorf("fatal error deleting topic: %s", result.String())
		}
	}

	return nil

}
