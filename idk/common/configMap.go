package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"sync/atomic"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/pkg/errors"
)

// LaunchKafkaEventConfirmer consumes the events channel for the messages Produced by the producer

func LaunchKafkaEventConfirmer(producer *confluent.Producer, finished *int32, iter *int64) chan struct{} {
	doneChan := make(chan struct{})
	go func() {
		defer func() {
			close(doneChan)
		}()
		cnt := int64(0)
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *confluent.Message:
				cnt += 1
				msg := ev
				if msg.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", msg.TopicPartition.Error)
				}
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
			check := atomic.LoadInt32(finished)
			if check > 0 {
				sent := atomic.LoadInt64(iter)
				if cnt >= sent {
					doneChan <- struct{}{}
				}
			}
		}
	}()
	return doneChan
}
func SetupConfluent(m *idk.ConfluentCommand) (*confluent.ConfigMap, error) {
	var err error
	configMap := &confluent.ConfigMap{}
	if m.KafkaConfiguration != "" {
		file, er := ioutil.ReadFile(m.KafkaConfiguration)
		if er != nil {
			return nil, er
		}
		er = json.Unmarshal([]byte(file), configMap)
		if er != nil {
			return nil, er
		}
	}
	err = configMap.SetKey("enable.auto.commit", false)
	if err != nil {
		return nil, errors.Wrap(err, "disabling auto commit")
	}
	if m.KafkaSocketTimeoutMs > 0 {
		err = configMap.SetKey("socket.timeout.ms", m.KafkaSocketTimeoutMs)
		if err != nil {
			return nil, errors.Wrap(err, "setting socket timeout")
		}
	} else {
		err = configMap.SetKey("socket.timeout.ms", 60000)
		if err != nil {
			return nil, errors.Wrap(err, "setting socket timeout")
		}
	}
	if len(m.KafkaBootstrapServers) > 0 {
		err = configMap.SetKey("bootstrap.servers", strings.Join(m.KafkaBootstrapServers, ","))
		if err != nil {
			return nil, errors.Wrap(err, "setting bootstrap")
		}

	} else {
		err = configMap.SetKey("bootstrap.servers", "localhost:9092")
		if err != nil {
			return nil, errors.Wrap(err, "setting bootstrap")
		}
	}

	//SSL
	if m.KafkaSslCaLocation != "" {
		err = configMap.SetKey("ssl.ca.location", m.KafkaSslCaLocation)
		if err != nil {
			return nil, err
		}
	}
	if m.KafkaSslKeyLocation != "" {
		err = configMap.SetKey("ssl.key.location", m.KafkaSslKeyLocation)
		if err != nil {
			return nil, err
		}
	}
	if m.KafkaSslKeyPassword != "" {
		err = configMap.SetKey("ssl.key.password", m.KafkaSslKeyPassword)
		if err != nil {
			return nil, err
		}
	}
	if m.KafkaSslCertificateLocation != "" {
		err = configMap.SetKey("ssl.certificate.location", m.KafkaSslCertificateLocation)
		if err != nil {
			return nil, err
		}
	}
	err = configMap.SetKey("enable.ssl.certificate.verification", m.KafkaEnableSslCertificateVerification)
	if err != nil {
		return nil, err
	}

	if m.KafkaSslEndpointIdentificationAlgorithm != "" {
		err = configMap.SetKey("ssl.endpoint.identification.algorithm", m.KafkaSslEndpointIdentificationAlgorithm)
		if err != nil {
			return nil, err
		}
	}

	//SSL
	if m.KafkaSslCaLocation != "" {
		err = configMap.SetKey("ssl.ca.location", m.KafkaSslCaLocation)
		if err != nil {
			return nil, err
		}
	}
	if m.KafkaSslKeyLocation != "" {
		err = configMap.SetKey("ssl.key.location", m.KafkaSslKeyLocation)
		if err != nil {
			return nil, err
		}
	}
	if m.KafkaSslCertificateLocation != "" {
		err = configMap.SetKey("ssl.certificate.location", m.KafkaSslCertificateLocation)
		if err != nil {
			return nil, err
		}
	}
	err = configMap.SetKey("enable.ssl.certificate.verification", m.KafkaEnableSslCertificateVerification)
	if err != nil {
		return nil, err
	}

	if m.KafkaSslEndpointIdentificationAlgorithm != "" {
		err = configMap.SetKey("ssl.endpoint.identification.algorithm", m.KafkaSslEndpointIdentificationAlgorithm)
		if err != nil {
			return nil, err
		}
	}
	// SASL,
	if m.KafkaSaslUsername != "" && m.KafkaSaslPassword != "" {
		err = configMap.SetKey("sasl.username", m.KafkaSaslUsername)
		if err != nil {
			return nil, err
		}
		err = configMap.SetKey("sasl.password", m.KafkaSaslPassword)
		if err != nil {
			return nil, err
		}
	}
	if m.KafkaSaslMechanism != "" {
		err = configMap.SetKey("sasl.mechanism", m.KafkaSaslMechanism)
		if err != nil {
			return nil, err
		}
	}
	if m.KafkaSecurityProtocol != "" {
		err = configMap.SetKey("security.protocol", m.KafkaSecurityProtocol)
		if err != nil {
			return nil, err
		}
	}
	if m.KafkaClientId != "" {
		err = configMap.SetKey("client.id", m.KafkaClientId)
		if err != nil {
			return nil, err
		}
	}
	if m.KafkaDebug != "" {
		err = configMap.SetKey("debug", m.KafkaDebug)
		if err != nil {
			return nil, err
		}
	}

	return configMap, nil
}
