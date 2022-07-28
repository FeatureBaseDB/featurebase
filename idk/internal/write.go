package internal

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/pkg/errors"
	segmentio "github.com/segmentio/kafka-go"
)

func CreateKafkaTopic(topic string, addr net.Addr) (err error) {
	conn, err := segmentio.Dial(addr.Network(), addr.String())
	if err != nil {
		return err
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing kafka connection")
		}
	}()

	controller, err := conn.Controller()
	if err != nil {
		return errors.Wrap(err, "finding kafka controller")
	}
	var controllerConn *segmentio.Conn
	controllerConn, err = segmentio.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return errors.Wrap(err, "connecting to kafka controller")
	}
	defer func() {
		if cerr := controllerConn.Close(); cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing kafka controller connection")
		}
	}()

	err = controllerConn.CreateTopics(segmentio.TopicConfig{
		Topic:             topic,
		NumPartitions:     64,
		ReplicationFactor: 1,
	})
	if err != nil {
		return err
	}

	return nil
}

func KafkaWriteWithBackoff(ctx context.Context, writer *segmentio.Writer, log func(string, ...interface{}), interval, maxInterval time.Duration, messages ...segmentio.Message) error {
	var berr error
	tries := 0
retry:
	for {
		tries++
		err := writer.WriteMessages(ctx, messages...)
		switch err := err.(type) {
		case nil:
			return nil

		case segmentio.Error:
			berr = err
			if !err.Temporary() {
				break retry
			}

		case segmentio.WriteErrors:
			var remaining []segmentio.Message
			for i, m := range messages {
				switch err := err[i].(type) {
				case nil:
					continue

				case segmentio.Error:
					if err.Temporary() {
						remaining = append(remaining, m)
						continue
					}
				}

				return errors.Wrap(err, "failed to deliver messages")
			}

			messages = remaining
			berr = err

		default:
			if berr == nil || err != context.DeadlineExceeded {
				berr = err
			}
			break retry
		}

		log("temporary write error: %v", err)

		interval *= 2
		if interval > maxInterval {
			interval = maxInterval
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			break retry
		}
	}

	return errors.Wrapf(berr, "failed to deliver messages after %d tries", tries)
}
