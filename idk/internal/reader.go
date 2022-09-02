package internal

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
	segmentio "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

type KafkaReader interface {
	FetchMessage(ctx context.Context) (segmentio.Message, error)
	CommitMessages(ctx context.Context, msgs ...segmentio.Message) error
	io.Closer
}

func BlendKafka(in map[string]KafkaReader) KafkaReader {
	if len(in) == 1 {
		for _, r := range in {
			return r
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)

	ch := make(chan segmentio.Message, 10)
	live := uint32(len(in))
	for topic, r := range in {
		topic, r := topic, r
		group.Go(func() (err error) {
			defer func() {
				if atomic.AddUint32(&live, ^uint32(0)) == 0 {
					// This is the last reader to close.
					// Close the channel.
					close(ch)
				}
			}()

			defer func() {
				// Close the reader.
				cerr := r.Close()
				if err != nil {
					err = cerr
				}
			}()

			done := ctx.Done()
			for {
				msg, err := r.FetchMessage(ctx)
				switch err {
				case nil:
				case io.EOF, context.Canceled:
					return nil
				default:
					return errors.Wrapf(err, "failed to fetch message from topic %q", topic)
				}

				select {
				case ch <- msg:
				case <-done:
					return nil
				}
			}
		})
	}

	return &kafkaBlender{
		readers: in,
		cancel:  cancel,
		eg:      group,
		ch:      ch,
	}
}

type kafkaBlender struct {
	readers map[string]KafkaReader
	cancel  context.CancelFunc
	eg      *errgroup.Group
	ch      <-chan segmentio.Message
}

func (b *kafkaBlender) FetchMessage(ctx context.Context) (segmentio.Message, error) {
	if err := ctx.Err(); err != nil {
		return segmentio.Message{}, err
	}

	select {
	case msg, ok := <-b.ch:
		if !ok {
			return segmentio.Message{}, io.EOF
		}

		return msg, nil

	case <-ctx.Done():
		return segmentio.Message{}, ctx.Err()
	}
}

func (b *kafkaBlender) CommitMessages(ctx context.Context, msgs ...segmentio.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	msgsByTopic := make(map[string][]segmentio.Message, len(b.readers))
	for _, m := range msgs {
		msgsByTopic[m.Topic] = append(msgsByTopic[m.Topic], m)
	}

	group, ctx := errgroup.WithContext(ctx)
	for topic, msgs := range msgsByTopic {
		topic, msgs := topic, msgs
		group.Go(func() error {
			reader, ok := b.readers[topic]
			if !ok {
				return errors.Errorf("cannot commit messages: no source for topic %q", topic)
			}

			err := reader.CommitMessages(ctx, msgs...)
			if err != nil {
				return errors.Wrapf(err, "failed to commit messages to topic %q", topic)
			}

			return nil
		})
	}

	return group.Wait()
}

func (b *kafkaBlender) Close() error {
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}

	return b.eg.Wait()
}

// RetryReader wraps a kafka reader with fetch retry logic.
type RetryReader struct {
	*segmentio.Reader
}

func (r RetryReader) FetchMessage(ctx context.Context) (segmentio.Message, error) {
try:
	msg, err := r.Reader.FetchMessage(ctx)
	if err != nil {
		if err := ctx.Err(); err != nil {
			return segmentio.Message{}, err
		}
		if err == segmentio.RebalanceInProgress {
			goto try
		}
		if err, ok := err.(segmentio.Error); ok && err.Temporary() {
			goto try
		}

		return segmentio.Message{}, err
	}

	return msg, nil
}

// KafkaTestReader is a testing implementation of the KafkaReader type.
type KafkaTestReader struct {
	Queue               []segmentio.Message
	FetchOff, CommitOff int
	Closed              bool
}

func (r *KafkaTestReader) FetchMessage(ctx context.Context) (segmentio.Message, error) {
	if err := ctx.Err(); err != nil {
		return segmentio.Message{}, err
	}

	if r.Closed {
		return segmentio.Message{}, io.EOF
	}

	if r.FetchOff == len(r.Queue) {
		<-ctx.Done()
		return segmentio.Message{}, ctx.Err()
	}

	msg := r.Queue[r.FetchOff]
	r.FetchOff++

	return msg, nil
}

func (r *KafkaTestReader) CommitMessages(ctx context.Context, msgs ...segmentio.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	coff := r.CommitOff
	for _, m := range msgs {
		switch {
		case coff == len(r.Queue):
			return errors.New("cannot commit beyond end of data")

		case r.Queue[coff].Topic != m.Topic:
			return errors.Errorf("topic mismatch: expected %q but found %q", m.Topic, r.Queue[coff].Topic)
		case r.Queue[coff].Partition != m.Partition:
			return errors.Errorf("partition mismatch: expected %d but found %d", m.Partition, r.Queue[coff].Partition)
		case r.Queue[coff].Offset != m.Offset:
			return errors.Errorf("offset mismatch: expected %d but found %d", m.Offset, r.Queue[coff].Offset)

		default:
			coff++
		}
	}
	r.CommitOff = coff

	return nil
}

func (r *KafkaTestReader) Close() error {
	r.Closed = true
	return nil
}
