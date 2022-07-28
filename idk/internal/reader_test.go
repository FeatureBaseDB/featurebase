package internal

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestBlender(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewSource(10))
	readers := make(map[string]KafkaReader)
	var nmsgs int
	for _, topic := range []string{"x", "y", "z"} {
		partitions := rand.Intn(10) + 1
		offs := make([]int64, partitions)
		for i := range offs {
			offs[i] = rand.Int63n(1000000)
		}

		msgs := make([]kafka.Message, rand.Intn(500))
		for i := range msgs {
			partition := rng.Intn(len(offs))
			offset := offs[partition]
			offs[partition]++
			msgs[i] = kafka.Message{
				Topic:     topic,
				Partition: partition,
				Offset:    offset,
			}
		}
		nmsgs += len(msgs)

		readers[topic] = &KafkaTestReader{
			Queue: msgs,
		}
	}

	blended := BlendKafka(readers)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for nmsgs > 0 {
		batch := make([]kafka.Message, rand.Intn(nmsgs)+1)
		for i := range batch {
			msg, err := blended.FetchMessage(ctx)
			if err != nil {
				t.Fatalf("failed to fetch message: %v", err)
			}

			batch[i] = msg
		}

		err := blended.CommitMessages(ctx, batch...)
		if err != nil {
			t.Fatalf("failed to commit batch: %v", err)
		}

		nmsgs -= len(batch)
	}

	err := blended.Close()
	if err != nil {
		t.Fatalf("failed to close reader: %v", err)
	}

	for topic, r := range readers {
		r := r.(*KafkaTestReader)
		if r.FetchOff < len(r.Queue) {
			t.Errorf("found %d/%d unread messages in topic %q", r.FetchOff-len(r.Queue), len(r.Queue), topic)
		}
		if r.CommitOff < len(r.Queue) {
			t.Errorf("found %d/%d uncomitted messages in topic %q", r.CommitOff-len(r.Queue), len(r.Queue), topic)
		}
		if !r.Closed {
			t.Errorf("topic %s reader not closed", topic)
		}
	}
}
