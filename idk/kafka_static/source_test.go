//go:build !kafka_sasl
// +build !kafka_sasl

package kafka_static

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/internal"
	"github.com/segmentio/kafka-go"
)

func configureSourceTestFlags(source *Source) {
	source.Hosts = []string{kafkaHost}
}

var tests = []struct {
	name   string
	header string
	data   []string
	exp    [][]interface{}
}{
	{
		name:   "Flat",
		header: "testdata/Flat.json",
		data:   []string{`{"first": "hello"}`},
		exp:    [][]interface{}{{"hello"}},
	},
	{
		name:   "Tree",
		header: "testdata/Tree.json",
		data:   []string{`{"protocol": "TCP", "port": 22, "client": "127.0.0.1", "server": "127.0.0.1", "metadata": {"suspicious": true, "region": "North America"}}`},
		exp: [][]interface{}{
			{"TCP", 22.0, "127.0.0.1", "127.0.0.1", true, "North America"},
		},
	},
}

func TestKafkaStaticSourceLocal(t *testing.T) {
	t.Parallel()

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			msgs := make([]kafka.Message, len(test.data))
			for i, rec := range test.data {
				msgs[i] = kafka.Message{
					Topic:     "test",
					Partition: 0,
					Offset:    int64(i),

					Value: []byte(rec),

					Time: time.Now(),
				}
			}

			src := NewSource()
			configureSourceTestFlags(src)
			{
				headerData, err := os.ReadFile(test.header)
				if err != nil {
					t.Fatalf("reading header file: %v", err)
				}
				schema, paths, err := idk.ParseHeader(headerData)
				if err != nil {
					t.Fatalf("processing header: %v", err)
				}
				src.schema = schema
				src.paths = paths
			}
			reader := &internal.KafkaTestReader{Queue: msgs}
			src.reader = reader

			for _, exp := range test.exp {
				rec, err := src.Record()
				if err != nil {
					t.Fatalf("unexpected error getting record: %v", err)
				}
				if rec == nil {
					t.Fatalf("should have a record")
				}

				data := rec.Data()
				if !reflect.DeepEqual(data, exp) {
					t.Errorf("data mismatch exp/got:\n%+v\n%+v", exp, data)
					if len(data) != len(exp) {
						t.Fatalf("mismatched lengths exp/got %d/%d", len(exp), len(data))
					}
					for j := range exp {
						if !reflect.DeepEqual(exp[j], data[j]) {
							t.Errorf("Mismatch at %d, exp/got\n%v of %[2]T\n%v of %[3]T", j, exp[j], data[j])
						}
					}
				}
			}
		})
	}
}

// TestKafkaStaticSourceIntegration uses a real Kafka, and a static schema.
func TestKafkaStaticSourceIntegration(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	key := fmt.Sprintf("%d", rnd.Int())
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			src := NewSource()
			configureSourceTestFlags(src)
			src.Topics = []string{"testKafkaStaticSourceIntegration" + test.name}
			src.Group = "group0"
			src.Header = test.header
			err := src.Open()
			if err != nil {
				t.Fatalf("opening source: %v", err)
			}

			addr := kafka.TCP(kafkaHost)
			tCreateTopic(t, src.Topics[0], addr)
			writer := &kafka.Writer{
				Addr:         addr,
				Topic:        src.Topics[0],
				Balancer:     &kafka.Hash{},
				BatchTimeout: time.Nanosecond,
			}
			defer writer.Close()

			for j, record := range test.data {
				tPutStringKafka(t, writer, key, record)

				rec, err := src.Record()
				if err != nil {
					t.Fatalf("unexpected error getting record: %v", err)
				}
				if rec == nil {
					t.Fatalf("should have a record")
				}

				data := rec.Data()
				if !reflect.DeepEqual(data, test.exp[j]) {
					t.Errorf("data mismatch exp/got:\n%+v\n%+v", test.exp[j], data)
					if len(data) != len(test.exp[j]) {
						t.Fatalf("mismatched lengths exp/got %d/%d", len(test.exp[j]), len(data))
					}
					for k := range test.exp[j] {
						if !reflect.DeepEqual(test.exp[j][k], data[k]) {
							t.Errorf("Mismatch at %d, exp/got\n%v of %[2]T\n%v of %[3]T", k, test.exp[j][k], data[k])
						}
					}
				}
			}
		})
	}
}

func tCreateTopic(t *testing.T, topic string, addr net.Addr) {
	t.Helper()

	err := internal.CreateKafkaTopic(topic, addr)
	if err != nil {
		t.Fatalf("creating kafka topic %q: %v", topic, err)
	}
}

func tPutStringKafka(t *testing.T, writer *kafka.Writer, key, value string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := internal.KafkaWriteWithBackoff(ctx, writer, t.Logf, 100*time.Millisecond, time.Second, kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	})
	if err != nil {
		t.Fatalf("writing record to kafka: %v", err)
	}
}

func TestPathTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name               string
		table              idk.PathTable
		allowMissingFields bool
		in                 interface{}
		out                []interface{}
		err                string
	}{
		{
			name: "nil",
			out:  []interface{}{},
		},
		{
			name: "Root",
			table: idk.PathTable{
				{},
			},
			in: 1,
			out: []interface{}{
				1,
			},
		},
		{
			name: "Flat",
			table: idk.PathTable{
				{"x"},
				{"y"},
			},
			in: map[string]interface{}{
				"x": 1,
				"y": 2,
			},
			out: []interface{}{
				1,
				2,
			},
		},
		{
			name: "Missing",
			table: idk.PathTable{
				{"x"},
				{"y"},
			},
			in: map[string]interface{}{
				"x": 1,
			},
			err: "failed to lookup path [y]: element \"y\" at [] is missing",
		},
		{
			name:               "Missing_Allowed",
			allowMissingFields: true,
			table: idk.PathTable{
				{"x"},
				{"y"},
			},
			in: map[string]interface{}{
				"x": 1,
			},
			out: []interface{}{
				1,
				nil,
			},
		},
		{
			name: "Tree",
			table: idk.PathTable{
				{"x", "y"},
				{"y", "z", "a"},
			},
			in: map[string]interface{}{
				"x": map[string]interface{}{
					"y": []interface{}{1, 2, 3},
					"z": map[string]interface{}{
						"h": "?",
					},
				},
				"y": map[string]interface{}{
					"z": map[string]interface{}{
						"a": "b",
					},
				},
			},
			out: []interface{}{
				[]interface{}{1, 2, 3},
				"b",
			},
		},
		{
			name: "TreeMissing",
			table: idk.PathTable{
				{"x", "y"},
				{"y", "z", "b"},
			},
			in: map[string]interface{}{
				"x": map[string]interface{}{
					"y": []interface{}{1, 2, 3},
					"z": map[string]interface{}{
						"h": "?",
					},
				},
				"y": map[string]interface{}{
					"z": map[string]interface{}{
						"a": "b",
					},
				},
			},
			err: "failed to lookup path [y z b]: element \"b\" at [y z] is missing",
		},
		{
			name: "TreeMissing_Allowed",
			table: idk.PathTable{
				{"x", "y"},
				{"a", "b", "c"},
			},
			allowMissingFields: true,
			in: map[string]interface{}{
				"x": map[string]interface{}{
					"y": []interface{}{1, 2, 3},
					"z": map[string]interface{}{
						"h": "?",
					},
				},
				"a": map[string]interface{}{
					"c": "words",
				},
			},
			out: []interface{}{
				[]interface{}{1, 2, 3},
				nil,
			},
		},
		{
			name: "WrongNodeType",
			table: idk.PathTable{
				{"x", "y"},
			},
			in: map[string]interface{}{
				"x": []interface{}{"z"},
			},
			err: "failed to lookup path [x y]: element at [x] is not an object",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			out, err := c.table.Lookup(c.in, c.allowMissingFields)
			if err != nil {
				switch {
				case c.err == "":
					t.Errorf("unexpected error: %v", err)
				case c.err != err.Error():
					t.Errorf("expected error %q but got %q", c.err, err.Error())
				}
			} else {
				switch {
				case c.err != "":
					t.Error("unexpected success")
				case !reflect.DeepEqual(out, c.out):
					t.Errorf("expected %v but got %v", c.out, out)
				}
			}
		})
	}
}
