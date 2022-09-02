//go:build kafka_sasl
// +build kafka_sasl

package kafka_sasl

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/common"
)

func configureSourceTestFlags(source *Source) {
	source.ConfluentCommand = configMapSource
}

var tests = []struct {
	name   string
	header string
	data   []string
	exp    [][]interface{}
}{
	{
		name:   "Flat",
		header: "../kafka_static/testdata/Flat.json",
		data:   []string{`{"first": "hello"}`},
		exp:    [][]interface{}{{"hello"}},
	},
	{
		name:   "Tree",
		header: "../kafka_static/testdata/Tree.json",
		data:   []string{`{"protocol": "TCP", "port": 22, "client": "127.0.0.1", "server": "127.0.0.1", "metadata": {"suspicious": true, "region": "North America"}}`},
		exp: [][]interface{}{
			{"TCP", 22.0, "127.0.0.1", "127.0.0.1", true, "North America"},
		},
	},
}

// TestKafkaStaticSourceIntegration uses a real Kafka, and a static schema.
func TestSaslKafkaStaticSourceIntegration(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	rand.Seed(time.Now().UnixNano())
	a := rand.Int()

	key := fmt.Sprintf("%d", a)
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			// t.Parallel()

			topic := "sasl_testKafkaStaticSourceIntegration" + test.name + strconv.Itoa(a)

			// create topic before openning the Source
			adminClient := tCreateConfluentAdmin(t)
			tCreateConfluentTopic(t, adminClient, topic)
			defer adminClient.Close()

			src := NewSource()
			defer src.Close()
			configureSourceTestFlags(src)
			src.Topics = []string{topic}
			src.Group = "group0"
			src.Header = test.header
			cfg, err := common.SetupConfluent(&configMapSource)
			if err != nil {
				t.Fatal(err)
			}
			src.ConfigMap = cfg

			err = src.Open()
			if err != nil {
				t.Fatalf("opening source: %v", err)
			}

			producer := tCreateConfluentProducer(t)

			for j, record := range test.data {
				tWriteConfluentMessage(t, producer, topic, key, record)

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
			defer producer.Close()
		})
	}

}

func TestSaslPathTable(t *testing.T) {
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
			name:               "Check Nil Delete",
			allowMissingFields: true,
			table: idk.PathTable{
				{"x"},
				{"y"},
			},
			in: map[string]interface{}{
				"x": nil,
			},
			out: []interface{}{
				idk.DELETE_SENTINEL,
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
			// t.Parallel()
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
