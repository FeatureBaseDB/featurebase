package datagen

import (
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/datagen/gen"
	"github.com/molecula/featurebase/v3/pql"
)

// Ensure KitchenSink implements interface.
var _ Sourcer = (*KitchenSink)(nil)

// KitchenSink implements Sourcer.
type KitchenSink struct {
	schema []idk.Field
}

// NewKitchenSink returns a new instance of KitchenSink.
func NewKitchenSink(cfg SourceGeneratorConfig) Sourcer {
	return &KitchenSink{
		schema: []idk.Field{
			idk.IDField{NameVal: "id"}, // 0
			idk.StringArrayField{NameVal: "set", CacheConfig: &idk.CacheConfig{CacheType: "lru", CacheSize: 1}}, // 1
			idk.IntField{NameVal: "int", Min: intptr(-9223372036854775807), Max: intptr(9223372036854775807)},   // 2
			idk.BoolField{NameVal: "bool"},                   // 3
			idk.StringField{NameVal: "time", Quantum: "YMD"}, // 4
			idk.StringField{NameVal: "mutex", Mutex: true, CacheConfig: &idk.CacheConfig{CacheType: "ranked", CacheSize: 500}}, // 5
			idk.StringField{NameVal: "string"},             // 6
			idk.DecimalField{NameVal: "decimal", Scale: 2}, // 7
		},
	}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (k *KitchenSink) Source(cfg SourceConfig) idk.Source {
	src := &KitchenSinkSource{
		cur:    cfg.startFrom,
		endAt:  cfg.endAt,
		schema: k.schema,
	}

	src.g = gen.New(gen.OptGenSeed(cfg.seed))
	src.record = make([]interface{}, len(src.schema))
	src.record[0] = uint64(0)
	src.record[6] = make([]byte, 12)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (k *KitchenSink) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (k *KitchenSink) DefaultEndAt() uint64 {
	return 20000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (k *KitchenSink) Info() string {
	return "Generates data to test everything but the kitchen sink."
}

// Ensure KitchenSinkSource implements interface.
var _ idk.Source = (*KitchenSinkSource)(nil)

// KitchenSinkSource is a data generator which generates
// data for all Pilosa field types.
type KitchenSinkSource struct {
	g *gen.Gen

	cur, endAt uint64

	schema []idk.Field
	record record
}

var endTime = time.Date(2020, time.May, 4, 12, 2, 28, 0, time.UTC)
var startTime = endTime.Add(-5 * 365 * 24 * time.Hour)
var timeSpan = endTime.Sub(startTime)

func (k *KitchenSinkSource) Record() (idk.Record, error) {
	if k.cur >= k.endAt {
		return nil, io.EOF
	}

	// Increment the ID.
	k.record[0] = k.cur
	k.cur++

	// Generate a random set field.
	set := k.g.Set(100, 1000, 10)
	vals := make([]string, 0, len(set))
	for v := range set {
		vals = append(vals, "v"+strconv.FormatUint(v, 10))
	}
	sort.Strings(vals)
	k.record[1] = vals

	// Generate a random int field.
	k.record[2] = ((2 * k.g.R.Int63n(2)) - 1) * k.g.R.Int63n(9223372036854775807)

	// Generate a random bool field.
	k.record[3] = k.g.R.Intn(2) == 0

	// Generate a random time field.
	k.record[4] = startTime.Add(time.Duration(k.g.R.Int63n(int64(timeSpan))))

	// Generate a random mutex value.
	k.record[5] = strconv.Itoa(k.g.R.Intn(20))

	// Generate a random string field.
	k.g.AlphaUpper(k.record[6].([]byte))

	// Generate a random floating point value.
	k.record[7] = pql.NewDecimal(k.g.R.Int63(), 2)

	return k.record, nil
}

func (k *KitchenSinkSource) Schema() []idk.Field {
	return k.schema
}

func (k *KitchenSinkSource) Close() error {
	return nil
}
