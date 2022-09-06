package datagen

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/datagen/gen"
)

// Ensure KitchenSinkKeyed implements interface.
var _ Sourcer = (*KitchenSinkKeyed)(nil)

// KitchenSinkKeyed implements Sourcer.
type KitchenSinkKeyed struct {
	schema []idk.Field
}

// NewKitchenSinkKeyed returns a new instance of KitchenSinkKeyed.
func NewKitchenSinkKeyed(cfg SourceGeneratorConfig) Sourcer {
	return &KitchenSinkKeyed{
		schema: []idk.Field{
			idk.StringField{NameVal: "pk"}, // 0
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
func (k *KitchenSinkKeyed) Source(cfg SourceConfig) idk.Source {
	src := &KitchenSinkKeyedSource{
		cur:    cfg.startFrom,
		endAt:  cfg.endAt,
		schema: k.schema,
	}

	src.g = gen.New(gen.OptGenSeed(cfg.seed))
	src.record = make([]interface{}, len(src.schema))
	src.record[0] = make([]byte, sha256.Size*(8/4))
	src.record[6] = make([]byte, 12)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (k *KitchenSinkKeyed) PrimaryKeyFields() []string {
	return []string{"pk"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (k *KitchenSinkKeyed) DefaultEndAt() uint64 {
	return 20000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (k *KitchenSinkKeyed) Info() string {
	return "Generates data to test everything but the kitchen sink. With Keys."
}

// Ensure KitchenSinkKeyedSource implements interface.
var _ idk.Source = (*KitchenSinkKeyedSource)(nil)

type KitchenSinkKeyedSource struct {
	g *gen.Gen

	cur, endAt uint64

	schema []idk.Field
	record record
}

func NewKitchenSinkKeyedSource(start, end uint64) *KitchenSinkKeyedSource {
	src := &KitchenSinkKeyedSource{
		cur:   start,
		endAt: end,

		schema: []idk.Field{
			idk.StringField{NameVal: "pk"}, // 0
			idk.StringArrayField{NameVal: "set", CacheConfig: &idk.CacheConfig{CacheType: "lru", CacheSize: 1}}, // 1
			idk.IntField{NameVal: "int", Min: intptr(-9223372036854775807), Max: intptr(9223372036854775807)},   // 2
			idk.BoolField{NameVal: "bool"},                   // 3
			idk.StringField{NameVal: "time", Quantum: "YMD"}, // 4
			idk.StringField{NameVal: "mutex", Mutex: true, CacheConfig: &idk.CacheConfig{CacheType: "ranked", CacheSize: 500}}, // 5
			idk.StringField{NameVal: "string"},             // 6
			idk.DecimalField{NameVal: "decimal", Scale: 2}, // 7
		},
	}

	src.record = make([]interface{}, len(src.schema))
	src.record[0] = make([]byte, sha256.Size*(8/4))
	src.record[6] = make([]byte, 12)

	return src

}

func (k *KitchenSinkKeyedSource) Record() (idk.Record, error) {
	if k.cur >= k.endAt {
		return nil, io.EOF
	}

	// Generate a unique string key.
	var nbytes [64 / 8]byte
	binary.LittleEndian.PutUint64(nbytes[:], k.cur)
	hash := sha256.Sum256(nbytes[:])
	hex.Encode(k.record[0].([]byte), hash[:])
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
	k.record[7] = float64(k.g.R.Int63()) / 100

	return k.record, nil
}

func (k *KitchenSinkKeyedSource) Schema() []idk.Field {
	return k.schema
}

func (k *KitchenSinkKeyedSource) Close() error {
	return nil
}
