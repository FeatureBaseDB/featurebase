package datagen

import (
	"io"
	"math/rand"

	pilosaclient "github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Ensure Sizing implements interface.
var _ Sourcer = (*Sizing)(nil)

// Sizing implements Sourcer.
type Sizing struct{}

// NewSizing returns a new instance of Sizing.
func NewSizing(cfg SourceGeneratorConfig) Sourcer {
	return &Sizing{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (s *Sizing) Source(cfg SourceConfig) idk.Source {
	src := &SizingSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,
		rand:  rand.New(rand.NewSource(22)),
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},
			idk.IntField{NameVal: "eightbit_random"},
			idk.IntField{NameVal: "sixteenbit_random"},
			idk.IntField{NameVal: "thirtytwobit_random"},
			idk.IntField{NameVal: "sixtythreebit_random"},
			idk.IntField{NameVal: "eightbit_zipf"},
			idk.IntField{NameVal: "sixteenbit_zipf"},
			idk.IntField{NameVal: "thirtytwobit_zipf"},
			idk.IntField{NameVal: "sixtythreebit_zipf"},
		},
	}
	src.record = make([]interface{}, len(src.schema))
	src.zipf8 = rand.NewZipf(src.rand, 1.01, 4, 255)
	src.zipf16 = rand.NewZipf(src.rand, 1.01, 4, 65535)
	src.zipf32 = rand.NewZipf(src.rand, 1.01, 4, 4294967295)
	src.zipf63 = rand.NewZipf(src.rand, 1.01, 4, 9223372036854775807)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (s *Sizing) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (s *Sizing) DefaultEndAt() uint64 {
	return pilosaclient.DefaultShardWidth - 1 // one shard width
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (s *Sizing) Info() string {
	return "Generates data for estimating index size relative to original data size."
}

// Ensure SizingSource implements interface.
var _ idk.Source = (*SizingSource)(nil)

// SizingSource is an idk.Source meant to generate data which is
// helpful in determining the on-disk footprint of different types of
// data which can be extrapolated to help estimate necessary
// infrastructure size for various data. Typically one shard width of
// data is generated.
type SizingSource struct {
	Log logger.Logger

	cur, endAt uint64

	rand   *rand.Rand
	zipf8  *rand.Zipf
	zipf16 *rand.Zipf
	zipf32 *rand.Zipf
	zipf63 *rand.Zipf

	schema []idk.Field
	record record
}

func (s *SizingSource) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.cur
	s.record[1] = s.rand.Intn(255)
	s.record[2] = s.rand.Intn(65535)
	s.record[3] = s.rand.Intn(4294967295)
	s.record[4] = s.rand.Intn(9223372036854775807)
	s.record[5] = int64(s.zipf8.Uint64())
	s.record[6] = int64(s.zipf16.Uint64())
	s.record[7] = int64(s.zipf32.Uint64())
	s.record[8] = int64(s.zipf63.Uint64())
	s.cur++
	return s.record, nil
}

func (s *SizingSource) Schema() []idk.Field {
	return s.schema
}

func (s *SizingSource) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*SizingSource)(nil)

func (s *SizingSource) Close() error {
	return nil
}
