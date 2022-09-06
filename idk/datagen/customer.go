package datagen

import (
	"io"
	"math/rand"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/logger"
)

// Ensure Customer implements interface.
var _ Sourcer = (*Customer)(nil)

// Customer implements Sourcer.
type Customer struct{}

// NewCustomer returns a new instance of Customer.
func NewCustomer(cfg SourceGeneratorConfig) Sourcer {
	return &Customer{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (c *Customer) Source(cfg SourceConfig) idk.Source {
	src := &CustomerSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,
		rand:  rand.New(rand.NewSource(22)),
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},
			idk.StringField{NameVal: "region"},
			idk.IntField{NameVal: "size"},
			idk.IDField{NameVal: "sales_channel"},
		},
	}
	src.record = make([]interface{}, len(src.schema))

	src.regionZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(regions))-1)

	src.record[0] = cfg.startFrom

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (c *Customer) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (c *Customer) DefaultEndAt() uint64 {
	return 100 // TODO: what should this be?
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (c *Customer) Info() string {
	return "TODO"
}

// Ensure CustomerSource implements interface.
var _ idk.Source = (*CustomerSource)(nil)

type CustomerSource struct {
	Log logger.Logger

	cur, endAt uint64

	rand       *rand.Rand
	regionZipf *rand.Zipf
	schema     []idk.Field
	record     record
}

var regions = []string{"US", "Canada", "Central America", "South America", "Western Europe", "Eastern Europe", "Africa", "China", "Japan", "Russia", "Middle East", "Pacific Islands", "Antarctica"}

func (s *CustomerSource) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.cur
	s.record[1] = regions[s.regionZipf.Uint64()]
	s.record[2] = s.rand.Intn(24990) + 10
	s.record[3] = uint64(s.rand.Intn(500))
	s.cur++
	return s.record, nil
}

func (s *CustomerSource) Schema() []idk.Field {
	return s.schema
}

func (s *CustomerSource) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*CustomerSource)(nil)

func (s *CustomerSource) Close() error {
	return nil
}
