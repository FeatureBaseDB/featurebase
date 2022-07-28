package datagen

import (
	"fmt"
	"io"
	"math/rand"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Warranty implements interface.
var _ Sourcer = (*Warranty)(nil)

// Warranty implements Sourcer.
type Warranty struct{}

// NewWarranty returns a new instance of Warranty.
func NewWarranty(cfg SourceGeneratorConfig) Sourcer {
	return &Warranty{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (w *Warranty) Source(cfg SourceConfig) idk.Source {
	min := int64(0)
	src := &WarrantySource{
		startFrom: cfg.startFrom,
		endAt:     cfg.endAt,
		rand:      rand.New(rand.NewSource(19)),
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},
			idk.IDField{NameVal: "type"},
			idk.StringArrayField{NameVal: "week_active"},
			idk.IntField{NameVal: "start", Min: &min},
			idk.IntField{NameVal: "end", Min: &min},
			idk.IntField{NameVal: "cust_id", Min: &min},
			idk.IntField{NameVal: "item_id", Min: &min},
		},
	}
	src.typeZipf = rand.NewZipf(src.rand, 1.03, 5, 100)
	src.custZipf = rand.NewZipf(src.rand, 1.01, 4, 100000000)

	src.record = make([]interface{}, len(src.schema))
	src.record[2] = make([]string, 52*5)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (w *Warranty) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (w *Warranty) DefaultEndAt() uint64 {
	return 2147483648
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (w *Warranty) Info() string {
	return "TODO"
}

// Ensure WarrantySource implements interface.
var _ idk.Source = (*WarrantySource)(nil)

type WarrantySource struct {
	Log logger.Logger

	startFrom uint64
	endAt     uint64

	rand     *rand.Rand
	typeZipf *rand.Zipf
	custZipf *rand.Zipf

	schema []idk.Field
	record record
}

func (s *WarrantySource) Record() (idk.Record, error) {
	if s.startFrom >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.startFrom
	s.startFrom++
	s.record[1] = s.typeZipf.Uint64()
	startDay := s.rand.Intn(daysSinceStart)
	days := numDays(s.rand)
	numWeeks := days / 7
	s.record[2] = s.record[2].([]string)[:numWeeks]
	populateActiveWeeks(s.record[2].([]string), startDay)
	s.record[3] = startDay
	s.record[4] = startDay + days
	s.record[5] = int64(s.custZipf.Uint64()) // 100M customers
	s.record[6] = s.rand.Intn(1500000000)    // 1.5B items
	return s.record, nil
}

func (s *WarrantySource) Schema() []idk.Field {
	return s.schema
}

func (s *WarrantySource) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*WarrantySource)(nil)

func numDays(r *rand.Rand) int {
	num := r.Intn(10)
	if num < 6 {
		return 365
	}
	if num < 8 {
		return 365 * 3
	}
	if num < 9 {
		return 365 * 2
	}
	return 365 * 5
}

func populateActiveWeeks(week_active []string, startDay int) {
	startWeek := startDay / 7
	startYear := startWeek / 52
	startWeek = startWeek%52 + 1
	startYear += 2015
	for i := range week_active {
		week_active[i] = fmt.Sprintf("%d-%d", startYear, startWeek)
		startWeek++
		if startWeek > 52 {
			startWeek = 1
			startYear++
		}
	}
}

func (s *WarrantySource) Close() error {
	return nil
}
