package datagen

import (
	"io"
	"math/rand"
	"time"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Item implements interface.
var _ Sourcer = (*Item)(nil)

// Item implements Sourcer.
type Item struct{}

// NewItem returns a new instance of Item.
func NewItem(cfg SourceGeneratorConfig) Sourcer {
	return &Item{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (i *Item) Source(cfg SourceConfig) idk.Source {
	min := int64(0)
	src := &ItemSource{
		rand:      rand.New(rand.NewSource(19)),
		startFrom: cfg.startFrom,
		endAt:     cfg.endAt,
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},
			idk.IntField{NameVal: "cust_id"},
			idk.DecimalField{NameVal: "cost", Scale: 2},
			idk.IntField{NameVal: "ship_date", Min: &min},
			idk.StringField{NameVal: "pli"},
			idk.IDField{NameVal: "prod"},
		},
	}
	src.pliZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(plis))-1)
	src.prodZipf = rand.NewZipf(src.rand, 1.01, 4, 3200)
	src.custZipf = rand.NewZipf(src.rand, 1.01, 4, 100000000)

	src.record = make([]interface{}, len(src.schema))

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (i *Item) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (i *Item) DefaultEndAt() uint64 {
	return 1543503872
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (i *Item) Info() string {
	return "TODO"
}

// Ensure ItemSource implements interface.
var _ idk.Source = (*ItemSource)(nil)

type ItemSource struct {
	Log logger.Logger

	startFrom uint64
	endAt     uint64

	rand     *rand.Rand
	pliZipf  *rand.Zipf
	prodZipf *rand.Zipf
	custZipf *rand.Zipf
	schema   []idk.Field
	record   record
}

type ItemSourceOption func(s *ItemSource) error

func OptItemStartFrom(start uint64) ItemSourceOption {
	return func(s *ItemSource) error {
		s.startFrom = start
		return nil
	}
}

func OptItemEndAt(end uint64) ItemSourceOption {
	return func(s *ItemSource) error {
		s.endAt = end
		return nil
	}
}

var plis = []string{"laptop", "desktop", "server", "rack", "phone", "fan", "enclosure", "switch", "monitor", "pli1", "pli2", "pli3", "pli4", "pli5", "pli6", "pli7", "pli8", "pli9", "pli10", "pli11", "pli12", "pli13", "pli14", "pli15", "pli16", "pli17", "pli18", "pli19", "pli20", "pli21", "pli22", "pli23", "pli24", "pli25", "pli26", "pli27", "pli28", "pli29", "pli30", "pli31", "pli32", "pli33", "pli34", "pli35", "pli36", "pli37", "pli38", "pli39", "pli40", "pli41", "pli42"}

func (s *ItemSource) Record() (idk.Record, error) {
	if s.startFrom >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.startFrom
	s.startFrom++
	s.record[1] = int64(s.custZipf.Uint64())                                            // cust_id
	s.record[2] = s.rand.Float64() * 10000                                              // max $10k // cost
	s.record[3] = int64(time.Duration(s.rand.Intn(int(currentDur))) / (time.Hour * 24)) // random number of days since start ship_date
	s.record[4] = plis[s.pliZipf.Uint64()]
	s.record[5] = s.prodZipf.Uint64()
	return s.record, nil
}

func (s *ItemSource) Schema() []idk.Field {
	return s.schema
}

func (s *ItemSource) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*ItemSource)(nil)

func (s *ItemSource) Close() error {
	return nil
}
