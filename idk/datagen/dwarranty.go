package datagen

import (
	"fmt"
	"io"
	"math/rand"

	"github.com/molecula/featurebase/v3/idk"
)

// Ensure DWarranty implements interface.
var _ Sourcer = (*DWarranty)(nil)

// DWarranty implements Sourcer.
type DWarranty struct{}

// NewDWarranty returns a new instance of DWarranty.
func NewDWarranty(cfg SourceGeneratorConfig) Sourcer {
	return &DWarranty{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (d *DWarranty) Source(cfg SourceConfig) idk.Source {
	min := int64(0)
	src := &DWarrantySource{
		totalGenerating: cfg.total,
		WarrantySource: WarrantySource{
			startFrom: cfg.startFrom,
			endAt:     cfg.endAt,
			rand:      rand.New(rand.NewSource(19)),
			schema: []idk.Field{
				idk.IDField{NameVal: "id"},
				idk.StringField{NameVal: "type"},
				idk.IntField{NameVal: "start", Min: &min},
				idk.IntField{NameVal: "last", Min: &min},
				idk.IntField{NameVal: "cust_id", Min: &min},
				idk.IntField{NameVal: "item_id", Min: &min},
				idk.StringField{NameVal: "prod"},
			},
		},
	}
	src.typeZipf = rand.NewZipf(src.rand, 1.03, 5, uint64(len(types)-1))
	src.prodZipf = rand.NewZipf(src.rand, 1.2, 8, uint64(len(prods)-1))
	src.custZipf = rand.NewZipf(src.rand, 1.01, 4, 100000000)

	src.record = make([]interface{}, len(src.schema))
	src.record[2] = make([]string, 52*5)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (d *DWarranty) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (d *DWarranty) DefaultEndAt() uint64 {
	return 2147483648
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (d *DWarranty) Info() string {
	return "TODO"
}

// Ensure DWarrantySource implements interface.
var _ idk.Source = (*DWarrantySource)(nil)

type DWarrantySource struct {
	WarrantySource

	totalGenerating uint64
	prodZipf        *rand.Zipf
}

func (s *DWarrantySource) Record() (idk.Record, error) {
	if s.startFrom >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.startFrom
	s.record[1] = types[s.typeZipf.Uint64()]
	startDay := (s.startFrom * uint64(daysSinceStart)) / s.totalGenerating
	days := numDays(s.rand)
	s.record[2] = startDay
	s.record[3] = startDay + uint64(days)
	s.record[4] = int64(s.custZipf.Uint64()) // 100M customers
	s.record[5] = s.rand.Intn(1500000000)    // 1.5B items
	s.record[6] = prods[s.prodZipf.Uint64()]
	s.startFrom++
	return s.record, nil
}

func (s *DWarrantySource) Schema() []idk.Field {
	return s.schema
}

var _ Seedable = (*DWarrantySource)(nil)

var types = make([]string, 100)
var prods = make([]string, 3200)

func init() {
	var baseTypes = []string{"AZ-PQ-SUP1", "AZ-PQ-GOLDSUP", "BASICSUPPORT", "LM-NO-BASICLOWSUP", "QZ-PG-NONE", "CLIENTFGBG", "PHONE-BUNDLE", "AZ-PQ-PLATSUP", "AZ-PQ-PARTSUP", "OTHERSUP"}
	for i := range types {
		bt := baseTypes[i%len(baseTypes)]
		types[i] = fmt.Sprintf("%s-%d", bt, i)
	}

	for i := range prods {
		prods[i] = fmt.Sprintf("PRD-%d", i)
	}
}

func (s *DWarrantySource) Close() error {
	return nil
}
