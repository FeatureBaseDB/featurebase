package datagen

import (
	"io"
	"math/rand"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Power1_2 implements interface.
var _ Sourcer = (*Power1_2)(nil)

// Power1_2 implements Sourcer.
type Power1_2 struct{}

// NewPower1_2 returns a new instance of Power1_2.
func NewPower1_2(cfg SourceGeneratorConfig) Sourcer {
	return &Power1_2{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (p *Power1_2) Source(cfg SourceConfig) idk.Source {
	src := &Power1_2Source{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,
		rand:  rand.New(rand.NewSource(22)),
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},               // 0
			idk.StringField{NameVal: "data_type"},    // 1
			idk.IntField{NameVal: "equip_id"},        // 2
			idk.StringField{NameVal: "group"},        // 3
			idk.IntField{NameVal: "int_value"},       // 4
			idk.StringField{NameVal: "string_value"}, // 5
			idk.IntField{NameVal: "timestamp"},       // 6
			idk.StringField{NameVal: "type"},         // 7
		},
	}

	src.record = make([]interface{}, len(src.schema))

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (p *Power1_2) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (p *Power1_2) DefaultEndAt() uint64 {
	return 3*uint64(len(equip_ids)) - 1
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (p *Power1_2) Info() string {
	return "Generates data representative of Power Analytics operations (#2)."
}

// Ensure Power1_2Source implements interface.
var _ idk.Source = (*Power1_2Source)(nil)

type Power1_2Source struct {
	Log logger.Logger

	rand *rand.Rand

	cur, endAt uint64

	schema []idk.Field

	record record
}

func (s *Power1_2Source) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = 101000000 + s.cur

	// timestamp
	//s.record[6] = timestamp

	// type
	s.record[7] = "Amperage"

	// group
	s.record[3] = "free"

	// equip_id
	s.record[2] = equip_ids[s.cur%uint64(len(equip_ids))]

	// timestamp
	s.record[6] = int(1509100001) + 3600*int(s.cur/uint64(len(equip_ids)))

	// data type and string or int value
	s.record[1] = "int"
	s.record[4] = s.rand.Intn(10000) + 50
	s.record[5] = nil

	s.cur++

	return s.record, nil
}

func (s *Power1_2Source) Schema() []idk.Field {
	return s.schema
}

func (s *Power1_2Source) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*Power1_2Source)(nil)

func (s *Power1_2Source) Close() error {
	return nil
}
