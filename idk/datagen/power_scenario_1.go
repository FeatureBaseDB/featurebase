package datagen

import (
	"io"
	"sync"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Power1 implements interface.
var _ Sourcer = (*Power1)(nil)

// Power1 implements Sourcer.
type Power1 struct {
	startEnds []startEnd

	mu         sync.Mutex
	sourceCall int
}

// NewPower1 returns a new instance of Power1.
func NewPower1(cfg SourceGeneratorConfig) Sourcer {
	start := cfg.StartFrom
	end := cfg.EndAt
	if start == 0 {
		start = 100000000
		if end == 0 {
			end = start + 630
		} else {
			end += 100000000
		}
	}
	// We need to adjust end to be exclusive.
	end++

	customCfg := SourceGeneratorConfig{
		StartFrom:   start,
		EndAt:       end,
		Concurrency: cfg.Concurrency,
	}

	_, ses, _ := startEnds(customCfg)

	return &Power1{
		startEnds: ses,
	}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (p *Power1) Source(cfg SourceConfig) idk.Source {
	p.mu.Lock()
	i := p.sourceCall
	p.sourceCall++
	p.mu.Unlock()
	src := &Power1Source{
		// We're goind to ignore the passed in start/end,
		// and instead used that calculated by NewPower1.
		cur:   p.startEnds[i].start,
		endAt: p.startEnds[i].end,
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
func (p *Power1) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (p *Power1) DefaultEndAt() uint64 {
	return 100000630
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (p *Power1) Info() string {
	return "Generates data representative of Power Analytics operations (#1)."
}

// Ensure Power1Source implements interface.
var _ idk.Source = (*Power1Source)(nil)

type Power1Source struct {
	Log logger.Logger

	cur, endAt uint64

	schema []idk.Field

	record record
}

func (s *Power1Source) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.cur

	// timestamp
	s.record[6] = int(509100000 + (10 * s.cur))

	// type
	s.record[7] = "Amperage"

	// group
	s.record[3] = "single"

	// equip_id
	s.record[2] = 712278 // breaker at a retail site

	// data type and string or int value
	s.record[1] = "int"
	s.record[4] = 30
	s.record[5] = nil

	s.cur++

	return s.record, nil
}

func (s *Power1Source) Schema() []idk.Field {
	return s.schema
}

func (s *Power1Source) Close() error {
	return nil
}
