package datagen

import (
	"io"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/datagen/gen"
)

// Ensure StringPK implements interface.
var _ Sourcer = (*StringPK)(nil)

// StringPK implements Sourcer.
type StringPK struct{}

// NewStringPK returns a new instance of StringPK.
func NewStringPK(cfg SourceGeneratorConfig) Sourcer {
	return &StringPK{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (s *StringPK) Source(cfg SourceConfig) idk.Source {
	sps := &StringPKSource{
		record: make(record, 2),
		n:      cfg.startFrom,
		endAt:  cfg.endAt,
	}

	sps.record[0] = make([]byte, 12)
	sps.g = gen.New(gen.OptGenSeed(cfg.seed))

	return sps
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (s *StringPK) PrimaryKeyFields() []string {
	return []string{"pk"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (s *StringPK) DefaultEndAt() uint64 {
	return 10000000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (s *StringPK) Info() string {
	return "TODO"
}

// Ensure StringPKSource implements interface.
var _ idk.Source = (*StringPKSource)(nil)

type StringPKSource struct {
	g *gen.Gen

	endAt uint64
	n     uint64

	record record
}

func (s *StringPKSource) Record() (idk.Record, error) {
	if s.n >= s.endAt {
		return nil, io.EOF
	}
	s.g.AlphaUpper(s.record[0].([]byte))
	s.record[1] = s.g.R.Intn(10)
	s.n++
	return s.record, nil
}

func (s *StringPKSource) Schema() []idk.Field {
	return []idk.Field{
		idk.StringField{NameVal: "pk"},
		idk.IDField{NameVal: "field1"},
	}
}

func (s *StringPKSource) Seed(seed int64) {
	s.g.R.Seed(seed)
}

var _ Seedable = (*StringPKSource)(nil)

func (s *StringPKSource) Close() error {
	return nil
}
