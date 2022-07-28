package datagen

import (
	"io"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/datagen/gen"
)

var _ Sourcer = (*LinkedInPerson)(nil)

type LinkedInPerson struct{}

func NewLinkedInPerson(cfg SourceGeneratorConfig) Sourcer {
	return &LinkedInPerson{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (p *LinkedInPerson) Source(cfg SourceConfig) idk.Source {
	g := gen.New(gen.OptGenSeed(cfg.seed))
	src := &LinkedInPersonSource{
		startFrom: cfg.startFrom,
		endAt:     cfg.endAt,
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},              // 0
			idk.StringField{NameVal: "city"},        // 1
			idk.StringField{NameVal: "zip_code"},    // 2
			idk.StringArrayField{NameVal: "skills"}, // 3
			idk.StringArrayField{NameVal: "titles"}, // 4
		},
		g: g,
	}

	src.record = make([]interface{}, len(src.schema))
	src.record[3] = []string{}
	src.record[4] = []string{}

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (e *LinkedInPerson) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (e *LinkedInPerson) DefaultEndAt() uint64 {
	return 100
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (p *LinkedInPerson) Info() string {
	return "Simulates adding LinkedIn data to existing customer segmentation data."
}

var _ idk.Source = (*LinkedInPersonSource)(nil)

type LinkedInPersonSource struct {
	count     uint64
	startFrom uint64
	endAt     uint64

	schema []idk.Field
	record record

	g *gen.Gen
}

func (lp *LinkedInPersonSource) Record() (idk.Record, error) {
	lp.count++
	if lp.count > lp.endAt-lp.startFrom {
		return nil, io.EOF
	}
	lp.record[0] = uint64(lp.g.R.Int63n(int64(lp.endAt-lp.startFrom))) + lp.startFrom
	lp.record[1] = lp.g.StringFromList(uscities)
	lp.record[2] = lp.g.StringFromList(zip_codes)
	lp.record[3] = lp.g.StringSliceFromListWeighted(lp.record[3].([]string), skills, 2, 5)
	lp.record[4] = lp.g.StringSliceFromListWeighted(lp.record[4].([]string), titles, 2, 5)

	return lp.record, nil
}

func (lp *LinkedInPersonSource) Schema() []idk.Field {
	return lp.schema
}
func (lp *LinkedInPersonSource) Close() error {
	return nil
}
