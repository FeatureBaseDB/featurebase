package datagen

import (
	"io"
	"math/rand"
	"time"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Claim implements interface.
var _ Sourcer = (*Claim)(nil)

// Claim implements Sourcer.
type Claim struct{}

// NewClaim returns a new instance of Claim.
func NewClaim(cfg SourceGeneratorConfig) Sourcer {
	return &Claim{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (c *Claim) Source(cfg SourceConfig) idk.Source {
	min := int64(0)
	src := &ClaimSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,
		rand:  rand.New(rand.NewSource(19)),
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},
			idk.StringField{NameVal: "service_type"},
			idk.IntField{NameVal: "date"},
			idk.IntField{NameVal: "item_id", Min: &min},
			idk.StringArrayField{NameVal: "part"},
		},
	}
	src.record = make([]interface{}, len(src.schema))

	src.partZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(parts)-1))

	src.record[4] = make([]string, 4)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (c *Claim) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (c *Claim) DefaultEndAt() uint64 {
	return 50000000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (c *Claim) Info() string {
	return "TODO"
}

// Ensure ClaimSource implements interface.
var _ idk.Source = (*ClaimSource)(nil)

type ClaimSource struct {
	Log logger.Logger

	cur, endAt uint64

	rand     *rand.Rand
	partZipf *rand.Zipf
	schema   []idk.Field
	record   record
}

var parts = []string{"cpu", "ram", "motherboard", "cooler", "case", "speaker", "microphone", "nic", "wlan", "screen", "keyboard", "disk", "ssd", "optical", "fan", "powersupp", "part0", "part1", "part2", "part3", "part4", "part5", "part6", "part7", "part8", "part9", "part10", "part11", "part12", "part13", "part14", "part15", "part16", "part17", "part18", "part19", "part20", "part21", "part22", "part23", "part24", "part25", "part26", "part27", "part28", "part29", "part30", "part31", "part32", "part33", "part34", "part35", "part36", "part37", "part38", "part39", "part40", "part41", "part42", "part43", "part44", "part45", "part46", "part47", "part48", "part49", "part50", "part51", "part52", "part53", "part54", "part55", "part56", "part57", "part58", "part59", "part60", "part61", "part62", "part63", "part64", "part65", "part66", "part67", "part68", "part69", "part70", "part71", "part72", "part73", "part74", "part75", "part76", "part77", "part78", "part79", "part80", "part81", "part82", "part83", "part84"}
var claimTypes = []string{"warranty", "damage", "spill", "intentional", "accidental", "collateral", "priority", "replacement", "reimbursement"}

func (s *ClaimSource) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.cur
	s.record[1] = claimTypes[s.rand.Intn(len(claimTypes))]
	s.record[2] = int64(time.Duration(s.rand.Intn(int(currentDur))) / (time.Hour * 24)) // day
	s.record[3] = s.rand.Intn(1500000000)
	s.record[4] = s.record[4].([]string)[:randNumParts(s.rand)]
	for i := range s.record[4].([]string) {
		s.record[4].([]string)[i] = parts[s.partZipf.Uint64()]
	}
	s.cur++
	return s.record, nil
}

func (s *ClaimSource) Schema() []idk.Field {
	return s.schema
}

func (s *ClaimSource) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*ClaimSource)(nil)

func randNumParts(r *rand.Rand) int {
	n := r.Intn(100)
	if n < 80 {
		return 1
	}
	if n < 90 {
		return 2
	}
	if n < 95 {
		return 3
	}
	return 4
}

func (s *ClaimSource) Close() error {
	return nil
}
