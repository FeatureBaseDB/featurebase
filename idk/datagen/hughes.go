package datagen

import (
	"io"
	"math/rand"
	"time"

	"github.com/molecula/featurebase/v3/idk"
)

// Ensure Hughes implements interface.
var _ Sourcer = (*Hughes)(nil)

// Implements Hughes as a Sourcer.
type Hughes struct{}

// NewHughes returns a new instance of Hughes.
func NewHughes(cfg SourceGeneratorConfig) Sourcer {
	return &Hughes{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
// NOTE: avro Name fields can only contain certain
// characters.
// See: https://avro.apache.org/docs/current/spec.html#names
func (e *Hughes) Source(cfg SourceConfig) idk.Source {
	epoch := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	layout := "2006-01-02 15:00"
	src := &HughesSource{
		cur:    cfg.startFrom,
		endAt:  cfg.endAt,
		rand:   rand.New(rand.NewSource(int64(cfg.startFrom))),
		epoch:  epoch,
		layout: layout,
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},      //0
			idk.StringField{NameVal: "sid"}, //1
			idk.DateIntField{ //2
				NameVal: "time_stamp",
				Epoch:   epoch,
				Unit:    idk.Hour,
				Layout:  layout,
			},
			idk.DecimalField{NameVal: "metered_wan_b_mb_usage", Scale: 2}, //3
			idk.DateIntField{ //4
				NameVal: "asap_billing_cycle_gmt_start_date_time",
				Epoch:   epoch,
				Unit:    idk.Hour,
				Layout:  layout,
			},
			idk.DecimalField{NameVal: "metered_vsat_wan_a_mb_usage", Scale: 2}, //5
		},
	}

	src.record = make([]interface{}, len(src.schema))
	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (e *Hughes) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (e *Hughes) DefaultEndAt() uint64 {
	return 6000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (e *Hughes) Info() string {
	return "Generates data representative of Baker Hughes' oil field service operations."
}

// Ensure HughesSource implements interface.
var _ idk.Source = (*HughesSource)(nil)

// HughesSource is an instance of a source generated
// by the Sourcer implementation Hughes.
type HughesSource struct {
	cur    uint64
	endAt  uint64
	epoch  time.Time
	layout string
	rand   *rand.Rand
	bDate  int

	schema []idk.Field
	record record
}

// Record implements idk.Source.
func (e *HughesSource) Record() (idk.Record, error) {
	if e.cur >= e.endAt {
		return nil, io.EOF
	}
	e.record[0] = e.cur
	if e.cur%6000 == 0 { // generates 6000 hours (January 1st to September 6th (23:00)) of data for each new unique SID
		e.record[1] = e.SIDHughes(24)
		e.bDate = e.rand.Intn(27) + 1
		e.record[4] = e.epoch.AddDate(0, -1, e.bDate).Format(e.layout)
	}
	e.record[2] = e.epoch.Add(time.Duration(e.cur%6000) * time.Hour).Format(e.layout)
	e.record[3] = e.floatRand(10000, 0)
	currDate := e.epoch.Add(time.Duration(e.cur%6000) * time.Hour)
	if currDate.Day() == e.bDate && currDate.Hour() == 0 {
		e.record[4] = currDate.Format(e.layout)
	}
	e.record[5] = e.floatRand(10000, 0)
	e.cur++
	return e.record, nil
}

// Schema implements idk.Source.
func (e *HughesSource) Schema() []idk.Field {
	return e.schema
}

// returns a random float between a range
func (e *HughesSource) floatRand(max float64, min float64) float64 {
	return min + e.rand.Float64()*(max-min)
}

const charsetHughes = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// returns a string with random characters from a predefined charset of a specified length
func (e *HughesSource) StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charsetHughes[e.rand.Intn(len(charsetHughes))]
	}
	return string(b)
}

// returns a string with random English alphabets of the specified length
func (e *HughesSource) SIDHughes(length int) string {
	return e.StringWithCharset(length, charsetHughes)
}

func (e *HughesSource) Close() error {
	return nil
}
