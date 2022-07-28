package datagen

import (
	"io"
	"math/rand"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Timeseries implements interface.
var _ Sourcer = (*Timeseries)(nil)

// Timeseries implements Sourcer.
type Timeseries struct {
	schema []idk.Field
}

// NewTimeseries returns a new instance of Timeseries.
func NewTimeseries(cfg SourceGeneratorConfig) Sourcer {
	return &Timeseries{
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
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (t *Timeseries) Source(cfg SourceConfig) idk.Source {
	src := &TimeseriesSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,

		rand: rand.New(rand.NewSource(22)),

		schema: t.schema,
	}

	src.typeZipf = rand.NewZipf(src.rand, 1.03, 4, uint64(len(tsTypes))-1)
	src.tsZipf = rand.NewZipf(src.rand, 3, 1.3, uint64(len(tsStrings))-1)

	src.record = make([]interface{}, len(src.schema))
	src.record[6] = int(1420070400 + cfg.startFrom)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (t *Timeseries) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (t *Timeseries) DefaultEndAt() uint64 {
	return 100000000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (t *Timeseries) Info() string {
	return "Generates timeseries device power state data (device, alert, status) - references data in sites, manufacturer, and equipment."
}

// Ensure TimeseriesSource implements interface.
var _ idk.Source = (*TimeseriesSource)(nil)

type TimeseriesSource struct {
	Log logger.Logger

	rand     *rand.Rand
	typeZipf *rand.Zipf
	tsZipf   *rand.Zipf

	schema []idk.Field

	cur, endAt uint64

	record record
}

func NewTimeseriesSource(start, end uint64) *TimeseriesSource {
	src := &TimeseriesSource{
		cur:   start,
		endAt: end,

		rand: rand.New(rand.NewSource(22)),

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

	src.typeZipf = rand.NewZipf(src.rand, 1.03, 4, uint64(len(tsTypes))-1)
	src.tsZipf = rand.NewZipf(src.rand, 3, 1.3, uint64(len(tsStrings))-1)

	src.record = make([]interface{}, len(src.schema))
	src.record[6] = int(1420070400 + start)

	return src
}

func (s *TimeseriesSource) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}

	s.record[0] = s.cur

	// type
	s.record[7] = tsTypes[s.typeZipf.Uint64()]
	groups, ok := groupsPerType[s.record[7].(string)]
	if !ok {
		s.Log.Printf("no %v in groups", s.record[7])
	}

	// group
	if len(groups) != 0 {
		s.record[3] = groups[s.rand.Intn(len(groups))]
	} else {
		s.record[3] = nil
	}

	// equip_id
	s.record[2] = int64(s.rand.Intn(5000000))

	// data type and string or int value
	if s.rand.Intn(1000) > 995 {
		s.record[1] = "string"
		s.record[4] = nil
		s.record[5] = tsStrings[s.tsZipf.Uint64()]
	} else {
		s.record[1] = "int"
		s.record[4] = s.rand.Intn(100000)
		s.record[5] = nil
	}

	// timestamp
	s.record[6] = s.record[6].(int) + s.rand.Intn(3)

	s.cur++

	return s.record, nil
}

func (s *TimeseriesSource) Schema() []idk.Field {
	return s.schema
}

func (s *TimeseriesSource) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*TimeseriesSource)(nil)

var tsStrings = []string{"on", "off", "alarm", "safe"}

var groupsPerType = map[string][]string{
	"Voltage":    {"calculated", "single", "moving"},
	"VoltageRMS": {"calculated", "single", "moving"},
	"Amperage":   {"calculated", "single", "effective"},
	"Resistance": {"fixed", "variable"},
	"Frequency":  {"sampled", "digital", "analog"},
	"Message":    {"event", "alert", "alarm"},
}

func (s *TimeseriesSource) Close() error {
	return nil
}
