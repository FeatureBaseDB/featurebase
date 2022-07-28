package datagen

import (
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/datagen/gen"
	"github.com/molecula/featurebase/v3/pql"
)

// Ensure AllFieldTypes implements interface.
var _ Sourcer = (*AllFieldTypes)(nil)

// AllFieldTypes implements Sourcer, and returns a data
// set containing one of every field type.
type AllFieldTypes struct{}

// NewAllFieldTypes returns a new instance of AllFieldTypes.
func NewAllFieldTypes(cfg SourceGeneratorConfig) Sourcer {
	return &AllFieldTypes{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (a *AllFieldTypes) Source(cfg SourceConfig) idk.Source {
	epoch := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
	layout := "2006-01-02"
	src := &AllFieldTypesSource{
		cur:    cfg.startFrom,
		endAt:  cfg.endAt,
		epoch:  epoch,
		layout: layout,
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},     // 0
			idk.BoolField{NameVal: "bool"}, // 1
			idk.DateIntField{
				NameVal: "dateint",
				Epoch:   epoch,
				Unit:    idk.Day,
				Layout:  layout,
			}, // 2
			idk.DecimalField{NameVal: "decimal", Scale: 3}, // 3
			idk.IDArrayField{NameVal: "idarray"},           // 4
			idk.IDField{NameVal: "idfield"},                // 5
			//idk.IgnoreField{}, // TODO: maybe an "ignore" property instead?
			idk.IntField{
				NameVal: "int",
				Min:     intptr(-100000),
				Max:     intptr(200000),
			}, // 6 // TODO: foreignIndex
			idk.RecordTimeField{
				NameVal: "recordtime",
				Layout:  layout,
			}, // 7
			idk.SignedIntBoolKeyField{NameVal: "signedintboolkey"}, // 8
			idk.StringArrayField{NameVal: "stringarray"},           // 9
			idk.StringField{NameVal: "string"},                     // 10

			// Include `mutex` fields.
			idk.IDField{NameVal: "idfieldmutex", Mutex: true},    // 11
			idk.StringField{NameVal: "stringmutex", Mutex: true}, // 12

			// Include `quantum` fields.
			idk.IDArrayField{NameVal: "idarrayquantum", Quantum: "YMD", TTL: "0s"},         // 13
			idk.IDField{NameVal: "idfieldquantum", Quantum: "YMD", TTL: "0s"},              // 14
			idk.StringArrayField{NameVal: "stringarrayquantum", Quantum: "YMD", TTL: "0s"}, // 15
			idk.StringField{NameVal: "stringquantum", Quantum: "YMD", TTL: "0s"},           // 16
			idk.TimestampField{NameVal: "timestamp", Unit: idk.Second},                     // 17
		},
	}

	src.record = make([]interface{}, len(src.schema))
	src.g = gen.New(gen.OptGenSeed(cfg.seed))

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (a *AllFieldTypes) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (a *AllFieldTypes) DefaultEndAt() uint64 {
	return 100
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (a *AllFieldTypes) Info() string {
	return "Generates data for all field types."
}

// Ensure ExampleSource implements interface.
var _ idk.Source = (*AllFieldTypesSource)(nil)

// AllFieldTypesSource is an instance of a source generated
// by the Sourcer implementation AllFieldTypes.
type AllFieldTypesSource struct {
	g *gen.Gen

	cur   uint64
	endAt uint64

	epoch  time.Time
	layout string

	schema []idk.Field
	record record
}

// Record implements idk.Source.
func (a *AllFieldTypesSource) Record() (idk.Record, error) {
	if a.cur >= a.endAt {
		return nil, io.EOF
	}

	// Increment the ID.
	a.record[0] = a.cur

	// 1 - Bool
	switch a.cur % 7 {
	case 0:
		a.record[1] = nil
	case 1:
		a.record[1] = true
	case 2:
		a.record[1] = false
	case 3:
		a.record[1] = "true"
	case 4:
		a.record[1] = "false"
	case 5:
		a.record[1] = 1
	case 6:
		a.record[1] = 0
	}

	// 2 - DateIntField
	switch a.cur % 2 {
	case 0:
		a.record[2] = nil
	case 1:
		a.record[2] = a.epoch.AddDate(0, 0, int(a.cur)).Format(a.layout)
	}

	// 3 - Decimal
	switch a.cur % 2 {
	case 0:
		a.record[3] = nil
	case 1:
		a.record[3] = pql.NewDecimal(int64(1003*a.cur), 2)
	}

	// 4 - IDArray
	switch a.cur % 8 {
	case 7:
		a.record[4] = nil
	default:
		// Generate a random set field.
		set := a.g.Set(100, 1000, 10)
		vals := make([]uint64, 0, len(set))
		for v := range set {
			vals = append(vals, v)
		}
		a.record[4] = vals
	}

	// 5 - ID (pilosa rowID field, not primary key)
	switch a.cur % 6 {
	case 5:
		a.record[5] = nil
	default:
		a.record[5] = a.g.R.Int63n(100000)
	}

	// x - IgnoreField

	// 6 - Int
	switch a.cur % 9 {
	case 8:
		a.record[6] = nil
	default:
		a.record[6] = a.g.R.Int63n(300000) - 100000
	}

	// 7 - RecordTime
	switch a.cur % 7 {
	case 8:
		a.record[7] = nil
	default:
		a.record[7] = a.epoch.AddDate(5, 0, int(a.cur)).Format(a.layout)
	}

	// 8 - SignedIntBoolKey
	switch a.cur % 10 {
	case 4:
		a.record[8] = nil
	default:
		a.record[8] = int64(2*(a.cur%2)-1) * a.g.R.Int63n(10000)
	}

	// 9 - StringArray
	switch a.cur % 7 {
	case 5:
		a.record[9] = nil
	default:
		// Generate a random set field.
		set := a.g.Set(100, 1000, 10)
		vals := make([]string, 0, len(set))
		for v := range set {
			vals = append(vals, "v"+strconv.FormatUint(v, 10))
		}
		sort.Strings(vals)
		a.record[9] = vals
	}

	// 10 - String
	switch a.cur % 8 {
	case 5:
		a.record[10] = nil
	default:
		rint := a.g.R.Int63n(2000)
		a.record[10] = "s" + strconv.FormatUint(uint64(rint), 10)
	}

	// 11 - ID, mutex
	switch a.cur % 16 {
	case 6:
		a.record[11] = nil
	default:
		a.record[11] = a.g.R.Int63n(100000)
	}

	// 12 - String, mutex
	switch a.cur % 17 {
	case 5:
		a.record[12] = nil
	default:
		rint := a.g.R.Int63n(2000)
		a.record[12] = "sm" + strconv.FormatUint(uint64(rint), 10)
	}

	// 13 - IDArray
	switch a.cur % 9 {
	case 2:
		a.record[13] = nil
	default:
		// Generate a random set field.
		set := a.g.Set(100, 1000, 10)
		vals := make([]uint64, 0, len(set))
		for v := range set {
			vals = append(vals, v)
		}
		a.record[13] = vals
	}

	// 14 - ID, quantum
	switch a.cur % 14 {
	case 7:
		a.record[14] = nil
	default:
		a.record[14] = a.g.R.Int63n(100000)
	}

	// 15 - StringArray, quantum
	switch a.cur % 18 {
	case 7:
		a.record[15] = nil
	default:
		// Generate a random set field.
		set := a.g.Set(100, 1000, 10)
		vals := make([]string, 0, len(set))
		for v := range set {
			vals = append(vals, "sa"+strconv.FormatUint(v, 10))
		}
		sort.Strings(vals)
		a.record[15] = vals
	}

	// 16 - String, quantum
	switch a.cur % 17 {
	case 5:
		a.record[16] = nil
	default:
		rint := a.g.R.Int63n(2000)
		a.record[16] = "sq" + strconv.FormatUint(uint64(rint), 10)
	}

	// 17 - Timestamp
	switch a.cur % 15 {
	case 8:
		a.record[17] = nil
	default:
		rint := a.g.R.Int63n(604800)     // seconds in a week
		a.record[17] = 1605041948 + rint // offset by sometime in 2021
	}

	a.cur++

	return a.record, nil
}

// Schema implements idk.Source.
func (a *AllFieldTypesSource) Schema() []idk.Field {
	return a.schema
}

func (a *AllFieldTypesSource) Close() error {
	return nil
}
