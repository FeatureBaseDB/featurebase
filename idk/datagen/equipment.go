package datagen

import (
	"errors"
	"fmt"
	"io"
	"math/rand"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Equipment implements interface.
var _ Sourcer = (*Equipment)(nil)

// Equipment implements Sourcer.
type Equipment struct{}

// NewEquipment returns a new instance of Equipment.
func NewEquipment(cfg SourceGeneratorConfig) Sourcer {
	return &Equipment{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (e *Equipment) Source(cfg SourceConfig) idk.Source {
	src := &EquipmentSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,

		rand: rand.New(rand.NewSource(22)),

		schema: []idk.Field{
			idk.IDField{NameVal: "id"},                                   // 0
			idk.StringField{NameVal: "domain"},                           // 1
			idk.IntField{NameVal: "cost"},                                // 2
			idk.IntField{NameVal: "site_id"},                             // 3
			idk.DateIntField{NameVal: "date_acquired", Unit: idk.Day},    // 4
			idk.DateIntField{NameVal: "last_maintenance", Unit: idk.Day}, // 5
			idk.StringField{NameVal: "name"},                             // 6
			idk.StringField{NameVal: "type"},                             // 7
			idk.StringField{NameVal: "manufacturer"},                     // 8
			idk.StringField{NameVal: "model"},                            // 9
		},
		mfgrZipfs:  make(map[string]map[string]*rand.Zipf),
		modelZipfs: make(map[string]map[string]map[string]*rand.Zipf),
	}

	src.dcZipf = rand.NewZipf(src.rand, 1.5, 6, 3000000)

	// set up zipfs for all equipment manufacturers and models
	//
	// TODO: I believe the fact that we make rand calls during map
	// iteration (where order is random) is making this datagen
	// non-reproducible.
	for dom, typeMap := range domains {
		src.mfgrZipfs[dom] = make(map[string]*rand.Zipf)
		src.modelZipfs[dom] = make(map[string]map[string]*rand.Zipf)
		for typ, mfgrMap := range typeMap {
			src.modelZipfs[dom][typ] = make(map[string]*rand.Zipf)
			src.mfgrZipfs[dom][typ] = rand.NewZipf(src.rand, 1.001+src.rand.Float64(), float64(src.rand.Intn(7))+src.rand.Float64()+1, uint64(len(mfgrMap))-1)
			for mfgr, models := range mfgrMap {
				src.modelZipfs[dom][typ][mfgr] = rand.NewZipf(src.rand, 1.001+src.rand.Float64(), float64(src.rand.Intn(7))+src.rand.Float64()+1, uint64(len(models))-1)
			}
		}
	}

	src.record = make([]interface{}, len(src.schema))

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (e *Equipment) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (e *Equipment) DefaultEndAt() uint64 {
	return 36000000 - 1
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (e *Equipment) Info() string {
	return "Generates equipment and device data (manufacturer, cost, etc) - includes a foreign key for network_ts, power_ts, transaction_ts."
}

// Ensure EquipmentSource implements interface.
var _ idk.Source = (*EquipmentSource)(nil)

type EquipmentSource struct {
	Log logger.Logger

	rand       *rand.Rand
	mfgrZipfs  map[string]map[string]*rand.Zipf
	modelZipfs map[string]map[string]map[string]*rand.Zipf
	dcZipf     *rand.Zipf
	schema     []idk.Field

	cur, endAt uint64

	record record
}

func (s *EquipmentSource) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.cur
	switch {
	case s.cur < 5000000:
		s.fillRecord("Power", s.powerType())
	case s.cur < 35000000:
		s.fillRecord("Communications", s.communicationsType())
	case s.cur < 36000000:
		s.fillRecord("Transactions", s.transactionsType())
	default:
		return nil, errors.New("exhausted equipment data set")
	}
	s.cur++

	return s.record, nil
}

func (s *EquipmentSource) fillRecord(domain, eqType string) {
	s.record[1] = domain

	// type
	s.record[7] = eqType

	// manufacturer
	mfgrs := mfgrLists[domain][eqType]
	mfgr := mfgrs[s.mfgrZipfs[domain][eqType].Uint64()]
	s.record[8] = mfgr

	// model
	models := domains[domain][eqType][mfgr]
	modelZipf, ok := s.modelZipfs[domain][eqType][mfgr]
	if !ok {
		s.Log.Printf("%s %s %s", domain, eqType, mfgr)
	}
	model := models[modelZipf.Uint64()]
	s.record[9] = model

	// cost
	s.record[2] = costs[domain][eqType].Cost(s.rand)

	// site id
	s.record[3] = s.siteID(eqType)

	// date acquired
	s.record[4] = startDayInt + s.rand.Int63n(365*10-200)

	// last maintenance
	s.record[5] = s.record[4].(int64) + s.rand.Int63n(todayInt-s.record[4].(int64))

}

func (s *EquipmentSource) powerType() string {
	num := s.rand.Intn(100)
	switch {
	case num < 75:
		return "Breaker"
	case num < 77:
		return "Generator"
	case num < 82:
		return "Air Handler"
	case num < 88:
		return "Chiller"
	default:
		return "UPS"
	}
}

func (s *EquipmentSource) siteID(typ string) int64 {
	switch typ {
	case "Breaker":
		return s.rand.Int63n(4680000)
	case "Generator":
		return s.rand.Int63n(4680000)
	case "Air Handler":
		return s.rand.Int63n(4680000)
	case "Chiller":
		return s.rand.Int63n(4680000)
	case "UPS":
		return s.rand.Int63n(4680000)
	case "Router":
		num := s.rand.Int63n(1000 + 7000 + 3000000)
		switch {
		case num < 8000:
			// MSC or Headend
			return num
		default:
			// datacenter
			return num + 1680000
		}
	case "Server":
		num := s.rand.Intn(100)
		// not a datacenter
		if num == 0 {
			// non-DC
			return s.rand.Int63n(1688000)
		} else {
			// DC
			return int64(s.dcZipf.Uint64()) + 1688000
		}
	case "BTS":
		// Cell Site
		return s.rand.Int63n(400000) + 488000
	case "CPE":
		// Retail
		return s.rand.Int63n(400000) + 88000
	case "POS":
		// Retail
		return s.rand.Int63n(400000) + 88000
	default:
		panic(fmt.Sprintf("unknown type %s", typ))
	}
}

func (s *EquipmentSource) communicationsType() string {
	// 30M communications eq from 5M to 35M
	// 1M router, 14M Server, 13M CPE, 2M BTS
	id := s.record[0].(uint64)
	switch {
	case id < 18000000:
		return "CPE"
	case id < 19000000:
		return "Router"
	case id < 33000000:
		return "Server"
	default:
		return "BTS"
	}
}

func (s *EquipmentSource) transactionsType() string { return "POS" }

func (s *EquipmentSource) Schema() []idk.Field {
	return s.schema
}

func (s *EquipmentSource) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*EquipmentSource)(nil)

var mfgrLists = make(map[string]map[string][]string)

func init() {
	for name, typeMap := range domains {
		mfgrLists[name] = make(map[string][]string)
		for typ, mfgrMap := range typeMap {
			mfgrs := make([]string, 0)
			for mfgr := range mfgrMap {
				mfgrs = append(mfgrs, mfgr)
			}
			mfgrLists[name][typ] = mfgrs
		}
	}
}

type minMax struct {
	min int64
	max int64
}

func (m minMax) Cost(r *rand.Rand) int64 {
	return r.Int63n(m.max-m.min) + m.min

}

func (s *EquipmentSource) Close() error {
	return nil
}
