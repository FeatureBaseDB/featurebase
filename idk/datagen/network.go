package datagen

import (
	"io"
	"math/rand"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Network implements interface.
var _ Sourcer = (*Network)(nil)

// Network implements Sourcer.
type Network struct {
	schema []idk.Field
}

// NewNetwork returns a new instance of Network.
func NewNetwork(cfg SourceGeneratorConfig) Sourcer {
	return &Network{
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},               // 0
			idk.IDField{NameVal: "qos_tier"},         // 1
			idk.IntField{NameVal: "source_equip_id"}, // 2
			idk.IntField{NameVal: "dest_equip_id"},   // 3
			idk.IntField{NameVal: "data_size"},       // 4
			idk.StringField{NameVal: "data_type"},    // 5
			idk.StringField{NameVal: "customer"},     // 6
			idk.IntField{NameVal: "timestamp"},       // 7
		},
	}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (n *Network) Source(cfg SourceConfig) idk.Source {
	src := &NetworkSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,

		rand: rand.New(rand.NewSource(22)),

		schema: n.schema,
	}

	src.typeZipf = rand.NewZipf(src.rand, 1.03, 4, uint64(len(dataTypeStrings))-1)

	src.record = make([]interface{}, len(src.schema))
	src.record[7] = int(1420070400 + cfg.startFrom)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (n *Network) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (n *Network) DefaultEndAt() uint64 {
	return 100000000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (n *Network) Info() string {
	return "Generates timeseries network transmission data (from_ip, to_ip, bytes) - references data in sites, manufacturer, and equipment."
}

// Ensure NetworkSource implements interface.
var _ idk.Source = (*NetworkSource)(nil)

type NetworkSource struct {
	Log logger.Logger

	rand     *rand.Rand
	typeZipf *rand.Zipf

	schema []idk.Field

	cur, endAt uint64

	record record
}

func (s *NetworkSource) Record() (idk.Record, error) {
	// ID
	if s.cur >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.cur
	s.cur++

	// QOS
	s.record[1] = s.qos()

	// TODO - equipment et. al
	dataType := s.dataType()
	s.record[5] = dataType

	s.record[2] = s.srcEquip(dataType)
	s.record[3] = s.dstEquip(dataType)

	s.record[4] = s.size(dataTypes[dataType])

	s.record[6] = s.customer(dataType)

	// timestamp
	s.record[7] = s.record[7].(int) + s.rand.Intn(3)

	return s.record, nil
}

func (s *NetworkSource) customer(dataType string) string {
	if dataType == "Transaction" {
		return customers[s.rand.Intn(4)]
	}
	return customers[s.rand.Intn(len(customers))]
}

func (s *NetworkSource) size(dt dataType) int64 {
	return s.rand.Int63n(dt.maxSize-dt.minSize) + dt.minSize
}

type dataType struct {
	minSize int64
	maxSize int64
}

var dataTypes = map[string]dataType{
	"Wireless Internet": {
		minSize: 51,
		maxSize: 22000000, // 22MB
	},
	"Wired Internet": {
		minSize: 63,
		maxSize: 40000000, // 40MB
	},
	"Wired Video": {
		minSize: 73,
		maxSize: 150000000, // 150MB
	},
	"Transaction": {
		minSize: 12,
		maxSize: 1200, // 7MB
	},
	"Wireless Video": {
		minSize: 75,
		maxSize: 50000000, // 50MB
	},
	"Wireless Voice": {
		minSize: 17,
		maxSize: 7000000, // 7MB
	},
	"Wired Voice": {
		minSize: 19,
		maxSize: 10000000, // 10MB
	},
}
var dataTypeStrings []string

func init() {
	for name := range dataTypes {
		dataTypeStrings = append(dataTypeStrings, name)
	}
}

func (s *NetworkSource) dataType() string {
	return dataTypeStrings[s.typeZipf.Uint64()]
}

func (s *NetworkSource) srcEquip(typ string) int64 {
	switch typ {
	case "Transaction":
		return s.rand.Int63n(1000000) + 35000000 // POS device
	case "Wired Video":
		return s.rand.Int63n(28000000) + 5000000 // all non BTS devices
	case "Wired Internet":
		return s.rand.Int63n(28000000) + 5000000 // all non BTS devices
	case "Wired Voice":
		return s.rand.Int63n(28000000) + 5000000 // all non BTS devices
	case "Wireless Video":
		return s.rand.Int63n(17000000) + 18000000 // all non CPE devices
	case "Wireless Internet":
		return s.rand.Int63n(17000000) + 18000000 // all non CPE devices
	case "Wireless Voice":
		return s.rand.Int63n(17000000) + 18000000 // all non CPE devices
	default:
		panic("srcEquip: unknown type " + typ)
	}
}
func (s *NetworkSource) dstEquip(typ string) int64 {
	switch typ {
	case "Transaction":
		return s.rand.Int63n(15000000) + 18000000 // server and router
	case "Wired Video":
		return s.rand.Int63n(28000000) + 5000000 // all non BTS devices
	case "Wired Internet":
		return s.rand.Int63n(28000000) + 5000000 // all non BTS devices
	case "Wired Voice":
		return s.rand.Int63n(28000000) + 5000000 // all non BTS devices
	case "Wireless Video":
		return s.rand.Int63n(17000000) + 18000000 // all non CPE devices
	case "Wireless Internet":
		return s.rand.Int63n(17000000) + 18000000 // all non CPE devices
	case "Wireless Voice":
		return s.rand.Int63n(17000000) + 18000000 // all non CPE devices
	default:
		panic("dstEquip: unknown type " + typ)
	}
}

func (s *NetworkSource) Schema() []idk.Field {
	return s.schema
}

func (s *NetworkSource) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*NetworkSource)(nil)

// qos returns a QoS value from 1-5 with a 5%/25%/40%/25%/5% weighting.
func (s *NetworkSource) qos() uint64 {
	num := s.rand.Intn(100)
	switch {
	case num < 5:
		return 1
	case num < 30:
		return 2
	case num < 70:
		return 3
	case num < 95:
		return 4
	default:
		return 5
	}
}

func (s *NetworkSource) Close() error {
	return nil
}
