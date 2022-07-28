package datagen

import (
	"io"
	"math/rand"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Transaction1 implements interface.
var _ Sourcer = (*Transaction1)(nil)

// Transaction1 implements Sourcer.
type Transaction1 struct {
	schema []idk.Field
}

// NewTransaction1 returns a new instance of Transaction1.
func NewTransaction1(cfg SourceGeneratorConfig) Sourcer {
	return &Transaction1{
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},                               // 0
			idk.StringField{NameVal: "transaction_type"},             // 1
			idk.StringField{NameVal: "payment_type"},                 // 2
			idk.IntField{NameVal: "site_id"},                         // 3
			idk.IntField{NameVal: "equip_id"},                        // 4
			idk.StringField{NameVal: "customer"},                     // 5
			idk.DecimalField{NameVal: "transaction_value", Scale: 2}, // 6
			idk.IntField{NameVal: "timestamp"},                       // 7
		},
	}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (t *Transaction1) Source(cfg SourceConfig) idk.Source {
	src := &Transaction1Source{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,

		rand: rand.New(rand.NewSource(22)),

		schema: t.schema,
	}

	src.instoreZipf = rand.NewZipf(src.rand, 1.3, 1.3, uint64(len(inStorePayments))-1)
	src.ecomZipf = rand.NewZipf(src.rand, 1.3, 1.3, uint64(len(ecomPayments))-1)
	src.valueZipf = rand.NewZipf(src.rand, 1.8, 4, 400)

	src.record = make([]interface{}, len(src.schema))
	src.record[7] = int(1509100000 + uint64(59.5*float64(cfg.startFrom)))

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (t *Transaction1) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (t *Transaction1) DefaultEndAt() uint64 {
	return 23682459
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (t *Transaction1) Info() string {
	return "TODO"
}

// Ensure Transaction1Source implements interface.
var _ idk.Source = (*Transaction1Source)(nil)

type Transaction1Source struct {
	Log logger.Logger

	cur, endAt uint64

	rand        *rand.Rand
	instoreZipf *rand.Zipf
	ecomZipf    *rand.Zipf
	valueZipf   *rand.Zipf

	schema []idk.Field

	record record
}

func NewTransaction1Source(start, end uint64) *Transaction1Source {
	src := &Transaction1Source{
		cur:   start,
		endAt: end,

		rand: rand.New(rand.NewSource(22)),

		schema: []idk.Field{
			idk.IDField{NameVal: "id"},                               // 0
			idk.StringField{NameVal: "transaction_type"},             // 1
			idk.StringField{NameVal: "payment_type"},                 // 2
			idk.IntField{NameVal: "site_id"},                         // 3
			idk.IntField{NameVal: "equip_id"},                        // 4
			idk.StringField{NameVal: "customer"},                     // 5
			idk.DecimalField{NameVal: "transaction_value", Scale: 2}, // 6
			idk.IntField{NameVal: "timestamp"},                       // 7
		},
	}

	src.instoreZipf = rand.NewZipf(src.rand, 1.3, 1.3, uint64(len(inStorePayments))-1)
	src.ecomZipf = rand.NewZipf(src.rand, 1.3, 1.3, uint64(len(ecomPayments))-1)
	src.valueZipf = rand.NewZipf(src.rand, 1.8, 4, 400)

	src.record = make([]interface{}, len(src.schema))
	src.record[7] = int(1509100000 + uint64(59.5*float64(start)))

	return src
}

func (s *Transaction1Source) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = 100000000 + s.cur

	// timestamp
	timestamp := s.record[7].(int) + s.rand.Intn(120)
	if timestamp > 1509106300 {
		return nil, io.EOF
	}
	s.record[7] = timestamp

	// transaction type
	transType := "In-store"
	s.record[1] = transType

	// payment type
	paymentType := s.paymentType(transType)
	s.record[2] = paymentType

	// site id
	s.record[3] = 88002 // Retail site which has breaker 712278

	// equipment id
	s.record[4] = []uint64{35100914, 35138991, 35144935, 35329399, 35894415}[s.cur%5] // POSs

	// customer
	s.record[5] = "CVS"

	// transaction_value
	s.record[6] = s.value(paymentType)

	s.cur++

	return s.record, nil
}

func (s *Transaction1Source) Schema() []idk.Field {
	return s.schema
}

func (s *Transaction1Source) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*Transaction1Source)(nil)

func (s *Transaction1Source) paymentType(transType string) string {
	switch {
	case transType == "Ecommerce":
		return ecomPayments[s.ecomZipf.Uint64()]
	case transType == "In-store":
		return inStorePayments[s.instoreZipf.Uint64()]
	default:
		panic("unknown transaction type " + transType)
	}
}

func (s *Transaction1Source) value(paymentType string) float64 {
	val := float64(s.valueZipf.Uint64() + 15) // 15-50k

	// 25% go lower ... to get some below $15
	if s.rand.Intn(4) == 0 {
		val -= float64(s.rand.Intn(14))
	}

	//  add cents
	val = val + s.rand.Float64()

	if paymentType == "Return" {
		val = val * -1
	}
	return val
}

func (s *Transaction1Source) Close() error {
	return nil
}
