package datagen

import (
	"io"
	"math/rand"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Transaction implements interface.
var _ Sourcer = (*Transaction)(nil)

// Transaction implements Sourcer.
type Transaction struct {
	schema []idk.Field
}

// NewTransaction returns a new instance of Transaction.
func NewTransaction(cfg SourceGeneratorConfig) Sourcer {
	return &Transaction{
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
func (t *Transaction) Source(cfg SourceConfig) idk.Source {
	src := &TransactionSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,

		rand: rand.New(rand.NewSource(22)),

		schema: t.schema,
	}

	src.typeZipf = rand.NewZipf(src.rand, 1.03, 4, uint64(len(tsTypes))-1)
	src.instoreZipf = rand.NewZipf(src.rand, 1.3, 1.3, uint64(len(inStorePayments))-1)
	src.ecomZipf = rand.NewZipf(src.rand, 1.3, 1.3, uint64(len(ecomPayments))-1)
	src.valueZipf = rand.NewZipf(src.rand, 1.2, 4, 50000)

	src.record = make([]interface{}, len(src.schema))
	src.record[7] = int(1420070400 + cfg.startFrom)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (t *Transaction) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (t *Transaction) DefaultEndAt() uint64 {
	return 100000000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (t *Transaction) Info() string {
	return "Generates timeseries transaction data (transaction_amount, POS, site) - references data in sites, manufacturer, and equipment."
}

// Ensure TransactionSource implements interface.
var _ idk.Source = (*TransactionSource)(nil)

type TransactionSource struct {
	Log logger.Logger

	rand        *rand.Rand
	typeZipf    *rand.Zipf
	instoreZipf *rand.Zipf
	ecomZipf    *rand.Zipf
	valueZipf   *rand.Zipf

	schema []idk.Field

	cur, endAt uint64

	record record
}

var transactionTypes = []string{"Ecommerce", "In-store"}

func (s *TransactionSource) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.cur
	s.cur++

	// transaction type
	transType := transactionTypes[s.rand.Intn(2)]
	s.record[1] = transType

	// payment type
	paymentType := s.paymentType(transType)
	s.record[2] = paymentType

	// site id
	s.record[3] = s.siteID(transType)

	// equipment id
	s.record[4] = s.equipID(transType)

	// customer
	s.record[5] = s.customer()

	// transaction_value
	s.record[6] = s.value(paymentType)

	// timestamp
	s.record[7] = s.record[7].(int) + s.rand.Intn(3)

	return s.record, nil
}

func (s *TransactionSource) Schema() []idk.Field {
	return s.schema
}

func (s *TransactionSource) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*TransactionSource)(nil)

func (s *TransactionSource) paymentType(transType string) string {
	switch {
	case transType == "Ecommerce":
		return ecomPayments[s.ecomZipf.Uint64()]
	case transType == "In-store":
		return inStorePayments[s.instoreZipf.Uint64()]
	default:
		panic("unknown transaction type " + transType)
	}
}

func (s *TransactionSource) siteID(transType string) int64 {
	switch {
	case transType == "Ecommerce":
		num := s.rand.Int63n(500000)
		if num < 400000 {
			return num + 88000 // retail locations
		} else {
			return num + 4000000 // subset of data centers
		}
	case transType == "In-store":
		return s.rand.Int63n(400000) + 88000
	default:
		panic("unknown transaction type (siteid)" + transType)
	}
}

func (s *TransactionSource) equipID(transType string) int64 {
	switch {
	case transType == "Ecommerce":
		return s.rand.Int63n(3000000) + 20000000 // subset of servers
	case transType == "In-store":
		return s.rand.Int63n(1000000) + 35000000 // POS devices
	default:
		panic("unknown transaction type (equipid)" + transType)

	}
}

func (s *TransactionSource) customer() string {
	return customers[s.rand.Intn(len(customers))]
}

func (s *TransactionSource) value(paymentType string) float64 {
	val := float64(s.valueZipf.Uint64() + 15) // 15-50k

	// 25% go lower ... to get some below $15
	if s.rand.Intn(4) == 0 {
		val -= float64(s.rand.Intn(14))
	}

	// half the time add cents
	if s.rand.Intn(2) == 1 {
		val = val + s.rand.Float64()
	}

	if paymentType == "Return" {
		val = val * -1
	}
	return val
}

func (s *TransactionSource) Close() error {
	return nil
}
