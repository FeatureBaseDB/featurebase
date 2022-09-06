package datagen

import (
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/datagen/gen"
)

// Ensure Bank implements interface.
var _ Sourcer = (*Bank)(nil)

// Bank implements Sourcer.
type Bank struct {
	schema []idk.Field
}

// NewBank returns a new instance of a Bank data generator.
func NewBank(cfg SourceGeneratorConfig) Sourcer {
	return &Bank{
		schema: []idk.Field{
			idk.StringField{NameVal: "aba", Mutex: true},
			idk.StringField{NameVal: "db", Mutex: true},
			idk.IntField{NameVal: "user_id"},
			idk.StringField{NameVal: "custom_audiences"},
			// TODO generalize
			idk.DecimalField{NameVal: floatNames[0]},
			idk.BoolField{NameVal: boolNames[0]},
			idk.BoolField{NameVal: boolNames[1]},
			idk.StringField{NameVal: stringNames[0]},
			idk.IntField{NameVal: intNames[0]},
		},
	}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (b *Bank) Source(cfg SourceConfig) idk.Source {
	src := &BankSource{
		cur:    cfg.startFrom,
		endAt:  cfg.endAt,
		schema: b.schema,
		rand:   rand.New(rand.NewSource(cfg.seed)),
	}

	src.g = gen.New(gen.OptGenSeed(cfg.seed))
	src.record = make([]interface{}, len(src.schema))
	// TODO alloc?
	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (b *Bank) PrimaryKeyFields() []string {
	return []string{"aba", "db", "user_id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (b *Bank) DefaultEndAt() uint64 {
	return 20000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (b *Bank) Info() string {
	return "Generates user data representative of a multi-site banking application"
}

// Ensure BankSource implements interface.
var _ idk.Source = (*BankSource)(nil)

// Ensure BankSource implements interface.
//var _ idk.KafkaSource = (*BankSource)(nil) // TODO

// BankSource is a data generator which generates
// data for all Pilosa field types.
type BankSource struct {
	g *gen.Gen

	cur, endAt uint64

	schema []idk.Field
	record record

	rand *rand.Rand
}

var (
	// kafka cruft a.k.a. kruftka
	floatMeta  = `"type": ["null", {"type": "float", "scale": 3}], "default": null`
	boolMeta   = `"type": ["null", "boolean"], "default": null`
	stringMeta = `"type": ["null", "string"], "default": null, "mutex": true`
	intMeta    = `"type": ["null", "int"], "default": null`

	floatNames = []string{
		"pfm_category_total_current_balance__personal_loan",
	}

	boolNames = []string{
		"pfm_boolean__personal_loan",
		"pfm_boolean__mortgage",
	}

	stringNames = []string{
		"survey_5dfe1a89_29fa_4505_ac00_4cd055b758ab",
	}

	intNames = []string{
		"product_recency__consumer_loan",
	}

	floatNullChance  = 0.1
	boolNullChance   = 0.1
	stringNullChance = 0.1
	intNullChance    = 0.1

	// TODO these are effectively consts right now
	floatCount  = 1
	boolCount   = 2
	stringCount = 1
	intCount    = 1
)

// Record implements idk.Source.
func (b *BankSource) Record() (idk.Record, error) {
	if b.cur >= b.endAt {
		return nil, io.EOF
	}

	// Increment the ID.
	//b.record[0] = b.cur
	b.cur++

	b.record[0] = b.ABA()
	b.record[1] = b.Db()
	b.record[2] = b.UserID()
	b.record[3] = b.CustomAudiences()

	// TODO generalize
	if floatNullChance < b.rand.Float64() {
		b.record[4] = b.rand.Float64()
	}
	// TODO: handle null chance

	if boolNullChance < b.rand.Float64() {
		b.record[5] = b.rand.Intn(2) == 1
	}
	// TODO: handle null chance

	if boolNullChance < b.rand.Float64() {
		b.record[6] = b.rand.Intn(2) == 1
	}
	// TODO: handle null chance

	if stringNullChance < b.rand.Float64() {
		b.record[7] = text(b.rand, 1, 6, true, true, true, false)
	}
	// TODO: handle null chance

	if intNullChance < b.rand.Float64() {
		b.record[8] = int64(b.rand.Intn(1000000))
	}
	// TODO: handle null chance

	return b.record, nil
}

// KafkaRecord might implement idk.KafkaSource
// TODO: going to want to generate the kafka record from the idk record
// automatic handling would be nice, but allowing per-source is probably important,
// e.g. to handle special cases for individual kafka environments
func (b *BankSource) KafkaRecord() string {
	fields := []string{
		`"aba": "` + b.ABA() + `"`,
		`"db": "` + b.Db() + `"`,
		`"user_id": ` + strconv.FormatInt(int64(b.UserID()), 10),
		`"custom_audiences": {"string": "` + b.CustomAudiences() + `"}`,
	}

	for n := 0; n < floatCount; n++ {
		if floatNullChance < b.rand.Float64() {
			floatStr := strconv.FormatFloat(b.rand.Float64(), 'f', 3, 64)
			fields = append(fields, `"float_`+floatNames[n]+`": {"float": `+floatStr+`}`)
		}
	}
	for n := 0; n < boolCount; n++ {
		if boolNullChance < b.rand.Float64() {
			boolStr := strconv.FormatBool(b.rand.Intn(2) == 1)
			fields = append(fields, `"bool_`+boolNames[n]+`": {"boolean": `+boolStr+`}`)
		}
	}

	for n := 0; n < stringCount; n++ {
		if stringNullChance < b.rand.Float64() {
			stringStr := text(b.rand, 1, 6, true, true, true, false)
			fields = append(fields, `"string_`+stringNames[n]+`": {"string": "`+stringStr+`"}`)
		}
	}

	for n := 0; n < intCount; n++ {
		if intNullChance < b.rand.Float64() {
			intStr := strconv.FormatInt(int64(b.rand.Intn(1000000)), 10)
			fields = append(fields, `"int_`+intNames[n]+`": {"int": `+intStr+`}`)
		}
	}

	return `{` + strings.Join(fields, ", ") + `}`
}

// ABA returns a random 9 numeric digit string with about 27000 possible values.
func (b *BankSource) ABA() string {
	num := b.rand.Intn(27000) + 22213
	num2 := num/10 - 1213
	numstr := strconv.Itoa(num)
	num2str := strconv.Itoa(num2)
	numstrbytes := append([]byte(numstr), num2str[3], numstr[0], numstr[1], numstr[2])
	return string(numstrbytes)
}

// Db returns a db
func (b *BankSource) Db() string {
	return text(b.rand, 1, 6, true, true, true, false)
}

// UserID returns a user ID
func (b *BankSource) UserID() int {
	return b.rand.Intn(10000000) // 10 mil
}

// CustomAudiences returns a fake Custom Audience string
func (b *BankSource) CustomAudiences() string {
	return text(b.rand, 1, 6, true, true, true, false)
}

// TODO: move to gen.go
var lowerLetters = []rune("abcdefghijklmnopqrstuvwxyz")
var upperLetters = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
var numeric = []rune("0123456789")
var specialChars = []rune(`!'@#$%^&*()_+-=[]{};:",./?`)

func text(rand *rand.Rand, atLeast, atMost int, allowLower, allowUpper, allowNumeric, allowSpecial bool) string {
	allowedChars := []rune{}
	if allowLower {
		allowedChars = append(allowedChars, lowerLetters...)
	}
	if allowUpper {
		allowedChars = append(allowedChars, upperLetters...)
	}
	if allowNumeric {
		allowedChars = append(allowedChars, numeric...)
	}
	if allowSpecial {
		allowedChars = append(allowedChars, specialChars...)
	}

	result := []rune{}
	nTimes := rand.Intn(atMost-atLeast+1) + atLeast
	for i := 0; i < nTimes; i++ {
		result = append(result, allowedChars[rand.Intn(len(allowedChars))])
	}
	return string(result)
}

// Schema implements idk.Source
func (b *BankSource) Schema() []idk.Field {
	return b.schema
}

// KafkaSchema implements idk.KafkaSource
func (b *BankSource) KafkaSchema() string {
	meta := `"type": "record",
	"name": "bank_user_data",
	"namespace": "bank.user.data",
	"doc": "Per-user bank data"`

	fields := []string{
		`{"name": "aba", "type": "string"}`,
		`{"name": "db", "type": "string"}`,
		`{"name": "user_id", "type": "int"}`,
		`{"name": "custom_audiences", "type": ["null", "string"], "default": null}`,
	}

	// TODO redmove loops if Schema() is not generalized
	for n := 0; n < floatCount; n++ {
		fields = append(fields, `{"name": "float_`+floatNames[n]+`", `+floatMeta+`}`)
	}
	for n := 0; n < boolCount; n++ {
		fields = append(fields, `{"name": "bool_`+boolNames[n]+`", `+boolMeta+`}`)
	}
	for n := 0; n < stringCount; n++ {
		fields = append(fields, `{"name": "string_`+stringNames[n]+`", `+stringMeta+`}`)
	}
	for n := 0; n < intCount; n++ {
		fields = append(fields, `{"name": "int_`+intNames[n]+`", `+intMeta+`}`)
	}
	schema := fmt.Sprintf(`{%s, "fields": [%s]}`, meta, strings.Join(fields, ", "))

	return schema
}

func (b *BankSource) Close() error {
	return nil
}
