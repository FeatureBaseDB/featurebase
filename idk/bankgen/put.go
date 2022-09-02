package bankgen

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	liavro "github.com/linkedin/goavro/v2"
	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/common"
	"github.com/molecula/featurebase/v3/idk/kafka/csrc"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/pkg/errors"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

// PutCmd represents a command to put data into Kafka
type PutCmd struct {
	idk.ConfluentCommand `flag:"!embed"`

	Topic     string `help:"Kafka topic to post to."`
	Subject   string
	Seed      int64  `help:"Random seed for data generation"`
	Save      string `help:"Filename of record log. '-' for stdout."`
	BatchSize int    `help:"Size of record batches to submit to Kafka."`

	NumRecords int `help:"Number of records to generate"`

	DryRun bool `help:"Dry run - just flag parsing."`

	log       logger.Logger
	rand      *rand.Rand
	configMap *confluent.ConfigMap
}

// NewPutCmd creates a new PutCmd
func NewPutCmd() (*PutCmd, error) {
	var err error
	p := &PutCmd{}
	p.ConfluentCommand = idk.ConfluentCommand{
		KafkaBootstrapServers: []string{"localhost:9092"},
		SchemaRegistryURL:     "localhost:8081",
	}

	p.Topic = "banktest"
	p.Subject = "test"
	p.Save = ""
	p.BatchSize = 1000
	p.NumRecords = 10
	p.log = logger.NewVerboseLogger(os.Stderr)
	return p, err
}

func (p *PutCmd) Log() logger.Logger { return p.log }

// Run is the entry point for PutCmd
func (p *PutCmd) Run() (err error) {
	p.configMap, err = common.SetupConfluent(&p.ConfluentCommand)
	if err != nil {
		return
	}
	var auth *csrc.BasicAuth
	if len(p.SchemaRegistryUsername) > 0 {
		auth = &csrc.BasicAuth{
			KafkaSchemaApiKey:    p.SchemaRegistryUsername,
			KafkaSchemaApiSecret: p.SchemaRegistryPassword,
		}
	}
	client := csrc.NewClient(p.SchemaRegistryURL, nil, auth)

	schemaStr := getBankSchema()

	resp, err := client.PostSubjects(p.Subject, schemaStr)
	if err != nil {
		return errors.Wrap(err, "posting schema")
	}
	p.log.Printf("Posted schema ID: %d\n", resp.ID)

	licodec, err := liavro.NewCodec(schemaStr)
	if err != nil {
		return errors.Wrap(err, "li decoding schema")
	}

	p.rand = rand.New(rand.NewSource(p.Seed))

	var w io.Writer = os.Stdout
	if p.Save != "" && p.Save != "-" {
		f, err := os.Create(p.Save)
		if err != nil {
			return errors.Wrap(err, "creating record log file")
		}
		defer f.Close()

		w = f
	}

	// librdkafka has a large (1000000) default buffer.
	// limit the go buffer size to reduce memory consumption
	// but allows for the intended buffered channel behavior
	err = p.configMap.SetKey("go.produce.channel.size", 1)
	if err != nil {
		return errors.Wrap(err, "setting go.produce.channel.size")
	}
	producer, err := confluent.NewProducer(p.configMap)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	finished := int32(0)
	iter := int64(0)
	doneChan := common.LaunchKafkaEventConfirmer(producer, &finished, &iter)
	for n := 0; n < p.NumRecords; n++ {
		data := p.generateRecord()
		rec, _, err := licodec.NativeFromTextual([]byte(data))
		if err != nil {
			return errors.Wrap(err, "decoding generated data")
		}

		buf := make([]byte, 5, 1000)
		buf[0] = 0
		binary.BigEndian.PutUint32(buf[1:], uint32(resp.ID))
		buf, err = licodec.BinaryFromNative(buf, rec.(map[string]interface{}))
		if err != nil {
			return errors.Errorf("encoding:\n%+v\nerr: %v", rec, err)
		}

		var uid int
		switch v := rec.(map[string]interface{})["user_id"].(type) {
		case int:
			uid = v
		case int32:
			uid = int(v)
		}
		producer.ProduceChannel() <- &confluent.Message{
			TopicPartition: confluent.TopicPartition{Topic: &p.Topic, Partition: confluent.PartitionAny},
			Key:            []byte(strconv.Itoa(uid)),
			Value:          buf,
		}
		atomic.AddInt64(&iter, 1)

		if p.Save != "" {
			fmt.Fprintf(w, "%v\n", rec.(map[string]interface{}))
		}

	}
	atomic.AddInt32(&finished, 1)
	<-doneChan //wait till all messages are acked
	producer.Flush(10 * 1000)
	p.log.Printf("Put %d generated records\n", iter)

	return nil
}

var (
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

	floatCount  = 1
	boolCount   = 2
	stringCount = 1
	intCount    = 1

	floatNullChance  = 0.1
	boolNullChance   = 0.1
	stringNullChance = 0.1
	intNullChance    = 0.1
)

func getBankSchema() string {
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

func (p *PutCmd) generateRecord() string {
	fields := []string{
		`"aba": "` + p.ABA() + `"`,
		`"db": "` + p.Db() + `"`,
		`"user_id": ` + strconv.FormatInt(int64(p.UserID()), 10),
		`"custom_audiences": {"string": "` + p.CustomAudiences() + `"}`,
	}

	for n := 0; n < floatCount; n++ {
		if floatNullChance < p.rand.Float64() {
			floatStr := strconv.FormatFloat(p.rand.Float64(), 'f', 3, 64)
			fields = append(fields, `"float_`+floatNames[n]+`": {"float": `+floatStr+`}`)
		}
	}
	for n := 0; n < boolCount; n++ {
		if boolNullChance < p.rand.Float64() {
			boolStr := strconv.FormatBool(p.rand.Intn(2) == 1)
			fields = append(fields, `"bool_`+boolNames[n]+`": {"boolean": `+boolStr+`}`)
		}
	}

	for n := 0; n < stringCount; n++ {
		if stringNullChance < p.rand.Float64() {
			stringStr := text(p.rand, 1, 6, true, true, true, false)
			fields = append(fields, `"string_`+stringNames[n]+`": {"string": "`+stringStr+`"}`)
		}
	}

	for n := 0; n < intCount; n++ {
		if intNullChance < p.rand.Float64() {
			intStr := strconv.FormatInt(int64(p.rand.Intn(1000000)), 10)
			fields = append(fields, `"int_`+intNames[n]+`": {"int": `+intStr+`}`)
		}
	}

	return `{` + strings.Join(fields, ", ") + `}`
}

// ABA returns a random 9 numeric digit string with about 27000 possible values.
func (p *PutCmd) ABA() string {
	num := p.rand.Intn(27000) + 22213
	num2 := num/10 - 1213
	numstr := strconv.Itoa(num)
	num2str := strconv.Itoa(num2)
	numstrbytes := append([]byte(numstr), num2str[3], numstr[0], numstr[1], numstr[2])
	return string(numstrbytes)
}

// Db returns a db
func (p *PutCmd) Db() string {
	return text(p.rand, 1, 6, true, true, true, false)
}

// UserID returns a user ID
func (p *PutCmd) UserID() int {
	return p.rand.Intn(10000000) // 10 mil
}

// CustomAudiences returns a fake Custom Audience string
func (p *PutCmd) CustomAudiences() string {
	return text(p.rand, 1, 6, true, true, true, false)
}

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
