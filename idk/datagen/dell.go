package datagen

import (
	"io"
	"math/rand"

	"github.com/molecula/featurebase/v3/idk"
)

// Ensure Dell implements interface.
var _ Sourcer = (*Dell)(nil)

// Dell implements Sourcer, and returns a very basic
// data set. It can be used as an Dell for writing
// additional custom Sourcers.
type Dell struct{}

// NewDell returns a new instance of Dell.
func NewDell(cfg SourceGeneratorConfig) Sourcer {
	return &Dell{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (e *Dell) Source(cfg SourceConfig) idk.Source {
	src := &DellSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,
		rand:  rand.New(rand.NewSource(19)),
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},                                                   // 0
			idk.StringField{NameVal: "service_tag"},                                      // 1
			idk.IntField{NameVal: "record_date", Min: intptr(10000), Max: intptr(30000)}, // 2
			idk.StringField{NameVal: "system_model"},                                     // 3
			idk.StringField{NameVal: "bios_version"},                                     // 4
			idk.StringField{NameVal: "operating_system"},                                 // 5
			idk.IntField{NameVal: "system_ram_gb", Min: intptr(1), Max: intptr(256)},     // 6
			idk.IntField{NameVal: "video_fan_speed", Min: intptr(0), Max: intptr(20)},    // 7
			idk.IntField{NameVal: "proc_fan_speed", Min: intptr(0), Max: intptr(20)},     // 8
			idk.StringField{NameVal: "epsa_fail_code"},                                   // 9
			idk.StringField{NameVal: "epsa_version"},                                     // 10
			idk.StringField{NameVal: "video_controller"},                                 // 11
			idk.StringField{NameVal: "ddv_revision"},                                     // 12
			idk.StringField{NameVal: "motherboard_eppid"},                                // 13
			idk.IntField{NameVal: "proc_fan_avg", Min: intptr(0), Max: intptr(30)},       // 14
			idk.StringField{NameVal: "proc_information"},                                 // 15
		},
	}

	src.system_modelZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(system_model))-1)
	src.operating_systemZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(operating_system))-1)
	src.system_ramZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(system_ram))-1)
	src.epsa_fail_codeZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(epsa_fail_code))-1)
	src.epsa_versionZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(epsa_version))-1)
	src.video_controllerZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(video_controller))-1)
	src.ddv_revisionZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(ddv_revision))-1)
	src.motherboard_eppidZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(motherboard_eppid))-1)
	src.processor_informationZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(processor_information))-1)
	src.bios_versionZipf = rand.NewZipf(src.rand, 1.01, 4, uint64(len(bios_version))-1)

	src.record = make([]interface{}, len(src.schema))
	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (e *Dell) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (e *Dell) DefaultEndAt() uint64 {
	return 100
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (e *Dell) Info() string {
	return "Generates data representative of Dell's internal hardware tracking operations."
}

// Ensure DellSource implements interface.
var _ idk.Source = (*DellSource)(nil)

// DellSource is an instance of a source generated
// by the Sourcer implementation Dell.
type DellSource struct {
	cur   uint64
	endAt uint64

	rand                      *rand.Rand
	system_modelZipf          *rand.Zipf
	operating_systemZipf      *rand.Zipf
	system_ramZipf            *rand.Zipf
	epsa_fail_codeZipf        *rand.Zipf
	epsa_versionZipf          *rand.Zipf
	video_controllerZipf      *rand.Zipf
	ddv_revisionZipf          *rand.Zipf
	motherboard_eppidZipf     *rand.Zipf
	processor_informationZipf *rand.Zipf
	bios_versionZipf          *rand.Zipf

	schema []idk.Field
	record record
}

// Record implements idk.Source.
func (e *DellSource) Record() (idk.Record, error) {
	if e.cur >= e.endAt {
		return nil, io.EOF
	}

	e.record[0] = e.cur
	e.record[1] = e.StringDell(7)
	e.record[2] = e.generateRandomInt(30000, 10000)
	e.record[3] = system_model[e.system_modelZipf.Uint64()]
	e.record[4] = bios_version[e.bios_versionZipf.Uint64()]
	e.record[5] = operating_system[e.operating_systemZipf.Uint64()]
	e.record[6] = system_ram[e.system_ramZipf.Uint64()]
	e.record[7] = e.generateRandomInt(20, 0)
	e.record[8] = e.generateRandomInt(20, 0)
	e.record[9] = epsa_fail_code[e.epsa_fail_codeZipf.Uint64()]
	e.record[10] = epsa_version[e.epsa_versionZipf.Uint64()]
	e.record[11] = video_controller[e.video_controllerZipf.Uint64()]
	e.record[12] = ddv_revision[e.ddv_revisionZipf.Uint64()]
	e.record[13] = motherboard_eppid[e.motherboard_eppidZipf.Uint64()]
	e.record[14] = e.generateRandomInt(30, 0)
	e.record[15] = processor_information[e.processor_informationZipf.Uint64()]

	e.cur++
	return e.record, nil
}

// Schema implements idk.Source.
func (e *DellSource) Schema() []idk.Field {
	return e.schema
}
func (e *DellSource) Seed(seed int64) {
	e.rand.Seed(seed)
}

var _ Seedable = (*DellSource)(nil)

const charsetDell = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// returns a string with random characters from a predefined charset of a specified length
func (e *DellSource) StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charsetDell[e.rand.Intn(len(charsetDell))]
	}
	return string(b)
}

// returns a string with random English alphabets of the specified length
func (e *DellSource) StringDell(length int) string {
	return e.StringWithCharset(length, charsetDell)
}

// generates a random int between range max and min (inclusive)
func (e *DellSource) generateRandomInt(max int, min int) int {
	return e.rand.Intn(max-min) + min
}

func (e *DellSource) Close() error {
	return nil
}
