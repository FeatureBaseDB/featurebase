package datagen

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/datagen/gen"
)

// Ensure TexasHealth implements interface.
var _ Sourcer = (*TexasHealth)(nil)

// TexasHealth implements Sourcer
type TexasHealth struct{}

// NewTexasHealth returns a new instance of NewTexasHealth.
func NewTexasHealth(cfg SourceGeneratorConfig) Sourcer {
	return &TexasHealth{}
}

type ciscoNetworkEvent struct {
	id            string
	possibleTypes []string
	protocol      string
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (t *TexasHealth) Source(cfg SourceConfig) idk.Source {

	startTime := time.Date(2020, time.December, 1, 0, 0, 0, 0, time.UTC)

	src := &TexasHealthSource{
		cur:                 cfg.startFrom,
		recordsGenForBucket: 20,
		currBucketSize:      0,
		currTimestamp:       startTime,
		endAt:               cfg.endAt,
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},                          // 0
			idk.IDField{NameVal: "event_id"},                    // 1
			idk.StringField{NameVal: "type"},                    // 2
			idk.StringField{NameVal: "protocol"},                // 3
			idk.IntField{NameVal: "probe"},                      // 4
			idk.IntField{NameVal: "severity"},                   // 5
			idk.StringField{NameVal: "level"},                   // 6
			idk.StringField{NameVal: "from_interface"},          // 7
			idk.StringField{NameVal: "from_address"},            // 8
			idk.StringField{NameVal: "from_port"},               // 9
			idk.StringField{NameVal: "to_interface"},            // 10
			idk.StringField{NameVal: "to_address"},              // 11
			idk.StringField{NameVal: "to_port"},                 // 12
			idk.IntField{NameVal: "duration_time"},              // 13
			idk.IntField{NameVal: "bytes"},                      // 14
			idk.StringField{NameVal: "hostname"},                // 15
			idk.TimestampField{NameVal: "timestamp", Unit: "s"}, // 16
			idk.StringField{NameVal: "timestamp_str"},           // 17
		},
	}

	src.g = gen.New(gen.OptGenSeed(cfg.seed))
	src.possibleEvents = []ciscoNetworkEvent{
		{
			id:            "302013",
			possibleTypes: []string{"Built inbound", "Built outbound"},
			protocol:      "TCP",
		},
		{
			id:            "302014",
			possibleTypes: []string{"Teardown"},
			protocol:      "TCP",
		},
		{
			id:            "302015",
			possibleTypes: []string{"Built inbound", "Built outbound"},
			protocol:      "UDP",
		}, {
			id:            "302016",
			possibleTypes: []string{"Teardown"},
			protocol:      "UDP",
		}, {
			id:            "302020",
			possibleTypes: []string{"Built inbound", "Built outbound"},
			protocol:      "ICMP",
		}, {
			id:            "302021",
			possibleTypes: []string{"Teardown"},
			protocol:      "ICMP",
		},
	}
	src.possibleLogLevels = []string{"Debug", "Informational", "Notification", "Warning", "Error", "Critical"}

	// generate hostnames
	var hostnames []string
	prefixes := []string{"dbprdfw", "dbrwanasa", "dbdgstsw", "dbrrassa",
		"phdasa", "dbrprdfw", "hfwasa", "phamasa", "thdrasa", "thalasa",
	}
	for _, prefix := range prefixes {
		for i := 1; i <= 10; i++ {
			hostnames = append(hostnames, fmt.Sprintf("%s%02d", prefix, i))
		}
	}
	src.possibleHostnames = hostnames

	// generate ip addresses
	var ipAddrs []string
	for i := 1; i <= 100; i++ {
		ipAddrs = append(ipAddrs, fmt.Sprintf("10.164.124.%d", i))
	}
	src.possibleIPAddrs = ipAddrs

	// ports
	src.possiblePorts = []string{"443", "80", "22", "23", "40", "8080",
		"53", "88", "115", "123", "143", "20", "21", "25", "101",
	}

	// interfaces
	src.possibleFromInterfaces = []string{"faddr",
		"Security_Cameras_250", "LAB", "PROD",
		"INSIDE", "provider", "OUTSIDE", "management",
		"Inside-DataCenter-DBR", "ATT_SBC_Outside", "Outside-WAN-DBR"}
	src.possibleToInterfaces = []string{"laddr",
		"OUTSIDE", "Inside-DataCenter", "DBR", "ATT_SBC_Inside", "Outside-WAN-DBR"}

	// rand.Zipf for generating bytes and duration
	src.randBytes = rand.NewZipf(src.g.R, 1.1, 10, 199)
	src.randDuration = rand.NewZipf(src.g.R, 1.1, 10, 9999)

	// rest
	src.record = make([]interface{}, len(src.schema))
	src.record[0] = uint64(0)
	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (t *TexasHealth) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (t *TexasHealth) DefaultEndAt() uint64 {
	return 20000
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (t *TexasHealth) Info() string {
	return "Generates sample data for the Texas Health POC."
}

// Ensure TexasHealthSource implements interface.
var _ idk.Source = (*TexasHealthSource)(nil)

// TexasHealthSource is an instance of a source generated
// by the Sourcer implementation TexasHleath.
type TexasHealthSource struct {
	g *gen.Gen

	cur   uint64
	endAt uint64

	schema []idk.Field
	record record

	recordsGenForBucket int
	currBucketSize      int
	currTimestamp       time.Time

	randBytes    *rand.Zipf
	randDuration *rand.Zipf

	possibleEvents         []ciscoNetworkEvent
	possibleLogLevels      []string
	possibleHostnames      []string
	possibleIPAddrs        []string
	possiblePorts          []string
	possibleFromInterfaces []string
	possibleToInterfaces   []string
}

func (s *TexasHealthSource) genTimestamp() time.Time {
	return s.currTimestamp
}

// advanceTick is a helper method to do some housekeeping
// after a record is generated. For one, it increments cur
// which keeps track of the number of records generated.
// Additionally, it advances the 'time-slot buckets' for
// lack of better naming. Each record generated belongs to
// a given time bucket and within that time bucket, all the
// records bear the same timestamp value (at second resolution).
// The bucket sizes are randomized and generally range between 1
// to 200. Currently, the bucket size value is generated randomly
// from a uniform distribution but a more realistic datagen should
// weight this value.
func (s *TexasHealthSource) advanceTick() {
	s.cur++
	s.recordsGenForBucket++

	if s.recordsGenForBucket >= s.currBucketSize {
		// reset bucket size to a random value between 1 and 200
		s.currBucketSize = s.g.R.Intn(200) + 1
		s.recordsGenForBucket = 0
		oneSecond := time.Duration(1e9)
		s.currTimestamp = s.currTimestamp.Add(oneSecond)
	}
}

// Record implements idk.Source.
func (s *TexasHealthSource) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}

	selectEventIndex := s.g.R.Intn(len(s.possibleEvents))
	event := s.possibleEvents[selectEventIndex]
	logLevelIndex := s.g.R.Intn(len(s.possibleLogLevels))
	timestamp := s.genTimestamp()

	s.record[0] = s.cur                                          // id
	s.record[1] = event.id                                       // event_id
	s.record[2] = s.g.StringFromList(event.possibleTypes)        // type
	s.record[3] = event.protocol                                 // protocol
	s.record[4] = 3457177187 + s.cur                             // probe
	s.record[5] = logLevelIndex + 1                              // severity
	s.record[6] = s.possibleLogLevels[logLevelIndex]             // level
	s.record[7] = s.g.StringFromList(s.possibleFromInterfaces)   // from_interface
	s.record[8] = s.g.StringFromListWeighted(s.possibleIPAddrs)  // from_address
	s.record[9] = s.g.StringFromListWeighted(s.possiblePorts)    // from_port
	s.record[10] = s.g.StringFromList(s.possibleToInterfaces)    // to_interface
	s.record[11] = s.g.StringFromListWeighted(s.possibleIPAddrs) // to_address
	s.record[12] = s.g.StringFromListWeighted(s.possiblePorts)   // to_port
	s.record[13] = s.randDuration.Uint64() + 1                   // duration_time
	s.record[14] = s.randBytes.Uint64() + 1                      // bytes
	s.record[15] = s.g.StringFromList(s.possibleHostnames)       // hostname
	s.record[16] = timestamp.Unix()                              // timestamp
	s.record[17] = timestamp.String()                            // timestamp_str

	s.advanceTick()

	return s.record, nil
}

// Schema implements idk.Source.
func (s *TexasHealthSource) Schema() []idk.Field {
	return s.schema
}
func (s *TexasHealthSource) Close() error {
	return nil
}
