package datagen

import (
	"errors"
	"io"
	"math/rand"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/logger"
)

// Ensure Site implements interface.
var _ Sourcer = (*Site)(nil)

// Site implements Sourcer.
type Site struct{}

// NewSite returns a new instance of Site.
func NewSite(cfg SourceGeneratorConfig) Sourcer {
	return &Site{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (s *Site) Source(cfg SourceConfig) idk.Source {
	src := &SiteSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,

		rand: rand.New(rand.NewSource(22)),

		schema: []idk.Field{
			idk.IDField{NameVal: "id"},                       // 0
			idk.DecimalField{NameVal: "latitude", Scale: 4},  // 1
			idk.DecimalField{NameVal: "longitude", Scale: 4}, // 2
			idk.StringField{NameVal: "maintainer"},           // 3
			idk.StringField{NameVal: "classification"},       // 4
			idk.StringField{NameVal: "type"},                 // 5
			idk.StringField{NameVal: "service_provider"},     // 6
			idk.StringField{NameVal: "region"},               // 7
			idk.DateIntField{NameVal: "last_inspection"},     // 8
		},
	}

	src.providerZipf = rand.NewZipf(src.rand, 1.03, 3, uint64(len(serviceProviders))-1)

	src.record = make([]interface{}, len(src.schema))
	src.record[7] = int(1420070400)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (s *Site) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (s *Site) DefaultEndAt() uint64 {
	return 4688000 - 1
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (s *Site) Info() string {
	return "Generates site data (customer, size, location) - includes a foreign key for network_ts, power_ts, transaction_ts."

}

// Ensure SiteSource implements interface.
var _ idk.Source = (*SiteSource)(nil)

type SiteSource struct {
	Log logger.Logger

	rand         *rand.Rand
	providerZipf *rand.Zipf

	schema []idk.Field

	cur, endAt uint64

	record record
}

func (s *SiteSource) Record() (idk.Record, error) {
	if s.cur >= s.endAt {
		return nil, io.EOF
	}
	s.record[0] = s.cur

	// lat/lon
	longitude, latitude := s.location()
	s.record[1] = latitude
	s.record[2] = longitude

	// service provider
	var domain string
	s.record[6], domain = s.serviceProviderDomain()

	// maintainer
	s.record[3] = s.emailName() + domain

	// classification
	s.record[4] = s.classification()

	// type
	s.record[5] = s.siteType(s.cur)
	if s.record[5].(string) == "" {
		return nil, errors.New("exhausted sites data set")
	}

	// region
	s.record[7] = region(latitude, longitude)

	// last_inspection
	s.record[8] = startDayInt + s.rand.Int63n(365*10-200)

	s.cur++

	return s.record, nil
}

func (s *SiteSource) classification() string {
	num := s.rand.Intn(100)
	switch {
	case num < 3:
		return "Tier 1"
	case num < 20:
		return "Tier 2"
	case num < 40:
		return "Tier 3"
	case num < 75:
		return "Tier 4"
	default:
		return "Tier 5"
	}
}

func (s *SiteSource) serviceProviderDomain() (string, string) {
	idx := s.providerZipf.Uint64()
	return serviceProviders[idx], maintainerDomains[idx]
}

func (s *SiteSource) emailName() string {
	idx := s.rand.Intn(len(lastNames))
	lastName := lastNames[idx]
	initial := uint8(s.rand.Intn(26) + 97)
	return string([]byte{initial, '.'}) + lastName
}

func (s *SiteSource) siteType(id uint64) string {
	switch {
	case id < 1000:
		return "MSC" // 1k
	case id < 8000:
		return "Headend" // 7k
	case id < 33000:
		return "Central Office" // 25k
	case id < 88000:
		return "Substation" // 55k
	case id < 488000:
		return "Retail" // 400k
	case id < 888000:
		return "Cell Site" // 400k
	case id < 1688000:
		return "NOC" // 800k
	case id < 4688000:
		return "Data Center" // 3M
	default:
		return ""
	}
}

func (s *SiteSource) location() (lon float64, lat float64) {
	loc := locations[s.rand.Intn(len(locations))]
	off1, off2 := (s.rand.Float64()-0.5)/50, (s.rand.Float64()-0.5)/50
	return loc.lon + off1, loc.lat + off2
}

func region(latitude, longitude float64) string {
	if longitude < 80 {
		return "East"
	}
	if longitude > 105 {
		return "West"
	}
	if latitude < 39 {
		return "South"
	}
	return "North"
}

func (s *SiteSource) Schema() []idk.Field {
	return s.schema
}

func (s *SiteSource) Seed(seed int64) {
	s.rand.Seed(seed)
}

var _ Seedable = (*SiteSource)(nil)

func (s *SiteSource) Close() error {
	return nil
}
