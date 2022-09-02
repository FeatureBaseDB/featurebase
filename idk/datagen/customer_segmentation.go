package datagen

import (
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"

	"github.com/molecula/featurebase/v3/idk"
	"github.com/molecula/featurebase/v3/idk/datagen/gen"
)

/*
go run cmd/datagen/main.go \
	--source=customer_segmentation --pilosa.index=customer_segmentation \
	--start-from=1 --end-at=100 --pilosa.batch-size=100

go run cmd/datagen/main.go \
	--source=example --pilosa.index=example \
	--start-from=1 --end-at=100 --pilosa.batch-size=100
*/

// Ensure Person implements interface.
var _ Sourcer = (*Person)(nil)

// Person implements Sourcer,
type Person struct{}

// NewPerson returns a new instance of Person.
func NewPerson(cfg SourceGeneratorConfig) Sourcer {
	return &Person{}
}

// Source returns an idk.Source which will generate
// records for a partition of the entire record space,
// determined by the concurrency value. It implements
// the Sourcer interface.
func (p *Person) Source(cfg SourceConfig) idk.Source {
	g := gen.New(gen.OptGenSeed(cfg.seed))
	src := &PersonSource{
		cur:   cfg.startFrom,
		endAt: cfg.endAt,
		schema: []idk.Field{
			idk.IDField{NameVal: "id"},                                // 0
			idk.StringField{NameVal: "sex"},                           // 1
			idk.IntField{NameVal: "age"},                              // 2
			idk.StringField{NameVal: "race"},                          // 3
			idk.BoolField{NameVal: "hispanic"},                        // 4
			idk.StringField{NameVal: "political_party_affiliation"},   // 5
			idk.StringArrayField{NameVal: "education"},                // 6
			idk.IntField{NameVal: "income"},                           // 7
			idk.IDField{NameVal: "general_election_voting_frequency"}, // 8
			idk.IDField{NameVal: "primary_election_voting_frequency"}, // 9
			idk.StringArrayField{NameVal: "hobbies"},                  // 10
		},
		g:              g,
		generatePerson: getGenRandPerson(g.R),
		hobbyStash:     []string{},
	}

	src.record = make([]interface{}, len(src.schema))
	src.record[0] = uint64(0)
	src.record[1] = make([]byte, 12)

	return src
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (e *Person) PrimaryKeyFields() []string {
	return []string{"id"}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface.
func (e *Person) DefaultEndAt() uint64 {
	return 100
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (p *Person) Info() string {
	return "Generates data for customer segmentation."
}

// Ensure PersonSource implements interface.
var _ idk.Source = (*PersonSource)(nil)

// PersonSource is an instance of a source generated
// by the Sourcer implementation Person.
type PersonSource struct {
	cur   uint64
	endAt uint64

	schema []idk.Field
	record record

	g              *gen.Gen
	generatePerson func() *person
	hobbyStash     []string
}

// Record implements idk.Source.
func (ps *PersonSource) Record() (idk.Record, error) {
	if ps.cur >= ps.endAt {
		return nil, io.EOF
	}

	p := ps.generatePerson()
	ps.setRandomHobbies()

	// Populate fields
	ps.record[0] = ps.cur
	ps.record[1] = p.sex
	ps.record[2] = p.age
	ps.record[3] = p.race
	ps.record[4] = p.hispanic
	ps.record[5] = p.politicalPartyAffiliation
	ps.record[6] = educationLevelToStringArr(p.education)
	ps.record[7] = p.income
	ps.record[8] = p.generalElectionVotingFrequency
	ps.record[9] = p.primaryElectionVotingFrequency
	ps.record[10] = ps.hobbyStash

	// Increment the ID.
	ps.cur++

	return ps.record, nil
}

func (ps *PersonSource) setRandomHobbies() {
	ps.hobbyStash = ps.g.StringSliceFromListWeighted(ps.hobbyStash, hobbies, 2, 5)
}

// Schema implements idk.Source.
func (ps *PersonSource) Schema() []idk.Field {
	return ps.schema
}

// ----------------------------------------------------
// 		THIS IS WHERE THE MAGIC HAPPENS
// ----------------------------------------------------

type randValue struct {
	value       interface{}
	probability float64 // probability of being picked, all in set should add to 1.0
	count       int     // number of times val has been picked so far
}

type randVarPickerFn func() interface{}
type getStatsFn func() string

func getRandVarPicker(rng *rand.Rand, vals []randValue) (randVarPickerFn, getStatsFn) {

	cumulativeSums := make([]float64, len(vals))
	currSum := float64(0)
	for i, v := range vals {
		currSum += v.probability
		cumulativeSums[i] = currSum
	}

	// call stats at any time to get, well, stats
	stats := func() string {
		// make copy to avoid modifying original
		valsCopy := make([]randValue, len(vals))
		copy(valsCopy, vals)
		// sort counts
		sort.Slice(valsCopy, func(i, j int) bool {
			return valsCopy[i].count > valsCopy[j].count
		})
		// get total gen so far
		total := 0
		for _, v := range valsCopy {
			total += v.count
		}
		// gen str summary
		var sb strings.Builder
		for _, v := range valsCopy {
			percentage := (float64(v.count) / float64(total)) * 100
			s := fmt.Sprintf("%v (%d/%d) %0.2f%%\n", v.value, v.count, total, percentage)
			sb.WriteString(s)
		}
		return sb.String()
	}

	picker := func() interface{} {
		rv := rng.Float64()
		i := sort.SearchFloat64s(cumulativeSums, rv)
		// note that if the probabilities do not add up to 1.0
		// i will == len(cumulativeSums), hence out of bounds error
		// however, floating points are tricky and require special care
		// so I will assume that the probabilities provided were meant to
		// add closely up to 1.0 (eg it ended up being 0.999900)
		// and if the rv generated is for example 0.9999000011, the last
		// value of the slice will be picked instead. In future, there
		// should be an assertion on the vals slice probabilities to ensure
		// they add up to 1 +/- some epsilon value.
		if i == len(vals) {
			i = len(vals) - 1
		}
		vals[i].count++
		return vals[i].value
	}

	return picker, stats
}

type numericRange struct {
	min int // inclusive
	max int // inclusive
}

type raceEthnicity struct {
	race       string
	isHispanic bool
}

type educationLevel struct {
	hasCompletedHighSchool bool
	hasAttendedSomeCollege bool
	hasCompletedBachelors  bool
	hasCompletedGraduate   bool
}

var cG []string = []string{"High school diploma or GED", "Some college", "Bachelor's degree", "Doctorate and/or Professional degree"}
var cB []string = []string{"High school diploma or GED", "Some college", "Bachelor's degree"}
var cASG []string = []string{"High school diploma or GED", "Some college"}
var cHS []string = []string{"High school diploma or GED"}

func educationLevelToStringArr(e educationLevel) []string {
	if e.hasCompletedGraduate {
		return cG
	}
	if e.hasCompletedBachelors {
		return cB
	}
	if e.hasAttendedSomeCollege {
		return cASG
	}
	if e.hasCompletedHighSchool {
		return cHS
	}
	return nil
}

func pickRandNumFromRange(rng *rand.Rand, nr numericRange) int {
	upperBoundExclusive := int32((nr.max - nr.min) + 1)
	rv := int(rng.Int31n(upperBoundExclusive)) // [0, upperBoundExclusive)
	return rv + nr.min
}

type person struct {
	sex                            string
	age                            int
	race                           string
	hispanic                       bool
	politicalPartyAffiliation      string
	education                      educationLevel
	income                         int
	generalElectionVotingFrequency int
	primaryElectionVotingFrequency int
}

func getGenRandPerson(rng *rand.Rand) func() *person {

	pickSex, _ := getRandVarPicker(rng, []randValue{
		{
			value:       "Male",
			probability: 0.475,
		},
		{
			value:       "Female",
			probability: 0.475,
		},
		{
			value:       "Unspecified",
			probability: 0.05,
		},
	})

	pickAge, _ := getRandVarPicker(rng, []randValue{
		{
			value: numericRange{
				min: 0,
				max: 14,
			},
			probability: 0.1873,
		},
		{
			value: numericRange{
				min: 15,
				max: 24,
			},
			probability: 0.1327,
		}, {
			value: numericRange{
				min: 25,
				max: 54,
			},
			probability: 0.3945,
		},
		{
			value: numericRange{
				min: 55,
				max: 64,
			},
			probability: 0.1291,
		},
		{
			value: numericRange{
				min: 65,
				max: 100,
			},
			probability: 0.1563,
		},
	})

	// 75.72% (53.89% Non-Hispanic) White
	// 16.06% (14.77% Non-Hispanic) African American
	// 7.09% Asian / Pacific Islander
	// 1.13% Native American / Alaskan Native
	pickRaceAndEthnicity, _ := getRandVarPicker(rng, []randValue{
		{
			value: raceEthnicity{
				race:       "White",
				isHispanic: false,
			},
			probability: 0.5389,
		},
		{
			value: raceEthnicity{
				race:       "White",
				isHispanic: true,
			},
			probability: 0.2183,
		},
		{
			value: raceEthnicity{
				race:       "African American",
				isHispanic: false,
			},
			probability: 0.1477,
		},
		{
			value: raceEthnicity{
				race:       "African American",
				isHispanic: true,
			},
			probability: 0.0129,
		},
		{
			value: raceEthnicity{
				race:       "Asian / Pacific Islander",
				isHispanic: false,
			},
			probability: 0.0709,
		},
		{
			value: raceEthnicity{
				race:       "Native American / Alaskan Native",
				isHispanic: false,
			},
			probability: 0.0113,
		},
	})

	// Democrat 27%
	// Republican 20%
	// Independent 19%
	// Democrat & Independent 6%
	// Republican & Independent 5%
	// Democrat & Republican 4%
	// Democrat & Republican & Independent 1%
	// None Reported/ Declined to reply 18%
	pickPoliticalPartyAffiliation, _ := getRandVarPicker(rng, []randValue{
		{
			value:       "Democrat",
			probability: 0.27,
		},
		{
			value:       "Republican",
			probability: 0.20,
		},
		{
			value:       "Independent",
			probability: 0.19,
		},
		{
			value:       "Democrat and Independent",
			probability: 0.06,
		},
		{
			value:       "Republican and Independent",
			probability: 0.05,
		},
		{
			value:       "Democrat and Republican",
			probability: 0.04,
		},
		{
			value:       "Democrat and Republican and Independent",
			probability: 0.01,
		},
		{
			value:       "",
			probability: 0.18,
		},
	})

	// 89.80% High school diploma or GED
	// 61.28% Some college
	// 34.98% Bachelor's degree
	// 13.04% Master's and/or doctorate and/or professional degree

	// Percentages are additive, meaning that 89.80% High School/GED
	// includes everyone with that education level or higher.

	// Let
	// 		H = HighSchool
	// 		SC = SomeCollege
	// 		B = Bachelor's
	// 		G = Graduate/Professional

	// P(H) = 0.8980
	// P(H & SC) = 0.6128
	// P(H & SC & B) = 0.3498
	// P(H & SC & B & G) = 0.1304

	// P(SC | H) = P(H & SC) / P(H) = 0.6824
	// P(B | H & SC) = P(H & SC & B) / P(H & SC) = 0.5708
	// P(G | H & SC & B) = P(H & SC & B & G) / P(SC & B & G) = 0.3728
	pickEducationLevel := func(rng *rand.Rand) educationLevel {
		var ed educationLevel
		if rng.Float64() > 0.8980 {
			return ed
		}
		ed.hasCompletedHighSchool = true
		if rng.Float64() > 0.6824 {
			return ed
		}
		ed.hasAttendedSomeCollege = true
		if rng.Float64() > 0.5708 {
			return ed
		}
		ed.hasCompletedBachelors = true
		if rng.Float64() > 0.3728 {
			return ed
		}
		ed.hasCompletedGraduate = true
		return ed
	}

	// 9.1% $0-$15000
	// 8% $15000-$24999
	// 8.3% $25000-$34999
	// 11.7% $35000-$49999
	// 16.5% $50000-$74999
	// 12.3% 75000-99999
	// 15.5% 100000-149999
	// 8.3% 150000-199999
	// 10.3% $200000+
	pickIncome, _ := getRandVarPicker(rng, []randValue{
		{
			value: numericRange{
				min: 0,
				max: 15000,
			},
			probability: 0.091,
		},
		{
			value: numericRange{
				min: 15000,
				max: 24999,
			},
			probability: 0.08,
		}, {
			value: numericRange{
				min: 25000,
				max: 34999,
			},
			probability: 0.083,
		},
		{
			value: numericRange{
				min: 35000,
				max: 49999,
			},
			probability: 0.117,
		},
		{
			value: numericRange{
				min: 50000,
				max: 74999,
			},
			probability: 0.165,
		},
		{
			value: numericRange{
				min: 75000,
				max: 99999,
			},
			probability: 0.123,
		},
		{
			value: numericRange{
				min: 100000,
				max: 149999,
			},
			probability: 0.155,
		},
		{
			value: numericRange{
				min: 150000,
				max: 199999,
			},
			probability: 0.083,
		},
		{
			value: numericRange{
				min: 200000,
				max: 500000,
			},
			probability: 0.103,
		},
	})

	// 0 -> 21%
	// 1 -> 17%
	// 2 -> 17%
	// 3 -> 15%
	// 4 -> 38%
	pickGeneralElectionVotingFrequency, _ := getRandVarPicker(rng, []randValue{
		{
			value:       0,
			probability: 0.21,
		},
		{
			value:       1,
			probability: 0.17,
		},
		{
			value:       2,
			probability: 0.17,
		},
		{
			value:       3,
			probability: 0.15,
		},
		{
			value:       4,
			probability: 0.38,
		},
	})

	// 0 -> 51%
	// 1 -> 19%
	// 2 -> 11%
	// 3 -> 7%
	// 4 -> 11%
	pickPrimaryElectionVotingFrequency, _ := getRandVarPicker(rng, []randValue{
		{
			value:       0,
			probability: 0.51,
		},
		{
			value:       1,
			probability: 0.19,
		},
		{
			value:       2,
			probability: 0.11,
		},
		{
			value:       3,
			probability: 0.07,
		},
		{
			value:       4,
			probability: 0.11,
		},
	})

	return func() *person {
		p := &person{}
		p.sex = pickSex().(string)
		p.age = pickRandNumFromRange(rng, pickAge().(numericRange))
		raceAndEthnicity := pickRaceAndEthnicity().(raceEthnicity)
		p.race = raceAndEthnicity.race
		p.hispanic = raceAndEthnicity.isHispanic
		p.politicalPartyAffiliation = pickPoliticalPartyAffiliation().(string)
		p.education = pickEducationLevel(rng)
		p.income = pickRandNumFromRange(rng, pickIncome().(numericRange))
		p.generalElectionVotingFrequency = pickGeneralElectionVotingFrequency().(int)
		p.primaryElectionVotingFrequency = pickPrimaryElectionVotingFrequency().(int)
		return p
	}
}

func (ps *PersonSource) Close() error {
	return nil
}
