package datagen

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"
)

// Ensure Custom implements Sourcer interface.
var _ Sourcer = (*Custom)(nil)

// Custom implements Sourcer
type Custom struct {
	err             error
	CustomConfig    *CustomConfig
	IDKAndGenFields *IDKAndGenFields
}

// NewCustom returns a new instance of Custom.
func NewCustom(cfg SourceGeneratorConfig) Sourcer {
	conf := &CustomConfig{}
	if bytes, err := os.ReadFile(cfg.CustomConfig); err != nil {
		return &Custom{err: errors.Wrap(err, "reading custom config file")}
	} else if err = yaml.Unmarshal(bytes, conf); err != nil {
		return &Custom{err: errors.Wrap(err, "unmarshaling custom config file")}
	} else if err := conf.PostUnmarshal(); err != nil {
		return &Custom{err: errors.Wrap(err, "post unmarshal")}
	} else if ig, err := conf.GetIDKFields(); err != nil {
		return &Custom{err: errors.Wrap(err, "getting IDK fields from custom config file")}
	} else {
		return &Custom{
			CustomConfig:    conf,
			IDKAndGenFields: ig,
		}
	}
}

// CustomConfig represents the JSON/yaml configuration for the "custom" datagen source.
type CustomConfig struct {
	// Fields describe the data to be generated.
	Fields []*GenField `json:"fields"`

	// IDKParams configure how the generated data should be treated by
	// the IDK (and probably ultimately ingested into FeatureBase).
	IDKParams IDKParams `json:"idk_params"`
}

// GenField describes one field of the data to be generated. Many of
// the struct fields are only used for certain values of the 'Type'
// field. See the commented custom.yaml example as a reference.
//
// GenField says nothing about what should be done with the generated
// data, or how it might map to FeatureBase fields.
//
// NOTE: If you add fields here, make sure you add them to the custom
// UnmarshalJSON method below.
type GenField struct {
	Name            string  `json:"name"`
	Type            string  `json:"type"`
	Distribution    string  `json:"distribution"`
	Min             int64   `json:"min"`
	Max             int64   `json:"max"`
	MinFloat        float64 `json:"min_float"`
	MaxFloat        float64 `json:"max_float"`
	Repeat          bool    `json:"repeat"`
	Step            int64   `json:"step"`
	S               float64 `json:"s"`
	V               float64 `json:"v"`
	SourceFile      string  `json:"source_file"`
	GeneratorType   string  `json:"generator_type"`
	Cardinality     uint64  `json:"cardinality"`
	Charset         string  `json:"charset"`
	MinLen          uint64  `json:"min_len"`
	MaxLen          uint64  `json:"max_len"`
	MinNum          uint64  `json:"min_num"`
	MaxNum          uint64  `json:"max_num"`
	MinStepDuration string  `json:"min_step_duration"`
	MaxStepDuration string  `json:"max_step_duration"`
	minStepDuration time.Duration
	maxStepDuration time.Duration
	MinDate         time.Time  `json:"min_date"`
	MaxDate         time.Time  `json:"max_date"`
	TimeFormat      TimeFormat `json:"time_format"`
	TimeUnit        idk.Unit   `json:"time_unit"`
	NullChance      float64    `json:"null_chance"` // between 0 and 1.0. Fraction of the time that we get a null value for this field
}

// PostUnmarshal does post-processing of the CustomConfig object after
// the basic JSON/yaml unmarshal, like converting time strings to
// durations. There's probably an elegant way to do this during the
// Unmarshal, but I couldn't figure it out.
func (c *CustomConfig) PostUnmarshal() error {
	for i, g := range c.Fields {
		if g.MinStepDuration != "" {
			dur, err := time.ParseDuration(g.MinStepDuration)
			if err != nil {
				return errors.Wrap(err, "parsing step_duration_min")
			}
			c.Fields[i].minStepDuration = dur
		}
		if g.MaxStepDuration != "" {
			dur, err := time.ParseDuration(g.MaxStepDuration)
			if err != nil {
				return errors.Wrap(err, "parsing step_duration_max")
			}
			c.Fields[i].maxStepDuration = dur
		}
	}
	return nil
}

// DefaultIDKField returns default IDK configuration for a given
// GenField. This is convenient for the common case of generated data
// wanting to be ingested into FeatureBase so that the user doesn't
// have to specify explicit "idk_params" configuration for every
// field.
func (g *GenField) DefaultIDKField() (idk.Field, error) {
	switch g.Type {
	case "uint":
		return idk.IDField{NameVal: g.Name}, nil
	case "int":
		var min int64 = g.Min
		var max int64 = g.Max
		return idk.IntField{NameVal: g.Name, Min: &min, Max: &max}, nil
	case "string":
		return idk.StringField{NameVal: g.Name}, nil
	case "string-set":
		return idk.StringArrayField{NameVal: g.Name}, nil
	case "uint-set":
		return idk.IDArrayField{NameVal: g.Name}, nil
	case "timestamp":
		return idk.TimestampField{NameVal: g.Name}, nil
	case "float":
		return idk.DecimalField{NameVal: g.Name, Scale: 2}, nil
	default:
		return nil, errors.Errorf("no default IDK field for type: '%s'", g.Type)
	}
}

// IDKParams describes how data from CustomConfig.Fields should map to IDK fields.
type IDKParams struct {
	Fields           map[string][]IngestField `json:"fields"`
	PrimaryKeyConfig PrimaryKeyConfig         `json:"primary_key_config"`
}

// IngestField is a json/yaml configuration for an IDK Field. See the
// ToIDKField method for how this configuration maps to each possible
// IDK Field.
type IngestField struct {
	Type         string    `json:"type"`
	Name         string    `json:"name"`
	Layout       string    `json:"layout"`
	Epoch        time.Time `json:"epoch"`
	Unit         string    `json:"unit"`
	Keyed        bool      `json:"keyed"`
	Mutex        bool      `json:"mutex"`
	TimeQuantum  string    `json:"time_quantum"`
	Min          *int64    `json:"min"`
	Max          *int64    `json:"max"`
	Scale        int64     `json:"scale"`
	ForeignIndex string    `json:"foreign_index"`
	Granularity  string    `json:"granularity"`
	TTL          string    `json:"ttl"`
}

// ToIDKField converts the general IngestField to a specific IDK Field.
func (f IngestField) ToIDKField(g *GenField) (idk.Field, error) {
	name := g.Name
	destName := g.Name
	if f.Name != "" {
		destName = f.Name
	}
	switch f.Type {
	case "ID":
		return idk.IDField{NameVal: name, DestNameVal: destName, Quantum: f.TimeQuantum, Mutex: f.Mutex, TTL: f.TTL}, nil
	case "IDArray":
		return idk.IDArrayField{NameVal: name, DestNameVal: destName, Quantum: f.TimeQuantum, TTL: f.TTL}, nil
	case "Int":
		return idk.IntField{NameVal: name, DestNameVal: destName, Min: f.Min, Max: f.Max, ForeignIndex: f.ForeignIndex}, nil
	case "RecordTime":
		return idk.RecordTimeField{NameVal: name, DestNameVal: destName, Layout: f.Layout, Epoch: f.Epoch, Unit: idk.Unit(f.Unit)}, nil
	case "String":
		return idk.StringField{NameVal: name, DestNameVal: destName, Mutex: f.Mutex, Quantum: f.TimeQuantum, TTL: f.TTL}, nil
	case "StringArray":
		return idk.StringArrayField{NameVal: name, DestNameVal: destName, Quantum: f.TimeQuantum, TTL: f.TTL}, nil
	case "Timestamp":
		return idk.TimestampField{NameVal: name, DestNameVal: destName, Layout: f.Layout, Epoch: f.Epoch, Unit: idk.Unit(f.Unit), Granularity: f.Granularity}, nil
	case "Decimal":
		return idk.DecimalField{NameVal: name, DestNameVal: destName, Scale: f.Scale}, nil
	default:
		return nil, errors.Errorf("unsupported idk field type: '%s' for field named: '%s'", f.Type, f.Name)

	}
}

// PrimaryKeyConfig specifies what should be interpreted as the record
// identifier in Featurebase.
type PrimaryKeyConfig struct {
	Field string `json:"field"`
}

// IDKAndGenFields keeps the IDK schema and generated data in
// sync. schema and genFields will always be the same length, and the
// idk.Fields in schema will always be non-nil. Items in genFields may
// be nil which indicates that the corresponding idk.Field in schema
// will use data generated by the last non-nil genField.
// e.g.
//
// schema:    IDField,   RecordTimeField,  TimestampField
// genFields: Gen{"id"}, Gen{"timestamp"}, nil
//
// The TimestampField will get values from Gen{"timestamp"} since its
// corresponding genField is nil.
type IDKAndGenFields struct {
	schema    []idk.Field
	genFields []*GenField
}

// Append adds to IDKAndGenFields and ensures that schema and genFields stay in sync.
func (ig *IDKAndGenFields) Append(f idk.Field, g *GenField) {
	ig.schema = append(ig.schema, f)
	ig.genFields = append(ig.genFields, g)
}

// GetIDKFields figures out what IDK Fields should be created based on
// the generated data fields and the IDK configuration parameters. If
// multiple IDK fields are generated for a single genField, the
// corresponding entries beyond the first in IDKAndGenFields.genFields
// will be nil. We'll only create one generator and use the value it
// generates for all those fields in the generated record.
func (cc *CustomConfig) GetIDKFields() (*IDKAndGenFields, error) {
	ig := &IDKAndGenFields{}
	for _, genField := range cc.Fields {
		idkFieldConfig, ok := cc.IDKParams.Fields[genField.Name]
		if !ok {
			idkField, err := genField.DefaultIDKField()
			if err != nil {
				return ig, errors.Wrapf(err, "getting default IDK field for %+v", genField)
			}
			ig.Append(idkField, genField)
			continue
		}
		for i, ingestField := range idkFieldConfig {
			idkField, err := ingestField.ToIDKField(genField)
			if err != nil {
				return ig, errors.Wrap(err, "getting configured IDK field")
			}
			if i == 0 {
				ig.Append(idkField, genField)
			} else {
				ig.Append(idkField, nil)
			}
		}
	}
	return ig, nil
}

// Source returns a configured Custom idk.Source. It does not
// currently have explicit support for concurrency or paritioned data
// generation as some of the other datagen sources do.
func (c *Custom) Source(cfg SourceConfig) idk.Source {
	if c.err != nil {
		log.Fatal(c.err) // TODO add error return arg to Sourcer interface
	}

	r := rand.New(rand.NewSource(cfg.seed))

	generators := make([]FieldGenerator, len(c.IDKAndGenFields.genFields))
	for i, g := range c.IDKAndGenFields.genFields {
		var err error
		if g == nil {
			generators[i] = nil
			continue
		}
		generators[i], err = g.Generator(r)
		log.Printf("Generator for %s, %+v\n", g.Name, generators[i])
		if err != nil {
			log.Fatal(errors.Wrap(err, "getting generators"))
		}
	}

	log.Printf("schema: %+v", c.IDKAndGenFields.schema)
	recordsToGenerate := uint64(0)
	if cfg.endAt > 0 {
		recordsToGenerate = cfg.endAt - cfg.startFrom
	}
	return &CustomSource{
		conf:   c.CustomConfig,
		schema: c.IDKAndGenFields.schema,

		generators:        generators,
		record:            make([]interface{}, len(generators)),
		recordsToGenerate: recordsToGenerate,
	}
}

// PrimaryKeyFields returns the fields from the schema
// which should be used as the index's primary key.
func (c *Custom) PrimaryKeyFields() []string {
	if c.err != nil {
		log.Fatal(errors.Wrap(c.err, "can't get primary key fields"))
	}
	return []string{c.CustomConfig.IDKParams.PrimaryKeyConfig.Field}
}

// DefaultEndAt sets the endAt record value for the
// case where one is not provided. It implements the
// Sourcer interface. Not used for Custom.
func (_ *Custom) DefaultEndAt() uint64 {
	return 0
}

// Info describes what this implementation of Sourcer
// generates. It implements the Sourcer interface.
func (_ *Custom) Info() string {
	return "Generates data from a yaml definition file."
}

// Ensure CustomSource implements interface.
var _ idk.Source = (*CustomSource)(nil)

// CustomSource is an instance of a source generated
// by the Sourcer implementation Custom.
type CustomSource struct {
	conf       *CustomConfig
	generators []FieldGenerator

	schema            []idk.Field
	record            record
	recordsToGenerate uint64
	recordCounter     uint64
}

// Schema returns the IDK fields which were determined from the data
// generation and ingest configuration parameters in the custom yaml
// file.
func (cs *CustomSource) Schema() []idk.Field {
	return cs.schema
}

// Record uses the configured generators to emit a custom record. If
// any of the generators are nil, it uses the value from the last
// non-nil generator.
func (cs *CustomSource) Record() (idk.Record, error) {
	// track last generated value
	var last interface{}
	for i, gen := range cs.generators {
		// this indicates that the same value is being used for multiple IDK fields.
		if gen == nil {
			cs.record[i] = last
			continue
		}
		var err error
		cs.record[i], err = gen.Generate(cs.record[i])
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, errors.Wrapf(err, "generating for %+v", cs.schema[i])
		}
		last = cs.record[i]
	}
	if cs.recordsToGenerate > 0 && cs.recordCounter >= cs.recordsToGenerate {
		// break when number records produced
		return nil, io.EOF
	} else {
		cs.recordCounter++
	}
	return cs.record, nil
}

func (c *CustomSource) Close() error {
	return nil
}

// FieldGenerator is an interface for generating values for a particular field.
type FieldGenerator interface {
	// Generate produces a value. It takes in the previous value as an
	// optimization—some implementations may opt to reuse a slice, for
	// example.
	Generate(oldval interface{}) (interface{}, error)
}

type NullChanceGenerator struct {
	R          *rand.Rand
	NullChance float64
	G          FieldGenerator
}

func (g *NullChanceGenerator) Generate(oldval interface{}) (interface{}, error) {
	if g.R.Float64() < g.NullChance {
		return nil, nil
	}
	return g.G.Generate(oldval)
}

// Generator creates a data generator based on the GenField.
func (g *GenField) Generator(r *rand.Rand) (fg FieldGenerator, err error) {
	if g.NullChance < 0 || g.NullChance > 1.0 {
		return nil, errors.Errorf("null_chance must be in the range [0.0, 1.0], but got: %f", g.NullChance)
	}
	defer func() {
		if err != nil {
			return
		}
		if g.NullChance == 0.0 {
			return
		}
		// if NullChance > 0, we'll wrap the generator we're going to return in a NullChanceGenerator
		fg = &NullChanceGenerator{
			R:          r,
			NullChance: g.NullChance,
			G:          fg,
		}
	}()
	switch g.Type {
	case "uint":
		if g.Distribution != "sequential" && g.Distribution != "" {
			return nil, errors.Errorf("unsupported distribution %s for uint field", g.Distribution)
		}
		if g.Step < 1 {
			return nil, errors.Errorf("step must be >=1 for uint, got: %d", g.Step)
		}
		return &SequentialUintFieldGenerator{
			cur:    uint64(g.Min),
			Min:    uint64(g.Min),
			Max:    uint64(g.Max),
			Repeat: g.Repeat,
			Step:   uint64(g.Step),
		}, nil
	case "int":
		return getIntGenerator(g, r)
	case "string":
		return getStringGenerator(g, r)
	case "string-set":
		sg, err := getStringGenerator(g, r)
		if err != nil {
			return nil, errors.Wrap(err, "getting string generator")
		}
		return NewStringSetGenerator(g, r, sg)
	case "uint-set":
		ig, err := getIntGenerator(g, r)
		if err != nil {
			return nil, errors.Wrap(err, "getting int generator")
		}
		return &UintSetGenerator{
			G: ig,
			R: r,

			MinNum: g.MinNum,
			MaxNum: g.MaxNum,
		}, nil
	case "timestamp":
		return getTimestampGenerator(g, r)
	case "float":
		switch g.Distribution {
		case "uniform", "":
			return &UniformFloatGenerator{
				R:        r,
				MinFloat: g.MinFloat,
				MaxFloat: g.MaxFloat,
			}, nil
		default:
			return nil, errors.Errorf("unsupported distribution: '%s'", g.Distribution)
		}
	default:
		return nil, errors.Errorf("unsupported type '%s'", g.Type)

	}
}

func getIntGenerator(g *GenField, r *rand.Rand) (FieldGenerator, error) {
	if g.Max == 0 && g.Min == 0 {
		g.Max = 1000 // default
	}
	if g.Max <= g.Min {
		return nil, errors.Errorf("max must be > min for int field min: %d, max: %d", g.Min, g.Max)
	}
	switch g.Distribution {
	case "uniform", "":
		return &UniformIntFieldGenerator{
			R:   r,
			Min: g.Min,
			Max: g.Max,
		}, nil
	case "zipfian":
		z := rand.NewZipf(r, g.S, g.V, uint64(g.Max-g.Min))
		if z == nil {
			return nil, errors.Errorf("err getting int zipf: s: %f, v: %f, imax: %d", g.S, g.V, uint64(g.Max-g.Min))
		}
		return &ZipfianIntFieldGenerator{
			Z:   z,
			Min: g.Min,
		}, nil
	default:
		return nil, errors.Errorf("unsupported distribution for int, got: %s", g.Distribution)
	}
}

func getStringGenerator(g *GenField, r *rand.Rand) (FieldGenerator, error) {
	if g.SourceFile != "" {
		vals, err := readLines(g.SourceFile)
		if err != nil {
			return nil, errors.Wrapf(err, "reading source file for field '%s'", g.Name)
		}
		switch g.Distribution {
		case "zipfian", "":
			if g.S == 0.0 && g.V == 0.0 {
				g.S, g.V = 1.1, 5.1 // sane defaults
			}
			z := rand.NewZipf(r, g.S, g.V, uint64(len(vals)-1))
			if z == nil {
				return nil, errors.Errorf("err getting string zipf: s: %f, v: %f, imax: %d", g.S, g.V, uint64(len(vals)-1))
			}
			return &ZipfianStringSourceFieldGenerator{
				Source: vals,
				Z:      z,
			}, nil
		case "uniform":
			return &UniformStringSourceFieldGenerator{
				Source: vals,
				R:      r,
			}, nil
		case "fixed": // use probabilities from file
			vals, probs, err := readLinesWithProbabilities(g.SourceFile)
			if err != nil {
				return nil, errors.Wrapf(err, "reading source file for field '%s'", g.Name)
			}
			return NewFixedProbabilitySourceFieldGenerator(vals, probs, r)
		default:
			return nil, errors.Errorf("unknown distribution: '%s'", g.Distribution)
		}
	} else if g.GeneratorType == "random-string" {
		if g.MinLen > g.MaxLen || g.MaxLen == 0 {
			return nil, errors.Errorf("invalid max and min lengths: min(<=max): %d, max(>0): %d", g.MinLen, g.MaxLen)
		}
		return &RandomStringGenerator{
			R:       r,
			MinLen:  g.MinLen,
			MaxLen:  g.MaxLen,
			Charset: g.Charset,
		}, nil
	} else if g.Distribution == "shifting" {
		return NewShiftingStringGenerator(g, r)
	}
	return nil, errors.Errorf("unsupported string config for field '%s'", g.Name)
}

func getTimestampGenerator(g *GenField, r *rand.Rand) (FieldGenerator, error) {
	switch g.Distribution {
	case "increasing":
		if g.TimeFormat == "" {
			g.TimeFormat = timestampFormat
		}
		if !(g.TimeFormat == unixFormat || g.TimeFormat == timestampFormat) {
			return nil, errors.Errorf("'%s' is not a valid output format", g.TimeFormat)
		}
		return &IncreasingTimestampGenerator{
			R:               r,
			MinDate:         g.MinDate,
			MaxDate:         g.MaxDate,
			MaxStepDuration: g.maxStepDuration,
			MinStepDuration: g.minStepDuration,
			Repeat:          g.Repeat,
			OutputFormat:    g.TimeFormat,
			Unit:            g.TimeUnit,
		}, nil
	default:
		return nil, errors.Errorf("unimplemented distribution: %s", g.Distribution)
	}
}

// SequentialUintFieldGenerator generates unsigned integers
// sequentially from a minimum up to a maximum with a configurable
// step. It will start from the beginning when it reaches the max if
// repeat==true.
type SequentialUintFieldGenerator struct {
	cur    uint64
	Min    uint64
	Max    uint64
	Repeat bool
	Step   uint64
}

func (g *SequentialUintFieldGenerator) Generate(_ interface{}) (interface{}, error) {
	ret := g.cur
	g.cur += g.Step
	if g.cur > g.Max {
		if g.Repeat {
			g.cur = g.Min
		} else {
			return nil, io.EOF
		}
	}
	return ret, nil
}

// UniformIntFieldGenerator generates random integers between Min and Max with a uniform distribution
type UniformIntFieldGenerator struct {
	R   *rand.Rand
	Min int64
	Max int64
}

func (g *UniformIntFieldGenerator) Generate(_ interface{}) (interface{}, error) {
	return g.R.Int63n(g.Max+1-g.Min) + g.Min, nil
}

type ZipfianIntFieldGenerator struct {
	Z   *rand.Zipf
	Min int64
}

func (g *ZipfianIntFieldGenerator) Generate(_ interface{}) (interface{}, error) {
	return int64(g.Z.Uint64()) + g.Min, nil
}

type ZipfianStringSourceFieldGenerator struct {
	Source []string
	Z      *rand.Zipf
}

func (g *ZipfianStringSourceFieldGenerator) Generate(_ interface{}) (interface{}, error) {
	return g.Source[g.Z.Uint64()], nil
}

type UniformStringSourceFieldGenerator struct {
	Source []string
	R      *rand.Rand
}

func (g *UniformStringSourceFieldGenerator) Generate(_ interface{}) (interface{}, error) {
	return g.Source[g.R.Intn(len(g.Source))], nil
}

func readLines(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	out := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		out = append(out, scanner.Text())
	}
	return out, nil
}

func readLinesWithProbabilities(filename string) ([]string, []float64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	values := make([]string, 0)
	probabilities := make([]float64, 0)
	r := csv.NewReader(file)
	r.FieldsPerRecord = -1
	r.ReuseRecord = true

	var record []string
	for record, err = r.Read(); err == nil; record, err = r.Read() {
		if len(record) == 0 {
			continue
		}
		values = append(values, record[0])
		if len(record) > 1 {
			var p float64
			p, err = strconv.ParseFloat(record[1], 64)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "could not parse '%s' as a probability", record[1])
			}
			probabilities = append(probabilities, p)
		} else {
			probabilities = append(probabilities, -1.0)
		}
	}
	if err == io.EOF {
		err = nil
	}
	return values, probabilities, err
}

type RandomStringGenerator struct {
	R *rand.Rand

	MinLen  uint64
	MaxLen  uint64
	Charset string
}

func (g *RandomStringGenerator) Generate(prev interface{}) (interface{}, error) {
	length := g.R.Int63n(int64(g.MaxLen-g.MinLen)+1) + int64(g.MinLen)
	buf, ok := prev.([]byte)
	if !ok {
		buf = make([]byte, g.MaxLen)
	}
	buf = buf[:0]
	for i := int64(0); i < length; i++ {
		buf = append(buf, g.Charset[g.R.Intn(len(g.Charset))])
	}
	return buf, nil
}

type StringSetGenerator struct {
	G FieldGenerator // for generating individual strings
	R *rand.Rand

	MinNum uint64
	MaxNum uint64
}

func NewStringSetGenerator(g *GenField, r *rand.Rand, fg FieldGenerator) (*StringSetGenerator, error) {
	return &StringSetGenerator{
		G: fg,
		R: r,

		MinNum: g.MinNum,
		MaxNum: g.MaxNum,
	}, nil
}

func (g *StringSetGenerator) Generate(prev interface{}) (interface{}, error) {
	sslice, ok := prev.([]string)
	if !ok {
		sslice = make([]string, g.MaxNum)
	}
	sslice = sslice[:0]
	num := g.R.Int63n(int64(g.MaxNum-g.MinNum)+1) + int64(g.MinNum)
	var val []byte
	for i := int64(0); i < num; i++ {
		vali, err := g.G.Generate(val)
		if err != nil {
			return nil, errors.Wrap(err, "generating string")
		}
		if val, ok := vali.([]byte); ok {
			sslice = append(sslice, string(val))
		} else {
			sslice = append(sslice, vali.(string))
		}
	}
	return sslice, nil
}

type UintSetGenerator struct {
	G FieldGenerator
	R *rand.Rand

	MinNum uint64
	MaxNum uint64
}

func (g *UintSetGenerator) Generate(_ interface{}) (interface{}, error) {
	num := g.R.Intn(int(g.MaxNum-g.MinNum)+1) + int(g.MinNum)
	// ok, so we allocate a new slice for each record here, gross, I
	// know. Problem is that IDK ultimately passes these to
	// client.Batch.Add which adds the slice directly to a thing. This
	// works differently for the string-set case (with []string) which
	// is kind of horrible, and we should really make Batch.Add very
	// clear and consistent about its semantics. TODO Right now
	// though, we have this function which allocates a new slice every
	// time, and the string ones above reuse the previous slice, and I
	// just wanted to make sure everyone knows that that's on purpose.
	uslice := make([]uint64, num)
	for i := 0; i < num; i++ {
		val, err := g.G.Generate(nil)
		if err != nil {
			return nil, errors.Wrap(err, "generating uint")
		}
		uslice[i] = uint64((val.(int64)))
	}
	return uslice, nil
}

type IncreasingTimestampGenerator struct {
	R    *rand.Rand
	prev time.Time

	MinDate         time.Time
	MaxDate         time.Time
	MinStepDuration time.Duration
	MaxStepDuration time.Duration
	Repeat          bool

	OutputFormat TimeFormat // "time", "int"
	Unit         idk.Unit   // "s", "ms", "us", "ns"
}

type TimeFormat string

const (
	timestampFormat = "timestamp" // time.Time
	unixFormat      = "unix"      // unix epoch number (int64)
)

func (g *IncreasingTimestampGenerator) Generate(_ interface{}) (interface{}, error) {
	step := time.Duration(g.R.Int63n(int64(g.MaxStepDuration)-int64(g.MinStepDuration)+1)) + g.MinStepDuration

	if g.prev.IsZero() {
		g.prev = g.MinDate
	}

	next := g.prev.Add(step)
	if next.After(g.MaxDate) {
		if !g.Repeat {
			return nil, io.EOF
		}
		next = g.MinDate.Add(step)
	}
	g.prev = next
	var ret interface{} = next
	if g.OutputFormat == unixFormat {
		unitDuration, err := g.Unit.Duration()
		if err != nil {
			return nil, err
		}
		ret = next.UnixNano() / int64(unitDuration)
	}
	return ret, nil
}

type UniformFloatGenerator struct {
	R        *rand.Rand
	MinFloat float64
	MaxFloat float64
}

func (g *UniformFloatGenerator) Generate(_ interface{}) (interface{}, error) {
	return g.R.Float64()*(g.MaxFloat-g.MinFloat) + g.MinFloat, nil
}

type FixedProbabilitySourceFieldGenerator struct {
	r             *rand.Rand
	source        []string
	probabilities []float64
}

func NewFixedProbabilitySourceFieldGenerator(vals []string, probs []float64, r *rand.Rand) (*FixedProbabilitySourceFieldGenerator, error) {
	total := float64(0)
	numUnspecified := 0
	for _, prob := range probs {
		if prob < 0 {
			numUnspecified++
			continue
		}
		total += prob
	}
	if total > 1.0 {
		return nil, errors.Errorf("sum of all probabilities is greater than 1: %f", total)
	}
	unspecProb := 0.0
	if numUnspecified > 0 {
		unspecProb = (1.0 - total) / float64(numUnspecified)
	}
	total = float64(0)
	for i, prob := range probs {
		if prob < 0 {
			probs[i] = unspecProb
		}
		total += prob
		probs[i] = total
	}
	return &FixedProbabilitySourceFieldGenerator{
		r:             r,
		source:        vals,
		probabilities: probs,
	}, nil
}

func (g *FixedProbabilitySourceFieldGenerator) Generate(_ interface{}) (interface{}, error) {
	val := g.r.Float64()
	idx := sort.SearchFloat64s(g.probabilities, val)
	if idx == len(g.probabilities) {
		return nil, nil
	}
	return g.source[idx], nil
}

type ShiftingStringGenerator struct {
	shiftAmnt   uint64
	z           *rand.Zipf
	i           uint64
	step        uint64
	baseLen     uint64
	lenVariance uint64
}

func (g *ShiftingStringGenerator) Generate(_ interface{}) (interface{}, error) {
	num := g.z.Uint64() + g.shiftAmnt
	g.i++
	if g.i%g.step == 0 {
		g.shiftAmnt++
	}
	length := g.baseLen + num%(g.lenVariance+1)
	return numToString(num, length), nil
}

func numToString(num uint64, length uint64) string {
	return fmt.Sprintf("%0*X", length, num)
}

func NewShiftingStringGenerator(g *GenField, r *rand.Rand) (*ShiftingStringGenerator, error) {
	if g.MinLen+g.MaxLen != 0 {
		if g.MinLen == 0 {
			g.MinLen = g.MaxLen
		} else if g.MaxLen == 0 {
			g.MaxLen = g.MinLen
		}
	}
	if g.MaxLen < g.MinLen {
		return nil, errors.Errorf("max_len(%d) must be greater than or equal to min_len(%d)", g.MaxLen, g.MinLen)
	}
	if g.Step < 0 {
		return nil, errors.Errorf("'step' for shifting must be positive, but got: %d", g.Step)
	}
	if g.Step == 0 {
		g.Step = 1 // default to 1 if no step is passed
	}
	if g.S == 0 {
		g.S = 1.01
	}
	if g.V == 0 {
		g.V = 200
	}
	z := rand.NewZipf(r, g.S, g.V, g.Cardinality-1)
	return &ShiftingStringGenerator{
		z:           z,
		step:        uint64(g.Step),
		baseLen:     g.MinLen,
		lenVariance: g.MaxLen - g.MinLen,
	}, nil
}
