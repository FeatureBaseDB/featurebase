// Copyright 2020 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	nethttp "net/http"
	"os"
	"strconv"
	"strings"
	"time"

	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/http"
	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/vprint"
)

// RandomQueryConfig
type RandomQueryConfig struct {

	// user facing flags
	HostPort    string    // -hostport
	TreeDepth   int       // -d
	QueryCount  int       // -n
	Verbose     bool      // -v
	VeryVerbose bool      // -V
	TimeFromArg string    // --time.from
	TimeToArg   string    // --time.to
	TimeFrom    time.Time // parsed time
	TimeTo      time.Time // parsed time
	TimeRange   int64     // hours between parsed times

	IndexMap map[string]*Features

	API  *pilosa.API
	Info []*pilosa.IndexInfo

	BitmapFunc []string

	Rnd *rand.Rand
}

type API interface {

	// InternalClient
	Schema(ctx context.Context) ([]*pilosa.IndexInfo, error)
	Query(ctx context.Context, index string, queryRequest *pilosa.QueryRequest) (*pilosa.QueryResponse, error)

	// API for contrast; just a little different:
	//Schema(ctx context.Context) []*IndexInfo
	//Query(ctx context.Context, req *pilosa.QueryRequest) (pilosa.QueryResponse, error)
}

// have to wrap because the ugly little differences between InternalClient and API
type wrapper struct {
	api *pilosa.API
}

func (w *wrapper) Schema(ctx context.Context) ([]*pilosa.IndexInfo, error) {
	return w.api.Schema(ctx, false)
}

func (w *wrapper) Query(ctx context.Context, index string, queryRequest *pilosa.QueryRequest) (*pilosa.QueryResponse, error) {
	r, err := w.api.Query(ctx, queryRequest)
	return &r, err
}

func wrapApiToInternalClient(api *pilosa.API) *wrapper {
	return &wrapper{api: api}
}

// These times are copied from the "kitchen sink" data generator to serve as defaults.
var defaultEndTime = time.Date(2020, time.May, 4, 12, 2, 28, 0, time.UTC)
var defaultStartTime = defaultEndTime.Add(-5 * 365 * 24 * time.Hour)

// call DefineFlags before myflags.Parse()
func (cfg *RandomQueryConfig) DefineFlags(fs *flag.FlagSet) {
	fs.StringVar(&cfg.HostPort, "hostport", "localhost:10101", "host:port of pilosa to run random queries on.")
	fs.IntVar(&cfg.TreeDepth, "d", 4, "depth of random queries to generate.")
	fs.IntVar(&cfg.QueryCount, "n", 100, "number of random queries to generate. Set to 0 for inifinite queries.")
	fs.BoolVar(&cfg.Verbose, "v", false, "show queries as they are generated")
	fs.BoolVar(&cfg.VeryVerbose, "V", false, "show query results")
	fs.StringVar(&cfg.TimeFromArg, "time.from", defaultStartTime.Format(time.RFC3339), "starting time for time fields (format: 2006-01-02T15:04:05Z07:00)")
	fs.StringVar(&cfg.TimeToArg, "time.to", defaultEndTime.Format(time.RFC3339), "starting time for time fields (format: 2006-01-02T15:04:05Z07:00)")
}

// call c.ValidateConfig() after myflags.Parse()
func (c *RandomQueryConfig) ValidateConfig() error {
	if c.TreeDepth < 1 {
		return fmt.Errorf("-d depth must be 1 or greater; saw %v", c.TreeDepth)
	}
	if c.QueryCount < 0 {
		return fmt.Errorf("-n count must be 0 or greater; saw %v", c.QueryCount)
	}
	var err error
	c.TimeFrom, err = time.Parse(time.RFC3339, c.TimeFromArg)
	if err != nil {
		return fmt.Errorf("-time.from value couldn't be parsed: %w", err)
	}
	c.TimeTo, err = time.Parse(time.RFC3339, c.TimeToArg)
	if err != nil {
		return fmt.Errorf("-time.to value couldn't be parsed: %w", err)
	}
	c.TimeRange = int64(c.TimeTo.Sub(c.TimeFrom).Hours())
	if c.TimeRange < 1 {
		return fmt.Errorf("time.to (%s) should be at least one hour after time.from (%s)",
			c.TimeToArg, c.TimeFromArg)
	}
	return nil
}

var ProgramName = "random-query"

func main() {

	myflags := flag.NewFlagSet(ProgramName, flag.ExitOnError)
	cfg := NewRandomQueryConfig()
	cfg.DefineFlags(myflags)

	err := myflags.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n%v\n", err.Error())
		os.Exit(1)
	}
	err = cfg.ValidateConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s error: %s\n", ProgramName, err)
		os.Exit(1)
	}

	err = cfg.Run()

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func (cfg *RandomQueryConfig) Run() (err error) {
	remoteClient := nethttp.DefaultClient
	cli, err := http.NewInternalClient(cfg.HostPort, remoteClient)
	if err != nil {
		return err
	}
	ctx := context.Background()
	totalQ := 0
	loops := 0
	t0 := time.Now()

	report := func() {
		dur := time.Since(t0)
		if dur > 0 {
			qps := 1e9 * float64(totalQ) / float64(dur)
			vprint.AlwaysPrintf("totalQueries run: %v   elapsed: %v   qps: %0.02f", totalQ, dur, qps)
		} else {
			vprint.AlwaysPrintf("totalQueries run: %v   elapsed: %v   qps: N/A", totalQ, dur)
		}
	}
	defer report()

NewSetup:
	err = cfg.Setup(cli)
	if err != nil {
		return err
	}

	if len(cfg.IndexMap) == 0 {
		return fmt.Errorf("no rows to query")
	}

	var indexes []string
	for index := range cfg.IndexMap {
		indexes = append(indexes, index)
	}

	for j := 0; ; j++ {
		if cfg.QueryCount > 0 {
			if j >= cfg.QueryCount {
				break
			}
		} else {
			// else keep doing queries forever...
			if loops > 0 && loops%500 == 0 {
				// ...but account for any new data arrived by getting
				// the schema and rows again every so often.
				loops++
				goto NewSetup
			}
		}
		if totalQ > 0 && totalQ%100 == 0 {
			report()
		}

		index := indexes[cfg.Rnd.Intn(len(indexes))]

		pql, err := cfg.GenQuery(index)
		vprint.PanicOn(err)

		if cfg.Verbose {
			fmt.Printf("pql = '%v'\n", pql)
		}

		// Query node0.
		res, err := cli.Query(ctx, index, &pilosa.QueryRequest{Index: index, Query: pql})
		if err != nil {
			vprint.AlwaysPrintf("QUERY FAILED! queries before this=%v; err = '%v', pql='%v'", loops, err, pql)
			return err
		}
		if cfg.VeryVerbose {
			fmt.Printf("success on pql = '%v'; res='%v'\n", pql, res.Results[0])
		}
		totalQ++
		loops++

	}

	return nil
}

type Features struct {
	Slc           []IndexFieldRow
	Ranges        []IndexFieldRange
	Distinctables []IndexFieldRange
	SlcWeight     int
	RangeWeight   int
}

// Pick either a feature entry or a random query on a range, weighted
// by number of features and approximate weight of ranges
func (f *Features) RandomQuery(cfg *RandomQueryConfig) *Tree {
	r := cfg.Rnd.Intn(f.SlcWeight + f.RangeWeight)
	if r < f.SlcWeight {
		return f.Slc[r].Query(cfg)
	}
	r = cfg.Rnd.Intn(len(f.Ranges))
	return f.Ranges[r].Query(cfg)
}

func NewRandomQueryConfig() *RandomQueryConfig {
	return &RandomQueryConfig{
		IndexMap: make(map[string]*Features),
	}
}

type IndexFieldRow struct {
	Index    string
	Field    string
	RowID    uint64
	RowKey   string
	IsRowKey bool
	HasTime  bool
	IsInt    bool
}

func (fea *IndexFieldRow) Query(cfg *RandomQueryConfig) *Tree {
	fromTo := ""
	// 5% of queries on a time field will use the standard view
	// anyway.
	if fea.HasTime && cfg.Rnd.Int63n(20) != 0 {
		startHours := (cfg.Rnd.Int63n(cfg.TimeRange - 1))
		endHours := cfg.Rnd.Int63n(cfg.TimeRange-startHours) + 1 + startHours
		startTime := cfg.TimeFrom.Add(time.Duration(startHours) * time.Hour)
		endTime := cfg.TimeFrom.Add(time.Duration(endHours) * time.Hour)
		fromTo = fmt.Sprintf(", from=%s, to=%s",
			startTime.Format(pilosaTimeFmt),
			endTime.Format(pilosaTimeFmt))
	}
	if fea.IsRowKey {
		return &Tree{S: fmt.Sprintf("Row(%v='%v'%s)", fea.Field, fea.RowKey, fromTo)}
	}
	return &Tree{S: fmt.Sprintf("Row(%v=%v%s)", fea.Field, fea.RowID, fromTo)}
}

type IndexFieldRange struct {
	Index           string
	Field           string
	Min, Max, Scale int64
	ScaleDiv        float64
	Range           uint64
}

// We want to pick one of (1) a single-operation filter, (2) a
// between-filter of some kind.
// So, that's one of <=, >=, ==, !=, >, <, or
// one of [<, <], [<, <=], [<=, <=], [<=, <].
var binaryOps = []string{
	"<=", ">=", "==", "!=", "<", ">",
}

func (i *IndexFieldRange) Query(cfg *RandomQueryConfig) *Tree {
	r := cfg.Rnd.Int63n(10)
	// this is unevenly weighted, but there's no Uint64N, and
	// Int63n can't represent the whole range.
	v1 := cfg.Rnd.Uint64() % i.Range
	v2 := cfg.Rnd.Uint64() % i.Range
	if v1 > v2 {
		v1, v2 = v2, v1
	}
	v1 = v1 + uint64(i.Min)
	v2 = v2 + uint64(i.Min)
	var v1s, v2s string
	if i.Scale != 0 {
		v1s = fmt.Sprintf("%.*f", i.Scale, float64(int64(v1))/i.ScaleDiv)
		v2s = fmt.Sprintf("%.*f", i.Scale, float64(int64(v2))/i.ScaleDiv)
	} else {
		v1s = strconv.FormatInt(int64(v1), 10)
		v2s = strconv.FormatInt(int64(v2), 10)
	}
	if r < 4 {
		lte := "<="
		op1 := lte[:1+(r&1)]
		op2 := lte[:1+((r>>1)&1)]
		return &Tree{S: fmt.Sprintf("Row(%s %s %s %s %s)",
			v1s, op1, i.Field, op2, v2s)}
	} else {
		if cfg.Rnd.Int63n(2) == 1 {
			v1s = v2s
		}
		return &Tree{S: fmt.Sprintf("Row(%s %s %s)", i.Field, binaryOps[r-4], v1s)}
	}
}

// Run a RandomQuery takes a list of RowIDFeatures and ColumnKeyObjects
// and spits back a PQL query
//
func (cfg *RandomQueryConfig) Setup(api API) (err error) {
	ctx := context.Background()
	cfg.Info, err = api.Schema(ctx)
	if err != nil {
		return err
	}
	foundIntField := false
	for i, ii := range cfg.Info {
		_ = i
		for k, fld := range ii.Fields {
			_ = k
			switch fld.Options.Type {
			case "set", "mutex", "time":
				pql := fmt.Sprintf("Rows(%v)", fld.Name)

				res, err := api.Query(ctx, ii.Name, &pilosa.QueryRequest{Index: ii.Name, Query: pql})
				vprint.PanicOn(err)
				if cfg.VeryVerbose {
					fmt.Printf("success on pql = '%v'; res='%v'\n", pql, res.Results[0])
				}
				// if the option is set to use RowKeys, then must get the Keys instead of the Rows from the RowIdentifiers.
				// e.g.
				// success on pql = 'Rows(aba)'; res='&pilosa.RowIdentifiers{Rows:[]uint64(nil), Keys:[]string{"aba1", "aba2"}
				// success on pql = 'Rows(f)'; res='pilosa.RowIdentifiers{Rows:[]uint64{0x1}, Keys:[]string(nil), field:"f"}'

				switch x := res.Results[0].(type) {
				case *pilosa.RowIdentifiers:
					// internalClient gets this
					cfg.AddResponse(ii.Name, fld.Name, x, fld.Options.Type == "time")
				case pilosa.RowIdentifiers:
					// test gets this
					cfg.AddResponse(ii.Name, fld.Name, &x, fld.Options.Type == "time")
				}
			case "int":
				foundIntField = true
				fallthrough // I bet you thought you'd never see this used
			case "decimal":
				cfg.AddIntField(ii.Name, fld.Name, fld.Options.Min, fld.Options.Max, fld.Options.Scale, fld.Options.Type == "decimal")
			default:
				vprint.AlwaysPrintf("ignoring field %q: unhandled type %q\n", fld.Name, fld.Options.Type)
			}
		}
	}

	cfg.BitmapFunc = []string{"Union", "Intersect", "Xor", "Not", "Difference"}
	if foundIntField {
		cfg.BitmapFunc = append(cfg.BitmapFunc, "Distinct")
	}
	seed := int64(42)
	cfg.Rnd = rand.New(rand.NewSource(seed))

	return nil
}

func (cfg *RandomQueryConfig) AddResponse(index, field string, x *pilosa.RowIdentifiers, hasTime bool) {
	for _, rowID := range x.Rows {
		cfg.AddFeature(index, field, rowID, "", false, hasTime)
	}
	for _, rowKey := range x.Keys {
		cfg.AddFeature(index, field, 0, rowKey, true, hasTime)
	}
}

const maxEffectiveRange = 1000000

func (cfg *RandomQueryConfig) AddIntField(index, field string, min, max pql.Decimal, scale int64, decimal bool) {
	f, ok := cfg.IndexMap[index]
	if !ok {
		f = &Features{}
		cfg.IndexMap[index] = f
	}
	if min.Scale != scale || max.Scale != scale {
		vprint.PanicOn(fmt.Sprintf("scale error; min scale %d, max scale %d, field scale %d, assumed they'd be equal",
			min.Scale, max.Scale, scale))
	}

	effectiveRange := uint64(max.Value) - uint64(min.Value) + 1
	// if you have INT64_MAX and INT64_MIN, effectiveRange is 1<<64, which
	// wraps to 0. Anything closer together will be fine. We accept the loss
	// of accuracy in the range from not representing quite the full value
	// in that edge case.
	if effectiveRange == 0 {
		effectiveRange--
	}

	// we assume that the Value of the field is already scaled, I guess?
	newRange := IndexFieldRange{
		Index:    index,
		Field:    field,
		Min:      min.Value,
		Max:      max.Value,
		Scale:    scale,
		ScaleDiv: math.Pow(10, float64(scale)),
		Range:    effectiveRange,
	}
	f.Ranges = append(f.Ranges, newRange)
	if !decimal {
		f.Distinctables = append(f.Distinctables, newRange)
	}
	// We want to add more values for larger int fields, but the
	// default KitchenSink field has a range of 1<<64 which would make
	// it completely dominate weights, so...
	if effectiveRange > maxEffectiveRange {
		effectiveRange = maxEffectiveRange
	}
	f.RangeWeight += int(effectiveRange)
}

func (cfg *RandomQueryConfig) GenQuery(index string) (pql string, err error) {

	tree := cfg.GenTree(index, cfg.TreeDepth)
	pql = tree.ToPQL()

	// avoid using too much bandwidth, just count the final bitmap.
	pql = fmt.Sprintf("Count(%v)", pql)
	return
}

type Tree struct {
	Chd  []*Tree
	S    string
	Args []string // Extra args to pass after children, such as a field for Distinct.
}

func (tr *Tree) StringIndent(ind int) (s string) {
	spc := strings.Repeat("    ", ind)
	spc1 := strings.Repeat("    ", ind+1)
	var chds []string
	leaf := true
	if len(tr.Chd) == 0 {
		// leaf
	} else {
		leaf = false
		for _, chd := range tr.Chd {
			chds = append(chds, chd.StringIndent(ind+1))
		}
	}
	if leaf {
		s += fmt.Sprintf("%v %v\n", spc1, tr.S)
	} else {
		for i, c := range chds {
			if i == 0 {
				s += fmt.Sprintf("%v %v\n%v", spc, tr.S, c)
			} else {
				s += fmt.Sprintf("%v", c)
			}
		}
	}
	return
}

const pilosaTimeFmt = "2006-01-02T15:04"

func (cfg *RandomQueryConfig) GenTree(index string, depth int) (tr *Tree) {
	features := cfg.IndexMap[index]
	if depth == 0 {
		return features.RandomQuery(cfg)
	}

	r := cfg.Rnd.Intn(len(cfg.BitmapFunc))
	f := cfg.BitmapFunc[r]
	tr = &Tree{S: f}
	numChild := 2
	switch f {
	case "Union", "Intersect", "Xor":
		numChild = cfg.Rnd.Intn(8) + 2
	case "Not":
		numChild = 1
	case "Difference":
		numChild = 2
	case "Distinct":
		numChild = 1
		r = cfg.Rnd.Intn(len(features.Distinctables))
		tr.Args = append(tr.Args, fmt.Sprintf("field=%s", features.Distinctables[r].Field))
	}
	for i := 0; i < numChild; i++ {
		tr.Chd = append(tr.Chd, cfg.GenTree(index, depth-1))
	}
	return
}

func (tr *Tree) ToPQL() (s string) {

	if len(tr.Chd) == 0 {
		// leaf
		return tr.S
	}

	var chds []string
	for _, c := range tr.Chd {
		chds = append(chds, c.ToPQL())
	}
	// If we had no extra args, this does nothing.
	chds = append(chds, tr.Args...)
	all := strings.Join(chds, ", ")
	return fmt.Sprintf("%v(%v)", tr.S, all)
}

func (cfg *RandomQueryConfig) AddFeature(index, field string, rowID uint64, rowKey string, isRowKey bool, hasTime bool) {

	f, ok := cfg.IndexMap[index]
	if !ok {
		f = &Features{}
		cfg.IndexMap[index] = f
	}
	f.Slc = append(f.Slc, IndexFieldRow{
		Index:    index,
		Field:    field,
		RowID:    rowID,
		RowKey:   rowKey,
		IsRowKey: isRowKey,
		HasTime:  hasTime,
	})
	f.SlcWeight++
}
