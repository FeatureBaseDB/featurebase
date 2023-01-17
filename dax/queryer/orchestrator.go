// Copyright 2021 Molecula Corp. All rights reserved.
package queryer

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/stats"
	"github.com/featurebasedb/featurebase/v3/tracing"
	"golang.org/x/sync/errgroup"
)

// Field types.
const (
	FieldTypeSet       = "set"
	FieldTypeInt       = "int"
	FieldTypeTime      = "time"
	FieldTypeMutex     = "mutex"
	FieldTypeBool      = "bool"
	FieldTypeDecimal   = "decimal"
	FieldTypeTimestamp = "timestamp"

	// Row ids used for boolean fields.
	falseRowID = uint64(0)
	trueRowID  = uint64(1)
)

var ErrFieldNotFound error = dax.NewErrFieldDoesNotExist("")

const (
	errConnectionRefused = "connect: connection refused"
)

type Topologer interface {
	ComputeNodes(ctx context.Context, index string, shards []uint64) ([]dax.ComputeNode, error)
}

type MDSTopology struct {
	noder dax.Noder
}

func (m *MDSTopology) ComputeNodes(ctx context.Context, index string, shards []uint64) ([]dax.ComputeNode, error) {
	var daxShards = make(dax.ShardNums, len(shards))
	for i, s := range shards {
		daxShards[i] = dax.ShardNum(s)
	}

	// TODO(tlt): this needs review; MDSTopology is converting from
	// string/uint64 to qtid/shardNum?? Perhaps we can get rid of the Topologer
	// interface altogether and replace it with dax.Noder.
	qtid := dax.TableKey(index).QualifiedTableID()

	return m.noder.ComputeNodes(ctx, qtid, daxShards...)
}

// TODO(jaffee) we need version info in here ASAP. whenever schema or topo
// changes, version gets bumped and nodes know to reject queries
// and update their info from the MDS instead of querying it every
// time.
type Translator interface {
	CreateIndexKeys(ctx context.Context, index string, keys []string) (map[string]uint64, error)
	CreateFieldKeys(ctx context.Context, index string, field string, keys []string) (map[string]uint64, error)
	FindIndexKeys(ctx context.Context, index string, keys []string) (map[string]uint64, error)
	FindFieldKeys(ctx context.Context, index, field string, keys []string) (map[string]uint64, error)
	// TODO(jaffee) the naming here is a cluster. TranslateIndexIDs takes a list, but TranslateFieldIDs takes a set, both have alternate methods that take the other thing. :facepalm:
	TranslateIndexIDs(ctx context.Context, index string, ids []uint64) ([]string, error)
	TranslateIndexIDSet(ctx context.Context, index string, ids map[uint64]struct{}) (map[uint64]string, error)
	TranslateFieldIDs(ctx context.Context, tableKeyer dax.TableKeyer, field string, ids map[uint64]struct{}) (map[uint64]string, error)
	TranslateFieldListIDs(ctx context.Context, index, field string, ids []uint64) ([]string, error)
}

// executor recursively executes calls in a PQL query across all shards.
type orchestrator struct {
	schema   featurebase.SchemaAPI
	topology Topologer
	trans    Translator

	// Client used for remote requests.
	client *featurebase.InternalClient

	logger logger.Logger
}

func emptyResult(c *pql.Call) interface{} {
	switch c.Name {
	case "Clear", "ClearRow":
		return false
	case "Row":
		return &featurebase.Row{Keys: []string{}}
	case "Rows":
		return featurebase.RowIdentifiers{Keys: []string{}}
	case "IncludesColumn":
		return false
	}
	return nil
}

// Execute executes a PQL query.
func (o *orchestrator) Execute(ctx context.Context, tableKeyer dax.TableKeyer, q *pql.Query, shards []uint64, opt *featurebase.ExecOptions) (featurebase.QueryResponse, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "orchestrator.Execute")
	span.LogKV("pql", q.String())
	defer span.Finish()

	resp := featurebase.QueryResponse{}

	qtbl, ok := tableKeyer.(*dax.QualifiedTable)
	if !ok {
		return resp, errors.New(errors.ErrUncoded, "orchestrator.Execute expects a dax.QualifiedTable")
	}

	// Check for query cancellation.
	if err := validateQueryContext(ctx); err != nil {
		return resp, err
	}

	// Default options.
	if opt == nil {
		opt = &featurebase.ExecOptions{}
	}

	results, err := o.execute(ctx, tableKeyer, q, shards, opt)
	if err != nil {
		return resp, err
	} else if err := validateQueryContext(ctx); err != nil {
		return resp, err
	}
	resp.Results = results

	if err := o.translateResults(ctx, qtbl, q.Calls, results, opt.MaxMemory); err != nil {
		if errors.Cause(err) == featurebase.ErrTranslatingKeyNotFound {
			// No error - return empty result
			resp.Results = make([]interface{}, len(q.Calls))
			for i, c := range q.Calls {
				resp.Results[i] = emptyResult(c)
			}
			return resp, nil
		}
		return resp, err
	} else if err := validateQueryContext(ctx); err != nil {
		return resp, err
	}

	return resp, nil
}

func (o *orchestrator) execute(ctx context.Context, tableKeyer dax.TableKeyer, q *pql.Query, shards []uint64, opt *featurebase.ExecOptions) ([]interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.execute")
	defer span.Finish()

	index := string(tableKeyer.Key())

	// Apply translations if necessary.
	var colTranslations map[string]map[string]uint64            // colID := colTranslations[index][key]
	var rowTranslations map[string]map[string]map[string]uint64 // rowID := rowTranslations[index][field][key]
	if !opt.Remote {
		cols, rows, err := o.preTranslate(ctx, index, q.Calls...)
		if err != nil {
			return nil, err
		}
		colTranslations, rowTranslations = cols, rows
	}

	// Execute each call serially.
	results := make([]interface{}, 0, len(q.Calls))
	for i, call := range q.Calls {

		if err := validateQueryContext(ctx); err != nil {
			return nil, err
		}

		// Apply call translation.
		if !opt.Remote && !opt.PreTranslated {
			translated, err := o.translateCall(ctx, call, tableKeyer, colTranslations, rowTranslations)
			if err != nil {
				return nil, errors.Wrap(err, "translating call")
			}
			if translated == nil {
				results = append(results, emptyResult(call))
				continue
			}

			call = translated
		}

		// If you actually make a top-level Distinct call, you
		// want a featurebase.SignedRow back. Otherwise, it's something else
		// that will be using it as a row, and we only care
		// about the positive values, because only positive values
		// are valid column IDs. So we don't actually eat top-level
		// pre calls.
		if call.Name == "Count" {
			// Handle count specially, skipping the level directly underneath it.
			for _, child := range call.Children {
				err := o.handlePreCallChildren(ctx, tableKeyer, child, shards, opt)
				if err != nil {
					return nil, err
				}
			}
		} else {
			err := o.handlePreCallChildren(ctx, tableKeyer, call, shards, opt)
			if err != nil {
				return nil, err
			}
		}
		var v interface{}
		var err error
		// Top-level calls don't need to precompute cross-index things,
		// because we can just pick whatever index we want, but we
		// still need to handle them. Since everything else was
		// already precomputed by handlePreCallChildren, though,
		// we don't need this logic in executeCall.
		newIndex := call.CallIndex()
		newTableKeyer := dax.StringTableKeyer(newIndex)
		if newIndex != "" && newIndex != index {
			v, err = o.executeCall(ctx, newTableKeyer, call, nil, opt)
		} else {
			v, err = o.executeCall(ctx, tableKeyer, call, shards, opt)
		}
		if err != nil {
			return nil, err
		}

		if vc, ok := v.(featurebase.ValCount); ok {
			vc.Cleanup()
			v = vc
		}

		results = append(results, v)
		// Some Calls can have significant data associated with them
		// that gets generated during processing, such as Precomputed
		// values. Dumping the precomputed data, if any, lets the GC
		// free the memory before we get there.
		o.dumpPrecomputedCalls(ctx, q.Calls[i])
	}
	return results, nil
}

// handlePreCalls traverses the call tree looking for calls that need
// precomputed values (e.g. Distinct, UnionRows, ConstRow...).
func (o *orchestrator) handlePreCalls(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) error {
	index := string(tableKeyer.Key())

	if c.Name == "Precomputed" {
		idx := c.Args["valueidx"].(int64)
		if idx >= 0 && idx < int64(len(opt.EmbeddedData)) {
			row := opt.EmbeddedData[idx]
			c.Precomputed = make(map[uint64]interface{}, len(row.Segments))
			for _, segment := range row.Segments {
				c.Precomputed[segment.Shard()] = &featurebase.Row{Segments: []featurebase.RowSegment{segment}}
			}
		} else {
			return fmt.Errorf("no precomputed data! index %d, len %d", idx, len(opt.EmbeddedData))
		}
		return nil
	}
	newIndex := c.CallIndex()
	// A cross-index query is handled by precall. This is inefficient,
	// but we have to do it for now because shards might be different and
	// we haven't implemented the local precalls that would be enough
	// in some cases.
	//
	// This makes simple cross-index queries noticably inefficient.
	//
	// If you're here because of that: We should be using PrecallLocal
	// in cases where the call isn't already PrecallGlobal, and
	// PrecallLocal should wait until we're running on a specific node
	// to do the farming-out of just the sub-queries it has to run
	// for its local shards.
	//
	// As is, we have one node querying every node, then sending out
	// all the data to every node, including the data that node already
	// has. We could reduce the actual copying around dramatically,
	// but only in the cases where local is good enough -- not something
	// like Distinct, where you can't predict output shard for a result
	// from the shard being queried.
	if newIndex != "" && newIndex != index {
		c.Type = pql.PrecallGlobal
		index = newIndex
		tableKeyer = dax.StringTableKeyer(index)
		// we need to recompute shards, then
		shards = nil
	}
	if err := o.handlePreCallChildren(ctx, tableKeyer, c, shards, opt); err != nil {
		return err
	}
	// child calls already handled, no precall for this, so we're done
	if c.Type == pql.PrecallNone {
		return nil
	}
	// We don't try to handle sub-calls from here. I'm not 100%
	// sure that's right, but I think the fact that they're happening
	// inside a precomputed call may mean they need different
	// handling. In any event, the sub-calls will get handled by
	// the executeCall when it gets to them...

	// We set c to look like a normal call, and actually execute it:
	c.Type = pql.PrecallNone
	// possibly override call index.
	v, err := o.executeCall(ctx, tableKeyer, c, shards, opt)
	if err != nil {
		return err
	}
	var row *featurebase.Row
	switch r := v.(type) {
	case *featurebase.Row:
		row = r
	case featurebase.SignedRow:
		row = r.Pos
	default:
		return fmt.Errorf("precomputed call %s returned unexpected non-Row data: %T", c.Name, v)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Children = []*pql.Call{}
	c.Name = "Precomputed"
	c.Args = map[string]interface{}{"valueidx": len(opt.EmbeddedData)}
	// stash a copy of the full results, which can be forwarded to other
	// shards if the query has to go to them
	opt.EmbeddedData = append(opt.EmbeddedData, row)
	// and stash a copy locally, so local calls can use it
	if row != nil {
		c.Precomputed = make(map[uint64]interface{}, len(row.Segments))
		for _, segment := range row.Segments {
			c.Precomputed[segment.Shard()] = &featurebase.Row{Segments: []featurebase.RowSegment{segment}}
		}
	}
	return nil
}

// dumpPrecomputedCalls throws away precomputed call data. this is used so we
// can drop any large data associated with a call once we've processed
// the call.
func (o *orchestrator) dumpPrecomputedCalls(ctx context.Context, c *pql.Call) {
	for _, call := range c.Children {
		o.dumpPrecomputedCalls(ctx, call)
	}
	c.Precomputed = nil
}

// handlePreCallChildren handles any pre-calls in the children of a given call.
func (o *orchestrator) handlePreCallChildren(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) error {
	for i := range c.Children {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := o.handlePreCalls(ctx, tableKeyer, c.Children[i], shards, opt); err != nil {
			return err
		}
	}
	for key, val := range c.Args {
		// Do not precompute GroupBy aggregates
		if key == "aggregate" {
			continue
		}
		// Handle Call() operations which exist inside named arguments, too.
		if call, ok := val.(*pql.Call); ok {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := o.handlePreCalls(ctx, tableKeyer, call, shards, opt); err != nil {
				return err
			}
		}
	}
	return nil
}

// preprocessQuery expands any calls that need preprocessing.
func (o *orchestrator) preprocessQuery(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (*pql.Call, error) {
	switch c.Name {
	case "All":
		_, hasLimit, err := c.UintArg("limit")
		if err != nil {
			return nil, err
		}
		_, hasOffset, err := c.UintArg("offset")
		if err != nil {
			return nil, err
		}
		if !hasLimit && !hasOffset {
			return c, nil
		}

		// Rewrite the All() w/ limit to Limit(All()).
		c.Children = []*pql.Call{
			{
				Name: "All",
			},
		}
		c.Name = "Limit"
		return c, nil

	default:
		// Recurse through child calls.
		out := make([]*pql.Call, len(c.Children))
		var changed bool
		for i, child := range c.Children {
			res, err := o.preprocessQuery(ctx, tableKeyer, child, shards, opt)
			if err != nil {
				return nil, err
			}
			if res != child {
				changed = true
			}
			out[i] = res
		}
		if changed {
			c = c.Clone()
			c.Children = out
		}
		return c, nil
	}
}

// executeCall executes a call.
func (o *orchestrator) executeCall(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeCall")
	defer span.Finish()

	if err := validateQueryContext(ctx); err != nil {
		return nil, err
	} else if err := o.validateCallArgs(c); err != nil {
		return nil, errors.Wrap(err, "validating args")
	}

	labels := prometheus.Labels{"index": string(tableKeyer.Key())}
	statFn := func(ctr *prometheus.CounterVec) {
		if !opt.Remote {
			ctr.With(labels).Inc()
		}
	}

	// Preprocess the query.
	c, err := o.preprocessQuery(ctx, tableKeyer, c, shards, opt)
	if err != nil {
		return nil, err
	}

	switch c.Name {
	case "Sum":
		statFn(featurebase.CounterQuerySumTotal)
		res, err := o.executeSum(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeSum")
	case "Min":
		statFn(featurebase.CounterQueryMinTotal)
		res, err := o.executeMin(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeMin")
	case "Max":
		statFn(featurebase.CounterQueryMaxTotal)
		res, err := o.executeMax(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeMax")
	case "MinRow":
		statFn(featurebase.CounterQueryMinRowTotal)
		res, err := o.executeMinRow(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeMinRow")
	case "MaxRow":
		statFn(featurebase.CounterQueryMaxRowTotal)
		res, err := o.executeMaxRow(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeMaxRow")
	// case "Clear":
	// 	statFn(featurebase.CounterQueryClearTotal)
	// 	res, err := o.executeClearBit(ctx, index, c, opt)
	// 	return res, errors.Wrap(err, "executeClearBit")
	// case "ClearRow":
	// 	statFn(featurebase.CounterQueryClearRowTotal)
	// 	res, err := o.executeClearRow(ctx, index, c, shards, opt)
	// 	return res, errors.Wrap(err, "executeClearRow")
	case "Distinct":
		statFn(featurebase.CounterQueryDistinctTotal)
		res, err := o.executeDistinct(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeDistinct")
	// case "Store":
	// 	statFn(featurebase.CounterQueryStoreTotal)
	// 	res, err := o.executeSetRow(ctx, index, c, shards, opt)
	// 	return res, errors.Wrap(err, "executeSetRow")
	case "Count":
		statFn(featurebase.CounterQueryCountTotal)
		res, err := o.executeCount(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeCount")
	// case "Set":
	// 	statFn(featurebase.CounterQuerySetTotal)
	// 	res, err := o.executeSet(ctx, index, c, opt)
	// 	return res, errors.Wrap(err, "executeSet")
	case "TopK":
		statFn(featurebase.CounterQueryTopKTotal)
		res, err := o.executeTopK(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeTopK")
	case "TopN":
		statFn(featurebase.CounterQueryTopNTotal)
		res, err := o.executeTopN(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeTopN")
	case "Rows":
		statFn(featurebase.CounterQueryRowsTotal)
		res, err := o.executeRows(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeRows")
	case "Extract":
		statFn(featurebase.CounterQueryExtractTotal)
		res, err := o.executeExtract(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeExtract")
	case "GroupBy":
		statFn(featurebase.CounterQueryGroupByTotal)
		res, err := o.executeGroupBy(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeGroupBy")
	case "Options":
		statFn(featurebase.CounterQueryOptionsTotal)
		res, err := o.executeOptionsCall(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeOptionsCall")
	case "IncludesColumn":
		statFn(featurebase.CounterQueryIncludesColumnTotal)
		res, err := o.executeIncludesColumnCall(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeIncludesColumnCall")
	case "FieldValue":
		statFn(featurebase.CounterQueryFieldValueTotal)
		res, err := o.executeFieldValueCall(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeFieldValueCall")
	case "Precomputed":
		statFn(featurebase.CounterQueryPrecomputedTotal)
		res, err := o.executePrecomputedCall(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executePrecomputedCall")
	case "UnionRows":
		statFn(featurebase.CounterQueryUnionRowsTotal)
		res, err := o.executeUnionRows(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeUnionRows")
	case "ConstRow":
		statFn(featurebase.CounterQueryConstRowTotal)
		res, err := o.executeConstRow(ctx, tableKeyer, c)
		return res, errors.Wrap(err, "executeConstRow")
	case "Limit":
		statFn(featurebase.CounterQueryLimitTotal)
		res, err := o.executeLimitCall(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeLimitCall")
	case "Percentile":
		statFn(featurebase.CounterQueryPercentileTotal)
		res, err := o.executePercentile(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executePercentile")
	// case "Delete":
	// 	statFn(featurebase.CounterQueryDeleteTotal)
	// 	res, err := o.executeDeleteRecords(ctx, index, c, shards, opt)
	// 	return res, errors.Wrap(err, "executeDelete")
	default: // o.g. "Row", "Union", "Intersect" or anything that returns a bitmap.
		res, err := o.executeBitmapCall(ctx, tableKeyer, c, shards, opt)
		return res, errors.Wrap(err, "executeBitmapCall")
	}
}

// validateCallArgs ensures that the value types in call.Args are expected.
func (o *orchestrator) validateCallArgs(c *pql.Call) error {
	if _, ok := c.Args["ids"]; ok {
		switch v := c.Args["ids"].(type) {
		case []int64, []uint64:
			// noop
		case []interface{}:
			b := make([]int64, len(v))
			for i := range v {
				b[i] = v[i].(int64)
			}
			c.Args["ids"] = b
		default:
			return fmt.Errorf("invalid call.Args[ids]: %s", v)
		}
	}
	return nil
}

func (o *orchestrator) executeOptionsCall(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeOptionsCall")
	defer span.Finish()

	optCopy := &featurebase.ExecOptions{}
	*optCopy = *opt
	if arg, ok := c.Args["shards"]; ok {
		if optShards, ok := arg.([]interface{}); ok {
			shards = []uint64{}
			for _, s := range optShards {
				if shard, ok := s.(int64); ok {
					shards = append(shards, uint64(shard))
				} else {
					return nil, errors.New(errors.ErrUncoded, "Query(): shards must be a list of unsigned integers")
				}

			}
		} else {
			return nil, errors.New(errors.ErrUncoded, "Query(): shards must be a list of unsigned integers")
		}
	}
	return o.executeCall(ctx, tableKeyer, c.Children[0], shards, optCopy)
}

// executeIncludesColumnCall executes an IncludesColumn() call.
func (o *orchestrator) executeIncludesColumnCall(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (bool, error) {
	// Get the shard containing the column, since that's the only
	// shard that needs to execute this query.
	var shard uint64
	col, ok, err := c.UintArg("column")
	if err != nil {
		return false, errors.Wrap(err, "getting column from args")
	} else if !ok {
		return false, errors.New(errors.ErrUncoded, "IncludesColumn call must specify a column")
	}
	shard = col / featurebase.ShardWidth

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(bool)
		return other || v.(bool)
	}

	result, err := o.mapReduce(ctx, tableKeyer, []uint64{shard}, c, opt, reduceFn)
	if err != nil {
		return false, err
	}
	return result.(bool), nil
}

// executeFieldValueCall executes a FieldValue() call.
func (o *orchestrator) executeFieldValueCall(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (_ featurebase.ValCount, err error) {
	fieldName, ok := c.Args["field"].(string)
	if !ok || fieldName == "" {
		return featurebase.ValCount{}, featurebase.ErrFieldRequired
	}

	colKey, ok := c.Args["column"]
	if !ok || colKey == "" {
		return featurebase.ValCount{}, featurebase.ErrColumnRequired
	}

	colID, ok, err := c.UintArg("column")
	if !ok || err != nil {
		return featurebase.ValCount{}, errors.Wrap(err, "getting column argument")
	}

	shard := colID / featurebase.ShardWidth

	// Select single returned result at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(featurebase.ValCount)
		if other.Count == 1 {
			return other
		}
		return v
	}

	result, err := o.mapReduce(ctx, tableKeyer, []uint64{shard}, c, opt, reduceFn)
	if err != nil {
		return featurebase.ValCount{}, errors.Wrap(err, "map reduce")
	}
	other, _ := result.(featurebase.ValCount)

	return other, nil
}

// executeLimitCall executes a Limit() call.
func (o *orchestrator) executeLimitCall(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (*featurebase.Row, error) {
	bitmapCall := c.Children[0]

	limit, hasLimit, err := c.UintArg("limit")
	if err != nil {
		return nil, errors.Wrap(err, "getting limit")
	}
	offset, _, err := c.UintArg("offset")
	if err != nil {
		return nil, errors.Wrap(err, "getting offset")
	}

	if !hasLimit {
		limit = math.MaxUint64
	}

	// Execute bitmap call, storing the full result on this node.
	res, err := o.executeCall(ctx, tableKeyer, bitmapCall, shards, opt)
	if err != nil {
		return nil, errors.Wrap(err, "limit map reduce")
	}
	if res == nil {
		res = featurebase.NewRow()
	}

	result, ok := res.(*featurebase.Row)
	if !ok {
		return nil, errors.Errorf("expected Row but got %T", result)
	}

	if offset != 0 {
		i := 0
		var leadingBits []uint64
		for i < len(result.Segments) && offset > 0 {
			seg := result.Segments[i]
			count := seg.Count()
			if count > offset {
				data := seg.Columns()
				data = data[offset:]
				leadingBits = data
				i++
				break
			}

			offset -= count
			i++
		}
		row := featurebase.NewRow(leadingBits...)
		row.Merge(&featurebase.Row{Segments: result.Segments[i:]})
		result = row
	}
	if limit < result.Count() {
		i := 0
		var trailingBits []uint64
		for i < len(result.Segments) && limit > 0 {
			seg := result.Segments[i]
			count := seg.Count()
			if count > limit {
				data := seg.Columns()
				data = data[:limit]
				trailingBits = data
				break
			}

			limit -= count
			i++
		}
		row := featurebase.NewRow(trailingBits...)
		row.Merge(&featurebase.Row{Segments: result.Segments[:i]})
		result = row
	}

	return result, nil
}

// executeSum executes a Sum() call.
func (o *orchestrator) executeSum(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (_ featurebase.ValCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeSum")
	defer span.Finish()

	fieldName, err := c.FirstStringArg("field", "_field")
	if err != nil {
		return featurebase.ValCount{}, errors.Wrap(err, "Sum(): field required")
	}

	if len(c.Children) > 1 {
		return featurebase.ValCount{}, errors.New(errors.ErrUncoded, "Sum() only accepts a single bitmap input")
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(featurebase.ValCount)
		return other.Add(v.(featurebase.ValCount))
	}

	result, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return featurebase.ValCount{}, err
	}
	other, _ := result.(featurebase.ValCount)

	if other.Count == 0 {
		return featurebase.ValCount{}, nil
	}

	// scale summed response if it's a decimal field and this is
	// not a remote query (we're about to return to original client).
	if !opt.Remote {
		field, err := o.schemaFieldInfo(ctx, tableKeyer, fieldName)
		if field == nil {
			return featurebase.ValCount{}, errors.Wrapf(err, "%q", fieldName)
		}
		if field.Options.Type == FieldTypeDecimal {
			dec := pql.NewDecimal(other.Val, field.Options.Scale)
			other.DecimalVal = &dec
			other.FloatVal = 0
			other.Val = 0
		}
	}

	return other, nil
}

// executeDistinct executes a Distinct call on a field. It returns a
// SignedRow for int fields and a *Row for set/mutex/time fields.
func (o *orchestrator) executeDistinct(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeDistinct")
	defer span.Finish()

	field, hasField, err := c.StringArg("field")
	if err != nil {
		return featurebase.SignedRow{}, errors.Wrap(err, "loading field option in Distinct query")
	} else if !hasField {
		return featurebase.SignedRow{}, fmt.Errorf("missing field option in Distinct query")
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch other := prev.(type) {
		case featurebase.SignedRow:
			return other.Union(v.(featurebase.SignedRow))
		case *featurebase.Row:
			if other == nil {
				return v
			} else if v.(*featurebase.Row) == nil {
				return other
			}
			return other.Union(v.(*featurebase.Row))
		case nil:
			return v
		case featurebase.DistinctTimestamp:
			return other.Union(v.(featurebase.DistinctTimestamp))
		default:
			return errors.Errorf("unexpected return type from executeDistinctShard: %+v %T", other, other)
		}
	}

	result, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return nil, errors.Wrap(err, "mapReduce")
	}

	if other, ok := result.(featurebase.SignedRow); ok {
		other.Field = field
	}
	return result, nil
}

// executeMin executes a Min() call.
func (o *orchestrator) executeMin(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (_ featurebase.ValCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMin")
	defer span.Finish()

	if _, err := c.FirstStringArg("field", "_field"); err != nil {
		return featurebase.ValCount{}, errors.Wrap(err, "Min(): field required")
	}

	if len(c.Children) > 1 {
		return featurebase.ValCount{}, errors.New(errors.ErrUncoded, "Min() only accepts a single bitmap input")
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(featurebase.ValCount)
		return other.Smaller(v.(featurebase.ValCount))
	}

	result, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return featurebase.ValCount{}, err
	}
	other, _ := result.(featurebase.ValCount)

	if other.Count == 0 {
		return featurebase.ValCount{}, nil
	}
	return other, nil
}

// executeMax executes a Max() call.
func (o *orchestrator) executeMax(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (_ featurebase.ValCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMax")
	defer span.Finish()

	if _, err := c.FirstStringArg("field", "_field"); err != nil {
		return featurebase.ValCount{}, errors.Wrap(err, "Max(): field required")
	}

	if len(c.Children) > 1 {
		return featurebase.ValCount{}, errors.New(errors.ErrUncoded, "Max() only accepts a single bitmap input")
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(featurebase.ValCount)
		return other.Larger(v.(featurebase.ValCount))
	}

	result, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return featurebase.ValCount{}, err
	}
	other, _ := result.(featurebase.ValCount)

	if other.Count == 0 {
		return featurebase.ValCount{}, nil
	}
	return other, nil
}

// TODO(jaffee) fix this... valcountize assumes access to field details like base
// executePercentile executes a Percentile() call.
func (o *orchestrator) executePercentile(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (_ featurebase.ValCount, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executePercentile")
	defer span.Finish()

	// get nth
	var nthFloat float64
	nthArg, ok := c.Args["nth"]
	if !ok {
		return featurebase.ValCount{}, errors.New(errors.ErrUncoded, "Percentile(): nth required")
	}
	switch nthArg := nthArg.(type) {
	case pql.Decimal:
		nthFloat = nthArg.Float64()
	case int64:
		nthFloat = float64(nthArg)
	default:
		return featurebase.ValCount{}, errors.Errorf("Percentile(): invalid nth='%v' of type (%[1]T), should be a number between 0 and 100 inclusive", c.Args["nth"])
	}
	if nthFloat < 0 || nthFloat > 100.0 {
		return featurebase.ValCount{}, errors.Errorf("Percentile(): invalid nth value (%f), should be a number between 0 and 100 inclusive", nthFloat)
	}

	// get field
	fieldName, err := c.FirstStringArg("field", "_field")
	if err != nil {
		return featurebase.ValCount{}, errors.New(errors.ErrUncoded, "Percentile(): field required")
	}
	field, err := o.schemaFieldInfo(ctx, tableKeyer, fieldName)
	if err != nil {
		return featurebase.ValCount{}, ErrFieldNotFound
	}

	// filter call for min & max
	var filterCall *pql.Call

	// check if filter provided
	if filterArg, ok := c.Args["filter"].(*pql.Call); ok && filterArg != nil {
		filterCall = filterArg
	}

	// get min
	q, _ := pql.ParseString(fmt.Sprintf(`Min(field="%s")`, fieldName))
	minCall := q.Calls[0]
	if filterCall != nil {
		minCall.Children = append(minCall.Children, filterCall)
	}
	minVal, err := o.executeMin(ctx, tableKeyer, minCall, shards, opt)
	if err != nil {
		return featurebase.ValCount{}, errors.Wrap(err, "executing Min call for Percentile")
	}
	if nthFloat == 0.0 {
		return minVal, nil
	}

	// get max
	q, _ = pql.ParseString(fmt.Sprintf(`Max(field="%s")`, fieldName))
	maxCall := q.Calls[0]
	if filterCall != nil {
		maxCall.Children = append(maxCall.Children, filterCall)
	}
	maxVal, err := o.executeMax(ctx, tableKeyer, maxCall, shards, opt)
	if err != nil {
		return featurebase.ValCount{}, errors.Wrap(err, "executing Max call for Percentile")
	}
	// set up reusables
	var countCall, rangeCall *pql.Call
	if filterCall == nil {
		countQuery, _ := pql.ParseString(fmt.Sprintf("Count(Row(%s < 0))", fieldName))
		countCall = countQuery.Calls[0]
		rangeCall = countCall.Children[0]
	} else {
		countQuery, _ := pql.ParseString(fmt.Sprintf(`Count(Intersect(Row(%s < 0)))`, fieldName))
		countCall = countQuery.Calls[0]
		intersectCall := countCall.Children[0]
		intersectCall.Children = append(intersectCall.Children, filterCall)
		rangeCall = intersectCall.Children[0]
	}

	k := (100 - nthFloat) / nthFloat

	min, max := minVal.Val, maxVal.Val
	// estimate nth val, eg median when nth=0.5
	for min < max {
		// compute average without integer overflow, then correct for division of
		// odd numbers by 2
		possibleNthVal := ((max / 2) + (min / 2)) + (((max % 2) + (min % 2)) / 2)
		// possibleNthVal = (max + min) / 2
		// get left count
		rangeCall.Args[fieldName] = &pql.Condition{
			Op:    pql.Token(pql.LT),
			Value: possibleNthVal,
		}
		leftCountUint64, err := o.executeCount(ctx, tableKeyer, countCall, shards, opt)
		if err != nil {
			return featurebase.ValCount{}, errors.Wrap(err, "executing Count call L for Percentile")
		}
		leftCount := int64(leftCountUint64)

		// get right count
		rangeCall.Args[fieldName] = &pql.Condition{
			Op:    pql.Token(pql.GT),
			Value: possibleNthVal,
		}
		rightCountUint64, err := o.executeCount(ctx, tableKeyer, countCall, shards, opt)
		if err != nil {
			return featurebase.ValCount{}, errors.Wrap(err, "executing Count call R for Percentile")
		}
		rightCount := int64(rightCountUint64)

		// 'weight' the left count as per k
		leftCountWeighted := int64(math.Round(k * float64(leftCount)))

		// binary search
		if leftCountWeighted > rightCount {
			max = possibleNthVal - 1
		} else if leftCountWeighted < rightCount {
			min = possibleNthVal + 1
		} else {
			return cookValCount(possibleNthVal, 1, field), nil
		}
	}

	return cookValCount(min, 1, field), nil
}

func cookValCount(val int64, cnt uint64, field *featurebase.FieldInfo) featurebase.ValCount {
	valCount := featurebase.ValCount{Count: int64(cnt)}
	base := field.Options.Base
	switch field.Options.Type {
	case featurebase.FieldTypeDecimal:
		dec := pql.NewDecimal(val+base, field.Options.Scale)
		valCount.DecimalVal = &dec
	case FieldTypeTimestamp:
		valCount.TimestampVal = time.Unix(0, (val+base)*featurebase.TimeUnitNanos(field.Options.TimeUnit)).UTC()
	}
	valCount.Val = val + base
	return valCount
}

// executeMinRow executes a MinRow() call.
func (o *orchestrator) executeMinRow(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (_ interface{}, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMinRow")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return featurebase.ValCount{}, errors.New(errors.ErrUncoded, "MinRow(): field required")
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		// if minRowID exists, and if it is smaller than the other one return it.
		// otherwise return the minRowID of the one which exists.
		if prev == nil {
			return v
		} else if v == nil {
			return prev
		}
		prevp, _ := prev.(featurebase.PairField)
		vp, _ := v.(featurebase.PairField)
		if prevp.Pair.Count > 0 && vp.Pair.Count > 0 {
			if prevp.Pair.ID < vp.Pair.ID {
				return prevp
			}
			return vp
		} else if prevp.Pair.Count > 0 {
			return prevp
		}
		return vp
	}

	return o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
}

// executeMaxRow executes a MaxRow() call.
func (o *orchestrator) executeMaxRow(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (_ interface{}, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeMaxRow")
	defer span.Finish()

	if field := c.Args["field"]; field == "" {
		return featurebase.ValCount{}, errors.New(errors.ErrUncoded, "MaxRow(): field required")
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		// if minRowID exists, and if it is smaller than the other one return it.
		// otherwise return the minRowID of the one which exists.
		if prev == nil {
			return v
		} else if v == nil {
			return prev
		}
		prevp, _ := prev.(featurebase.PairField)
		vp, _ := v.(featurebase.PairField)
		if prevp.Pair.Count > 0 && vp.Pair.Count > 0 {
			if prevp.Pair.ID > vp.Pair.ID {
				return prevp
			}
			return vp
		} else if prevp.Pair.Count > 0 {
			return prevp
		}
		return vp
	}

	return o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
}

// executePrecomputedCall pretends to execute a call that we have a precomputed value for.
func (o *orchestrator) executePrecomputedCall(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (_ *featurebase.Row, err error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executePrecomputedCall")
	defer span.Finish()
	result := featurebase.NewRow()

	for _, row := range c.Precomputed {
		result.Merge(row.(*featurebase.Row))
	}
	return result, nil
}

// executeBitmapCall executes a call that returns a bitmap.
func (o *orchestrator) executeBitmapCall(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (_ *featurebase.Row, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeBitmapCall")
	span.LogKV("pqlCallName", c.Name)
	defer span.Finish()

	labels := prometheus.Labels{"index": string(tableKeyer.Key())}
	statFn := func(ctr *prometheus.CounterVec) {
		if !opt.Remote {
			ctr.With(labels).Inc()
		}
	}

	if !opt.Remote {
		switch c.Name {
		case "Row":
			if c.HasConditionArg() {
				statFn(featurebase.CounterQueryRowBSITotal)
			} else {
				statFn(featurebase.CounterQueryRowTotal)
			}
		case "Range":
			statFn(featurebase.CounterQueryRangeTotal)
		case "Difference":
			statFn(featurebase.CounterQueryBitmapTotal)
		case "Intersect":
			statFn(featurebase.CounterQueryIntersectTotal)
		case "Union":
			statFn(featurebase.CounterQueryUnionTotal)
		case "InnerUnionRows":
			statFn(featurebase.CounterQueryInnerUnionRowsTotal)
		case "Xor":
			statFn(featurebase.CounterQueryXorTotal)
		case "Not":
			statFn(featurebase.CounterQueryNotTotal)
		case "Shift":
			statFn(featurebase.CounterQueryShiftTotal)
		case "All":
			statFn(featurebase.CounterQueryAllTotal)
		default:
			statFn(featurebase.CounterQueryBitmapTotal)
		}
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(*featurebase.Row)
		if other == nil {
			// TODO... what's going on on the following line
			other = featurebase.NewRow() // bug! this row ends up containing Badger Txn data that should be accessed outside the Txn.
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		other.Merge(v.(*featurebase.Row))
		return other
	}

	other, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return nil, errors.Wrap(err, "map reduce")
	}

	row, _ := other.(*featurebase.Row)

	return row, nil
}

type Error string // TODO(jaffee) convert to standard error package

func (e Error) Error() string { return string(e) }

const ViewNotFound = Error("view not found")
const FragmentNotFound = Error("fragment not found")

func (o *orchestrator) executeTopK(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (interface{}, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTopK")
	defer span.Finish()

	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		x, _ := prev.([]*featurebase.Row)
		y, _ := v.([]*featurebase.Row)
		return ([]*featurebase.Row)(featurebase.AddBSI(x, y))
	}

	other, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.([]*featurebase.Row)

	if opt.Remote {
		return results, nil
	}

	k, hasK, err := c.UintArg("k")
	if err != nil {
		return nil, errors.Wrap(err, "fetching k")
	}

	var limit *uint64
	if hasK {
		limit = &k
	}

	var dst []featurebase.Pair
	featurebase.BSIData(results).PivotDescending(featurebase.NewRow().Union(results...), 0, limit, nil, func(count uint64, ids ...uint64) {
		for _, id := range ids {
			dst = append(dst, featurebase.Pair{
				ID:    id,
				Count: count,
			})
		}
	})

	fieldName, hasFieldName, err := c.StringArg("_field")
	if err != nil {
		return nil, errors.Wrap(err, "fetching TopK field")
	} else if !hasFieldName {
		return nil, errors.New(errors.ErrUncoded, "missing field in TopK")
	}

	return &featurebase.PairsField{
		Pairs: dst,
		Field: fieldName,
	}, nil
}

// uint64Slice represents a sortable slice of uint64 numbers.
type uint64Slice []uint64

func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }

// executeTopN executes a TopN() call.
// This first performs the TopN() to determine the top results and then
// requeries to retrieve the full counts for each of the top results.
func (o *orchestrator) executeTopN(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (*featurebase.PairsField, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTopN")
	defer span.Finish()

	idsArg, _, err := c.UintSliceArg("ids")
	if err != nil {
		return nil, fmt.Errorf("executeTopN: %v", err)
	}

	fieldName, _ := c.Args["_field"].(string)
	n, _, err := c.UintArg("n")
	if err != nil {
		return nil, fmt.Errorf("executeTopN: %v", err)
	}

	// Execute original query.
	pairs, err := o.executeTopNShards(ctx, tableKeyer, c, shards, opt)
	if err != nil {
		return nil, errors.Wrap(err, "finding top results")
	}

	// If this call is against specific ids, or we didn't get results,
	// or we are part of a larger distributed query then don't refetch.
	if len(pairs.Pairs) == 0 || len(idsArg) > 0 || opt.Remote {
		return &featurebase.PairsField{
			Pairs: pairs.Pairs,
			Field: fieldName,
		}, nil
	}
	// Only the original caller should refetch the full counts.
	// TODO(@kuba--): ...but do we really need `Clone` here?
	other := c.Clone()

	ids := featurebase.Pairs(pairs.Pairs).Keys()
	sort.Sort(uint64Slice(ids))
	other.Args["ids"] = ids

	trimmedList, err := o.executeTopNShards(ctx, tableKeyer, other, shards, opt)
	if err != nil {
		return nil, errors.Wrap(err, "retrieving full counts")
	}

	if n != 0 && int(n) < len(trimmedList.Pairs) {
		trimmedList.Pairs = trimmedList.Pairs[0:n]
	}

	return &featurebase.PairsField{
		Pairs: trimmedList.Pairs,
		Field: fieldName,
	}, nil
}

func (o *orchestrator) executeTopNShards(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (*featurebase.PairsField, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTopNShards")
	defer span.Finish()

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(*featurebase.PairsField)
		vpf, _ := v.(*featurebase.PairsField)
		if other == nil {
			return vpf
		} else if vpf == nil {
			return other
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		other.Pairs = featurebase.Pairs(other.Pairs).Add(vpf.Pairs)
		return other
	}

	other, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.(*featurebase.PairsField)

	// Sort final merged results.
	sort.Sort(featurebase.Pairs(results.Pairs))

	return results, nil
}

// order denotes sort order—can be asc or desc (see constants below).
type order bool

const (
	asc  order = true
	desc order = false
)

// groupCountSorter sorts the output of a GroupBy request (a
// []GroupCount) according to sorting instructions encoded in "fields"
// and "order".
//
// Each field in "fields" is an integer which can be -1 to denote
// sorting on the Count and -2 to denote sorting on the
// sum/aggregate. Currently nothing else is supported, but the idea
// was that if there were positive integers they would be indexes into
// GroupCount.FieldRow and allowing sorting on the values of different
// fields in the group. Each item in "order" corresponds to the same
// index in "fields" and denotes the order of the sort.
type groupCountSorter struct {
	fields []int
	order  []order
	data   []featurebase.GroupCount
}

func (g *groupCountSorter) Len() int      { return len(g.data) }
func (g *groupCountSorter) Swap(i, j int) { g.data[i], g.data[j] = g.data[j], g.data[i] }
func (g *groupCountSorter) Less(i, j int) bool {
	gci, gcj := g.data[i], g.data[j]
	for idx, fieldIndex := range g.fields {
		fieldOrder := g.order[idx]
		switch fieldIndex {
		case -1: // Count
			if gci.Count < gcj.Count {
				return fieldOrder == asc
			} else if gci.Count > gcj.Count {
				return fieldOrder == desc
			}
		case -2: // Aggregate
			if gci.Agg < gcj.Agg {
				return fieldOrder == asc
			} else if gci.Agg > gcj.Agg {
				return fieldOrder == desc
			}
		default:
			panic("impossible")
		}
	}
	return false
}

// getSorter hackily parses the sortSpec and figures out how to sort
// the GroupBy results.
func getSorter(sortSpec string) (*groupCountSorter, error) {
	gcs := &groupCountSorter{
		fields: []int{},
		order:  []order{},
	}
	sortOn := strings.Split(sortSpec, ",")
	for _, sortField := range sortOn {
		sortField = strings.TrimSpace(sortField)
		fieldDir := strings.Fields(sortField)
		if len(fieldDir) == 0 {
			return nil, errors.Errorf("invalid sorting directive: '%s'", sortField)
		} else if fieldDir[0] == "count" {
			gcs.fields = append(gcs.fields, -1)
		} else if fieldDir[0] == "aggregate" || fieldDir[0] == "sum" {
			gcs.fields = append(gcs.fields, -2)
		} else {
			return nil, errors.Errorf("sorting is only supported on count, aggregate, or sum, not '%s'", fieldDir[0])
		}

		if len(fieldDir) == 1 {
			gcs.order = append(gcs.order, desc)
		} else if len(fieldDir) > 2 {
			return nil, errors.Errorf("parsing sort directive: '%s': too many elements", sortField)
		} else if fieldDir[1] == "asc" {
			gcs.order = append(gcs.order, asc)
		} else if fieldDir[1] == "desc" {
			gcs.order = append(gcs.order, desc)
		} else {
			return nil, errors.Errorf("unknown sort direction '%s'", fieldDir[1])
		}
	}
	return gcs, nil
}

// findGroupCounts gets a safe-to-use but possibly empty []GroupCount from
// an interface which might be a *GroupCounts or a []GroupCount.
func findGroupCounts(v interface{}) []featurebase.GroupCount {
	switch gc := v.(type) {
	case []featurebase.GroupCount:
		return gc
	case *featurebase.GroupCounts:
		return gc.Groups()
	}
	return nil
}

func (o *orchestrator) executeGroupBy(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (*featurebase.GroupCounts, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeGroupBy")
	defer span.Finish()

	// validate call
	if len(c.Children) == 0 {
		return nil, errors.New(errors.ErrUncoded, "need at least one child call")
	}
	limit := int(^uint(0) >> 1)
	if lim, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, err
	} else if hasLimit {
		limit = int(lim)
	}
	filter, _, err := c.CallArg("filter")
	if err != nil {
		return nil, err
	}

	var sorter *groupCountSorter
	if sortSpec, found, err := c.StringArg("sort"); err != nil {
		return nil, errors.Wrap(err, "getting sort arg")
	} else if found {
		sorter, err = getSorter(sortSpec)
		if err != nil {
			return nil, errors.Wrap(err, "parsing sort spec")
		}
		// don't want to prematurely limit the results if we're sorting
		limit = int(^uint(0) >> 1)
	}
	having, hasHaving, err := c.CallArg("having")
	if err != nil {
		return nil, errors.Wrap(err, "getting 'having' argument")
	} else if hasHaving {
		// don't want to prematurely limit the results if we're filtering some out
		limit = int(^uint(0) >> 1)
	}

	// perform necessary Rows queries (any that have limit or columns args) -
	// TODO, call async? would only help if multiple Rows queries had a column
	// or limit arg.
	// TODO support TopN in here would be really cool - and pretty easy I think.
	childRows := make([]featurebase.RowIDs, len(c.Children))
	for i, child := range c.Children {
		// Check "field" first for backwards compatibility, then set _field.
		// TODO: remove at Pilosa 2.0
		if fieldName, ok := child.Args["field"].(string); ok {
			child.Args["_field"] = fieldName
		}

		if child.Name != "Rows" {
			return nil, errors.Errorf("'%s' is not a valid child query for GroupBy, must be 'Rows'", child.Name)
		}
		_, hasLimit, err := child.UintArg("limit")
		if err != nil {
			return nil, errors.Wrap(err, "getting limit")
		}
		_, hasCol, err := child.UintArg("column")
		if err != nil {
			return nil, errors.Wrap(err, "getting column")
		}
		_, hasLike, err := child.StringArg("like")
		if err != nil {
			return nil, errors.Wrap(err, "getting like")
		}
		_, hasIn, err := child.UintSliceArg("in")
		if err != nil {
			return nil, errors.Wrap(err, "getting 'in'")
		}

		if hasLimit || hasCol || hasLike || hasIn { // we need to perform this query cluster-wide ahead of executeGroupByShard
			if idx, ok := child.Args["valueidx"].(int64); ok {
				// The rows query was already completed on the initiating node.
				childRows[i] = opt.EmbeddedData[idx].Columns()
				continue
			}

			r, er := o.executeRows(ctx, tableKeyer, child, shards, opt)
			if er != nil {
				return nil, errors.Wrap(er, "getting rows for ")
			}
			// need to sort because filters assume ordering
			sort.Slice(r, func(x, y int) bool { return r[x] < r[y] })
			childRows[i] = r
			if len(childRows[i]) == 0 { // there are no results because this field has no values.
				return &featurebase.GroupCounts{}, nil
			}

			// Stuff the result into opt.EmbeddedData so that it gets sent to other nodes in the map-reduce.
			// This is flagged as "NoSplit" to ensure that the entire row gets sent out.
			rowsRow := featurebase.NewRow(childRows[i]...)
			rowsRow.NoSplit = true
			child.Args["valueidx"] = int64(len(opt.EmbeddedData))
			opt.EmbeddedData = append(opt.EmbeddedData, rowsRow)
		}
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other := findGroupCounts(prev)
		if err := ctx.Err(); err != nil {
			return err
		}
		return mergeGroupCounts(other, findGroupCounts(v), limit)
	}
	// Get full result set.
	other, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return nil, errors.Wrap(err, "mapReduce")
	}
	results, _ := other.([]featurebase.GroupCount)

	// If there's no sorting, we want to apply limits before
	// calculating the Distinct aggregate which is expensive on a
	// per-result basis.
	if sorter == nil && !hasHaving {
		results, err = applyLimitAndOffsetToGroupByResult(c, results)
		if err != nil {
			return nil, errors.Wrap(err, "applying limit/offset")
		}
	}

	// TODO as an optimization, we could apply some "having"
	// conditions here long as they aren't on the Count(Distinct)
	// aggregate

	// Calculate Count(Distinct) aggregate if requested.
	aggregate, _, err := c.CallArg("aggregate")
	if err == nil && aggregate != nil && aggregate.Name == "Count" && len(aggregate.Children) > 0 && aggregate.Children[0].Name == "Distinct" && !opt.Remote {
		for n, gc := range results {
			intersectRows := make([]*pql.Call, 0, len(gc.Group))
			for _, fr := range gc.Group {
				var value interface{} = fr.RowID
				// use fr.Value instead of fr.RowID if set (from int fields)
				if fr.Value != nil {
					value = &pql.Condition{Op: pql.EQ, Value: *fr.Value}
				}
				intersectRows = append(intersectRows, &pql.Call{Name: "Row", Args: map[string]interface{}{fr.Field: value}})
			}
			// apply any filter, if present
			if filter != nil {
				intersectRows = append(intersectRows, filter)
			}
			// also intersect with any children of Distinct
			if len(aggregate.Children[0].Children) > 0 {
				intersectRows = append(intersectRows, aggregate.Children[0].Children[0])
			}

			countDistinctIntersect := &pql.Call{
				Name: "Count",
				Children: []*pql.Call{
					{
						Name: "Distinct",
						Children: []*pql.Call{
							{
								Name:     "Intersect",
								Children: intersectRows,
							},
						},
						Args: aggregate.Children[0].Args,
						Type: pql.PrecallGlobal,
					},
				},
			}

			opt.PreTranslated = true
			aggregateCount, err := o.execute(ctx, tableKeyer, &pql.Query{Calls: []*pql.Call{countDistinctIntersect}}, []uint64{}, opt)
			if err != nil {
				return nil, err
			}
			results[n].Agg = int64(aggregateCount[0].(uint64))
		}
	}

	// Apply having.
	if hasHaving && !opt.Remote {
		// parse the condition as PQL
		if having.Name != "Condition" {
			return nil, errors.New(errors.ErrUncoded, "the only supported having call is Condition()")
		}
		if len(having.Args) != 1 {
			return nil, errors.New(errors.ErrUncoded, "Condition() must contain a single condition")
		}
		for subj, cond := range having.Args {
			switch subj {
			case "count", "sum":
				results = featurebase.ApplyConditionToGroupCounts(results, subj, cond.(*pql.Condition))
			default:
				return nil, errors.New(errors.ErrUncoded, "Condition() only supports count or sum")
			}
		}
	}

	if sorter != nil && !opt.Remote {
		sorter.data = results
		sort.Stable(sorter)
		results, err = applyLimitAndOffsetToGroupByResult(c, results)
		if err != nil {
			return nil, errors.Wrap(err, "applying limit/offset")
		}
	} else if hasHaving && !opt.Remote {
		results, err = applyLimitAndOffsetToGroupByResult(c, results)
		if err != nil {
			return nil, errors.Wrap(err, "applying limit/offset")
		}

	}

	aggType := ""
	if aggregate != nil {
		switch aggregate.Name {
		case "Sum":
			aggType = "sum"
		case "Count":
			aggType = "aggregate"
		}
	}
	for _, res := range results {
		if res.DecimalAgg != nil && aggType == "sum" {
			aggType = "decimalSum"
			break
		}
	}

	return featurebase.NewGroupCounts(aggType, results...), nil
}

func applyLimitAndOffsetToGroupByResult(c *pql.Call, results []featurebase.GroupCount) ([]featurebase.GroupCount, error) {
	// Apply offset.
	if offset, hasOffset, err := c.UintArg("offset"); err != nil {
		return nil, err
	} else if hasOffset {
		if int(offset) < len(results) {
			results = results[offset:]
		}
	}
	// Apply limit.
	if limit, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, err
	} else if hasLimit {
		if int(limit) < len(results) {
			results = results[:limit]
		}
	}
	return results, nil
}

// mergeGroupCounts merges two slices of GroupCounts throwing away any that go
// beyond the limit. It assume that the two slices are sorted by the row ids in
// the fields of the group counts. It may modify its arguments.
func mergeGroupCounts(a, b []featurebase.GroupCount, limit int) []featurebase.GroupCount {
	if limit > len(a)+len(b) {
		limit = len(a) + len(b)
	}
	ret := make([]featurebase.GroupCount, 0, limit)
	i, j := 0, 0
	for i < len(a) && j < len(b) && len(ret) < limit {
		switch a[i].Compare(b[j]) {
		case -1:
			ret = append(ret, a[i])
			i++
		case 0:
			a[i].Count += b[j].Count
			a[i].Agg += b[j].Agg
			if a[i].DecimalAgg != nil && b[j].DecimalAgg != nil {
				sum := pql.AddDecimal(*a[i].DecimalAgg, *b[j].DecimalAgg)
				a[i].DecimalAgg = &sum
			}
			ret = append(ret, a[i])
			i++
			j++
		case 1:
			ret = append(ret, b[j])
			j++
		}
	}
	for ; i < len(a) && len(ret) < limit; i++ {
		ret = append(ret, a[i])
	}
	for ; j < len(b) && len(ret) < limit; j++ {
		ret = append(ret, b[j])
	}
	return ret
}

func (o *orchestrator) executeRows(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (featurebase.RowIDs, error) {
	// Fetch field name from argument.
	// Check "field" first for backwards compatibility.
	// TODO: remove at Pilosa 2.0
	var fieldName string
	var ok bool
	if fieldName, ok = c.Args["field"].(string); ok {
		c.Args["_field"] = fieldName
	}
	if fieldName, ok = c.Args["_field"].(string); !ok {
		return nil, errors.New(errors.ErrUncoded, "Rows() field required")
	}

	// TODO(tlt): this is here to prevent the linter from complaining.
	// Presumably this fieldName is/was used in code which is no longer here or
	// is currently commented out.
	_ = fieldName

	if columnID, ok, err := c.UintArg("column"); err != nil {
		return nil, errors.Wrap(err, "getting column")
	} else if ok {
		shards = []uint64{columnID / featurebase.ShardWidth}
	}

	// TODO, support "in" in conjunction w/ other args... or at least error if they're present together
	if ids, found, err := c.UintSliceArg("in"); err != nil {
		return nil, errors.Wrapf(err, "'in' argument of Rows must be a slice")
	} else if found {
		// "in" not supported with other args, so check here
		for arg := range c.Args {
			if arg != "field" && arg != "_field" && arg != "in" {
				return nil, errors.Errorf("Rows call with 'in' does not support other arguments, but found '%s'", arg)
			}
		}
		return ids, nil
	}

	// Determine limit so we can use it when reducing.
	limit := int(^uint(0) >> 1)
	if lim, hasLimit, err := c.UintArg("limit"); err != nil {
		return nil, err
	} else if hasLimit {
		limit = int(lim)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(featurebase.RowIDs)
		if err := ctx.Err(); err != nil {
			return err
		}
		return other.Merge(v.(featurebase.RowIDs), limit)
	}
	// Get full result set.
	other, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.(featurebase.RowIDs)

	// TODO(jaffee) enable "like" support
	// if !opt.Remote {
	// 	if like, hasLike, err := c.StringArg("like"); err != nil {
	// 		return nil, errors.Wrap(err, "getting like pattern")
	// 	} else if hasLike {
	// 		matches, err := e.Cluster.matchField(ctx, e.Holder.Field(index, fieldName), like)
	// 		if err != nil {
	// 			return nil, errors.Wrap(err, "matching like pattern")
	// 		}

	// 		i, j, k := 0, 0, 0
	// 		for i < len(results) && j < len(matches) {
	// 			x, y := results[i], matches[j]
	// 			switch {
	// 			case x < y:
	// 				i++
	// 			case y < x:
	// 				j++
	// 			default:
	// 				results[k] = x
	// 				i++
	// 				j++
	// 				k++
	// 			}
	// 		}
	// 		results = results[:k]
	// 	}
	// }

	return results, nil
}

func (o *orchestrator) executeExtract(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (featurebase.ExtractedIDMatrix, error) {
	// Extract the column filter call.
	if len(c.Children) < 1 {
		return featurebase.ExtractedIDMatrix{}, errors.New(errors.ErrUncoded, "missing column filter in Extract")
	}

	// Extract fields from rows calls.
	fields := make([]string, len(c.Children)-1)
	for i, rows := range c.Children[1:] {
		if rows.Name != "Rows" {
			return featurebase.ExtractedIDMatrix{}, errors.Errorf("child call of Extract is %q but expected Rows", rows.Name)
		}
		var fieldName string
		var ok bool
		for k, v := range rows.Args {
			switch k {
			case "field", "_field":
				fieldName = v.(string)
				ok = true
			default:
				return featurebase.ExtractedIDMatrix{}, errors.Errorf("unsupported Rows argument for Extract: %q", k)
			}
		}
		if !ok {
			return featurebase.ExtractedIDMatrix{}, errors.New(errors.ErrUncoded, "missing field specification in Rows")
		}
		fields[i] = fieldName
	}
	// TODO(tlt): is `fields` used?

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(featurebase.ExtractedIDMatrix)
		if err := ctx.Err(); err != nil {
			return err
		}
		other.Append(v.(featurebase.ExtractedIDMatrix))
		return other
	}

	// Get full result set.
	other, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return featurebase.ExtractedIDMatrix{}, err
	}
	results, _ := other.(featurebase.ExtractedIDMatrix)
	sort.Slice(results.Columns, func(i, j int) bool {
		return results.Columns[i].ColumnID < results.Columns[j].ColumnID
	})
	return results, nil
}

func (o *orchestrator) executeConstRow(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call) (res *featurebase.Row, err error) {
	// Fetch user-provided columns list.
	ids, ok := c.Args["columns"].([]uint64)
	if !ok {
		return nil, errors.New(errors.ErrUncoded, "missing columns list")
	}

	return featurebase.NewRow(ids...), nil
}

func (o *orchestrator) executeUnionRows(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (*featurebase.Row, error) {
	// Turn UnionRows(Rows(...)) into Union(Row(...), ...).
	var rows []*pql.Call
	for _, child := range c.Children {
		// Check that we can use the call.
		switch child.Name {
		case "Rows":
		case "TopN":
		default:
			return nil, errors.Errorf("cannot use %v as a rows query", child)
		}

		// Execute the call.
		rowsResult, err := o.executeCall(ctx, tableKeyer, child, shards, opt)
		if err != nil {
			return nil, err
		}

		// Turn the results into rows calls.
		var resultRows []*pql.Call
		switch rowsResult := rowsResult.(type) {
		case *featurebase.PairsField:
			// Translate pairs into rows calls.
			for _, p := range rowsResult.Pairs {
				var val interface{}
				switch {
				case p.Key != "":
					val = p.Key
				default:
					val = p.ID
				}
				resultRows = append(resultRows, &pql.Call{
					Name: "Row",
					Args: map[string]interface{}{
						rowsResult.Field: val,
					},
				})
			}
		case featurebase.RowIDs:
			// Translate Row IDs into Row calls.
			for _, id := range rowsResult {
				resultRows = append(resultRows, &pql.Call{
					Name: "Row",
					Args: map[string]interface{}{
						child.Args["_field"].(string): id,
					},
				})
			}
		default:
			return nil, errors.Errorf("unexpected Rows type %T", rowsResult)
		}

		// Propogate any special properties of the call.
		switch child.Name {
		case "Rows":
			// Propogate "from" time, if set.
			if v, ok := child.Args["from"]; ok {
				for _, rowCall := range resultRows {
					rowCall.Args["from"] = v
				}
			}

			// Propogate "to" time, if set.
			if v, ok := child.Args["to"]; ok {
				for _, rowCall := range resultRows {
					rowCall.Args["to"] = v
				}
			}
		}

		rows = append(rows, resultRows...)
	}

	// Generate a Union call over the rows.
	c = &pql.Call{
		Name:     "Union",
		Children: rows,
	}

	// Execute the generated Union() call.
	return o.executeBitmapCall(ctx, tableKeyer, c, shards, opt)
}

// executeCount executes a count() call.
func (o *orchestrator) executeCount(ctx context.Context, tableKeyer dax.TableKeyer, c *pql.Call, shards []uint64, opt *featurebase.ExecOptions) (uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeCount")
	defer span.Finish()

	if len(c.Children) == 0 {
		return 0, errors.New(errors.ErrUncoded, "Count() requires an input bitmap")
	} else if len(c.Children) > 1 {
		return 0, errors.New(errors.ErrUncoded, "Count() only accepts a single bitmap input")
	}

	child := c.Children[0]

	// If the child is distinct/similar, execute it directly here and count the result.
	if child.Type == pql.PrecallGlobal {
		result, err := o.executeCall(ctx, tableKeyer, child, shards, opt)
		if err != nil {
			return 0, err
		}

		switch row := result.(type) {
		case *featurebase.Row:
			return row.Count(), nil
		case featurebase.SignedRow:
			return row.Pos.Count() + row.Neg.Count(), nil
		case featurebase.DistinctTimestamp:
			return uint64(len(row.Values)), nil
		default:
			return 0, errors.Errorf("cannot count result of type %T from call %q", row, child.String())
		}
	}

	// Merge returned results at coordinating node.
	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		other, _ := prev.(uint64)
		return other + v.(uint64)
	}

	result, err := o.mapReduce(ctx, tableKeyer, shards, c, opt, reduceFn)
	if err != nil {
		return 0, err
	}
	n, _ := result.(uint64)

	return n, nil
}

// remoteExec executes a PQL query remotely for a set of shards on a node.
func (o *orchestrator) remoteExec(ctx context.Context, node dax.Address, index string, q *pql.Query, shards []uint64, embed []*featurebase.Row) (results []interface{}, err error) { // nolint: interfacer
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeExec")
	defer span.Finish()

	// Encode request object.
	pbreq := &featurebase.QueryRequest{
		Query:        q.String(),
		Shards:       shards,
		Remote:       true,
		EmbeddedData: embed,
	}

	resp, err := o.client.QueryNode(ctx, node, index, pbreq)
	if err != nil {
		return nil, err
	}

	return resp.Results, resp.Err
}

// mapReduce maps and reduces data across the cluster.
//
// If a mapping of shards to a node fails then the shards are resplit across
// secondary nodes and retried. This continues to occur until all nodes are exhausted.
//
// mapReduce has to ensure that it never returns before any work it spawned has
// terminated. It's not enough to cancel the jobs; we have to wait for them to be
// done, or we can unmap resources they're still using.
func (o *orchestrator) mapReduce(ctx context.Context, tableKeyer dax.TableKeyer, shards []uint64, c *pql.Call, opt *featurebase.ExecOptions, reduceFn reduceFunc) (result interface{}, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.mapReduce")
	defer span.Finish()

	index := string(tableKeyer.Key())

	ch := make(chan mapResponse)

	// Wrap context with a cancel to kill goroutines on exit.
	ctx, cancel := context.WithCancel(ctx)
	// Create an errgroup so we can wait for all the goroutines to exit
	eg, ctx := errgroup.WithContext(ctx)

	// After we're done processing, we have to wait for any outstanding
	// functions in the ErrGroup to complete. If we didn't have an error
	// already at that point, we'll report any errors from the ErrGroup
	// instead.
	defer func() {
		cancel()
		errWait := eg.Wait()
		if err == nil {
			err = errWait
		}
	}()

	nodes, err := o.topology.ComputeNodes(ctx, index, shards)
	if err != nil {
		return nil, errors.Wrapf(err, "getting nodes/shards for index '%q'", index)
	}

	// Start mapping across all primary owners.
	if err = o.mapper(ctx, eg, ch, index, nodes, c, opt, reduceFn); err != nil {
		return nil, errors.Wrap(err, "starting mapper")
	}

	// Iterate over all map responses and reduce.
	expected := 0
	for _, n := range nodes {
		expected += len(n.Shards)
	}
	done := ctx.Done()
	for expected > 0 {
		select {
		case <-done:
			return nil, ctx.Err()
		case resp := <-ch:
			if resp.err != nil {
				cancel() // TODO(jaffee) I added this... seems right, but wasn't there before
				return nil, errors.Wrap(resp.err, "mapping on primary node")
			}
			// if we got a response that we aren't discarding
			// because it's an error, subtract it from our count...
			expected -= len(resp.shards)

			// Reduce value.
			result = reduceFn(ctx, result, resp.result)
			var ok bool
			// note *not* shadowed.
			if err, ok = result.(error); ok {
				cancel()
				return nil, err
			}
		}
	}
	// note the deferred Wait above which might override this nil.
	return result, nil
}

// makeEmbeddedDataForShards produces new rows containing the RowSegments
// that would correspond to a given set of shards.
func makeEmbeddedDataForShards(allRows []*featurebase.Row, shards []uint64) []*featurebase.Row {
	if len(allRows) == 0 || len(shards) == 0 {
		return nil
	}
	newRows := make([]*featurebase.Row, len(allRows))
	for i, row := range allRows {
		if row == nil || len(row.Segments) == 0 {
			continue
		}
		if row.NoSplit {
			newRows[i] = row
			continue
		}
		segments := row.Segments
		segmentIndex := 0
		newRows[i] = &featurebase.Row{
			Index: row.Index,
			Field: row.Field,
		}
		for _, shard := range shards {
			for segmentIndex < len(segments) && segments[segmentIndex].Shard() < shard {
				segmentIndex++
			}
			// no more segments in this row
			if segmentIndex >= len(segments) {
				break
			}
			if segments[segmentIndex].Shard() == shard {
				newRows[i].Segments = append(newRows[i].Segments, segments[segmentIndex])
				segmentIndex++
				if segmentIndex >= len(segments) {
					// no more segments, we're done
					break
				}
			}
			// if we got here, segments[segmentIndex].shard exists
			// but is greater than the current shard, so we continue.
		}
	}
	return newRows
}

func (o *orchestrator) mapper(ctx context.Context, eg *errgroup.Group, ch chan mapResponse, index string, nodes []dax.ComputeNode, c *pql.Call, opt *featurebase.ExecOptions, reduceFn reduceFunc) (reterr error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.mapper")
	defer span.Finish()

	// Group shards together by nodes.
	done := ctx.Done()

	// Execute each node in a separate goroutine.
	for _, node := range nodes {
		node := node
		shards := make([]uint64, len(node.Shards))
		for i, dshard := range node.Shards {
			shards[i] = uint64(dshard)
		}
		eg.Go(func() error {
			resp := mapResponse{node: node.Address, shards: shards}

			var embeddedRowsForNode []*featurebase.Row
			if opt.EmbeddedData != nil {
				embeddedRowsForNode = makeEmbeddedDataForShards(opt.EmbeddedData, shards)
			}

			attempts := 0
			for ; attempts == 0 || (resp.err != nil && strings.Contains(resp.err.Error(), errConnectionRefused) && attempts < 3); attempts++ {
				// On error retry against remaining nodes. If an error returns then
				// the context will cancel and cause all open goroutines to return.
				//
				// We distinguish here between an error which indicates that the
				// node is not available (and therefore we need to failover to a
				// replica) and a valid error from a healthy node. In the case of
				// the latter, there's no need to retry a replica, we should trust
				// the error from the healthy node and return that immediately.
				// TODO(jaffee) retries should contact MDS and find out who is up and has access to shards needed
				results, err := o.remoteExec(ctx, node.Address, index, &pql.Query{Calls: []*pql.Call{c}}, shards, embeddedRowsForNode)
				if len(results) > 0 {
					resp.result = results[0]
				}
				resp.err = err
			}
			// Return response to the channel.
			select {
			case <-done:
				// If someone just canceled the context
				// arbitrarily, we could end up here with this
				// being the first non-nil error handed to
				// the ErrGroup, in which case, it's the best
				// explanation we have for why everything's
				// stopping.
				return ctx.Err()
			case ch <- resp:
				return nil
			}
		})
		if reterr != nil {
			return reterr // exit early if error occurs when running serially
		}
	}
	return nil
}

func (o *orchestrator) preTranslate(ctx context.Context, index string, calls ...*pql.Call) (cols map[string]map[string]uint64, rows map[string]map[string]map[string]uint64, err error) {
	// Collect all of the required keys.
	collector := keyCollector{
		createCols: make(map[string][]string),
		findCols:   make(map[string][]string),
		createRows: make(map[string]map[string][]string),
		findRows:   make(map[string]map[string][]string),
	}
	for _, call := range calls {
		err := o.collectCallKeys(&collector, call, index)
		if err != nil {
			return nil, nil, err
		}
	}

	// Create keys.
	// Both rows and columns need to be created first because of foreign index keys.
	cols = make(map[string]map[string]uint64)
	rows = make(map[string]map[string]map[string]uint64)
	for index, keys := range collector.createCols {
		translations, err := o.trans.CreateIndexKeys(ctx, index, keys)
		if err != nil {
			return nil, nil, errors.Wrap(err, "creating query column keys")
		}
		cols[index] = translations
	}
	for index, fields := range collector.createRows {
		idxRows := make(map[string]map[string]uint64)
		for field, keys := range fields {
			translations, err := o.trans.CreateFieldKeys(ctx, index, field, keys)
			if err != nil {
				return nil, nil, errors.Wrap(err, "creating query row keys")
			}
			idxRows[field] = translations
		}
		rows[index] = idxRows
	}

	// Find other keys.
	for index, keys := range collector.findCols {
		translations, err := o.trans.FindIndexKeys(ctx, index, keys)
		if err != nil {
			return nil, nil, errors.Wrap(err, "finding query column keys")
		}
		if prev := cols[index]; prev != nil {
			for key, id := range translations {
				prev[key] = id
			}
		} else {
			cols[index] = translations
		}
	}
	for index, fields := range collector.findRows {
		idxRows := rows[index]
		if idxRows == nil {
			idxRows = make(map[string]map[string]uint64)
			rows[index] = idxRows
		}
		for field, keys := range fields {
			translations, err := o.trans.FindFieldKeys(ctx, index, field, keys)
			if err != nil {
				return nil, nil, errors.Wrap(err, "finding query row keys")
			}
			if prev := idxRows[field]; prev != nil {
				for key, id := range translations {
					prev[key] = id
				}
			} else {
				idxRows[field] = translations
			}
		}
	}

	return cols, rows, nil
}

func (o *orchestrator) collectCallKeys(dst *keyCollector, c *pql.Call, index string) error {
	// Check for an overriding 'index' argument.
	// This also applies to all child calls.
	if callIndex := c.CallIndex(); callIndex != "" {
		index = callIndex
	}

	// Handle the field arg.
	switch c.Name {
	case "Set":
		if field, err := c.FieldArg(); err == nil {
			if arg, ok := c.Args[field].(string); ok {
				dst.CreateRows(index, field, arg)
			}
		}

	// TODO: will have to consider how to support Store... creating the field if it doesn't exist will not be a thing though.
	case "Store":
		return errors.New(errors.ErrUncoded, "Store query currently unsupported")
	case "Clear", "Row", "Range", "ClearRow":
		if field, err := c.FieldArg(); err == nil {
			switch arg := c.Args[field].(type) {
			case string:
				dst.FindRows(index, field, arg)
			case *pql.Condition:
				// This is a workaround to allow `==` and `!=` to work on foreign index fields.
				if key, ok := arg.Value.(string); ok {
					switch arg.Op {
					case pql.EQ, pql.NEQ:
						dst.FindRows(index, field, key)
					default:
						return errors.Errorf("operator %v not defined on strings", arg.Op)
					}
				}
			}
		}
	}

	// Handle _col.
	if col, ok := c.Args["_col"].(string); ok {
		switch c.Name {
		case "Set":
			dst.CreateColumns(index, col)
		default:
			dst.FindColumns(index, col)
		}
	}

	// Handle _row.
	if row, ok := c.Args["_row"].(string); ok {
		// Find the field.
		field, ok, err := c.StringArg("_field")
		if err != nil {
			return errors.Wrap(err, "finding field")
		}
		if !ok {
			return errors.Wrap(ErrFieldNotFound, "finding field for _row argument")
		}

		dst.FindRows(index, field, row)
	}

	// Handle queries that need a "column" argument.
	switch c.Name {
	case "Rows", "GroupBy", "FieldValue", "IncludesColumn":
		if col, ok := c.Args["column"].(string); ok {
			dst.FindColumns(index, col)
		}
	}

	// Handle special per-query arguments.
	switch c.Name {
	case "ConstRow":
		// Translate the columns list.
		if cols, ok := c.Args["columns"].([]interface{}); ok {
			keys := make([]string, 0, len(cols))
			for _, v := range cols {
				switch v := v.(type) {
				case string:
					keys = append(keys, v)
				case uint64:
				case int64:
				default:
					return errors.Errorf("invalid column identifier %v of type %T", c, c)
				}
			}
			dst.FindColumns(index, keys...)
		}

	case "Rows":
		// Find the field.
		var field string
		if f, ok1, err := c.StringArg("_field"); err != nil {
			return errors.Wrap(err, "finding _field for Rows previous translation")
		} else if ok1 {
			field = f
		} else if f, ok2, err := c.StringArg("field"); err != nil {
			return errors.Wrap(err, "finding field for Rows previous translation")
		} else if ok2 {
			field = f
		} else {
			return errors.New(errors.ErrUncoded, "missing field in Rows call")
		}
		if prev, ok := c.Args["previous"].(string); ok {
			dst.FindRows(index, field, prev)
		}
		if in, ok := c.Args["in"]; ok {
			inIn, ok := in.([]interface{})
			if !ok {
				return errors.Errorf("unexpected type for argument 'in' %v of %[1]T", inIn)
			}
			inStrs := make([]string, 0)
			for _, v := range inIn {
				if vstr, ok := v.(string); ok {
					inStrs = append(inStrs, vstr)
				}
			}
			dst.FindRows(index, field, inStrs...)
		}
	}

	// Collect keys from child calls.
	for _, child := range c.Children {
		err := o.collectCallKeys(dst, child, index)
		if err != nil {
			return err
		}
	}

	// Collect keys from argument calls.
	for _, arg := range c.Args {
		argCall, ok := arg.(*pql.Call)
		if !ok {
			continue
		}

		err := o.collectCallKeys(dst, argCall, index)
		if err != nil {
			return err
		}
	}

	return nil
}

type keyCollector struct {
	createCols, findCols map[string][]string            // map[index] -> column keys
	createRows, findRows map[string]map[string][]string // map[index]map[field] -> row keys
}

func (c *keyCollector) CreateColumns(index string, columns ...string) {
	if len(columns) == 0 {
		return
	}
	c.createCols[index] = append(c.createCols[index], columns...)
}

func (c *keyCollector) FindColumns(index string, columns ...string) {
	if len(columns) == 0 {
		return
	}
	c.findCols[index] = append(c.findCols[index], columns...)
}

func (c *keyCollector) CreateRows(index string, field string, columns ...string) {
	if len(columns) == 0 {
		return
	}
	idx := c.createRows[index]
	if idx == nil {
		idx = make(map[string][]string)
		c.createRows[index] = idx
	}
	idx[field] = append(idx[field], columns...)
}

func (c *keyCollector) FindRows(index string, field string, columns ...string) {
	if len(columns) == 0 {
		return
	}
	idx := c.findRows[index]
	if idx == nil {
		idx = make(map[string][]string)
		c.findRows[index] = idx
	}
	idx[field] = append(idx[field], columns...)
}

func fieldValidateValue(f *featurebase.FieldInfo, val interface{}) error {
	if val == nil {
		return nil
	}

	// Validate special types.
	switch val := val.(type) {
	case string:
		if !f.Options.Keys {
			return errors.Errorf("string value on unkeyed field %q", f.Name)
		}
		return nil
	case *pql.Condition:
		switch v := val.Value.(type) {
		case nil:
		case string:
		case uint64:
		case int64:
		case float64:
		case pql.Decimal:
		case time.Time:
		case []interface{}:
			for _, v := range v {
				if err := fieldValidateValue(f, v); err != nil {
					return err
				}
			}
			return nil
		default:
			return errors.Errorf("invalid value %v in condition %q", v, val.String())
		}
		return fieldValidateValue(f, val.Value)
	}

	switch f.Options.Type {
	case FieldTypeSet, FieldTypeMutex, FieldTypeTime:
		switch v := val.(type) {
		case uint64:
		case int64:
			if v < 0 {
				return errors.Errorf("negative ID %d for set field %q", v, f.Name)
			}
		default:
			return errors.Errorf("invalid value %v for field %q of type %s", v, f.Name, f.Options.Type)
		}
		if f.Options.Keys {
			return errors.Errorf("found integer ID %d on keyed field %q", val, f.Name)
		}
	case FieldTypeBool:
		switch v := val.(type) {
		case bool:
		default:
			return errors.Errorf("invalid value %v for bool field %q", v, f.Name)
		}
	case FieldTypeInt:
		switch v := val.(type) {
		case uint64:
			if v > 1<<63 {
				return errors.Errorf("oversized integer %d for int field %q (range: -2^63 to 2^63-1)", v, f.Name)
			}
		case int64:
		default:
			return errors.Errorf("invalid value %v for int field %q", v, f.Name)
		}
	case FieldTypeDecimal:
		switch v := val.(type) {
		case uint64:
		case int64:
		case float64:
		case pql.Decimal:
		default:
			return errors.Errorf("invalid value %v for decimal field %q", v, f.Name)
		}
	case FieldTypeTimestamp:
		switch v := val.(type) {
		case time.Time:
		default:
			return errors.Errorf("invalid value %v for timestamp field %q", v, f.Name)
		}
	default:
		return errors.Errorf("unsupported type %s of field %q", f.Options.Type, f.Name)
	}

	return nil
}

func (o *orchestrator) translateCall(ctx context.Context, c *pql.Call, tableKeyer dax.TableKeyer, columnKeys map[string]map[string]uint64, rowKeys map[string]map[string]map[string]uint64) (*pql.Call, error) {
	index := string(tableKeyer.Key())

	// Check for an overriding 'index' argument.
	// This also applies to all child calls.
	if callIndex := c.CallIndex(); callIndex != "" {
		index = callIndex
		// TODO(tlt): checking for prefix like this is bad form. Ideally, the
		// argument stored in the Call.Args map would be of type TableKey
		// (currently they are restricted to type: string). In that case we
		// could just pass it through without doing this conversion. (This would
		// require changing the logic in Queryer.convertIndex() to set "index"
		// to a TableKeyer).
		if strings.HasPrefix(index, dax.PrefixTable+dax.TableKeyDelimiter) {
			qtid, err := dax.QualifiedTableIDFromKey(index)
			if err != nil {
				return nil, errors.Wrapf(err, "getting qtid from key: %s", index)
			}
			tableKeyer = qtid
		} else {
			tableKeyer = dax.StringTableKeyer(index)
		}
	}

	idx, err := o.schemaIndexInfo(ctx, tableKeyer)
	if err != nil {
		return nil, errors.Wrapf(err, "translating query on index %q", index)
	}

	// Fetch the column keys list for this index.
	indexCols, indexRows := columnKeys[index], rowKeys[index]

	// Handle the field arg.
	switch c.Name {
	case "Set", "Store":
		if field, err := c.FieldArg(); err == nil {
			f, err := o.schemaFieldInfo(ctx, tableKeyer, field)
			if err != nil {
				return nil, errors.Wrapf(err, "validating value for field %q", field)
			}
			arg := c.Args[field]
			if err := fieldValidateValue(f, arg); err != nil {
				return nil, errors.Wrap(err, "validating store value")
			}
			switch arg := arg.(type) {
			case string:
				if translation, ok := indexRows[field][arg]; ok {
					c.Args[field] = translation
				} else {
					return nil, errors.Wrapf(featurebase.ErrTranslatingKeyNotFound, "destination key not found %q in %q in index %q", arg, field, index)
				}
			case bool:
				if arg {
					c.Args[field] = trueRowID
				} else {
					c.Args[field] = falseRowID
				}
			}
		}

	case "Clear", "Row", "Range", "ClearRow":
		if field, err := c.FieldArg(); err == nil {
			f, err := o.schemaFieldInfo(ctx, tableKeyer, field)
			if err != nil {
				return nil, errors.Wrapf(err, "validating value for field %q", field)
			}
			arg := c.Args[field]
			if err := fieldValidateValue(f, arg); err != nil {
				return nil, errors.Wrap(err, "validating field parameter value")
			}
			if c.Name == "Row" {
				switch f.Options.Type {
				case FieldTypeInt, FieldTypeDecimal, FieldTypeTimestamp:
					if _, ok := arg.(*pql.Condition); !ok {
						// This is workaround to support pql.ASSIGN ('=') as condition ('==') for BSI fields.
						arg = &pql.Condition{
							Op:    pql.EQ,
							Value: arg,
						}
						c.Args[field] = arg
					}
				}
			}
			switch arg := arg.(type) {
			case string:
				if translation, ok := indexRows[field][arg]; ok {
					c.Args[field] = translation
				} else {
					// Rewrite the call into a zero value call.
					return o.callZero(c), nil
				}
			case bool:
				if arg {
					c.Args[field] = trueRowID
				} else {
					c.Args[field] = falseRowID
				}
			case *pql.Condition:
				// This is a workaround to allow `==` and `!=` to work on foreign index fields.
				if key, ok := arg.Value.(string); ok {
					switch arg.Op {
					case pql.EQ, pql.NEQ:
						if translation, ok := indexRows[field][key]; ok {
							arg.Value = translation
						} else {
							// Rewrite the call into a zero value call.
							return o.callZero(c), nil
						}
					default:
						return nil, errors.Errorf("operator %v not defined on strings", arg.Op)
					}
				}
			}
		}
	}

	// Handle _col.
	if col, ok := c.Args["_col"].(string); ok {
		if !idx.Options.Keys {
			return nil, errors.Wrapf(featurebase.ErrTranslatingKeyNotFound, "translating column on unkeyed index %q", index)
		}
		if id, ok := indexCols[col]; ok {
			c.Args["_col"] = id
		} else {
			switch c.Name {
			case "Set":
				return nil, errors.Wrapf(featurebase.ErrTranslatingKeyNotFound, "destination key not found %q in index %q", col, index)
			default:
				return o.callZero(c), nil
			}
		}
	}

	// Handle _row.
	if row, ok := c.Args["_row"]; ok {
		// Find the field.
		var field string
		if f, ok1, err := c.StringArg("_field"); err != nil {
			return nil, errors.Wrap(err, "finding _field")
		} else if ok1 {
			field = f
		} else if f, ok2, err := c.StringArg("field"); err != nil {
			return nil, errors.Wrap(err, "finding field")
		} else if ok2 {
			field = f
		} else {
			return nil, errors.New(errors.ErrUncoded, "missing field")
		}

		f, err := o.schemaFieldInfo(ctx, tableKeyer, field)
		if err != nil {
			return nil, errors.Wrapf(err, "validating value for field %q", field)
		}
		if err := fieldValidateValue(f, row); err != nil {
			return nil, errors.Wrap(err, "validating row value")
		}
		switch row := row.(type) {
		case string:
			if translation, ok := indexRows[field][row]; ok {
				c.Args["_row"] = translation
			} else {
				return o.callZero(c), nil
			}
		}
	}

	// Handle queries that need a "column" argument.
	switch c.Name {
	case "Rows", "GroupBy", "FieldValue", "IncludesColumn":
		if col, ok := c.Args["column"].(string); ok {
			if translation, ok := indexCols[col]; ok {
				c.Args["column"] = translation
			} else {
				// Rewrite the call into a zero value call.
				return o.callZero(c), nil
			}
		}
	}

	// Handle special per-query arguments.
	switch c.Name {
	case "ConstRow":
		// Translate the columns list.
		if cols, ok := c.Args["columns"].([]interface{}); ok {
			out := make([]uint64, 0, len(cols))
			for _, v := range cols {
				switch v := v.(type) {
				case string:
					if id, ok := indexCols[v]; ok {
						out = append(out, id)
					}
				case uint64:
					out = append(out, v)
				case int64:
					out = append(out, uint64(v))
				default:
					return nil, errors.Errorf("invalid column identifier %v of type %T", c, c)
				}
			}
			c.Args["columns"] = out
		}

	case "Rows":
		// Find the field.
		var field string
		if f, ok1, err := c.StringArg("_field"); err != nil {
			return nil, errors.Wrap(err, "finding _field for Rows previous translation")
		} else if ok1 {
			field = f
		} else if f, ok2, err := c.StringArg("field"); err != nil {
			return nil, errors.Wrap(err, "finding field for Rows previous translation")
		} else if ok2 {
			field = f
		} else {
			return nil, errors.New(errors.ErrUncoded, "missing field in Rows call")
		}
		// Translate the previous row key.
		if prev, ok := c.Args["previous"]; ok {
			// Validate the type.
			f, err := o.schemaFieldInfo(ctx, tableKeyer, field)
			if err != nil {
				return nil, errors.Wrapf(err, "validating value for field %q", field)
			}
			if err := fieldValidateValue(f, prev); err != nil {
				return nil, errors.Wrap(err, "validating prev value")
			}

			switch prev := prev.(type) {
			case string:
				// Look up a translation for the previous row key.
				if translation, ok := indexRows[field][prev]; ok {
					c.Args["previous"] = translation
				} else {
					return nil, errors.Wrapf(featurebase.ErrTranslatingKeyNotFound, "translating previous key %q from field %q in index %q in Rows call", prev, field, index)
				}
			case bool:
				if prev {
					c.Args["previous"] = trueRowID
				} else {
					c.Args["previous"] = falseRowID
				}
			}
		}

		// Check if "like" argument is applied to keyed fields.
		if _, found := c.Args["like"].(string); found {
			fieldName, err := c.FirstStringArg("_field", "field")
			if err != nil || fieldName == "" {
				return nil, fmt.Errorf("cannot read field name for Rows call")
			}
			if f, err := o.schemaFieldInfo(ctx, tableKeyer, fieldName); err != nil {
				return nil, errors.Wrapf(err, "getting field %q", fieldName)
			} else if !f.Options.Keys {
				return nil, fmt.Errorf("'%s' is not a set/mutex/time field with a string key", fieldName)
			}
		}

		if in, ok := c.Args["in"]; ok {
			inIn, ok := in.([]interface{})
			if !ok {
				return nil, errors.Errorf("unexpected type for argument 'in' %v of %[1]T", in)
			}
			inIDs := make([]interface{}, 0, len(inIn))
			for _, inVal := range inIn {
				if inStr, ok := inVal.(string); ok {
					id, found := rowKeys[index][field][inStr]
					if found {
						inIDs = append(inIDs, id)
					}
				} else {
					inIDs = append(inIDs, inVal)
				}
			}
			c.Args["in"] = inIDs
		}
	}

	// Translate child calls.
	for i, child := range c.Children {
		translated, err := o.translateCall(ctx, child, tableKeyer, columnKeys, rowKeys)
		if err != nil {
			return nil, err
		}
		c.Children[i] = translated
	}

	// Translate argument calls.
	for k, arg := range c.Args {
		argCall, ok := arg.(*pql.Call)
		if !ok {
			continue
		}

		translated, err := o.translateCall(ctx, argCall, tableKeyer, columnKeys, rowKeys)
		if err != nil {
			return nil, err
		}

		c.Args[k] = translated
	}

	return c, nil
}

func (o *orchestrator) callZero(c *pql.Call) *pql.Call {
	switch c.Name {
	case "Row", "Range":
		if field, err := c.FieldArg(); err == nil {
			if cond, ok := c.Args[field].(*pql.Condition); ok {
				if cond.Op == pql.NEQ {
					// Turn not nothing into everything.
					return &pql.Call{Name: "All"}
				}
			}
		}

		// Use an empty union as a placeholder.
		return &pql.Call{Name: "Union"}

	default:
		return nil
	}
}

func (o *orchestrator) translateResults(ctx context.Context, qtbl *dax.QualifiedTable, calls []*pql.Call, results []interface{}, memoryAvailable int64) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.translateResults")
	defer span.Finish()

	idx := featurebase.TableToIndexInfo(&qtbl.Table)

	idMap := make(map[uint64]string)
	if idx.Options.Keys {
		// Collect all index ids.
		idSet := make(map[uint64]struct{})
		for i := range calls {
			if err := o.collectResultIDs(ctx, idx, calls[i], results[i], idSet); err != nil {
				return err
			}
		}
		if idMap, err = o.trans.TranslateIndexIDSet(ctx, string(qtbl.Key()), idSet); err != nil {
			return err
		}
	}

	for i := range results {
		results[i], err = o.translateResult(ctx, qtbl, calls[i], results[i], idMap)
		if err != nil {
			return err
		}
	}
	return nil
}

// translationStrategy denotes the several different ways the bits in
// a *Row could be translated to string keys.
type translationStrategy int

const (
	// byCurrentIndex means to interpret the bits as IDs in "top
	// level" index for this query (e.g. the index specified in the
	// path of the HTTP request).
	byCurrentIndex translationStrategy = iota + 1
	// byRowField means that the bits in this *Row are row IDs which
	// should be translated using the field's (*Row.Field) translation store.
	byRowField
	// byRowFieldForeignIndex means that the bits in this *Row should
	// be interpreted as IDs in the foreign index of the *Row.Field.
	byRowFieldForeignIndex
	// byRowIndex means the bits in this *Row should be translated
	// according to the index named by *Row.Index
	byRowIndex
	// noTranslation means the bits should not be translated to string
	// keys.
	noTranslation
)

// howToTranslate determines how a *Row object's bits should be
// translated to keys (if at all). There are several different options
// detailed by the various const values of translationStrategy. In
// order to do this it has to figure out the row's index and field
// which it also returns as the caller may need them to actually
// execute the translation or do whatever else it's doing with the
// translationStrategy information.
func (o *orchestrator) howToTranslate(ctx context.Context, idx *featurebase.IndexInfo, row *featurebase.Row) (rowIdx *featurebase.IndexInfo, rowField *featurebase.FieldInfo, strat translationStrategy, err error) {
	// First get the index and field the row specifies (if any).
	rowIdx = idx
	if row.Index != "" && row.Index != idx.Name {
		rowIdx, err = o.schemaIndexInfo(ctx, dax.TableKey(row.Index))
		if err != nil {
			return nil, nil, 0, errors.Wrapf(err, "got a row with unknown index: %s", row.Index)
		}
	}
	if row.Field != "" {
		rowField, err = o.schemaFieldInfo(ctx, dax.TableKey(row.Index), row.Field)
		if err != nil {
			return nil, nil, 0, errors.Wrapf(err, "got a row with unknown index/field %s/%s", idx.Name, row.Field)
		}
	}

	// Handle the case where the Row has specified a field.
	if rowField != nil {
		// Handle the case where field has a foreign index.
		if rowField.Options.ForeignIndex != "" {
			fidx, err := o.schemaIndexInfo(ctx, dax.StringTableKeyer(rowField.Options.ForeignIndex))
			if err != nil {
				return nil, nil, 0, errors.Errorf("foreign index %s not found for field %s in index %s", rowField.Options.ForeignIndex, rowField.Name, rowIdx.Name)
			}
			if fidx.Options.Keys {
				return rowIdx, rowField, byRowFieldForeignIndex, nil
			}
		} else if rowField.Options.Keys {
			return rowIdx, rowField, byRowField, nil
		}
		return rowIdx, rowField, noTranslation, nil
	}

	// In this case, the row has specified an index, but not a field,
	// so we translate according to that index.
	if rowIdx != idx && rowIdx.Options.Keys {
		return rowIdx, rowField, byRowIndex, nil
	}

	// Handle the normal case (row represents a set of records in
	// the top level index, Row has not specifed a different index
	// or field).
	if rowIdx == idx && idx.Options.Keys && rowField == nil {
		return rowIdx, rowField, byCurrentIndex, nil
	}
	return rowIdx, rowField, noTranslation, nil
}

func (o *orchestrator) collectResultIDs(ctx context.Context, idx *featurebase.IndexInfo, call *pql.Call, result interface{}, idSet map[uint64]struct{}) error {
	switch result := result.(type) {
	case *featurebase.Row:
		// Only collect result IDs if they are in the current index.
		_, _, strategy, err := o.howToTranslate(ctx, idx, result)
		if err != nil {
			return errors.Wrap(err, "determining how to translate")
		}
		if strategy == byCurrentIndex {
			for _, segment := range result.Segments {
				for _, col := range segment.Columns() {
					idSet[col] = struct{}{}
				}
			}
		}
	case featurebase.ExtractedIDMatrix:
		for _, col := range result.Columns {
			idSet[col.ColumnID] = struct{}{}
		}
	}

	return nil
}

// preTranslateMatrixSet translates the IDs of a set field in an extracted matrix.
func (o *orchestrator) preTranslateMatrixSet(ctx context.Context, mat featurebase.ExtractedIDMatrix, fieldIdx uint, tableKeyer dax.TableKeyer, field string) (map[uint64]string, error) {
	ids := make(map[uint64]struct{}, len(mat.Columns))
	for _, col := range mat.Columns {
		for _, v := range col.Rows[fieldIdx] {
			ids[v] = struct{}{}
		}
	}

	return o.trans.TranslateFieldIDs(ctx, tableKeyer, field, ids)
}

func (o *orchestrator) translateResult(ctx context.Context, qtbl *dax.QualifiedTable, call *pql.Call, result interface{}, idSet map[uint64]string) (_ interface{}, err error) {
	idx := featurebase.TableToIndexInfo(&qtbl.Table)

	switch result := result.(type) {
	case *featurebase.Row:
		rowIdx, rowField, strategy, err := o.howToTranslate(ctx, idx, result)
		if err != nil {
			return nil, errors.Wrap(err, "determining translation strategy")
		}
		switch strategy {
		case byCurrentIndex:
			other := &featurebase.Row{}
			for _, segment := range result.Segments {
				for _, col := range segment.Columns() {
					other.Keys = append(other.Keys, idSet[col])
				}
			}
			return other, nil
		case byRowField:
			keys, err := o.trans.TranslateFieldListIDs(ctx, result.Index, rowField.Name, result.Columns())
			if err != nil {
				return nil, errors.Wrap(err, "translating Row to field keys")
			}
			result.Keys = keys
		case byRowFieldForeignIndex:
			if _, err := o.schemaIndexInfo(ctx, dax.StringTableKeyer(rowField.Options.ForeignIndex)); err != nil {
				return nil, errors.Wrapf(err, "foreign index %s not found for field %s in index %s", rowField.Options.ForeignIndex, rowField.Name, rowIdx.Name)
			}
			for _, segment := range result.Segments {
				keys, err := o.trans.TranslateIndexIDs(ctx, rowField.Options.ForeignIndex, segment.Columns())
				if err != nil {
					return nil, errors.Wrap(err, "translating index ids")
				}
				result.Keys = append(result.Keys, keys...)
			}

		case byRowIndex:
			for _, segment := range result.Segments {
				keys, err := o.trans.TranslateIndexIDs(ctx, rowIdx.Name, segment.Columns())
				if err != nil {
					return nil, errors.Wrap(err, "translating index ids")
				}
				result.Keys = append(result.Keys, keys...)
			}
			return result, nil

		case noTranslation:
			return result, nil
		default:
			return nil, errors.Errorf("unknown translation strategy %d", strategy)
		}
	case featurebase.SignedRow:
		sr, err := func() (*featurebase.SignedRow, error) {
			fieldName := callArgString(call, "field")
			if fieldName == "" {
				return nil, nil
			}

			field, err := o.schemaFieldInfo(ctx, qtbl, fieldName)
			if err != nil {
				return nil, nil
			}

			if field.Options.Keys {
				rslt := result.Pos
				if rslt == nil {
					return &featurebase.SignedRow{Pos: &featurebase.Row{}}, nil
				}
				other := &featurebase.Row{}
				for _, segment := range rslt.Segments {
					keys, err := o.trans.TranslateIndexIDs(ctx, field.Options.ForeignIndex, segment.Columns())
					if err != nil {
						return nil, errors.Wrap(err, "translating index ids")
					}
					other.Keys = append(other.Keys, keys...)
				}
				return &featurebase.SignedRow{Pos: other}, nil
			}

			return nil, nil
		}()
		if err != nil {
			return nil, err
		} else if sr != nil {
			return *sr, nil
		}

	case featurebase.PairField:
		if fieldName := callArgString(call, "field"); fieldName != "" {
			field, err := o.schemaFieldInfo(ctx, qtbl, fieldName)
			if err != nil {
				return nil, fmt.Errorf("field %q not found", fieldName)
			}
			if field.Options.Keys {
				// TODO(jaffee) get index name from call? CallIndex? (not just here)
				keys, err := o.trans.TranslateFieldListIDs(ctx, idx.Name, fieldName, []uint64{result.Pair.ID})
				if err != nil {
					return nil, err
				}
				key := keys[0]
				if call.Name == "MinRow" || call.Name == "MaxRow" {
					result.Pair.Key = key
					return result, nil
				}
				return featurebase.PairField{
					Pair:  featurebase.Pair{Key: key, Count: result.Pair.Count},
					Field: fieldName,
				}, nil
			}
		}

	case *featurebase.PairsField:
		if fieldName := callArgString(call, "_field"); fieldName != "" {
			field, err := o.schemaFieldInfo(ctx, qtbl, fieldName)
			if err != nil {
				return nil, errors.Wrapf(err, "field '%q'", fieldName)
			}
			if field.Options.Keys {
				ids := make([]uint64, len(result.Pairs))
				for i := range result.Pairs {
					ids[i] = result.Pairs[i].ID
				}
				keys, err := o.trans.TranslateFieldListIDs(ctx, idx.Name, fieldName, ids)
				if err != nil {
					return nil, err
				}
				other := make([]featurebase.Pair, len(result.Pairs))
				for i := range result.Pairs {
					other[i] = featurebase.Pair{Key: keys[i], Count: result.Pairs[i].Count}
				}
				return &featurebase.PairsField{
					Pairs: other,
					Field: fieldName,
				}, nil
			}
		}

	case *featurebase.GroupCounts:
		fieldIDs := make(map[*featurebase.FieldInfo]map[uint64]struct{})
		foreignIDs := make(map[*featurebase.FieldInfo]map[uint64]struct{})
		groups := result.Groups()
		for _, gl := range groups {
			for _, g := range gl.Group {
				field, err := o.schemaFieldInfo(ctx, qtbl, g.Field)
				if err != nil {
					return nil, errors.Wrapf(err, "getting field '%q", g.Field)
				}
				if field.Options.Keys {
					if g.Value != nil {
						if fi := field.Options.ForeignIndex; fi != "" {
							m, ok := foreignIDs[field]
							if !ok {
								m = make(map[uint64]struct{}, len(groups))
								foreignIDs[field] = m
							}

							m[uint64(*g.Value)] = struct{}{}
							continue
						}
					}

					m, ok := fieldIDs[field]
					if !ok {
						m = make(map[uint64]struct{}, len(groups))
						fieldIDs[field] = m
					}

					m[g.RowID] = struct{}{}
				}
			}
		}

		fieldTranslations := make(map[string]map[uint64]string)
		for field, ids := range fieldIDs {
			trans, err := o.trans.TranslateFieldIDs(ctx, qtbl, field.Name, ids)
			if err != nil {
				return nil, errors.Wrapf(err, "translating IDs in field '%q'", field.Name)
			}
			fieldTranslations[field.Name] = trans
		}

		foreignTranslations := make(map[string]map[uint64]string)
		for field, ids := range foreignIDs {
			trans, err := o.trans.TranslateIndexIDSet(ctx, field.Options.ForeignIndex, ids)
			if err != nil {
				return nil, errors.Wrapf(err, "translating foreign IDs from index %q", field.Options.ForeignIndex)
			}
			foreignTranslations[field.Name] = trans
		}

		// We are reluctant to smash result, and I'm not sure we need
		// to be but I'm not sure we don't need to be.
		newGroups := make([]featurebase.GroupCount, len(groups))
		copy(newGroups, groups)
		for gi, gl := range groups {

			group := make([]featurebase.FieldRow, len(gl.Group))
			for i, g := range gl.Group {
				if ft, ok := fieldTranslations[g.Field]; ok {
					g.RowKey = ft[g.RowID]
				} else if ft, ok := foreignTranslations[g.Field]; ok && g.Value != nil {
					g.RowKey = ft[uint64(*g.Value)]
					g.Value = nil
				}

				group[i] = g
			}
			// Replace with translated group.
			newGroups[gi].Group = group
		}
		if result != nil {
			return featurebase.NewGroupCounts(result.AggregateColumn(), newGroups...), nil
		}
		return &featurebase.GroupCounts{}, nil
	case featurebase.RowIDs:
		fieldName := callArgString(call, "_field")
		if fieldName == "" {
			return nil, ErrFieldNotFound
		}

		other := featurebase.RowIdentifiers{
			Field: fieldName,
		}

		if field, err := o.schemaFieldInfo(ctx, qtbl, fieldName); err != nil {
			return nil, errors.Wrapf(err, "'%q'", fieldName)
		} else if field.Options.Keys {
			keys, err := o.trans.TranslateFieldListIDs(ctx, idx.Name, field.Name, result)
			if err != nil {
				return nil, errors.Wrap(err, "translating row IDs")
			}
			other.Keys = keys
		} else {
			other.Rows = result
		}

		return other, nil

	case featurebase.ExtractedIDMatrix:
		type fieldMapper = func([]uint64) (_ interface{}, err error)

		fields := make([]featurebase.ExtractedTableField, len(result.Fields))
		mappers := make([]fieldMapper, len(result.Fields))
		for i, v := range result.Fields {
			field, err := o.schemaFieldInfo(ctx, qtbl, v)
			if err != nil {
				return nil, errors.Wrapf(err, "'%q'", v)
			}

			var mapper fieldMapper
			var datatype string
			switch typ := field.Options.Type; typ {
			case FieldTypeBool:
				datatype = "bool"
				mapper = func(ids []uint64) (_ interface{}, err error) {
					switch len(ids) {
					case 0:
						return nil, nil
					case 1:
						switch ids[0] {
						case 0:
							return false, nil
						case 1:
							return true, nil
						default:
							return nil, errors.Errorf("invalid ID for boolean %q: %d", field.Name, ids[0])
						}
					default:
						return nil, errors.Errorf("boolean %q has too many values: %v", field.Name, ids)
					}
				}
			case FieldTypeSet, FieldTypeTime:
				if field.Options.Keys {
					datatype = "[]string"
					translations, err := o.preTranslateMatrixSet(ctx, result, uint(i), qtbl, field.Name)
					if err != nil {
						return nil, errors.Wrapf(err, "orch: translating IDs of field %q", v)
					}
					mapper = func(ids []uint64) (interface{}, error) {
						keys := make([]string, len(ids))
						for i, id := range ids {
							keys[i] = translations[id]
						}
						return keys, nil
					}
				} else {
					datatype = "[]uint64"
					mapper = func(ids []uint64) (interface{}, error) {
						if ids == nil {
							ids = []uint64{}
						}
						return ids, nil
					}
				}
			case FieldTypeMutex:
				if field.Options.Keys {
					datatype = "string"
					translations, err := o.preTranslateMatrixSet(ctx, result, uint(i), qtbl, field.Name)
					if err != nil {
						return nil, errors.Wrapf(err, "orch: translating IDs of field %q", v)
					}
					mapper = func(ids []uint64) (interface{}, error) {
						switch len(ids) {
						case 0:
							return nil, nil
						case 1:
							return translations[ids[0]], nil
						default:
							return nil, errors.Errorf("mutex %q has too many values: %v", field.Name, ids)
						}
					}
				} else {
					datatype = "uint64"
					mapper = func(ids []uint64) (_ interface{}, err error) {
						switch len(ids) {
						case 0:
							return nil, nil
						case 1:
							return ids[0], nil
						default:
							return nil, errors.Errorf("mutex %q has too many values: %v", field.Name, ids)
						}
					}
				}
			case FieldTypeInt:
				if fi := field.Options.ForeignIndex; fi != "" {
					if field.Options.Keys {
						datatype = "string"
						ids := make(map[uint64]struct{}, len(result.Columns))
						for _, col := range result.Columns {
							for _, v := range col.Rows[i] {
								ids[v] = struct{}{}
							}
						}
						trans, err := o.trans.TranslateIndexIDSet(ctx, field.Options.ForeignIndex, ids)
						if err != nil {
							return nil, errors.Wrapf(err, "translating foreign IDs from index %q", field.Options.ForeignIndex)
						}
						mapper = func(ids []uint64) (interface{}, error) {
							switch len(ids) {
							case 0:
								return nil, nil
							case 1:
								return trans[ids[0]], nil
							default:
								return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name, ids)
							}
						}
					} else {
						datatype = "uint64"
						mapper = func(ids []uint64) (interface{}, error) {
							switch len(ids) {
							case 0:
								return nil, nil
							case 1:
								return ids[0], nil
							default:
								return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name, ids)
							}
						}
					}
				} else {
					datatype = "int64"
					mapper = func(ids []uint64) (interface{}, error) {
						switch len(ids) {
						case 0:
							return nil, nil
						case 1:
							return int64(ids[0]), nil
						default:
							return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name, ids)
						}
					}
				}
			case FieldTypeDecimal:
				datatype = "decimal"
				scale := field.Options.Scale
				mapper = func(ids []uint64) (_ interface{}, err error) {
					switch len(ids) {
					case 0:
						return nil, nil
					case 1:
						return pql.NewDecimal(int64(ids[0]), scale), nil
					default:
						return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name, ids)
					}
				}
			case FieldTypeTimestamp:
				datatype = "timestamp"
				mapper = func(ids []uint64) (_ interface{}, err error) {
					switch len(ids) {
					case 0:
						return nil, nil
					case 1:
						return time.Unix(0, int64(ids[0])*int64(featurebase.TimeUnitNanos(field.Options.TimeUnit))).UTC(), nil
					default:
						return nil, errors.Errorf("BSI field %q has too many values: %v", field.Name, ids)
					}
				}
			default:
				return nil, errors.Errorf("field type %q not yet supported", typ)
			}
			mappers[i] = mapper
			fields[i] = featurebase.ExtractedTableField{
				Name: v,
				Type: datatype,
			}
		}

		var translateCol func(uint64) (featurebase.KeyOrID, error)
		if idx.Options.Keys {
			translateCol = func(id uint64) (featurebase.KeyOrID, error) {
				return featurebase.KeyOrID{Keyed: true, Key: idSet[id]}, nil
			}
		} else {
			translateCol = func(id uint64) (featurebase.KeyOrID, error) {
				return featurebase.KeyOrID{ID: id}, nil
			}
		}

		cols := make([]featurebase.ExtractedTableColumn, len(result.Columns))
		colData := make([]interface{}, len(cols)*len(result.Fields))
		for i, col := range result.Columns {
			data := colData[i*len(result.Fields) : (i+1)*len(result.Fields) : (i+1)*len(result.Fields)]
			for j, rows := range col.Rows {
				v, err := mappers[j](rows)
				if err != nil {
					return nil, errors.Wrap(err, "translating extracted table value")
				}
				data[j] = v
			}

			colTrans, err := translateCol(col.ColumnID)
			if err != nil {
				return nil, errors.Wrap(err, "translating column ID in extracted table")
			}

			cols[i] = featurebase.ExtractedTableColumn{
				Column: colTrans,
				Rows:   data,
			}
		}

		return featurebase.ExtractedTable{
			Fields:  fields,
			Columns: cols,
		}, nil
	}

	return result, nil
}

// validateQueryContext returns a query-appropriate error if the context is done.
func validateQueryContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		switch err := ctx.Err(); err {
		case context.Canceled:
			return featurebase.ErrQueryCancelled
		case context.DeadlineExceeded:
			return featurebase.ErrQueryTimeout
		default:
			return err
		}
	default:
		return nil
	}
}

type reduceFunc func(ctx context.Context, prev, v interface{}) interface{}

type mapResponse struct {
	node   dax.Address
	shards []uint64

	result interface{}
	err    error
}

func callArgString(call *pql.Call, key string) string {
	value, ok := call.Args[key]
	if !ok {
		return ""
	}
	s, _ := value.(string)
	return s
}

type qualifiedOrchestrator struct {
	*orchestrator
	qdbid dax.QualifiedDatabaseID
}

func newQualifiedOrchestrator(orch *orchestrator, qdbid dax.QualifiedDatabaseID) *qualifiedOrchestrator {
	return &qualifiedOrchestrator{
		orchestrator: orch,
		qdbid:        qdbid,
	}
}

func (o *qualifiedOrchestrator) Execute(ctx context.Context, tableKeyer dax.TableKeyer, q *pql.Query, shards []uint64, opt *featurebase.ExecOptions) (featurebase.QueryResponse, error) {
	resp := featurebase.QueryResponse{}

	var qtbl *dax.QualifiedTable

	switch keyer := tableKeyer.(type) {
	case *dax.Table:
		qtbl = dax.NewQualifiedTable(o.qdbid, keyer)
	case *dax.QualifiedTable:
		qtbl = keyer
	default:
		return resp, errors.Errorf("qualifiedOrchestrator.Execute expects a *dax.Table or *dax.QualifiedTable, but got: %T", tableKeyer)
	}

	return o.orchestrator.Execute(ctx, qtbl, q, shards, opt)
}

// schemaFieldInfo is a function introduced when we replaced
// `schema.FieldInfo()` calls, where schema was a `featurebase.SchemaInfoAPI` to
// `schema.Table().Field()` calls, where schema is a `pilosa.SchemaAPI`. In the
// future, when we're no longer dealing with IndexInfo and FieldInfo, and
// instead use dax.Table and dax.Field, this helper function can be factored
// out.
func (o *orchestrator) schemaFieldInfo(ctx context.Context, tableKeyer dax.TableKeyer, fieldName string) (*featurebase.FieldInfo, error) {
	var tbl *dax.Table
	var err error

	switch v := tableKeyer.(type) {
	case *dax.QualifiedTable:
		tbl = &v.Table
	case *dax.Table:
		tbl = v
	case dax.QualifiedTableID:
		tbl, err = o.schema.TableByID(ctx, v.ID)
		if err != nil {
			return nil, errors.Wrapf(err, "getting table by id: %s", v.ID)
		}
	case dax.StringTableKeyer:
		tbl, err = o.schema.TableByName(ctx, dax.TableName(v))
		if err != nil {
			return nil, errors.Wrapf(err, "getting table by name: %s", v)
		}
	case dax.TableKey:
		qtid := v.QualifiedTableID()
		tbl, err = o.schema.TableByID(ctx, qtid.ID)
		if err != nil {
			return nil, errors.Wrapf(err, "getting table by ID from TableKey: %s", v)
		}
	default:
		return nil, errors.Errorf("unsupport table keyer type in schemaFieldInfo: %T", tableKeyer)
	}

	fld, ok := tbl.Field(dax.FieldName(fieldName))
	if !ok {
		return nil, errors.Errorf("field not found: %s", fieldName)
	}

	return featurebase.FieldToFieldInfo(fld), nil
}

// schemaIndexInfo - see comment on schemaFieldInfo.
func (o *orchestrator) schemaIndexInfo(ctx context.Context, tableKeyer dax.TableKeyer) (*featurebase.IndexInfo, error) {
	var tbl *dax.Table
	var err error

	switch v := tableKeyer.(type) {
	case *dax.QualifiedTable:
		tbl = &v.Table
	case *dax.Table:
		tbl = v
	case dax.QualifiedTableID:
		tbl, err = o.schema.TableByID(ctx, v.ID)
		if err != nil {
			return nil, errors.Wrapf(err, "getting table by id: %s", v.ID)
		}
	case dax.TableKey:
		qtid := v.QualifiedTableID()
		tbl, err = o.schema.TableByID(ctx, qtid.ID)
		if err != nil {
			return nil, errors.Wrapf(err, "getting table by ID from TableKey: %s", v)
		}
	case dax.StringTableKeyer:
		tbl, err = o.schema.TableByName(ctx, dax.TableName(v))
		if err != nil {
			return nil, errors.Wrapf(err, "getting table by name: %s", v)
		}
	default:
		return nil, errors.Errorf("unsupport table keyer type in schemaIndexInfo: %T", tableKeyer)
	}

	return featurebase.TableToIndexInfo(tbl), nil
}
