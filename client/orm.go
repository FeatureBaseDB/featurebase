// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/molecula/featurebase/v3/client/types"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/pkg/errors"
)

const timeFormat = "2006-01-02T15:04"

// Schema contains the index properties
type Schema struct {
	mu      sync.RWMutex
	indexes map[string]*Index
}

func (s *Schema) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("%s", s.indexes)
}

// NewSchema creates a new Schema
func NewSchema() *Schema {
	return &Schema{
		indexes: make(map[string]*Index),
	}
}

// Index returns an index with a name.
func (s *Schema) Index(name string, options ...IndexOption) *Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index, ok := s.indexes[name]; ok {
		return index
	}
	indexOptions := &IndexOptions{}
	indexOptions.addOptions(options...)
	return s.indexWithOptions(name, 0, 0, indexOptions)
}

func (s *Schema) indexWithOptions(name string, createdAt int64, shardWidth uint64, options *IndexOptions) *Index {
	index := NewIndex(name)
	if createdAt != 0 {
		index.createdAt = createdAt
	}

	index.options = options.withDefaults()
	index.shardWidth = shardWidth
	if index.Opts().TrackExistence() {
		index.Field("_exists")
	}
	s.indexes[name] = index
	return index
}

// Indexes return a copy of the indexes in this schema
func (s *Schema) Indexes() map[string]*Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*Index)
	for k, v := range s.indexes {
		result[k] = v.copy()
	}
	return result
}

// HasIndex returns true if the given index is in the schema.
func (s *Schema) HasIndex(indexName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.indexes[indexName]
	return ok
}

func (s *Schema) diff(other *Schema) *Schema {
	result := NewSchema()
	for indexName, index := range s.indexes {
		if otherIndex, ok := other.indexes[indexName]; !ok {
			// if the index doesn't exist in the other schema, simply copy it
			result.indexes[indexName] = index.copy()
		} else {
			// the index exists in the other schema; check the fields
			resultIndex := NewIndex(indexName)
			for fieldName, field := range index.fields {
				if _, ok := otherIndex.fields[fieldName]; !ok {
					// the field doesn't exist in the other schema, copy it
					resultIndex.fields[fieldName] = field.copy()
				}
			}
			// check whether we modified result index
			if len(resultIndex.fields) > 0 {
				// if so, move it to the result
				result.indexes[indexName] = resultIndex
			}
		}
	}
	return result
}

type SerializedQuery interface {
	String() string
	HasWriteKeys() bool
}

type serializedQuery struct {
	query        string
	hasWriteKeys bool
}

func newSerializedQuery(query string, hasWriteKeys bool) serializedQuery {
	return serializedQuery{
		query:        query,
		hasWriteKeys: hasWriteKeys,
	}
}

func (s serializedQuery) String() string {
	return s.query
}

func (s serializedQuery) HasWriteKeys() bool {
	return s.hasWriteKeys
}

// PQLQuery is an interface for PQL queries.
type PQLQuery interface {
	Index() *Index
	Serialize() SerializedQuery
	Error() error
}

// PQLBaseQuery is the base implementation for PQLQuery.
type PQLBaseQuery struct {
	index   *Index
	pql     string
	err     error
	hasKeys bool
}

// NewPQLBaseQuery creates a new PQLQuery with the given PQL and index.
func NewPQLBaseQuery(pql string, index *Index, err error) *PQLBaseQuery {
	var hasKeys bool
	if index != nil {
		hasKeys = index.options.keys
	}
	return &PQLBaseQuery{
		index:   index,
		pql:     pql,
		err:     err,
		hasKeys: hasKeys,
	}
}

// Index returns the index for this query
func (q *PQLBaseQuery) Index() *Index {
	return q.index
}

func (q *PQLBaseQuery) Serialize() SerializedQuery {
	return newSerializedQuery(q.pql, q.hasKeys)
}

// Error returns the error or nil for this query.
func (q PQLBaseQuery) Error() error {
	return q.err
}

// PQLRowQuery is the return type for row queries.
type PQLRowQuery struct {
	index   *Index
	pql     string
	err     error
	hasKeys bool
}

// Index returns the index for this query/
func (q *PQLRowQuery) Index() *Index {
	return q.index
}

func (q *PQLRowQuery) Serialize() SerializedQuery {
	return q.serialize()
}

func (q *PQLRowQuery) serialize() SerializedQuery {
	return newSerializedQuery(q.pql, q.hasKeys)
}

// Error returns the error or nil for this query.
func (q PQLRowQuery) Error() error {
	return q.err
}

// PQLBatchQuery contains a batch of PQL queries.
// Use Index.BatchQuery function to create an instance.
//
// Usage:
//
//	repo, err := NewIndex("repository")
//	stargazer, err := repo.Field("stargazer")
//	query := repo.BatchQuery(
//		stargazer.Row(5),
//		stargazer.Row(15),
//		repo.Union(stargazer.Row(20), stargazer.Row(25)))
type PQLBatchQuery struct {
	index   *Index
	queries []string
	err     error
	hasKeys bool
}

// Index returns the index for this query.
func (q *PQLBatchQuery) Index() *Index {
	return q.index
}

func (q *PQLBatchQuery) Serialize() SerializedQuery {
	query := strings.Join(q.queries, "")
	return newSerializedQuery(query, q.hasKeys)
}

func (q *PQLBatchQuery) Error() error {
	return q.err
}

// Add adds a query to the batch.
func (q *PQLBatchQuery) Add(query PQLQuery) {
	err := query.Error()
	if err != nil {
		q.err = err
	}
	serializedQuery := query.Serialize()
	q.hasKeys = q.hasKeys || serializedQuery.HasWriteKeys()
	q.queries = append(q.queries, serializedQuery.String())
}

// NewPQLRowQuery creates a new PqlRowQuery.
func NewPQLRowQuery(pql string, index *Index, err error) *PQLRowQuery {
	return &PQLRowQuery{
		index:   index,
		pql:     pql,
		err:     err,
		hasKeys: index.options.keys,
	}
}

// IndexOptions contains options to customize Index objects.
type IndexOptions struct {
	keys              bool
	keysSet           bool
	trackExistence    bool
	trackExistenceSet bool
}

func (io *IndexOptions) withDefaults() (updated *IndexOptions) {
	// copy options so the original is not updated
	updated = &IndexOptions{}
	*updated = *io
	if !updated.keysSet {
		updated.keys = false
	}
	if !updated.trackExistenceSet {
		updated.trackExistence = true
	}
	return
}

// Keys return true if this index has keys.
func (io IndexOptions) Keys() bool {
	return io.keys
}

// TrackExistence returns true if existence is tracked for this index.
func (io IndexOptions) TrackExistence() bool {
	return io.trackExistence
}

// String serializes this index to a JSON string.
func (io IndexOptions) String() string {
	mopt := map[string]interface{}{}
	if io.keysSet {
		mopt["keys"] = io.keys
	}
	if io.trackExistenceSet {
		mopt["trackExistence"] = io.trackExistence
	}
	return fmt.Sprintf(`{"options":%s}`, encodeMap(mopt))
}

func (io *IndexOptions) addOptions(options ...IndexOption) {
	for _, option := range options {
		if option == nil {
			continue
		}
		option(io)
	}
}

// IndexOption is used to pass an option to Index function.
type IndexOption func(options *IndexOptions)

// OptIndexKeys sets whether index uses string keys.
func OptIndexKeys(keys bool) IndexOption {
	return func(options *IndexOptions) {
		options.keys = keys
		options.keysSet = true
	}
}

// OptIndexTrackExistence enables keeping track of existence of columns.
func OptIndexTrackExistence(trackExistence bool) IndexOption {
	return func(options *IndexOptions) {
		options.trackExistence = trackExistence
		options.trackExistenceSet = true
	}
}

// OptionsOptions is used to pass an option to Option call.
type OptionsOptions struct {
	shards []uint64
}

func (oo OptionsOptions) marshal() string {
	if oo.shards != nil {
		shardsStr := make([]string, len(oo.shards))
		for i, shard := range oo.shards {
			shardsStr[i] = strconv.FormatUint(shard, 10)
		}
		return fmt.Sprintf("shards=[%s]", strings.Join(shardsStr, ","))
	}

	return ""
}

// OptionsOption is an option for Index.Options call.
type OptionsOption func(options *OptionsOptions)

// OptOptionsShards run the query using only the data from the given shards.
// By default, the entire data set (i.e. data from all shards) is used.
func OptOptionsShards(shards ...uint64) OptionsOption {
	return func(options *OptionsOptions) {
		options.shards = shards
	}
}

// Index is a Pilosa index. The purpose of the Index is to represent a data namespace.
// You cannot perform cross-index queries.
type Index struct {
	mu         sync.RWMutex
	name       string
	createdAt  int64
	options    *IndexOptions
	fields     map[string]*Field
	shardWidth uint64
}

func (idx *Index) String() string {
	return fmt.Sprintf(`{name: "%s", options: "%s", fields: %s, shardWidth: %d}`, idx.name, idx.options, idx.fields, idx.shardWidth)
}

// NewIndex creates an index with a name.
func NewIndex(name string) *Index {
	options := &IndexOptions{}
	return &Index{
		name:    name,
		options: options.withDefaults(),
		fields:  map[string]*Field{},
	}
}

func (idx *Index) ShardWidth() uint64 {
	return idx.shardWidth
}

// Fields return a copy of the fields in this index
func (idx *Index) Fields() map[string]*Field {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	result := make(map[string]*Field)
	for k, v := range idx.fields {
		result[k] = v.copy()
	}
	return result
}

// HasFields returns true if the given field exists in the index.
func (idx *Index) HasField(fieldName string) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	_, ok := idx.fields[fieldName]
	return ok
}

func (idx *Index) copy() *Index {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	fields := make(map[string]*Field)
	for name, f := range idx.fields {
		fields[name] = f.copy()
	}
	index := &Index{
		name:       idx.name,
		createdAt:  idx.createdAt,
		options:    &IndexOptions{},
		fields:     fields,
		shardWidth: idx.shardWidth,
	}
	*index.options = *idx.options
	return index
}

// Name returns the name of this index.
func (idx *Index) Name() string {
	return idx.name
}

func (idx *Index) CreatedAt() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.createdAt
}

// Opts returns the options of this index.
func (idx *Index) Opts() IndexOptions {
	return *idx.options
}

// Field creates a Field struct with the specified name and defaults.
func (idx *Index) Field(name string, options ...FieldOption) *Field {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if field, ok := idx.fields[name]; ok {
		return field
	}
	fieldOptions := &FieldOptions{}
	fieldOptions = fieldOptions.withDefaults()
	fieldOptions.addOptions(options...)
	return idx.fieldWithOptions(name, 0, fieldOptions)
}

func (idx *Index) fieldWithOptions(name string, createdAt int64, fieldOptions *FieldOptions) *Field {
	field := newField(name, idx)
	if createdAt != 0 {
		field.createdAt = createdAt
	}
	fieldOptions = fieldOptions.withDefaults()
	field.options = fieldOptions
	idx.fields[name] = field
	return field
}

// BatchQuery creates a batch query with the given queries.
func (idx *Index) BatchQuery(queries ...PQLQuery) *PQLBatchQuery {
	stringQueries := make([]string, 0, len(queries))
	hasKeys := false
	for _, query := range queries {
		serializedQuery := query.Serialize()
		hasKeys = hasKeys || serializedQuery.HasWriteKeys()
		stringQueries = append(stringQueries, serializedQuery.String())
	}
	return &PQLBatchQuery{
		index:   idx,
		queries: stringQueries,
		hasKeys: hasKeys,
	}
}

// RawQuery creates a query with the given string.
// Note that the query is not validated before sending to the server.
func (idx *Index) RawQuery(query string) *PQLBaseQuery {
	q := NewPQLBaseQuery(query, idx, nil)
	// NOTE: raw queries always assumed to have keys set
	q.hasKeys = true
	return q
}

// Union creates a Union query.
// Union performs a logical OR on the results of each ROW_CALL query passed to it.
func (idx *Index) Union(rows ...*PQLRowQuery) *PQLRowQuery {
	return idx.rowOperation("Union", rows...)
}

// Intersect creates an Intersect query.
// Intersect performs a logical AND on the results of each ROW_CALL query passed to it.
func (idx *Index) Intersect(rows ...*PQLRowQuery) *PQLRowQuery {
	if len(rows) < 1 {
		return NewPQLRowQuery("", idx, errors.New("Intersect operation requires at least 1 row"))
	}
	return idx.rowOperation("Intersect", rows...)
}

// Difference creates an Intersect query.
// Difference returns all of the columns from the first ROW_CALL argument passed to it, without the columns from each subsequent ROW_CALL.
func (idx *Index) Difference(rows ...*PQLRowQuery) *PQLRowQuery {
	if len(rows) < 1 {
		return NewPQLRowQuery("", idx, errors.New("Difference operation requires at least 1 row"))
	}
	return idx.rowOperation("Difference", rows...)
}

// Xor creates an Xor query.
func (idx *Index) Xor(rows ...*PQLRowQuery) *PQLRowQuery {
	if len(rows) < 2 {
		return NewPQLRowQuery("", idx, errors.New("Xor operation requires at least 2 rows"))
	}
	return idx.rowOperation("Xor", rows...)
}

// Not creates a Not query.
func (idx *Index) Not(row *PQLRowQuery) *PQLRowQuery {
	return NewPQLRowQuery(fmt.Sprintf("Not(%s)", row.serialize()), idx, row.Error())
}

// Count creates a Count query.
// Returns the number of set columns in the ROW_CALL passed in.
func (idx *Index) Count(row *PQLRowQuery) *PQLBaseQuery {
	serializedQuery := row.serialize()
	q := NewPQLBaseQuery(fmt.Sprintf("Count(%s)", serializedQuery.String()), idx, nil)
	q.hasKeys = q.hasKeys || serializedQuery.HasWriteKeys()
	return q
}

// All creates an All query.
// Returns the set columns with existence true.
func (idx *Index) All() *PQLRowQuery {
	q := NewPQLRowQuery("All()", idx, nil)
	return q
}

// TODO: impelement AllLimit(limit, offset uint64) *PQLRowQuery

// Options creates an Options query.
func (idx *Index) Options(row *PQLRowQuery, opts ...OptionsOption) *PQLBaseQuery {
	oo := &OptionsOptions{}
	for _, opt := range opts {
		opt(oo)
	}
	text := fmt.Sprintf("Options(%s,%s)", row.serialize(), oo.marshal())
	return NewPQLBaseQuery(text, idx, nil)
}

type groupByBuilder struct {
	rows      []*PQLRowsQuery
	limit     int64
	filter    *PQLRowQuery
	aggregate *PQLBaseQuery
	having    *PQLBaseQuery
}

// GroupByBuilderOption is a functional option type for index.GroupBy
type GroupByBuilderOption func(g *groupByBuilder) error

// OptGroupByBuilderRows is a functional option on groupByBuilder
// used to set the rows.
func OptGroupByBuilderRows(rows ...*PQLRowsQuery) GroupByBuilderOption {
	return func(g *groupByBuilder) error {
		g.rows = rows
		return nil
	}
}

// OptGroupByBuilderLimit is a functional option on groupByBuilder
// used to set the limit.
func OptGroupByBuilderLimit(l int64) GroupByBuilderOption {
	return func(g *groupByBuilder) error {
		g.limit = l
		return nil
	}
}

// OptGroupByBuilderFilter is a functional option on groupByBuilder
// used to set the filter.
func OptGroupByBuilderFilter(q *PQLRowQuery) GroupByBuilderOption {
	return func(g *groupByBuilder) error {
		g.filter = q
		return nil
	}
}

// OptGroupByBuilderAggregate is a functional option on groupByBuilder
// used to set the aggregate.
func OptGroupByBuilderAggregate(agg *PQLBaseQuery) GroupByBuilderOption {
	return func(g *groupByBuilder) error {
		g.aggregate = agg
		return nil
	}
}

// OptGroupByBuilderHaving is a functional option on groupByBuilder
// used to set the having clause.
func OptGroupByBuilderHaving(having *PQLBaseQuery) GroupByBuilderOption {
	return func(g *groupByBuilder) error {
		g.having = having
		return nil
	}
}

// GroupByBase creates a GroupBy query with the given functional options.
func (idx *Index) GroupByBase(opts ...GroupByBuilderOption) *PQLBaseQuery {
	bldr := &groupByBuilder{}
	for _, opt := range opts {
		err := opt(bldr)
		if err != nil {
			return NewPQLBaseQuery("", idx, errors.Wrap(err, "applying option"))
		}
	}

	if len(bldr.rows) < 1 {
		return NewPQLBaseQuery("", idx, errors.New("there should be at least one rows query"))
	}
	if bldr.limit < 0 {
		return NewPQLBaseQuery("", idx, errors.New("limit must be non-negative"))
	}

	// rows
	text := fmt.Sprintf("GroupBy(%s", strings.Join(serializeGroupBy(bldr.rows...), ","))

	// limit
	if bldr.limit > 0 {
		text += fmt.Sprintf(",limit=%d", bldr.limit)
	}

	// filter
	if bldr.filter != nil {
		filterText := bldr.filter.serialize().String()
		text += fmt.Sprintf(",filter=%s", filterText)
	}

	// aggregate
	if bldr.aggregate != nil {
		aggregateText := bldr.aggregate.Serialize().String()
		text += fmt.Sprintf(",aggregate=%s", aggregateText)
	}

	// having
	if bldr.having != nil {
		havingText := bldr.having.Serialize().String()
		text += fmt.Sprintf(",having=%s", havingText)
	}

	text += ")"
	return NewPQLBaseQuery(text, idx, nil)
}

// GroupBy creates a GroupBy query with the given Rows queries
func (idx *Index) GroupBy(rowsQueries ...*PQLRowsQuery) *PQLBaseQuery {
	if len(rowsQueries) < 1 {
		return NewPQLBaseQuery("", idx, errors.New("there should be at least one rows query"))
	}
	text := fmt.Sprintf("GroupBy(%s)", strings.Join(serializeGroupBy(rowsQueries...), ","))
	return NewPQLBaseQuery(text, idx, nil)
}

// GroupByLimit creates a GroupBy query with the given limit and Rows queries
func (idx *Index) GroupByLimit(limit int64, rowsQueries ...*PQLRowsQuery) *PQLBaseQuery {
	if len(rowsQueries) < 1 {
		return NewPQLBaseQuery("", idx, errors.New("there should be at least one rows query"))
	}
	if limit < 0 {
		return NewPQLBaseQuery("", idx, errors.New("limit must be non-negative"))
	}
	text := fmt.Sprintf("GroupBy(%s,limit=%d)", strings.Join(serializeGroupBy(rowsQueries...), ","), limit)
	return NewPQLBaseQuery(text, idx, nil)
}

// GroupByFilter creates a GroupBy query with the given filter and Rows queries
func (idx *Index) GroupByFilter(filterQuery *PQLRowQuery, rowsQueries ...*PQLRowsQuery) *PQLBaseQuery {
	if len(rowsQueries) < 1 {
		return NewPQLBaseQuery("", idx, errors.New("there should be at least one rows query"))
	}
	filterText := filterQuery.serialize().String()
	text := fmt.Sprintf("GroupBy(%s,filter=%s)", strings.Join(serializeGroupBy(rowsQueries...), ","), filterText)
	return NewPQLBaseQuery(text, idx, nil)
}

// GroupByLimitFilter creates a GroupBy query with the given filter and Rows queries
func (idx *Index) GroupByLimitFilter(limit int64, filterQuery *PQLRowQuery, rowsQueries ...*PQLRowsQuery) *PQLBaseQuery {
	if len(rowsQueries) < 1 {
		return NewPQLBaseQuery("", idx, errors.New("there should be at least one rows query"))
	}
	if limit < 0 {
		return NewPQLBaseQuery("", idx, errors.New("limit must be non-negative"))
	}
	filterText := filterQuery.serialize().String()
	text := fmt.Sprintf("GroupBy(%s,limit=%d,filter=%s)", strings.Join(serializeGroupBy(rowsQueries...), ","), limit, filterText)
	return NewPQLBaseQuery(text, idx, nil)
}

func (idx *Index) rowOperation(name string, rows ...*PQLRowQuery) *PQLRowQuery {
	var err error
	args := make([]string, 0, len(rows))
	for _, row := range rows {
		if err = row.Error(); err != nil {
			return NewPQLRowQuery("", idx, err)
		}
		args = append(args, row.serialize().String())
	}
	query := NewPQLRowQuery(fmt.Sprintf("%s(%s)", name, strings.Join(args, ",")), idx, nil)
	return query
}

func serializeGroupBy(rowsQueries ...*PQLRowsQuery) []string {
	qs := make([]string, 0, len(rowsQueries))
	for _, qry := range rowsQueries {
		qs = append(qs, qry.serialize().String())
	}
	return qs
}

// FieldInfo represents schema information for a field.
type FieldInfo struct {
	Name string `json:"name"`
}

// FieldOptions contains options to customize Field objects and field queries.
type FieldOptions struct {
	fieldType      FieldType
	timeQuantum    types.TimeQuantum
	ttl            time.Duration
	cacheType      CacheType
	cacheSize      int
	min            pql.Decimal
	max            pql.Decimal
	scale          int64
	keys           bool
	noStandardView bool
	foreignIndex   string
	timeUnit       string
	base           int64
	epoch          time.Time
}

// Type returns the type of the field. Currently "set", "int", or "time".
func (fo FieldOptions) Type() FieldType {
	return fo.fieldType
}

// Base returns the base of the field.
func (fo FieldOptions) Base() int64 {
	return fo.base
}

// TimeQuantum returns the configured time quantum for a time field. Empty
// string otherwise.
func (fo FieldOptions) TimeQuantum() types.TimeQuantum {
	return fo.timeQuantum
}

// TTL returns the configured ttl for a time field.
func (fo FieldOptions) TTL() time.Duration {
	return fo.ttl
}

// CacheType returns the configured cache type for a "set" field. Empty string
// otherwise.
func (fo FieldOptions) CacheType() CacheType {
	return fo.cacheType
}

// CacheSize returns the cache size for a set field. Zero otherwise.
func (fo FieldOptions) CacheSize() int {
	return fo.cacheSize
}

// Min returns the minimum accepted value for an integer field. Zero otherwise.
func (fo FieldOptions) Min() pql.Decimal {
	return fo.min
}

// Max returns the maximum accepted value for an integer field. Zero otherwise.
func (fo FieldOptions) Max() pql.Decimal {
	return fo.max
}

// Scale returns the scale for a decimal field.
func (fo FieldOptions) Scale() int64 {
	return fo.scale
}

// Keys returns whether this field uses keys instead of IDs
func (fo FieldOptions) Keys() bool {
	return fo.keys
}

func (fo FieldOptions) ForeignIndex() string {
	return fo.foreignIndex
}

func (fo FieldOptions) TimeUnit() string {
	return fo.timeUnit
}

// NoStandardView suppresses creating the standard view for supported field types (currently, time)
func (fo FieldOptions) NoStandardView() bool {
	return fo.noStandardView
}

func (fo *FieldOptions) withDefaults() (updated *FieldOptions) {
	// copy options so the original is not updated
	updated = &FieldOptions{}
	*updated = *fo
	if updated.fieldType == "" {
		updated.fieldType = FieldTypeSet
	}
	return
}

func (fo FieldOptions) String() string {
	mopt := map[string]interface{}{}

	switch fo.fieldType {
	case FieldTypeSet, FieldTypeMutex:
		if fo.cacheType != CacheTypeDefault {
			mopt["cacheType"] = string(fo.cacheType)
		}
		if fo.cacheSize > 0 {
			mopt["cacheSize"] = fo.cacheSize
		}
	case FieldTypeInt:
		mopt["min"] = fo.min
		mopt["max"] = fo.max
	case FieldTypeDecimal:
		mopt["min"] = fo.min
		mopt["max"] = fo.max
		mopt["scale"] = fo.scale
	case FieldTypeTime:
		mopt["timeQuantum"] = string(fo.timeQuantum)
		mopt["noStandardView"] = fo.noStandardView
		mopt["ttl"] = fo.ttl.String()
	case FieldTypeTimestamp:
		mopt["min"] = fo.min
		mopt["max"] = fo.max
		mopt["timeUnit"] = fo.timeUnit
		mopt["epoch"] = fo.epoch
	}

	if fo.fieldType != FieldTypeDefault {
		mopt["type"] = string(fo.fieldType)
	}
	if fo.keys {
		mopt["keys"] = fo.keys
	}
	if fo.foreignIndex != "" {
		mopt["foreignIndex"] = fo.foreignIndex
	}
	return fmt.Sprintf(`{"options":%s}`, encodeMap(mopt))
}

func (fo *FieldOptions) addOptions(options ...FieldOption) {
	for _, option := range options {
		if option == nil {
			continue
		}
		option(fo)
	}
}

// MinTimestamp returns the minimum value for a timestamp field.
func (o FieldOptions) MinTimestamp() time.Time {
	return time.Unix(0, o.min.ToInt64(0)*int64(types.TimeUnitNano(o.TimeUnit())))
}

// MaxTimestamp returns the maxnimum value for a timestamp field.
func (o FieldOptions) MaxTimestamp() time.Time {
	return time.Unix(0, o.max.ToInt64(0)*int64(types.TimeUnitNano(o.TimeUnit())))
}

// FieldOption is used to pass an option to index.Field function.
type FieldOption func(options *FieldOptions)

// OptFieldTypeSet adds a set field.
// Specify CacheTypeDefault for the default cache type.
// Specify CacheSizeDefault for the default cache size.
func OptFieldTypeSet(cacheType CacheType, cacheSize int) FieldOption {
	return func(options *FieldOptions) {
		options.fieldType = FieldTypeSet
		options.cacheType = cacheType
		options.cacheSize = cacheSize
	}
}

// OptFieldTypeInt adds an integer field.
// No arguments: min = min_int, max = max_int
// 1 argument: min = limit[0], max = max_int
// 2 or more arguments: min = limit[0], max = limit[1]
func OptFieldTypeInt(limits ...int64) FieldOption {
	min := pql.NewDecimal(math.MinInt64, 0)
	max := pql.NewDecimal(math.MaxInt64, 0)

	if len(limits) > 2 {
		panic("error: OptFieldTypeInt accepts at most 2 arguments")
	}
	if len(limits) > 0 {
		min = pql.NewDecimal(limits[0], 0)
	}
	if len(limits) > 1 {
		max = pql.NewDecimal(limits[1], 0)
	}

	return func(options *FieldOptions) {
		options.fieldType = FieldTypeInt
		options.min = min
		options.max = max
	}
}

// OptFieldTypeTime adds a time field.
func OptFieldTypeTime(quantum types.TimeQuantum, opts ...bool) FieldOption {
	return func(options *FieldOptions) {
		options.fieldType = FieldTypeTime
		options.timeQuantum = quantum
		if len(opts) > 0 && opts[0] {
			options.noStandardView = true
		}
	}
}

func OptFieldTTL(dur time.Duration) FieldOption {
	return func(options *FieldOptions) {
		options.ttl = dur
	}
}

// Timestamp field range.
var (
	DefaultEpoch = time.Unix(0, 0).UTC() // 1970-01-01T00:00:00Z

	MinTimestamp = time.Unix(-1<<32, 0).UTC() // 1833-11-24T17:31:44Z
	MaxTimestamp = time.Unix(1<<32, 0).UTC()  // 2106-02-07T06:28:16Z
)

// TimeUnitNanos returns the number of nanoseconds in unit.
func TimeUnitNanos(unit string) int64 {
	switch unit {
	case types.TimeUnitSeconds:
		return int64(time.Second)
	case types.TimeUnitMilliseconds:
		return int64(time.Millisecond)
	case types.TimeUnitMicroseconds:
		return int64(time.Microsecond)
	default:
		return int64(time.Nanosecond)
	}
}

func OptFieldTypeTimestamp(epoch time.Time, timeUnit string) FieldOption {
	return func(fo *FieldOptions) {
		epochValue := epoch.UnixNano() / TimeUnitNanos(timeUnit)
		fo.fieldType = FieldTypeTimestamp
		fo.timeUnit = timeUnit
		fo.min = pql.NewDecimal(MinTimestamp.UnixNano()/TimeUnitNanos(timeUnit), 0)
		fo.max = pql.NewDecimal(MaxTimestamp.UnixNano()/TimeUnitNanos(timeUnit), 0)
		fo.base = epochValue
		fo.epoch = epoch
	}
}

// OptFieldTypeMutex adds a mutex field.
func OptFieldTypeMutex(cacheType CacheType, cacheSize int) FieldOption {
	return func(options *FieldOptions) {
		options.fieldType = FieldTypeMutex
		options.cacheType = cacheType
		options.cacheSize = cacheSize
	}
}

// OptFieldTypeBool adds a bool field.
func OptFieldTypeBool() FieldOption {
	return func(options *FieldOptions) {
		options.fieldType = FieldTypeBool
	}
}

func OptFieldTypeDecimal(scale int64, minmax ...pql.Decimal) FieldOption {
	min, max := pql.MinMax(scale)
	if len(minmax) > 2 {
		panic("error: OptFieldTypeDecimal accepts at most 2 arguments")
	}
	if len(minmax) > 0 {
		min = minmax[0]
	}
	if len(minmax) > 1 {
		max = minmax[1]
	}
	return func(options *FieldOptions) {
		options.fieldType = FieldTypeDecimal
		options.scale = scale
		options.min = min
		options.max = max
	}
}

// OptFieldKeys sets whether field uses string keys.
func OptFieldKeys(keys bool) FieldOption {
	return func(options *FieldOptions) {
		options.keys = keys
	}
}

func OptFieldForeignIndex(index string) FieldOption {
	return func(options *FieldOptions) {
		options.foreignIndex = index
	}
}

// Field structs are used to segment and define different functional characteristics within your entire index.
// You can think of a Field as a table-like data partition within your Index.
type Field struct {
	name      string
	createdAt int64
	index     *Index
	options   *FieldOptions
}

func (f *Field) String() string {
	return fmt.Sprintf(`{name: "%s", index: "%s", options: "%s"}`, f.name, f.index.name, f.options)
}

func newField(name string, index *Index) *Field {
	return &Field{
		name:    name,
		index:   index,
		options: &FieldOptions{},
	}
}

// Name returns the name of the field
func (f *Field) Name() string {
	return f.name
}

func (f *Field) CreatedAt() int64 {
	return f.createdAt
}

// Opts returns the options of the field
func (f *Field) Opts() FieldOptions {
	return *f.options
}

func (f *Field) copy() *Field {
	field := newField(f.name, f.index)
	field.createdAt = f.createdAt
	*field.options = *f.options
	return field
}

// Row creates a Row query.
// Row retrieves the indices of all the set columns in a row.
func (f *Field) Row(rowIDOrKey interface{}) *PQLRowQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLRowQuery("", f.index, err)
	}
	text := fmt.Sprintf("Row(%s=%s)", f.name, rowStr)
	q := NewPQLRowQuery(text, f.index, nil)
	return q
}

// Set creates a Set query.
// Set, assigns a value of 1 to a bit in the binary matrix, thus associating the given row in the given field with the given column.
func (f *Field) Set(rowIDOrKey, colIDOrKey interface{}) *PQLBaseQuery {
	rowStr, colStr, err := formatRowColIDKey(rowIDOrKey, colIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	text := fmt.Sprintf("Set(%s,%s=%s)", colStr, f.name, rowStr)
	q := NewPQLBaseQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// SetTimestamp creates a Set query with timestamp.
// Set, assigns a value of 1 to a column in the binary matrix,
// thus associating the given row in the given field with the given column.
func (f *Field) SetTimestamp(rowIDOrKey, colIDOrKey interface{}, timestamp time.Time) *PQLBaseQuery {
	rowStr, colStr, err := formatRowColIDKey(rowIDOrKey, colIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	text := fmt.Sprintf("Set(%s,%s=%s,%s)", colStr, f.name, rowStr, timestamp.Format(timeFormat))
	q := NewPQLBaseQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// Clear creates a Clear query.
// Clear, assigns a value of 0 to a bit in the binary matrix, thus disassociating the given row in the given field from the given column.
func (f *Field) Clear(rowIDOrKey, colIDOrKey interface{}) *PQLBaseQuery {
	rowStr, colStr, err := formatRowColIDKey(rowIDOrKey, colIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	text := fmt.Sprintf("Clear(%s,%s=%s)", colStr, f.name, rowStr)
	q := NewPQLBaseQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// ClearRow creates a ClearRow query.
// ClearRow sets all bits to 0 in a given row of the binary matrix, thus disassociating the given row in the given field from all columns.
func (f *Field) ClearRow(rowIDOrKey interface{}) *PQLBaseQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	text := fmt.Sprintf("ClearRow(%s=%s)", f.name, rowStr)
	q := NewPQLBaseQuery(text, f.index, nil)
	return q
}

// TopN creates a TopN query with the given item count.
// Returns the id and count of the top n rows (by count of columns) in the field.
func (f *Field) TopN(n uint64) *PQLRowQuery {
	q := NewPQLRowQuery(fmt.Sprintf("TopN(%s,n=%d)", f.name, n), f.index, nil)
	return q
}

// RowTopN creates a TopN query with the given item count and row.
// This variant supports customizing the row query.
func (f *Field) RowTopN(n uint64, row *PQLRowQuery) *PQLRowQuery {
	q := NewPQLRowQuery(fmt.Sprintf("TopN(%s,%s,n=%d)",
		f.name, row.serialize(), n), f.index, nil)
	return q
}

// Range creates a Range query.
// Similar to Row, but only returns columns which were set with timestamps between the given start and end timestamps.
// *Deprecated at Pilosa 1.3*
func (f *Field) Range(rowIDOrKey interface{}, start time.Time, end time.Time) *PQLRowQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLRowQuery("", f.index, err)
	}
	text := fmt.Sprintf("Range(%s=%s,%s,%s)", f.name, rowStr, start.Format(timeFormat), end.Format(timeFormat))
	q := NewPQLRowQuery(text, f.index, nil)
	return q
}

// RowRange creates a Row query with timestamps.
// Similar to Row, but only returns columns which were set with timestamps between the given start and end timestamps.
// *Introduced at Pilosa 1.3*
func (f *Field) RowRange(rowIDOrKey interface{}, start time.Time, end time.Time) *PQLRowQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLRowQuery("", f.index, err)
	}
	text := fmt.Sprintf("Row(%s=%s,from='%s',to='%s')", f.name, rowStr, start.Format(timeFormat), end.Format(timeFormat))
	q := NewPQLRowQuery(text, f.index, nil)
	return q
}

// Store creates a Store call.
// Store writes the result of the row query to the specified row. If the row already exists, it will be replaced. The destination field must be of field type set.
func (f *Field) Store(row *PQLRowQuery, rowIDOrKey interface{}) *PQLBaseQuery {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	return NewPQLBaseQuery(fmt.Sprintf("Store(%s,%s=%s)", row.serialize().String(), f.name, rowStr), f.index, nil)
}

func formatIDKey(idKey interface{}) (string, error) {
	switch v := idKey.(type) {
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case string:
		v = strings.ReplaceAll(v, `\`, `\\`)
		return fmt.Sprintf(`'%s'`, strings.ReplaceAll(v, `'`, `\'`)), nil
	default:
		return "", errors.Errorf("id/key is not a string or integer type: %#v", idKey)
	}
}

func formatIDKeyBool(idKeyBool interface{}) (string, error) {
	if b, ok := idKeyBool.(bool); ok {
		return strconv.FormatBool(b), nil
	}
	if flt, ok := idKeyBool.(float64); ok {
		return fmt.Sprintf("%f", flt), nil
	}
	return formatIDKey(idKeyBool)
}

func formatRowColIDKey(rowIDOrKey, colIDOrKey interface{}) (string, string, error) {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return "", "", errors.Wrap(err, "formatting row")
	}
	colStr, err := formatIDKey(colIDOrKey)
	if err != nil {
		return "", "", errors.Wrap(err, "formatting column")
	}
	return rowStr, colStr, err
}

// FieldType is the type of a field.
// See: https://www.pilosa.com/docs/latest/data-model/#field-type
type FieldType string

const (
	// FieldTypeDefault is the default field type.
	FieldTypeDefault FieldType = ""
	// FieldTypeSet is the set field type.
	// See: https://www.pilosa.com/docs/latest/data-model/#set
	FieldTypeSet FieldType = "set"
	// FieldTypeInt is the int field type.
	// See: https://www.pilosa.com/docs/latest/data-model/#int
	FieldTypeInt FieldType = "int"
	// FieldTypeTime is the time field type.
	// See: https://www.pilosa.com/docs/latest/data-model/#time
	FieldTypeTime FieldType = "time"
	// FieldTypeMutex is the mutex field type.
	// See: https://www.pilosa.com/docs/latest/data-model/#mutex
	FieldTypeMutex FieldType = "mutex"
	// FieldTypeBool is the boolean field type.
	// See: https://www.pilosa.com/docs/latest/data-model/#boolean
	FieldTypeBool FieldType = "bool"
	// FieldTypeDecimal can store floating point numbers as integers
	// with a scale factor. This field type is only available in
	// Molecula's Pilosa with enterprise extensions.
	FieldTypeDecimal   FieldType = "decimal"
	FieldTypeTimestamp FieldType = "timestamp"
)

// CacheType represents cache type for a field
type CacheType string

// CacheType constants
const (
	CacheTypeDefault CacheType = ""
	CacheTypeLRU     CacheType = "lru"
	CacheTypeRanked  CacheType = "ranked"
	CacheTypeNone    CacheType = "none"
)

// CacheSizeDefault is the default cache size
const CacheSizeDefault = 0

// Options returns the options set for the field. Which fields of the
// FieldOptions struct are actually being used depends on the field's type.
// *DEPRECATED*
func (f *Field) Options() *FieldOptions {
	return f.options
}

type IntOrFloat interface{}

type intOrFloatVal struct {
	IntOrFloat
}

func (i intOrFloatVal) String() string {
	switch i.IntOrFloat.(type) {
	case float64:
		// In order to test expected values, we set the precision
		// to 8. TODO: It's likely we'll need to address this
		// at some point.
		return fmt.Sprintf("%.8f", i.IntOrFloat)
	default:
		return fmt.Sprintf("%d", i.IntOrFloat)
	}
}

// LT creates a less than query.
func (f *Field) LT(n IntOrFloat) *PQLRowQuery {
	return f.binaryOperation("<", n)
}

// LTE creates a less than or equal query.
func (f *Field) LTE(n IntOrFloat) *PQLRowQuery {
	return f.binaryOperation("<=", n)
}

// GT creates a greater than query.
func (f *Field) GT(n IntOrFloat) *PQLRowQuery {
	return f.binaryOperation(">", n)
}

// GTE creates a greater than or equal query.
func (f *Field) GTE(n IntOrFloat) *PQLRowQuery {
	return f.binaryOperation(">=", n)
}

// Equals creates an equals query.
func (f *Field) Equals(n IntOrFloat) *PQLRowQuery {
	return f.binaryOperation("==", n)
}

// NotEquals creates a not equals query.
func (f *Field) NotEquals(n IntOrFloat) *PQLRowQuery {
	return f.binaryOperation("!=", n)
}

// NotNull creates a not equal to null query.
func (f *Field) NotNull() *PQLRowQuery {
	text := fmt.Sprintf("Row(%s != null)", f.name)
	q := NewPQLRowQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// Between creates a between query.
func (f *Field) Between(a IntOrFloat, b IntOrFloat) *PQLRowQuery {
	text := fmt.Sprintf("Row(%s >< [%s,%s])", f.name, intOrFloatVal{a}, intOrFloatVal{b})
	q := NewPQLRowQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

// Sum creates a sum query.
func (f *Field) Sum(row *PQLRowQuery) *PQLBaseQuery {
	return f.valQuery("Sum", row)
}

// Min creates a min query.
func (f *Field) Min(row *PQLRowQuery) *PQLBaseQuery {
	return f.valQuery("Min", row)
}

// Max creates a max query.
func (f *Field) Max(row *PQLRowQuery) *PQLBaseQuery {
	return f.valQuery("Max", row)
}

// MinRow creates a min row query.
func (f *Field) MinRow() *PQLBaseQuery {
	q := fmt.Sprintf("MinRow(field='%s')", f.name)
	return NewPQLBaseQuery(q, f.index, nil)
}

// MaxRow creates a max row query.
func (f *Field) MaxRow() *PQLBaseQuery {
	q := fmt.Sprintf("MaxRow(field='%s')", f.name)
	return NewPQLBaseQuery(q, f.index, nil)
}

// SetIntValue creates a Set query.
func (f *Field) SetIntValue(colIDOrKey interface{}, value int) *PQLBaseQuery {
	colStr, err := formatIDKey(colIDOrKey)
	if err != nil {
		return NewPQLBaseQuery("", f.index, err)
	}
	q := fmt.Sprintf("Set(%s, %s=%d)", colStr, f.name, value)
	return NewPQLBaseQuery(q, f.index, nil)
}

// PQLRowsQuery is the return type for Rows calls.
type PQLRowsQuery struct {
	index *Index
	pql   string
	err   error
}

// NewPQLRowsQuery creates a new PQLRowsQuery.
func NewPQLRowsQuery(pql string, index *Index, err error) *PQLRowsQuery {
	return &PQLRowsQuery{
		index: index,
		pql:   pql,
		err:   err,
	}
}

// Index returns the index for this query/
func (q *PQLRowsQuery) Index() *Index {
	return q.index
}

func (q *PQLRowsQuery) Serialize() SerializedQuery {
	return q.serialize()
}

func (q *PQLRowsQuery) serialize() SerializedQuery {
	return newSerializedQuery(q.pql, false)
}

// Error returns the error or nil for this query.
func (q PQLRowsQuery) Error() error {
	return q.err
}

// Union returns the union of all matched rows.
func (q *PQLRowsQuery) Union() *PQLRowQuery {
	return NewPQLRowQuery(fmt.Sprintf("UnionRows(%s)", q.serialize().String()), q.index, nil)
}

// Rows creates a Rows query with defaults
func (f *Field) Rows() *PQLRowsQuery {
	text := fmt.Sprintf("Rows(field='%s')", f.name)
	return NewPQLRowsQuery(text, f.index, nil)
}

// Like creates a Rows query filtered by a pattern.
// An underscore ('_') can be used as a placeholder for a single UTF-8 codepoint or a percent sign ('%') can be used as a placeholder for 0 or more codepoints.
// All other codepoints in the pattern are matched exactly.
func (f *Field) Like(pattern string) *PQLRowsQuery {
	pattern = strings.ReplaceAll(pattern, `\`, `\\`)
	pattern = strings.ReplaceAll(pattern, `'`, `\'`)
	text := fmt.Sprintf("Rows(field='%s',like='%s')", f.name, pattern)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsPrevious creates a Rows query with the given previous row ID/key
func (f *Field) RowsPrevious(rowIDOrKey interface{}) *PQLRowsQuery {
	idKey, err := formatIDKey(rowIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	text := fmt.Sprintf("Rows(field='%s',previous=%s)", f.name, idKey)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsLimit creates a Rows query with the given limit
func (f *Field) RowsLimit(limit int64) *PQLRowsQuery {
	if limit < 0 {
		return NewPQLRowsQuery("", f.index, errors.New("rows limit must be non-negative"))
	}
	text := fmt.Sprintf("Rows(field='%s',limit=%d)", f.name, limit)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsColumn creates a Rows query with the given column ID/key
func (f *Field) RowsColumn(columnIDOrKey interface{}) *PQLRowsQuery {
	idKey, err := formatIDKey(columnIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	text := fmt.Sprintf("Rows(field='%s',column=%s)", f.name, idKey)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsPreviousLimit creates a Rows query with the given previous row ID/key and limit
func (f *Field) RowsPreviousLimit(rowIDOrKey interface{}, limit int64) *PQLRowsQuery {
	idKey, err := formatIDKey(rowIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	if limit < 0 {
		return NewPQLRowsQuery("", f.index, errors.New("rows limit must be non-negative"))
	}
	text := fmt.Sprintf("Rows(field='%s',previous=%s,limit=%d)", f.name, idKey, limit)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsPreviousColumn creates a Rows query with the given previous row ID/key and column ID/key
func (f *Field) RowsPreviousColumn(rowIDOrKey interface{}, columnIDOrKey interface{}) *PQLRowsQuery {
	rowIDKey, err := formatIDKey(rowIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	columnIDKey, err := formatIDKey(columnIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	text := fmt.Sprintf("Rows(field='%s',previous=%s,column=%s)", f.name, rowIDKey, columnIDKey)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsLimitColumn creates a Row query with the given limit and column ID/key
func (f *Field) RowsLimitColumn(limit int64, columnIDOrKey interface{}) *PQLRowsQuery {
	if limit < 0 {
		return NewPQLRowsQuery("", f.index, errors.New("rows limit must be non-negative"))
	}
	columnIDKey, err := formatIDKey(columnIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	text := fmt.Sprintf("Rows(field='%s',limit=%d,column=%s)", f.name, limit, columnIDKey)
	return NewPQLRowsQuery(text, f.index, nil)
}

// RowsPreviousLimitColumn creates a Row query with the given previous row ID/key, limit and column ID/key
func (f *Field) RowsPreviousLimitColumn(rowIDOrKey interface{}, limit int64, columnIDOrKey interface{}) *PQLRowsQuery {
	rowIDKey, err := formatIDKey(rowIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	if limit < 0 {
		return NewPQLRowsQuery("", f.index, errors.New("rows limit must be non-negative"))
	}
	columnIDKey, err := formatIDKey(columnIDOrKey)
	if err != nil {
		return NewPQLRowsQuery("", f.index, err)
	}
	text := fmt.Sprintf("Rows(field='%s',previous=%s,limit=%d,column=%s)", f.name, rowIDKey, limit, columnIDKey)
	return NewPQLRowsQuery(text, f.index, nil)
}

// Distinct creates a Distinct query.
func (f *Field) Distinct() *PQLRowQuery {
	text := fmt.Sprintf("Distinct(Row(%s!=null),index='%s',field='%s')", f.name, f.index.Name(), f.name)
	return NewPQLRowQuery(text, f.index, nil)
}

// RowDistinct creates a Distinct query with the given row filter.
func (f *Field) RowDistinct(row *PQLRowQuery) *PQLRowQuery {
	text := fmt.Sprintf("Distinct(%s,index='%s',field='%s')", row.serialize(), f.index.Name(), f.name)
	return NewPQLRowQuery(text, f.index, nil)
}

func (f *Field) binaryOperation(op string, n IntOrFloat) *PQLRowQuery {
	text := fmt.Sprintf("Row(%s %s %s)", f.name, op, intOrFloatVal{n})
	q := NewPQLRowQuery(text, f.index, nil)
	q.hasKeys = f.options.keys || f.index.options.keys
	return q
}

func (f *Field) valQuery(op string, row *PQLRowQuery) *PQLBaseQuery {
	rowStr := ""
	hasKeys := f.options.keys || f.index.options.keys
	if row != nil {
		serializedRow := row.serialize()
		hasKeys = hasKeys || serializedRow.HasWriteKeys()
		rowStr = fmt.Sprintf("%s,", serializedRow.String())
	}
	text := fmt.Sprintf("%s(%sfield='%s')", op, rowStr, f.name)
	q := NewPQLBaseQuery(text, f.index, nil)
	q.hasKeys = hasKeys
	return q
}

func encodeMap(m map[string]interface{}) string {
	result, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(result)
}
