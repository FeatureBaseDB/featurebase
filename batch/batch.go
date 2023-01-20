// Copyright 2021 Molecula Corp. All rights reserved.
// Package batch provides tooling to prepare batches of records for ingest.
package batch

import (
	"bytes"
	"context"
	"math/bits"
	"sort"
	"sync"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/batch/egpool"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/roaring"
	"github.com/pkg/errors"
)

// Batch defaults.
const (
	DefaultKeyTranslateBatchSize = 100000
	existenceFieldName           = "_exists"
)

// TODO if using column translation, column ids might get way out of
// order. Could be worth sorting everything after translation (as an
// option?). Instead of sorting all simultaneously, it might be faster
// (more cache friendly) to sort ids and save the swap ops to apply to
// everything else that needs to be sorted. Note: we're already doing
// some sorting in importValueData and importMutexData, so if we
// implement it at the top level, remember to remove it there.

// TODO support clearing values? nil values in records are ignored,
// but perhaps we could have a special type indicating that a bit or
// value should explicitly be cleared?

// RecordBatch is a Pilosa ingest interface designed to allow for
// maximum throughput on common workloads. Users should call Add()
// with a Row object until it returns ErrBatchNowFull, at which time
// they should call Import(), and then repeat.
//
// Add will not modify or otherwise retain the Row once it returns, so
// it is recommended that callers reuse the same Row with repeated
// calls to Add, just modifying its values appropriately in between
// calls. This avoids allocating a new slice of Values for each
// inserted Row.
//
// The supported types of the values in Row.Values are implementation
// defined. Similarly, the supported types for Row.ID are
// implementation defined.
type RecordBatch interface {
	Add(Row) error

	// Import does translation, creates the fragment files, and then,
	// if we're not using split batch mode, imports everything to
	// Pilosa. It then resets internal data structures for the next
	// batch. If we are using split batch mode, it saves the fragment
	// data to the batch, resets all other internal structures, and
	// continues.
	// Split batch mode DOES NOT CURRENTLY SUPPORT MUTEX OR INT FIELDS!
	Import() error

	// Len reports the number of records which have been added to the
	// batch since the last call to Import (or since it was created).
	Len() int

	// Flush is only applicable in split batch mode where it actually
	// imports the stored data to Pilosa. Otherwise it simply returns
	// nil.
	Flush() error
}

// agedTranslation combines a translation with a recording of when it was last used.
type agedTranslation struct {
	id       uint64
	lastUsed uint64
}

// Batch implements RecordBatch.
//
// It supports Values of type string, uint64, int64, float64, or nil. The
// following table describes what Pilosa field each type of value must map to.
// Fields are set up when calling "NewBatch".
//
// | type   | pilosa field type | options   |
// |--------+-------------------+-----------|
// | string | set               | keys=true |
// | uint64 | set               | any       |
// | int64  | int               | any       |
// | float64| decimal           | scale     |
// | bool   | bool              | any       |
// | nil    | any               |           |
//
// nil values are ignored.
type Batch struct {
	importer  featurebase.Importer
	tbl       *dax.Table
	header    []*featurebase.FieldInfo
	headerMap map[string]*featurebase.FieldInfo

	// prevDuration records the time that each doImport() takes. This
	// is used to set the timeout for transactions to a reasonable
	// value based on the last import. It starts with a conservative
	// default set in NewBatch.
	prevDuration time.Duration

	// ids is a slice of length batchSize of record IDs
	ids []uint64

	// rowIDs is a map of field index (in the header) to slices of
	// length batchSize which contain row IDs.
	rowIDs map[int][]uint64
	// clearRowIDs is a map[fieldIndex][idsIndex]rowID we don't expect
	// clears to happen very often, so we store the idIndex/value
	// mapping in a map rather than a slice as we do for rowIDs. This
	// is a potentially temporary workaround to allow packed boolean
	// fields to clear "false" values. Packed fields may be more
	// completely supported by Pilosa in future.
	clearRowIDs map[int]map[int]uint64

	// rowIDSets is a map from field name to a batchSize slice of
	// slices of row IDs. When a given record can have more than one
	// value for a field, rowIDSets stores that information.
	rowIDSets map[string][][]uint64

	// values holds the values for each record of an int field
	values map[string][]int64

	// boolValues is a map[fieldName][idsIndex]bool, which holds the values for
	// each record of a bool field. It is a map of maps in order to accomodate
	// nil values (they just aren't recorded in the map[int]).
	boolValues map[string]map[int]bool

	// boolNulls holds a slice of indices into b.ids for each bool field which
	// has nil values.
	boolNulls map[string][]uint64

	// times holds a time for each record. (if any of the fields are time fields)
	times []QuantizedTime

	// nullIndices holds a slice of indices into b.ids for each
	// integer field which has nil values.
	nullIndices map[string][]uint64

	// TODO support bool fields.

	// for each field, keep a map of key to which record indexes that key mapped to
	toTranslate      map[int]map[string][]int
	toTranslateClear map[int]map[string][]int

	// toTranslateSets is a map from field name to a map of string
	// keys that need to be translated to sets of record indexes which
	// those keys map to.
	toTranslateSets map[string]map[string][]int

	// toTranslateID maps each string key to a record index - this
	// will get translated into Batch.rowIDs
	toTranslateID []string

	colTranslations map[string]agedTranslation
	rowTranslations map[string]map[string]agedTranslation
	cycle           uint64
	maxAge          uint64

	// staleTime tracks the time the first record of the batch was inserted
	// plus the maxStaleness, in order to raise ErrBatchNowStale if the
	// maxStaleness has elapsed
	staleTime    time.Time
	maxStaleness time.Duration

	// Maximum number of keys to translate at one time.
	keyTranslateBatchSize int

	log logger.Logger

	// experimental — only used by FlushToFragments which is an
	// alternative to Import which just builds the bitmap data for a
	// batch without actually importing it.
	splitBatchMode bool
	frags          fragments
	clearFrags     fragments

	useShardTransactionalEndpoint bool
}

func (b *Batch) Len() int { return len(b.ids) }

// BatchOption is a functional option for Batch objects.
type BatchOption func(b *Batch) error

func OptLogger(l logger.Logger) BatchOption {
	return func(b *Batch) error {
		b.log = l
		return nil
	}
}

func OptSplitBatchMode(on bool) BatchOption {
	return func(b *Batch) error {
		b.splitBatchMode = on
		return nil
	}
}

func OptCacheMaxAge(age uint64) BatchOption {
	return func(b *Batch) error {
		b.maxAge = age
		return nil
	}
}

func OptMaxStaleness(t time.Duration) BatchOption {
	return func(b *Batch) error {
		b.maxStaleness = t
		return nil
	}
}

func OptKeyTranslateBatchSize(v int) BatchOption {
	return func(b *Batch) error {
		b.keyTranslateBatchSize = v
		return nil
	}
}

// OptUseShardTransactionalEndpoint tells the batch to import using
// the newer shard-transactional endpoint.
func OptUseShardTransactionalEndpoint(use bool) BatchOption {
	return func(b *Batch) error {
		b.useShardTransactionalEndpoint = use
		return nil
	}
}

func OptImporter(i featurebase.Importer) BatchOption {
	return func(b *Batch) error {
		b.importer = i
		return nil
	}
}

// NewBatch initializes a new Batch object which will use the given Importer,
// index, set of fields, and will take "size" records before returning
// ErrBatchNowFull. The positions of the Fields in 'fields' correspond to the
// positions of values in the Row's Values passed to Batch.Add().
func NewBatch(importer featurebase.Importer, size int, tbl *dax.Table, fields []*featurebase.FieldInfo, opts ...BatchOption) (*Batch, error) {
	if len(fields) == 0 {
		return nil, errors.New("can't batch with no fields")
	} else if size == 0 {
		return nil, errors.New("can't batch with no batch size")
	}

	headerMap := make(map[string]*featurebase.FieldInfo, len(fields))
	rowIDs := make(map[int][]uint64, len(fields))
	values := make(map[string][]int64)
	boolValues := make(map[string]map[int]bool)
	boolNulls := make(map[string][]uint64)
	tt := make(map[int]map[string][]int, len(fields))
	ttSets := make(map[string]map[string][]int)
	hasTime := false
	for i, field := range fields {
		headerMap[field.Name] = field
		opts := field.Options

		// The client package has a FieldTypeDefault, but featurebase does not.
		// When this code was moved from the client package to the batch
		// package, FieldTypeDefault was no longer available. It probably isn't
		// necessary, but to ensure backwards compatiblity, we continue to
		// support it here with an unexported variable.
		fieldTypeDefault := ""

		switch typ := opts.Type; typ {
		case fieldTypeDefault, featurebase.FieldTypeSet, featurebase.FieldTypeTime:
			if opts.Keys {
				tt[i] = make(map[string][]int)
				ttSets[field.Name] = make(map[string][]int)
			}
			hasTime = typ == featurebase.FieldTypeTime || hasTime
		case featurebase.FieldTypeInt, featurebase.FieldTypeDecimal, featurebase.FieldTypeTimestamp:
			// tt line only needed if int field is string foreign key
			tt[i] = make(map[string][]int)
			values[field.Name] = make([]int64, 0, size)
		case featurebase.FieldTypeMutex:
			// similar to set/time fields, but no need to support sets
			// of values (hence no ttSets)
			if opts.Keys {
				tt[i] = make(map[string][]int)
			}
			rowIDs[i] = make([]uint64, 0, size)
		case featurebase.FieldTypeBool:
			boolValues[field.Name] = make(map[int]bool)
		default:
			return nil, errors.Errorf("field type '%s' is not currently supported through Batch", typ)
		}
	}

	b := &Batch{
		importer:              importer,
		header:                fields,
		headerMap:             headerMap,
		prevDuration:          time.Minute * 11,
		tbl:                   tbl,
		ids:                   make([]uint64, 0, size),
		rowIDs:                rowIDs,
		clearRowIDs:           make(map[int]map[int]uint64),
		rowIDSets:             make(map[string][][]uint64),
		values:                values,
		boolValues:            boolValues,
		boolNulls:             boolNulls,
		nullIndices:           make(map[string][]uint64),
		toTranslate:           tt,
		toTranslateClear:      make(map[int]map[string][]int),
		toTranslateSets:       ttSets,
		colTranslations:       make(map[string]agedTranslation),
		rowTranslations:       make(map[string]map[string]agedTranslation),
		maxAge:                64,
		maxStaleness:          time.Duration(0),
		keyTranslateBatchSize: DefaultKeyTranslateBatchSize,

		log: logger.NopLogger,

		frags:      make(fragments),
		clearFrags: make(fragments),
	}
	if hasTime {
		b.times = make([]QuantizedTime, 0, size)
	}
	for _, opt := range opts {
		err := opt(b)
		if err != nil {
			return nil, errors.Wrap(err, "applying options")
		}
	}

	return b, nil
}

// Row represents a single record which can be added to a Batch.
type Row struct {
	ID interface{}
	// Values map to the slice of fields in Batch.header
	Values []interface{}
	// Clears' int key is an index into Batch.header
	Clears map[int]interface{}
	// Time applies to all time fields
	Time QuantizedTime
}

// QuantizedTime represents a moment in time down to some granularity
// (year, month, day, or hour).
type QuantizedTime struct {
	ymdh [10]byte
}

// Set sets the Quantized time to the given timestamp (down to hour
// granularity).
func (qt *QuantizedTime) Set(t time.Time) {
	copy(qt.ymdh[:], t.Format("2006010215"))
}

// SetYear sets the quantized time's year, but leaves month, day, and
// hour untouched.
func (qt *QuantizedTime) SetYear(year string) {
	copy(qt.ymdh[:4], year)
}

// SetMonth sets the QuantizedTime's month, but leaves year, day, and
// hour untouched.
func (qt *QuantizedTime) SetMonth(month string) {
	copy(qt.ymdh[4:6], month)
}

// SetDay sets the QuantizedTime's day, but leaves year, month, and
// hour untouched.
func (qt *QuantizedTime) SetDay(day string) {
	copy(qt.ymdh[6:8], day)
}

// SetHour sets the QuantizedTime's hour, but leaves year, month, and
// day untouched.
func (qt *QuantizedTime) SetHour(hour string) {
	copy(qt.ymdh[8:10], hour)
}

func (qt *QuantizedTime) Time() (time.Time, error) {
	return time.Parse("2006010215", string(qt.ymdh[:]))
}

// Reset sets the time to the zero value which generates no time views.
func (qt *QuantizedTime) Reset() {
	for i := range qt.ymdh {
		qt.ymdh[i] = 0
	}
}

// views builds the list of Pilosa views for this particular time,
// given a quantum.
func (qt *QuantizedTime) views(q featurebase.TimeQuantum) ([]string, error) {
	zero := QuantizedTime{}
	if *qt == zero {
		return nil, nil
	}
	views := make([]string, 0, len(q))
	for _, unit := range q {
		switch unit {
		case 'Y':
			if qt.ymdh[0] == 0 {
				return nil, errors.New("no data set for year")
			}
			views = append(views, string(qt.ymdh[:4]))
		case 'M':
			if qt.ymdh[4] == 0 {
				return nil, errors.New("no data set for month")
			}
			views = append(views, string(qt.ymdh[:6]))
		case 'D':
			if qt.ymdh[6] == 0 {
				return nil, errors.New("no data set for day")
			}
			views = append(views, string(qt.ymdh[:8]))
		case 'H':
			if qt.ymdh[8] == 0 {
				return nil, errors.New("no data set for hour")
			}
			views = append(views, string(qt.ymdh[:10]))
		}
	}
	return views, nil
}

func (b *Batch) getColTranslation(key string) (uint64, bool) {
	trans, ok := b.colTranslations[key]
	if ok {
		trans.lastUsed = b.cycle
		b.colTranslations[key] = trans
	}
	return trans.id, ok
}

func (b *Batch) getRowTranslation(field, key string) (uint64, bool) {
	trans, ok := b.rowTranslations[field][key]
	if ok {
		trans.lastUsed = b.cycle
		b.rowTranslations[field][key] = trans
	}
	return trans.id, ok
}

// Add adds a record to the batch. Performance will be best if record
// IDs are shard-sorted. That is, all records which belong to the same
// Pilosa shard are added adjacent to each other. If the records are
// also in-order within a shard this will likely help as well. Add
// clears rec.Clears when it returns normally (either a nil error or
// BatchNowFull).
func (b *Batch) Add(rec Row) error {
	// Clear recValues and rec.Clears upon return.
	defer func() {
		for i := range rec.Values {
			rec.Values[i] = nil
		}
		for k := range rec.Clears {
			delete(rec.Clears, k)
		}
	}()

	if len(b.ids) == cap(b.ids) {
		return ErrBatchAlreadyFull
	}
	if len(rec.Values) != len(b.header) {
		return errors.Errorf("record needs to match up with batch fields, got %d fields and %d record", len(b.header), len(rec.Values))
	}

	handleStringID := func(rid string) error {
		if rid == "" {
			return errors.Errorf("record identifier cannot be an empty string")
		}
		if colID, ok := b.getColTranslation(rid); ok {
			b.ids = append(b.ids, colID)
		} else {
			if b.toTranslateID == nil {
				b.toTranslateID = make([]string, cap(b.ids))
			}
			b.toTranslateID[len(b.ids)] = rid
			b.ids = append(b.ids, 0)
		}
		return nil
	}
	var err error

	switch rid := rec.ID.(type) {
	case uint64:
		b.ids = append(b.ids, rid)
	case string:
		err := handleStringID(rid)
		if err != nil {
			return err
		}
	case []byte:
		err = handleStringID(string(rid))
		if err != nil {
			return err
		}
	default: // TODO support nil ID as being auto-allocated.
		return errors.Errorf("unsupported id type %T value %v", rid, rid)
	}

	// curPos is the current position in b.ids, rowIDs[*], etc.
	curPos := len(b.ids) - 1

	if b.times != nil {
		b.times = append(b.times, rec.Time)
	}

	for i := 0; i < len(rec.Values); i++ {
		field := b.header[i]
		switch val := rec.Values[i].(type) {
		case string:
			switch field.Options.Type {
			case featurebase.FieldTypeInt:
				if val == "" {
					// copied from the `case nil:` section for ints and decimals
					b.values[field.Name] = append(b.values[field.Name], 0)
					nullIndices, ok := b.nullIndices[field.Name]
					if !ok {
						nullIndices = make([]uint64, 0)
					}
					nullIndices = append(nullIndices, uint64(curPos))
					b.nullIndices[field.Name] = nullIndices
				} else if intVal, ok := b.getRowTranslation(field.Name, val); ok {
					b.values[field.Name] = append(b.values[field.Name], int64(intVal))
				} else {
					ints, ok := b.toTranslate[i][val]
					if !ok {
						ints = make([]int, 0)
					}
					ints = append(ints, curPos)
					b.toTranslate[i][val] = ints
					b.values[field.Name] = append(b.values[field.Name], 0)
				}
			case featurebase.FieldTypeBool:
				// If we want to support bools as string values, we would do
				// that here.
			default:
				// nil-extend
				for len(b.rowIDs[i]) < curPos {
					b.rowIDs[i] = append(b.rowIDs[i], nilSentinel)
				}
				rowIDs := b.rowIDs[i]
				// empty string is not a valid value at this point (Pilosa refuses to translate it)
				if val == "" { //
					b.rowIDs[i] = append(rowIDs, nilSentinel)
				} else if rowID, ok := b.getRowTranslation(field.Name, val); ok {
					b.rowIDs[i] = append(rowIDs, rowID)
				} else {
					ints, ok := b.toTranslate[i][val]
					if !ok {
						ints = make([]int, 0)
					}
					ints = append(ints, curPos)
					b.toTranslate[i][val] = ints
					b.rowIDs[i] = append(rowIDs, 0)
				}
			}
		case uint64:
			// nil-extend
			for len(b.rowIDs[i]) < curPos {
				b.rowIDs[i] = append(b.rowIDs[i], nilSentinel)
			}
			b.rowIDs[i] = append(b.rowIDs[i], val)
		case int64:
			b.values[field.Name] = append(b.values[field.Name], val)
		case []string:
			if len(val) == 0 {
				continue
			}
			rowIDSets, ok := b.rowIDSets[field.Name]
			if !ok {
				rowIDSets = make([][]uint64, len(b.ids)-1, cap(b.ids))
				b.rowIDSets[field.Name] = rowIDSets
			}
			for len(rowIDSets) < len(b.ids)-1 {
				rowIDSets = append(rowIDSets, nil) // nil extend
			}

			rowIDs := make([]uint64, 0, len(val))
			for _, k := range val {
				if k == "" {
					continue
				}
				if rowID, ok := b.getRowTranslation(field.Name, k); ok {
					rowIDs = append(rowIDs, rowID)
				} else {
					ttsets, ok := b.toTranslateSets[field.Name]
					if !ok {
						ttsets = make(map[string][]int)
						b.toTranslateSets[field.Name] = make(map[string][]int)
					}
					ints, ok := ttsets[k]
					if !ok {
						ints = make([]int, 0, 1)
					}
					ints = append(ints, curPos)
					b.toTranslateSets[field.Name][k] = ints
				}
			}
			b.rowIDSets[field.Name] = append(rowIDSets, rowIDs)
		case []uint64:
			if len(val) == 0 {
				continue
			}
			rowIDSets, ok := b.rowIDSets[field.Name]
			if !ok {
				rowIDSets = make([][]uint64, len(b.ids)-1, cap(b.ids))
			}
			for len(rowIDSets) < len(b.ids)-1 {
				rowIDSets = append(rowIDSets, nil) // nil extend
			}
			b.rowIDSets[field.Name] = append(rowIDSets, val)
		case nil:
			switch field.Options.Type {
			case featurebase.FieldTypeInt, featurebase.FieldTypeDecimal, featurebase.FieldTypeTimestamp:
				b.values[field.Name] = append(b.values[field.Name], 0)
				nullIndices, ok := b.nullIndices[field.Name]
				if !ok {
					nullIndices = make([]uint64, 0)
				}
				nullIndices = append(nullIndices, uint64(curPos))
				b.nullIndices[field.Name] = nullIndices

			case featurebase.FieldTypeBool:
				boolNulls, ok := b.boolNulls[field.Name]
				if !ok {
					boolNulls = make([]uint64, 0)
				}
				boolNulls = append(boolNulls, uint64(curPos))
				b.boolNulls[field.Name] = boolNulls

			default:
				// only append nil to rowIDs if this field already has
				// rowIDs. Otherwise, this could be a []string or
				// []uint64 field where we've only seen nil values so
				// far. when we see a uint64 or string value, we'll
				// "nil-extend" rowIDs to make sure it's the right
				// length.
				if rowIDs, ok := b.rowIDs[i]; ok {
					b.rowIDs[i] = append(rowIDs, nilSentinel)
				}
			}

		case bool:
			b.boolValues[field.Name][curPos] = val

		case pql.Decimal:
			b.values[field.Name] = append(b.values[field.Name], val.ToInt64(field.Options.Scale))

		default:
			return errors.Errorf("Val %v Type %[1]T is not currently supported. Use string, uint64 (row id), or int64 (integer value)", val)
		}
	}

	for i, uval := range rec.Clears {
		field := b.header[i]
		if _, ok := b.clearRowIDs[i]; !ok {
			b.clearRowIDs[i] = make(map[int]uint64)
		}
		switch val := uval.(type) {
		case string:
			clearRows := b.clearRowIDs[i]
			// translate val and add to clearRows
			if rowID, ok := b.getRowTranslation(field.Name, val); ok {
				clearRows[curPos] = rowID
			} else {
				_, ok := b.toTranslateClear[i]
				if !ok {
					b.toTranslateClear[i] = make(map[string][]int)
				}
				ints, ok := b.toTranslateClear[i][val]
				if !ok {
					ints = make([]int, 0)
				}
				ints = append(ints, curPos)
				b.toTranslateClear[i][val] = ints
			}
		case uint64:
			b.clearRowIDs[i][curPos] = val
		case nil:
			if field.Options.Type == featurebase.FieldTypeMutex {
				for len(b.rowIDs[i]) <= curPos {
					b.rowIDs[i] = append(b.rowIDs[i], nilSentinel)
				}
				b.rowIDs[i][len(b.rowIDs[i])-1] = clearSentinel
			}
		default:
			return errors.Errorf("Clearing a value '%v' Type %[1]T is not currently supported (field '%s')", val, field.Name)
		}
		// nil extend b.rowIDs so we don't run into a horrible bug
		// where we skip doing clears because b.rowIDs doesn't have a
		// value for this field
		for len(b.rowIDs[i]) <= curPos {
			b.rowIDs[i] = append(b.rowIDs[i], nilSentinel)
		}

	}

	if len(b.ids) == cap(b.ids) {
		return ErrBatchNowFull
	}
	if b.maxStaleness != time.Duration(0) { // set maxStaleness to 0 to disable staleness checking
		if len(b.ids) == 1 {
			b.staleTime = time.Now().Add(b.maxStaleness)
		} else if time.Now().After(b.staleTime) {
			return ErrBatchNowStale
		}
	}
	return nil
}

// ErrBatchNowFull — similar to io.EOF — is a marker error to notify the user of
// a batch that it is time to call Import.
var ErrBatchNowFull = errors.New("batch is now full - you cannot add any more records (though the one you just added was accepted)")

// ErrBatchAlreadyFull is a real error saying that Batch.Add did not
// complete because the batch was full.
var ErrBatchAlreadyFull = errors.New("batch was already full, record was rejected")

// ErrBatchNowStale indicates that the oldest record in the batch is older than
// the maxStaleness value of the batch. Like ErrBatchNowFull, the error does
// not mean the record was rejected.
var ErrBatchNowStale = errors.New("batch is stale and needs to be imported (however, record was accepted)")

// Import does translation, creates the fragment files, and then,
// if we're not using split batch mode, imports everything to
// Pilosa. It then resets internal data structures for the next
// batch. If we are using split batch mode, it saves the fragment
// data to the batch, resets all other internal structures, and
// continues.  split batch mode DOES NOT CURRENTLY SUPPORT MUTEX
// OR INT FIELDS!
func (b *Batch) Import() error {
	ctx := context.Background()
	start := time.Now()
	if !b.useShardTransactionalEndpoint {
		trns, err := b.importer.StartTransaction(ctx, "", b.prevDuration*10, false, time.Hour)
		if err != nil {
			return errors.Wrap(err, "starting transaction")
		}
		defer func() {
			if trns != nil {
				if trnsl, err := b.importer.FinishTransaction(ctx, trns.ID); err != nil {
					b.log.Errorf("error finishing transaction: %v. trns: %+v", err, trnsl)
				}
			}
		}()
	}
	defer func() {
		featurebase.SummaryBatchImportDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	size := len(b.ids)
	transStart := time.Now()
	// first we need to translate the toTranslate, then fill out the missing row IDs
	err := b.doTranslation()
	if err != nil {
		return errors.Wrap(err, "doing Translation")
	}
	transTime := time.Now()
	b.log.Printf("translating batch of %d took: %v", size, transTime.Sub(transStart))

	frags, clearFrags, err := b.makeFragments(b.frags, b.clearFrags)
	if err != nil {
		return errors.Wrap(err, "making fragments (flush)")
	}
	if b.useShardTransactionalEndpoint {
		frags, clearFrags, err = b.makeSingleValFragments(frags, clearFrags)
		if err != nil {
			return errors.Wrap(err, "making single val fragments")
		}
	}

	makeTime := time.Now()
	b.log.Printf("making fragments for batch of %d took %v", size, makeTime.Sub(transTime))

	if b.splitBatchMode {
		b.frags = frags
		b.clearFrags = clearFrags
	} else {
		b.frags = make(fragments)
		b.clearFrags = make(fragments)
		// create bitmaps out of each field in b.rowIDs and import. Also
		// import int data.
		if !b.useShardTransactionalEndpoint {
			err = b.doImport(frags, clearFrags)
			if err != nil {
				return errors.Wrap(err, "doing import")
			}
			b.log.Printf("importing fragments took %v", time.Since(makeTime))
		} else {
			err = b.doImportShardTransactional(frags, clearFrags)
			if err != nil {
				return errors.Wrap(err, "doing shard transactional import")
			}
		}
	}

	b.reset()
	return nil
}

// Flush is only applicable in split batch mode where it actually
// imports the stored data to Pilosa. Otherwise it simply returns
// nil.
func (b *Batch) Flush() error {
	ctx := context.Background()

	if !b.splitBatchMode {
		return nil
	}
	start := time.Now()

	trns, err := b.importer.StartTransaction(ctx, "", b.prevDuration*10, false, time.Hour)
	if err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	defer func() {
		trnsl, err := b.importer.FinishTransaction(ctx, trns.ID)
		if err != nil {
			b.log.Errorf("error finishing transaction: %v. trns: %+v", err, trnsl)
		}
		featurebase.SummaryBatchFlushDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	importStart := time.Now()
	err = b.doImport(b.frags, b.clearFrags)
	if err != nil {
		return errors.Wrap(err, "doing import (ImportFragments)")
	}

	b.log.Debugf("superbatch import took %v", time.Since(importStart))

	b.reset()
	b.frags = make(fragments)
	b.clearFrags = make(fragments)
	return nil
}

func (b *Batch) doTranslation() error {
	eg := egpool.Group{PoolSize: 20}

	// Translate the column keys.
	eg.Go(func() error {
		// Deduplicate keys to translate.
		dedup := make(map[string]struct{})
		var keys []string
		for _, key := range b.toTranslateID {
			if key == "" {
				continue
			}

			if _, ok := dedup[key]; ok {
				continue
			}
			dedup[key] = struct{}{}

			keys = append(keys, key)
		}
		if len(keys) == 0 {
			// There are no column keys to translate.
			return nil
		}

		// Create the keys.
		start := time.Now()
		trans, err := b.createIndexKeys(keys...)
		if err != nil {
			return errors.Wrap(err, "translating col keys")
		}
		if len(trans) != len(keys) {
			return errors.Errorf("requested IDs for %d column keys but got %d back", len(keys), len(trans))
		}
		b.log.Debugf("translating %d column keys took %v", len(keys), time.Since(start))

		// Apply keys to translation cache.
		for key, id := range trans {
			b.colTranslations[key] = agedTranslation{
				id:       id,
				lastUsed: b.cycle,
			}
		}

		// Translate remaining keys in batch.
		for index, ttkey := range b.toTranslateID {
			if ttkey == "" {
				continue
			}

			b.ids[index] = trans[ttkey]
		}

		return nil
	})

	// creating a lock up here for the rowCache(s) which we get
	// below. Usually this isn't needed, but sometimes I think the
	// same rowCache gets used repeatedly because the same field is in
	// there multiple times, and that can lead to race
	// conditions. Need to understand this better, but gonna see if
	// this avoids the races.
	rowCacheLock := &sync.Mutex{}

	// Translate the row keys.
	for i, tt := range b.toTranslate {
		// Skip this if there are no keys to translate.
		ttc := b.toTranslateClear[i]
		if len(tt) == 0 && len(ttc) == 0 {
			continue
		}

		// Look up the associated field.
		field := b.header[i]
		fieldName := field.Name

		// Fetch the translation cache.
		rowCache := b.rowTranslations[fieldName]
		if rowCache == nil {
			rowCache = make(map[string]agedTranslation)
			b.rowTranslations[fieldName] = rowCache
		}

		i, tt := i, tt
		eg.Go(func() error {
			// Collect the keys to translate.
			keys := make([]string, 0, len(tt)+len(ttc))
			for k := range tt {
				keys = append(keys, k)
			}
			for k := range ttc {
				keys = append(keys, k)
			}

			// Create the keys.
			start := time.Now()
			trans, err := b.createFieldKeys(field, keys...)
			if err != nil {
				return errors.Wrap(err, "translating field keys")
			}
			b.log.Debugf("translating %d field keys for %s took %v", len(trans), fieldName, time.Since(start))

			// Apply keys to translation cache.
			rowCacheLock.Lock()
			for key, id := range trans {
				rowCache[key] = agedTranslation{
					id:       id,
					lastUsed: b.cycle,
				}
			}
			rowCacheLock.Unlock()

			switch ftype := field.Options.Type; ftype {
			case featurebase.FieldTypeSet, featurebase.FieldTypeMutex, featurebase.FieldTypeTime:
				// Fill out missing IDs in local batch records with translated IDs.
				rows := b.rowIDs[i]
				for key, idxs := range tt {
					id, ok := trans[key]
					if !ok {
						return errors.Errorf("key translation missing: %q in field %q", key, fieldName)
					}

					for _, i := range idxs {
						rows[i] = id
					}
				}

				// Fill out missing IDs in clear lists.
				clearRows := b.clearRowIDs[i]
				for key, idxs := range ttc {
					id, ok := trans[key]
					if !ok {
						return errors.Errorf("key translation missing: %q in field %q", key, fieldName)
					}

					for _, i := range idxs {
						clearRows[i] = id
					}
				}

			case featurebase.FieldTypeInt:
				// Handle foreign key int fields — fill out b.values instead of b.rows.
				vals := b.values[fieldName]
				for key, idxs := range tt {
					id, ok := trans[key]
					if !ok {
						return errors.Errorf("key translation missing: %q in field %q", key, fieldName)
					}

					for _, i := range idxs {
						vals[i] = int64(id)
					}
				}

			default:
				return errors.Errorf("unexpected field type for translation: %q", ftype)
			}

			return nil
		})
	}

	for fieldName, tt := range b.toTranslateSets {
		// Skip this if there are no keys to translate.
		if len(tt) == 0 {
			continue
		}

		// Look up the associated field.
		field := b.headerMap[fieldName]

		// Fetch the translation cache.
		rowCache := b.rowTranslations[fieldName]
		if rowCache == nil {
			rowCache = make(map[string]agedTranslation)
			b.rowTranslations[fieldName] = rowCache
		}

		fieldName, tt := fieldName, tt
		eg.Go(func() error {
			// Collect the keys to translate.
			keys := make([]string, 0, len(tt))
			for k := range tt {
				keys = append(keys, k)
			}

			// Create the keys.
			start := time.Now()
			trans, err := b.createFieldKeys(field, keys...)
			if err != nil {
				return errors.Wrap(err, "translating field keys")
			}
			b.log.Debugf("translating %d field keys for %s took %v", len(trans), fieldName, time.Since(start))

			// Apply keys to translation cache.
			rowCacheLock.Lock()
			for key, id := range trans {
				rowCache[key] = agedTranslation{
					id:       id,
					lastUsed: b.cycle,
				}
			}
			rowCacheLock.Unlock()

			// Fill out missing IDs in local batch records with translated IDs.
			rowIDSets := b.rowIDSets[fieldName]
			for key, idxs := range tt {
				id, ok := trans[key]
				if !ok {
					return errors.Errorf("key translation missing: %q in field %q", key, fieldName)
				}

				for _, i := range idxs {
					rowIDSets[i] = append(rowIDSets[i], id)
				}
			}

			return nil
		})
	}

	return eg.Wait()
}

func (b *Batch) createIndexKeys(keys ...string) (map[string]uint64, error) {
	ctx := context.Background()

	batchSize := b.keyTranslateBatchSize
	if batchSize <= 0 || len(keys) <= batchSize {
		return b.importer.CreateTableKeys(ctx, b.tbl.ID, keys...)
	}

	results := make(map[string]uint64, len(keys))
	for len(keys) > 0 {
		keySlice := keys
		if len(keySlice) > batchSize {
			keySlice = keySlice[:batchSize]
		}

		trans, err := b.importer.CreateTableKeys(ctx, b.tbl.ID, keySlice...)
		if err != nil {
			return nil, err
		} else if len(trans) != len(keySlice) {
			return nil, errors.Errorf("requested IDs for %d column keys but got %d back", len(keySlice), len(trans))
		}
		for key, id := range trans {
			results[key] = id
		}

		keys = keys[len(keySlice):]
	}

	return results, nil
}

func (b *Batch) createFieldKeys(field *featurebase.FieldInfo, keys ...string) (map[string]uint64, error) {
	ctx := context.Background()

	batchSize := b.keyTranslateBatchSize
	if batchSize <= 0 || len(keys) <= batchSize {
		return b.importer.CreateFieldKeys(ctx, b.tbl.ID, dax.FieldName(field.Name), keys...)
	}

	results := make(map[string]uint64, len(keys))
	for len(keys) > 0 {
		keySlice := keys
		if len(keySlice) > batchSize {
			keySlice = keySlice[:batchSize]
		}

		trans, err := b.importer.CreateFieldKeys(ctx, b.tbl.ID, dax.FieldName(field.Name), keySlice...)
		if err != nil {
			return nil, err
		} else if len(trans) != len(keySlice) {
			return nil, errors.Errorf("requested IDs for %d row keys but got %d back", len(keySlice), len(trans))
		}
		for key, id := range trans {
			results[key] = id
		}

		keys = keys[len(keySlice):]
	}

	return results, nil
}

func (b *Batch) doImportShardTransactional(frags, clearFrags fragments) error {
	ctx := context.Background()

	start := time.Now()
	requests := make(map[uint64]*featurebase.ImportRoaringShardRequest)
	getOrCreate := func(requests map[uint64]*featurebase.ImportRoaringShardRequest, shard uint64) *featurebase.ImportRoaringShardRequest {
		request, ok := requests[shard]
		if !ok {
			request = &featurebase.ImportRoaringShardRequest{
				Remote: true, // the client will send to all replicas TODO probably rename before merge
				Views:  make([]featurebase.RoaringUpdate, 0, 1),
			}
			requests[shard] = request
		}
		return request
	}

	for fragKey, viewMap := range frags {
		request := getOrCreate(requests, fragKey.shard)

		for view, bitmap := range viewMap {
			buf := &bytes.Buffer{}
			_, err := bitmap.WriteTo(buf)
			if err != nil {
				return errors.Wrap(err, "serializing bitmap")
			}
			request.Views = append(request.Views, featurebase.RoaringUpdate{Field: fragKey.field, View: view, Set: buf.Bytes()})

			// handle clear bitmap now if it exists so we don't have to go searching later
			if clearVM := clearFrags.GetViewMap(fragKey.shard, fragKey.field); clearVM != nil {
				if clearBitmap, ok := clearVM[view]; ok {
					clearBuf := &bytes.Buffer{}
					_, err := clearBitmap.WriteTo(clearBuf)
					if err != nil {
						return errors.Wrap(err, "serializing clear bitmap")
					}
					request.Views[len(request.Views)-1].Clear = clearBuf.Bytes()
					// delete from clearFrags so any remaining we know for sure must be added new
					clearFrags.DeleteView(fragKey.shard, fragKey.field, view)
				}
			}
		}
	}

	for fragKey, viewMap := range clearFrags {
		request := getOrCreate(requests, fragKey.shard)

		for view, bitmap := range viewMap {
			buf := &bytes.Buffer{}
			_, err := bitmap.WriteTo(buf)
			if err != nil {
				return errors.Wrap(err, "serializing bitmap")
			}
			request.Views = append(request.Views, featurebase.RoaringUpdate{Field: fragKey.field, View: view, Clear: buf.Bytes()})
		}
	}

	featurebase.SummaryBatchShardImportBuildRequestsSeconds.Observe(time.Since(start).Seconds())
	start = time.Now()
	eg := egpool.Group{PoolSize: 20}
	for shard, request := range requests {
		shard := shard
		request := request
		eg.Go(func() error {
			return b.importer.ImportRoaringShard(ctx, b.tbl.ID, shard, request)
		})
	}
	err := eg.Wait()
	dur := time.Since(start)
	featurebase.SummaryBatchImportDurationSeconds.Observe(dur.Seconds())
	b.log.Printf("import shard took: %v\n", dur)
	return errors.Wrap(err, "doing shard-transactional imports")
}

func (b *Batch) doImport(frags, clearFrags fragments) error {
	ctx := context.Background()

	start := time.Now()
	eg := egpool.Group{PoolSize: 20}
	// TODO, currently this relies on upstream behavior of
	// makeFragments to guarantee that any shard/field combination in
	// clearFrags also has a shard/field in frags. We're only
	// iterating over frags and then checking to see if clearFrags has
	// the same keys. If we optimized makeFragments to skip adding
	// things to frags which had no set bits (e.g. if we were only
	// clearing things), then this code would need to be updated to
	// ensure that it looked at the things in clearFrags which were
	// *not* in frags.
	for fragmentKey, viewMap := range frags {
		field := fragmentKey.field
		shard := fragmentKey.shard
		viewMap := viewMap

		eg.Go(func() error {
			clearViewMap := clearFrags.GetViewMap(shard, field)
			if len(clearViewMap) > 0 {
				startx := time.Now()
				fld, err := b.tableField(dax.FieldName(field))
				if err != nil {
					return errors.Wrapf(err, "getting tablefield: %s", field)
				}
				if err := b.importer.ImportRoaringBitmap(ctx, b.tbl.ID, fld, shard, clearViewMap, true); err != nil {
					return errors.Wrapf(err, "import clearing clearing data for %s", field)
				}
				b.log.Debugf("imp-roar-clr %s,shard:%d,views:%d %v", field, shard, len(clearViewMap), time.Since(startx))
			}

			starty := time.Now()
			fld, err := b.tableField(dax.FieldName(field))
			if err != nil {
				return errors.Wrapf(err, "getting tablefield: %s", field)
			}

			ferr := b.importer.ImportRoaringBitmap(ctx, b.tbl.ID, fld, shard, viewMap, false)
			b.log.Debugf("imp-roar    field: %s, shard:%d, views:%d %v", field, shard, len(clearViewMap), time.Since(starty))
			return errors.Wrapf(ferr, "importing data for %s", field)
		})
	}
	eg.Go(func() error { return b.importValueData() })
	eg.Go(func() error { return b.importMutexData() })

	err := eg.Wait()
	if err != nil {
		if pferr := anyCause(ErrPreconditionFailed, eg.Errors()...); pferr != nil {
			return pferr
		}
		return err
	}
	b.prevDuration = time.Since(start)
	return nil
}

// tableField is a helper function which was introduced when we switched the
// index and field types from being client types (e.g client.Index,
// client.Field) to being featurebase types (e.g. featurebase.IndexInfo,
// featurebase.FieldInfo), and then to being dax types (e.g. dax.Table,
// dax.Field). Unlike client.Index, featurebase.IndexInfo (and also dax.Table)
// is not expected to contain the "_exists" field. So calling Field("_exists")
// on IndexInfo results in a nil field. This method creates an instance of
// dax.Field for the "_exists" field.
func (b *Batch) tableField(fname dax.FieldName) (*dax.Field, error) {
	if fname == existenceFieldName {
		return &dax.Field{
			Name: existenceFieldName,
		}, nil
	}

	fld, ok := b.tbl.Field(fname)
	if !ok {
		return nil, errors.Errorf("field not in table: %s", fname)
	}
	return fld, nil
}

func anyCause(cause error, errs ...error) error {
	if cause == nil {
		return nil
	}

	for _, err := range errs {
		if errors.Cause(err) == cause {
			return err
		}
	}
	return nil
}

func (b *Batch) shardWidth() uint64 {
	return featurebase.ShardWidth
}

// this is kind of bad as it means we can never import column id
// ^uint64(0) which is a valid column ID. I think it's unlikely to
// matter much in practice (we could maybe special case it somewhere
// if needed though).
var nilSentinel = ^uint64(0)

// clearSentinel indicates that we're trying to clear all values for
// this field of this record
var clearSentinel = nilSentinel - 1

func (b *Batch) makeFragments(frags, clearFrags fragments) (fragments, fragments, error) {
	shardWidth := b.shardWidth()
	emptyClearRows := make(map[int]uint64)

	// create _exists fragments
	var curBM *roaring.Bitmap
	curShard := ^uint64(0) // impossible sentinel value for shard.
	for _, col := range b.ids {
		if col/shardWidth != curShard {
			curShard = col / shardWidth
			curBM = frags.GetOrCreate(curShard, "_exists", "")
		}
		curBM.DirectAdd(col % shardWidth)
	}

	for i, rowIDs := range b.rowIDs {
		if len(rowIDs) == 0 {
			continue // this can happen when the values that came in for this field were string slices
		}
		clearRows := b.clearRowIDs[i]
		if clearRows == nil {
			clearRows = emptyClearRows
		}
		field := b.header[i]
		opts := field.Options
		if opts.Type == featurebase.FieldTypeMutex {
			continue // we handle mutex fields separately — they can't use importRoaring
		}
		curShard := ^uint64(0) // impossible sentinel value for shard.
		var curBM *roaring.Bitmap
		var clearBM *roaring.Bitmap
		for j := range b.ids {
			col := b.ids[j]
			row := nilSentinel
			if len(rowIDs) > j {
				// this is to protect against what i believe is a bug in the idk.DeleteSentinel logic in handling nil entries
				// this will prevent a crash by assuming missing entries are nil entries which i think is ok
				// TODO (twg) find where the nil entry was not added on the idk side ~ingest.go batchFromSchema method
				row = rowIDs[j]
			}

			if col/shardWidth != curShard {
				curShard = col / shardWidth
				curBM = frags.GetOrCreate(curShard, field.Name, "")
				clearBM = clearFrags.GetOrCreate(curShard, field.Name, "")
			}
			if row != nilSentinel {
				// TODO this is super ugly, but we want to avoid setting
				// bits on the standard view in the specific case when
				// there isn't one. Should probably refactor this whole
				// loop to be more general w.r.t. views. Also... tests for
				// the NoStandardView case would be great.
				if !(opts.Type == featurebase.FieldTypeTime && opts.NoStandardView) {
					curBM.DirectAdd(row*shardWidth + (col % shardWidth))
				}
				if opts.Type == featurebase.FieldTypeTime {
					views, err := b.times[j].views(opts.TimeQuantum)
					if err != nil {
						return nil, nil, errors.Wrap(err, "calculating views")
					}
					for _, view := range views {
						tbm := frags.GetOrCreate(curShard, field.Name, view)
						tbm.DirectAdd(row*shardWidth + (col % shardWidth))
					}
				}
			}

			clearRow, ok := clearRows[j]
			if ok {
				clearBM.DirectAddN(clearRow*shardWidth + (col % shardWidth))
				// we're going to execute the clear before the set, so
				// we want to make sure that at this point, the "set"
				// fragments don't contain the bit that we're clearing
				curBM.DirectRemoveN(clearRow*shardWidth + (col % shardWidth))
			}
		}
	}

	for fname, rowIDSets := range b.rowIDSets {
		if len(rowIDSets) == 0 {
			continue
		} else if len(rowIDSets) < len(b.ids) {
			// rowIDSets is guaranteed to have capacity == to b.ids,
			// but if the last record had a nil for this field, it
			// might not have the same length, so we re-slice it to
			// ensure the lengths are the same.
			rowIDSets = rowIDSets[:len(b.ids)]
		}
		field := b.headerMap[fname]
		opts := field.Options
		curShard := ^uint64(0) // impossible sentinel value for shard.
		var curBM *roaring.Bitmap
		for j := range b.ids {
			col, rowIDs := b.ids[j], rowIDSets[j]
			if len(rowIDs) == 0 {
				continue
			}
			if col/shardWidth != curShard {
				curShard = col / shardWidth
				curBM = frags.GetOrCreate(curShard, fname, "")
			}
			// TODO this is super ugly, but we want to avoid setting
			// bits on the standard view in the specific case when
			// there isn't one. Should probably refactor this whole
			// loop to be more general w.r.t. views. Also... tests for
			// the NoStandardView case would be great.
			if !(opts.Type == featurebase.FieldTypeTime && opts.NoStandardView) {
				for _, row := range rowIDs {
					curBM.DirectAdd(row*shardWidth + (col % shardWidth))
				}
			}
			if opts.Type == featurebase.FieldTypeTime {
				views, err := b.times[j].views(opts.TimeQuantum)
				if err != nil {
					return nil, nil, errors.Wrap(err, "calculating views")
				}
				for _, view := range views {
					tbm := frags.GetOrCreate(curShard, fname, view)
					for _, row := range rowIDs {
						tbm.DirectAdd(row*shardWidth + (col % shardWidth))
					}
				}
			}
		}
	}
	return frags, clearFrags, nil
}

func (b *Batch) makeSingleValFragments(frags, clearFrags fragments) (fragments, fragments, error) {
	shardWidth := b.shardWidth()
	ids := make([]uint64, len(b.ids))

	// -------------------------
	// int-like fields
	// -------------------------
	for fieldName, bvalues := range b.values {
		ids = ids[:len(b.ids)]

		// trim out null values from ids and values.
		nullIndices := b.nullIndices[fieldName]

		i, n := uint64(0), 0
		for _, nullIndex := range nullIndices {
			copy(ids[n:], b.ids[i:nullIndex])
			n += copy(bvalues[n:], bvalues[i:nullIndex])
			i = nullIndex + 1
		}

		copy(ids[n:], b.ids[i:])
		n += copy(bvalues[n:], bvalues[i:])
		ids, bvalues = ids[:n], bvalues[:n]

		if len(ids) == 0 {
			continue
		}

		sc := &valsByIDsSortable{ids: ids, vals: bvalues, width: shardWidth}
		if !sort.IsSorted(sc) {
			sort.Stable(sc)
		}
		field := b.headerMap[fieldName]
		base := field.Options.Base
		if field.Options.Type == featurebase.FieldTypeTimestamp {
			base = 0
		}

		shard := ids[0] / shardWidth
		bitmap := frags.GetOrCreate(shard, fieldName, "bsig_"+fieldName) // TODO... grab bsig_ prefix from elsewhere
		for i, id := range ids {
			if i+1 < len(ids) {
				// we only want the last value set for each id
				if ids[i+1] == id {
					continue
				}
			}
			if shard != id/shardWidth {
				shard = id / shardWidth
				bitmap = frags.GetOrCreate(shard, fieldName, "bsig_"+fieldName)
			}
			fragmentColumn := id % shardWidth
			bitmap.Add(fragmentColumn) // existence bit
			svalue := bvalues[i] - base
			negative := svalue < 0
			var value uint64
			if negative {
				bitmap.Add(shardWidth + fragmentColumn) // set sign bit
				value = uint64(svalue * -1)
			} else {
				value = uint64(svalue)
			}
			lz := bits.LeadingZeros64(value)
			row := uint64(2)
			for mask := uint64(0x1); mask <= 1<<(64-lz) && mask != 0; mask = mask << 1 {
				if value&mask > 0 {
					bitmap.Add(row*shardWidth + fragmentColumn)
				}
				row++
			}
		}
	}

	// -------------------------
	// mutex fields
	// -------------------------
	for findex, rowIDs := range b.rowIDs {
		field := b.header[findex]
		if field.Options.Type != featurebase.FieldTypeMutex {
			continue
		}
		ids = ids[:0]

		// get slice of column ids for non-nil rowIDs and cut nil row
		// IDs out of rowIDs.
		idsIndex := 0
		for i, id := range b.ids {
			rowID := rowIDs[i]
			if rowID == nilSentinel {
				continue
			}
			rowIDs[idsIndex] = rowID
			ids = append(ids, id)
			idsIndex++
		}
		rowIDs = rowIDs[:idsIndex]

		if len(ids) == 0 {
			continue
		}

		sc := &rowsByIDsSortable{ids: ids, rows: rowIDs, width: shardWidth}
		if !sort.IsSorted(sc) {
			sort.Stable(sc)
		}

		shard := ids[0] / shardWidth
		bitmap := frags.GetOrCreate(shard, field.Name, "standard")
		clearBM := clearFrags.GetOrCreate(shard, field.Name, "standard")
		for i, id := range ids {
			if i+1 < len(ids) {
				// we only want the last value set for each id
				if ids[i+1] == id {
					continue
				}
			}
			row := rowIDs[i]
			if shard != id/shardWidth {
				shard = id / shardWidth
				bitmap = frags.GetOrCreate(shard, field.Name, "standard")
				clearBM = clearFrags.GetOrCreate(shard, field.Name, "standard")
			}
			fragmentColumn := id % shardWidth
			clearBM.Add(fragmentColumn) // Will use this to clear columns.
			if row != clearSentinel {
				// clearSentinel is used for deletion
				// so this value should only be added if its not clearSentinel
				bitmap.Add(row*shardWidth + fragmentColumn)
			}
		}
	}
	// -------------------------
	// Boolean fields
	// -------------------------
	falseRowOffset := 0 * shardWidth // fragment row 0
	trueRowOffset := 1 * shardWidth  // fragment row 1

	// For bools which have been set to null, clear both the true and false
	// values for the record. Because this ends up going through the
	// API.ImportRoaringShard() method (which handles `bool` fields the same as
	// `mutex` fields), we don't actually set the true and false rows of the
	// boolean fragment; rather, we just set the first row to indicate which
	// records (for all rows) to clear.
	for fieldname, boolNulls := range b.boolNulls {
		field := b.headerMap[fieldname]
		if field.Options.Type != featurebase.FieldTypeBool {
			continue
		}
		for _, pos := range boolNulls {
			recID := b.ids[pos]
			shard := recID / shardWidth
			clearBM := clearFrags.GetOrCreate(shard, field.Name, "standard")

			fragmentColumn := recID % shardWidth

			clearBM.Add(fragmentColumn)
		}
	}

	// For bools which have been set to a non-nil value, set the appropriate
	// value for the record, and unset the opposing values. For example, if the
	// bool is set to `false`, then set the bit in the "false" row, and clear
	// the bit in the "true" row.
	for fieldname, boolMap := range b.boolValues {
		field := b.headerMap[fieldname]
		if field.Options.Type != featurebase.FieldTypeBool {
			continue
		}

		for pos, boolVal := range boolMap {
			recID := b.ids[pos]

			shard := recID / shardWidth
			bitmap := frags.GetOrCreate(shard, field.Name, "standard")
			clearBM := clearFrags.GetOrCreate(shard, field.Name, "standard")

			fragmentColumn := recID % shardWidth
			clearBM.Add(fragmentColumn)

			if boolVal {
				bitmap.Add(trueRowOffset + fragmentColumn)
			} else {
				bitmap.Add(falseRowOffset + fragmentColumn)
			}
		}
	}

	return frags, clearFrags, nil
}

type valsByIDsSortable struct {
	ids  []uint64
	vals []int64
	// shard width so we can compare by shard instead of ID
	width uint64
}

func (v *valsByIDsSortable) Len() int { return len(v.ids) }

func (v *valsByIDsSortable) Less(i, j int) bool { return v.ids[i] < v.ids[j] }
func (v *valsByIDsSortable) Swap(i, j int) {
	v.ids[i], v.ids[j] = v.ids[j], v.ids[i]
	v.vals[i], v.vals[j] = v.vals[j], v.vals[i]
}

// importValueData imports data for int fields.
func (b *Batch) importValueData() error {
	ctx := context.Background()

	shardWidth := uint64(featurebase.ShardWidth)
	eg := egpool.Group{PoolSize: 20}

	ids := make([]uint64, len(b.ids))
	for fieldName, bvalues := range b.values {
		ids = ids[:len(b.ids)]

		// trim out null values from ids and values.
		nullIndices := b.nullIndices[fieldName]

		i, n := uint64(0), 0
		for _, nullIndex := range nullIndices {
			copy(ids[n:], b.ids[i:nullIndex])
			n += copy(bvalues[n:], bvalues[i:nullIndex])
			i = nullIndex + 1
		}

		copy(ids[n:], b.ids[i:])
		n += copy(bvalues[n:], bvalues[i:])
		ids, bvalues = ids[:n], bvalues[:n]

		// now do imports by shard
		if len(ids) == 0 {
			continue // TODO test this "all nil" case
		}

		sc := &valsByIDsSortable{ids: ids, vals: bvalues, width: shardWidth}
		if !sort.IsSorted(sc) {
			sort.Stable(sc) // TODO(jaffee) this was sort.Sort which I think is a bug. If we get multiple of the same record w/in a batch with different int values, the last one needs to win. We need a test for this.
		}

		curShard := ids[0] / shardWidth
		startIdx := 0
		for i := 1; i <= len(ids); i++ {
			var recordID uint64
			if i < len(ids) {
				recordID = ids[i]
			} else {
				recordID = (curShard + 2) * shardWidth
			}

			if recordID/shardWidth != curShard {
				endIdx := i
				shard := curShard
				field := b.headerMap[fieldName]
				fld := featurebase.FieldInfoToField(field)
				path, data, err := b.importer.EncodeImportValues(ctx, b.tbl.ID, fld, shard, bvalues[startIdx:endIdx], ids[startIdx:endIdx], false)
				if err != nil {
					return errors.Wrap(err, "encoding import values")
				}
				eg.Go(func() error {
					start := time.Now()
					fld := featurebase.FieldInfoToField(field)
					err := b.importer.DoImport(ctx, b.tbl.ID, fld, shard, path, data)
					b.log.Debugf("imp-vals    field: %s, shard: %d, data: %d %v", field.Name, shard, len(data), time.Since(start))
					return errors.Wrapf(err, "importing values for field = %s", field.Name)
				})
				startIdx = i
				curShard = recordID / shardWidth
			}
		}
	}
	err := eg.Wait()
	if err != nil {
		if pferr := anyCause(ErrPreconditionFailed, eg.Errors()...); pferr != nil {
			return pferr
		}
		return err
	}
	return errors.Wrap(err, "importing value data")
}

type rowsByIDsSortable struct {
	ids  []uint64
	rows []uint64
	// shard width so we can compare by shard instead of ID
	width uint64
}

func (v *rowsByIDsSortable) Len() int { return len(v.ids) }

func (v *rowsByIDsSortable) Less(i, j int) bool { return v.ids[i] < v.ids[j] }
func (v *rowsByIDsSortable) Swap(i, j int) {
	v.ids[i], v.ids[j] = v.ids[j], v.ids[i]
	v.rows[i], v.rows[j] = v.rows[j], v.rows[i]
}

// TODO this should work for bools as well - just need to support them
// at batch creation time and when calling Add, I think.
func (b *Batch) importMutexData() error {
	ctx := context.Background()

	shardWidth := uint64(featurebase.ShardWidth)

	eg := egpool.Group{PoolSize: 20}
	ids := make([]uint64, 0, len(b.ids))
	for findex, rowIDs := range b.rowIDs {
		field := b.header[findex]
		if field.Options.Type != featurebase.FieldTypeMutex {
			continue
		}
		ids = ids[:0]

		// get slice of column ids for non-nil rowIDs and cut nil row
		// IDs out of rowIDs.
		idsIndex := 0
		for i, id := range b.ids {
			rowID := rowIDs[i]
			if rowID == nilSentinel {
				continue
			}
			rowIDs[idsIndex] = rowID
			ids = append(ids, id)
			idsIndex++
		}
		rowIDs = rowIDs[:idsIndex]

		if len(ids) == 0 {
			continue
		}

		sc := &rowsByIDsSortable{ids: ids, rows: rowIDs, width: shardWidth}
		if !sort.IsSorted(sc) {
			sort.Stable(sc)
		}

		curShard := ids[0] / shardWidth
		startIdx := 0
		for i := 1; i <= len(ids); i++ {
			var recordID uint64
			if i < len(ids) {
				recordID = ids[i]
			} else {
				recordID = (curShard + 2) * shardWidth
			}

			if recordID/shardWidth != curShard {
				endIdx := i
				shard := curShard
				field := field
				fld := featurebase.FieldInfoToField(field)
				path, data, err := b.importer.EncodeImport(ctx, b.tbl.ID, fld, shard, rowIDs[startIdx:endIdx], ids[startIdx:endIdx], false)
				if err != nil {
					return errors.Wrap(err, "encoding mutex import")
				}
				eg.Go(func() error {
					start := time.Now()
					fld := featurebase.FieldInfoToField(field)
					err := b.importer.DoImport(ctx, b.tbl.ID, fld, shard, path, data)
					b.log.Debugf("imp-mux %s,shard:%d,data:%d %v", field.Name, shard, len(data), time.Since(start))
					return errors.Wrapf(err, "importing values for field = %s", field.Name)
				})
				startIdx = i
				curShard = recordID / shardWidth
			}
		}
	}
	err := eg.Wait()
	if err != nil {
		if pferr := anyCause(ErrPreconditionFailed, eg.Errors()...); pferr != nil {
			return pferr
		}
		return err
	}
	return errors.Wrap(err, "importing mutex data")
}

// reset is called at the end of importing to ready the batch for the
// next round. Where possible it does not re-allocate memory.
func (b *Batch) reset() {
	b.ids = b.ids[:0]
	b.times = b.times[:0]
	for i, rowIDs := range b.rowIDs {
		b.rowIDs[i] = rowIDs[:0]
	}
	for _, tt := range b.toTranslate {
		for k := range tt {
			delete(tt, k) // TODO pool these slices
		}
	}
	for _, tts := range b.toTranslateSets {
		for k := range tts {
			delete(tts, k)
		}
	}
	for field, rowIDSet := range b.rowIDSets {
		for i := range rowIDSet {
			rowIDSet[i] = nil
		}
		b.rowIDSets[field] = rowIDSet[:0]
	}
	for _, rowIDs := range b.clearRowIDs {
		for k := range rowIDs {
			delete(rowIDs, k)
		}
	}
	for _, clearMap := range b.toTranslateClear {
		for k := range clearMap {
			delete(clearMap, k)
		}
	}
	for _, boolsMap := range b.boolValues {
		for k := range boolsMap {
			delete(boolsMap, k)
		}
	}
	for k := range b.boolNulls {
		delete(b.boolNulls, k) // TODO pool these slices
	}
	for i := range b.toTranslateID {
		b.toTranslateID[i] = ""
	}
	for k := range b.values {
		delete(b.values, k) // TODO pool these slices
	}
	for k := range b.nullIndices {
		delete(b.nullIndices, k) // TODO pool these slices
	}
	b.cycle++
	for k, trans := range b.colTranslations {
		if trans.lastUsed-b.cycle > b.maxAge {
			delete(b.colTranslations, k)
		}
	}
	for field, rowTranslations := range b.rowTranslations {
		for k, trans := range rowTranslations {
			if trans.lastUsed-b.cycle > b.maxAge {
				delete(rowTranslations, k)
			}
		}

		if len(rowTranslations) == 0 {
			delete(b.rowTranslations, field)
		}
	}
}

// map[shard][field][view]fragmentData
type fragments map[fragmentKey]map[string]*roaring.Bitmap

type fragmentKey struct {
	shard uint64
	field string
}

func (f fragments) GetOrCreate(shard uint64, field, view string) *roaring.Bitmap {
	key := fragmentKey{shard, field}
	viewMap, ok := f[key]
	if !ok {
		viewMap = make(map[string]*roaring.Bitmap)
		f[key] = viewMap
	}
	bm, ok := viewMap[view]
	if !ok {
		bm = roaring.NewBTreeBitmap()
		viewMap[view] = bm
	}
	return bm
}

func (f fragments) GetViewMap(shard uint64, field string) map[string]*roaring.Bitmap {
	key := fragmentKey{shard, field}
	viewMap, ok := f[key]
	if !ok {
		return nil
	}
	// Remove any views which have an empty bitmap.
	// TODO: Ideally we would prevent allocating the empty bitmap to begin with,
	// but the logic is a bit tricky, and since we don't want to spend too much
	// time on it right now, we're leaving that for a future exercise.
	for k, v := range viewMap {
		if v.Count() == 0 {
			delete(viewMap, k)
		}
	}
	return viewMap
}

func (f fragments) DeleteView(shard uint64, field, view string) {
	vm := f.GetViewMap(shard, field)
	if vm == nil {
		return
	}
	delete(vm, view)
}
