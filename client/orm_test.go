// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	clienttypes "github.com/featurebasedb/featurebase/v3/client/types"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/pkg/errors"
)

func TestORM(t *testing.T) {
	var schema = NewSchema()
	var sampleIndex = schema.Index("sample-index")
	var sampleField = sampleIndex.Field("sample-field")
	var projectIndex = schema.Index("project-index")
	var collabField = projectIndex.Field("collaboration")
	var b1 = sampleField.Row(10)
	var b2 = sampleField.Row(20)
	var b3 = sampleField.Row(42)
	var b4 = collabField.Row(2)

	t.Run("SchemaDiff", func(t *testing.T) {
		schema1 := NewSchema()
		index11 := schema1.Index("diff-index1")
		index11.Field("field1-1")
		index11.Field("field1-2")
		index12 := schema1.Index("diff-index2", OptIndexKeys(true), OptIndexTrackExistence(false))
		index12.Field("field2-1")

		schema2 := NewSchema()
		index21 := schema2.Index("diff-index1")
		index21.Field("another-field")

		targetDiff12 := NewSchema()
		targetIndex1 := targetDiff12.Index("diff-index1", OptIndexTrackExistence(false))
		targetIndex1.Field("field1-1")
		targetIndex1.Field("field1-2")
		targetIndex2 := targetDiff12.Index("diff-index2", OptIndexKeys(true), OptIndexTrackExistence(false))
		targetIndex2.Field("field2-1")
		targetIndex1.options = &IndexOptions{}

		diff12 := schema1.diff(schema2)

		strTargetDiff12 := fmt.Sprintf("%+v", targetDiff12.indexes)
		strDiff12 := fmt.Sprintf("%+v", diff12.indexes)
		if strDiff12 != strTargetDiff12 {
			t.Fatalf("The diff must be correctly calculated, but exp/got\n%s\n%s", strTargetDiff12, strDiff12)
		}
	})

	t.Run("SchemaIndexes", func(t *testing.T) {
		schema1 := NewSchema()
		index11 := schema1.Index("diff-index1")
		index12 := schema1.Index("diff-index2")
		indexes := schema1.Indexes()
		target := map[string]*Index{
			"diff-index1": index11,
			"diff-index2": index12,
		}
		if !reflect.DeepEqual(target, indexes) {
			t.Fatalf("calling schema.Indexes should return indexes")
		}
	})

	t.Run("SchemaToString", func(t *testing.T) {
		schema1 := NewSchema()
		_ = schema1.Index("test-index")
		target := `map[test-index:{name: "test-index", options: "{"options":{}}", fields: map[_exists:{name: "_exists", index: "test-index", options: "{"options":{"type":"set"}}"}], shardWidth: 0}]`
		if target != schema1.String() {
			t.Fatalf("%s != %s", target, schema1.String())
		}
	})

	t.Run("NewIndex", func(t *testing.T) {
		index1 := schema.Index("index-name")
		if index1.Name() != "index-name" {
			t.Fatalf("index name was not set")
		}
		// calling schema.Index again should return the same index
		index2 := schema.Index("index-name")
		if index1 != index2 {
			t.Fatalf("calling schema.Index again should return the same index")
		}
		if !schema.HasIndex("index-name") {
			t.Fatalf("HasIndex should return true")
		}
		if schema.HasIndex("index-x") {
			t.Fatalf("HasIndex should return false")
		}
	})

	t.Run("NewIndexCopy", func(t *testing.T) {
		index := schema.Index("my-index-4copy", OptIndexKeys(true))
		index.Field("my-field-4copy", OptFieldTypeTime(clienttypes.TimeQuantumDayHour))
		copiedIndex := index.copy()
		if !reflect.DeepEqual(index, copiedIndex) {
			t.Fatalf("copied index should be equivalent")
		}
	})

	t.Run("NewIndexOptions", func(t *testing.T) {
		schemal := NewSchema()
		// test the defaults
		index := schemal.Index("index-default-options")
		target := `{"options":{}}`
		if target != index.options.String() {
			t.Fatalf("%s != %s", target, index.options.String())
		}

		index = schemal.Index("index-keys", OptIndexKeys(true))
		if true != index.Opts().Keys() {
			t.Fatalf("index keys %v != %v", true, index.Opts().Keys())
		}
		target = `{"options":{"keys":true}}`
		if target != index.options.String() {
			t.Fatalf("%s != %s", target, index.options.String())
		}

		index = schemal.Index("index-trackexistence", OptIndexTrackExistence(false))
		if false != index.Opts().TrackExistence() {
			t.Fatalf("index trackExistene %v != %v", true, index.Opts().TrackExistence())
		}
		target = `{"options":{"trackExistence":false}}`
		if target != index.options.String() {
			t.Fatalf("%s != %s", target, index.options.String())
		}
	})

	t.Run("NilIndexOption", func(t *testing.T) {
		schema.Index("index-with-nil-option", nil)
	})

	t.Run("IndexFields", func(t *testing.T) {
		schema1 := NewSchema()
		index11 := schema1.Index("diff-index1", OptIndexTrackExistence(false))
		field11 := index11.Field("field1-1")
		field12 := index11.Field("field1-2")
		fields := index11.Fields()
		target := map[string]*Field{
			"field1-1": field11,
			"field1-2": field12,
		}
		if !reflect.DeepEqual(target, fields) {
			t.Fatalf("calling index.Fields should return fields")
		}
		if !index11.HasField("field1-1") {
			t.Fatalf("HasField should return true")
		}
		if index11.HasField("field-x") {
			t.Fatalf("HasField should return false")
		}
	})

	t.Run("IndexToString", func(t *testing.T) {
		schema1 := NewSchema()
		index := schema1.Index("test-index")
		target := `{name: "test-index", options: "{"options":{}}", fields: map[_exists:{name: "_exists", index: "test-index", options: "{"options":{"type":"set"}}"}], shardWidth: 0}`
		if target != index.String() {
			t.Fatalf("indexes not equal exp/got:\n%s\n%s", target, index.String())
		}
	})

	t.Run("Field", func(t *testing.T) {
		field1 := sampleIndex.Field("nonexistent-field")
		field2 := sampleIndex.Field("nonexistent-field")
		if field1 != field2 {
			t.Fatalf("calling index.Field again should return the same field")
		}
		if field1.Name() != "nonexistent-field" {
			t.Fatalf("calling field.Name should return field's name")
		}
	})

	t.Run("FieldCopy", func(t *testing.T) {
		field := sampleIndex.Field("my-field-4copy", OptFieldTypeSet(CacheTypeRanked, 123456))
		copiedField := field.copy()
		if !reflect.DeepEqual(field, copiedField) {
			t.Fatalf("copied field should be equivalent")
		}
	})

	t.Run("FieldToString", func(t *testing.T) {
		schema1 := NewSchema()
		index := schema1.Index("test-index")
		field := index.Field("test-field")
		target := `{name: "test-field", index: "test-index", options: "{"options":{"type":"set"}}"}`
		if target != field.String() {
			t.Fatalf("%s != %s", target, field.String())
		}
	})

	t.Run("NilFieldOption", func(t *testing.T) {
		schema1 := NewSchema()
		index := schema1.Index("test-index")
		index.Field("test-field-with-nil-option", nil)
	})

	t.Run("FieldSetType", func(t *testing.T) {
		schema1 := NewSchema()
		index := schema1.Index("test-index")
		field := index.Field("test-set-field", OptFieldTypeSet(CacheTypeLRU, 1000), OptFieldKeys(true))
		target := `{"options":{"type":"set","cacheType":"lru","cacheSize":1000,"keys":true}}`
		if sortedString(target) != sortedString(field.options.String()) {
			t.Fatalf("%s != %s", target, field.options.String())
		}

		field = index.Field("test-set-field2", OptFieldTypeSet(CacheTypeLRU, -10), OptFieldKeys(true))
		target = `{"options":{"type":"set","cacheType":"lru","keys":true}}`
		if sortedString(target) != sortedString(field.options.String()) {
			t.Fatalf("%s != %s", target, field.options.String())
		}
	})

	t.Run("Row", func(t *testing.T) {
		comparePQL(t,
			"Row(collaboration=5)",
			collabField.Row(5))

		comparePQL(t,
			"Row(collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
			collabField.Row("b7feb014-8ea7-49a8-9cd8-19709161ab63"))

		q := collabField.Row(nil)
		if q.err == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("Set", func(t *testing.T) {
		comparePQL(t,
			"Set(10,collaboration=5)",
			collabField.Set(5, 10))

		comparePQL(t,
			`Set('some_id',collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')`,
			collabField.Set("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id"))

		q := collabField.Set(nil, 10)
		if q.err == nil {
			t.Fatalf("should have failed")
		}
		q = collabField.Set(5, false)
		if q.err == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("Timestamp", func(t *testing.T) {
		timestamp := time.Date(2017, time.April, 24, 12, 14, 0, 0, time.UTC)
		comparePQL(t,
			"Set(20,collaboration=10,2017-04-24T12:14)",
			collabField.SetTimestamp(10, 20, timestamp))

		comparePQL(t,
			"Set('mycol',collaboration='myrow',2017-04-24T12:14)",
			collabField.SetTimestamp("myrow", "mycol", timestamp))

		q := collabField.SetTimestamp(nil, 20, timestamp)
		if q.err == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("Clear", func(t *testing.T) {
		comparePQL(t,
			"Clear(10,collaboration=5)",
			collabField.Clear(5, 10))

		comparePQL(t,
			"Clear('some_id',collaboration='b7feb014-8ea7-49a8-9cd8-19709161ab63')",
			collabField.Clear("b7feb014-8ea7-49a8-9cd8-19709161ab63", "some_id"))
		comparePQL(t,
			`Clear('bill\'s',collaboration='will\'s')`,
			collabField.Clear("will's", "bill's"))

		q := collabField.Clear(nil, 10)
		if q.err == nil {
			t.Fatalf("should have failed")
		}
		q = collabField.Clear(5, false)
		if q.err == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("ClearRow", func(t *testing.T) {
		comparePQL(t,
			"ClearRow(collaboration=5)",
			collabField.ClearRow(5))

		comparePQL(t,
			"ClearRow(collaboration='five')",
			collabField.ClearRow("five"))

		comparePQL(t,
			"ClearRow(collaboration=true)",
			collabField.ClearRow(true))

		q := collabField.ClearRow(nil)
		if q.err == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("Union", func(t *testing.T) {
		comparePQL(t,
			"Union(Row(sample-field=10),Row(sample-field=20))",
			sampleIndex.Union(b1, b2))
		comparePQL(t,
			"Union(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
			sampleIndex.Union(b1, b2, b3))
		comparePQL(t,
			"Union(Row(sample-field=10),Row(collaboration=2))",
			sampleIndex.Union(b1, b4))
		comparePQL(t,
			"Union(Row(sample-field=10))",
			sampleIndex.Union(b1))
		comparePQL(t,
			"Union()",
			sampleIndex.Union())
	})

	t.Run("Intersect", func(t *testing.T) {
		comparePQL(t,
			"Intersect(Row(sample-field=10),Row(sample-field=20))",
			sampleIndex.Intersect(b1, b2))
		comparePQL(t,
			"Intersect(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
			sampleIndex.Intersect(b1, b2, b3))
		comparePQL(t,
			"Intersect(Row(sample-field=10),Row(collaboration=2))",
			sampleIndex.Intersect(b1, b4))
		comparePQL(t,
			"Intersect(Row(sample-field=10))",
			sampleIndex.Intersect(b1))
	})

	t.Run("Difference", func(t *testing.T) {
		comparePQL(t,
			"Difference(Row(sample-field=10),Row(sample-field=20))",
			sampleIndex.Difference(b1, b2))
		comparePQL(t,
			"Difference(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
			sampleIndex.Difference(b1, b2, b3))
		comparePQL(t,
			"Difference(Row(sample-field=10),Row(collaboration=2))",
			sampleIndex.Difference(b1, b4))
		comparePQL(t,
			"Difference(Row(sample-field=10))",
			sampleIndex.Difference(b1))
	})

	t.Run("Xor", func(t *testing.T) {
		comparePQL(t,
			"Xor(Row(sample-field=10),Row(sample-field=20))",
			sampleIndex.Xor(b1, b2))
		comparePQL(t,
			"Xor(Row(sample-field=10),Row(sample-field=20),Row(sample-field=42))",
			sampleIndex.Xor(b1, b2, b3))
		comparePQL(t,
			"Xor(Row(sample-field=10),Row(collaboration=2))",
			sampleIndex.Xor(b1, b4))
	})

	t.Run("Not", func(t *testing.T) {
		comparePQL(t,
			"Not(Row(sample-field=10))",
			sampleIndex.Not(sampleField.Row(10)))
	})

	t.Run("TopN", func(t *testing.T) {
		comparePQL(t,
			"TopN(collaboration,n=27)",
			collabField.TopN(27))
		comparePQL(t,
			"TopN(collaboration,Row(collaboration=3),n=10)",
			collabField.RowTopN(10, collabField.Row(3)))
	})

	t.Run("FieldLT", func(t *testing.T) {
		comparePQL(t,
			"Row(collaboration < 10)",
			collabField.LT(10))
		comparePQL(t,
			"Row(collaboration < 10.12300000)",
			collabField.LT(10.123))
	})

	t.Run("FieldLTE", func(t *testing.T) {
		comparePQL(t,
			"Row(collaboration <= 10)",
			collabField.LTE(10))
		comparePQL(t,
			"Row(collaboration <= 10.12300000)",
			collabField.LTE(10.123))
	})

	t.Run("FieldGT", func(t *testing.T) {
		comparePQL(t,
			"Row(collaboration > 10)",
			collabField.GT(10))
		comparePQL(t,
			"Row(collaboration > 10.12300000)",
			collabField.GT(10.123))
	})

	t.Run("FieldGTE", func(t *testing.T) {
		comparePQL(t,
			"Row(collaboration >= 10)",
			collabField.GTE(10))
		comparePQL(t,
			"Row(collaboration >= 10.12300000)",
			collabField.GTE(10.123))
	})

	t.Run("FieldEQ", func(t *testing.T) {
		comparePQL(t,
			"Row(collaboration == 10)",
			collabField.Equals(10))
		comparePQL(t,
			"Row(collaboration == 10.12300000)",
			collabField.Equals(10.123))
	})

	t.Run("FieldNEQ", func(t *testing.T) {
		comparePQL(t,
			"Row(collaboration != 10)",
			collabField.NotEquals(10))
		comparePQL(t,
			"Row(collaboration != 10.12300000)",
			collabField.NotEquals(10.123))
	})

	t.Run("FieldNotNull", func(t *testing.T) {
		comparePQL(t,
			"Row(collaboration != null)",
			collabField.NotNull())
	})

	t.Run("FieldBetween", func(t *testing.T) {
		comparePQL(t,
			"Row(collaboration >< [10,20])",
			collabField.Between(10, 20))
		comparePQL(t,
			"Row(collaboration >< [10.12300000,20.45600000])",
			collabField.Between(10.123, 20.456))
	})

	t.Run("FieldSum", func(t *testing.T) {
		comparePQL(t,
			"Sum(Row(collaboration=10),field='collaboration')",
			collabField.Sum(collabField.Row(10)))
		comparePQL(t,
			"Sum(field='collaboration')",
			collabField.Sum(nil))
	})

	t.Run("FieldMinRow", func(t *testing.T) {
		comparePQL(t,
			"MinRow(field='sample-field')",
			sampleField.MinRow())
	})

	t.Run("FieldMaxRow", func(t *testing.T) {
		comparePQL(t,
			"MaxRow(field='sample-field')",
			sampleField.MaxRow())
	})

	t.Run("FieldSetValue", func(t *testing.T) {
		comparePQL(t,
			"Set(50, collaboration=15)",
			collabField.SetIntValue(50, 15))

		comparePQL(t,
			"Set('mycol', sample-field=22)",
			sampleField.SetIntValue("mycol", 22))

		q := sampleField.SetIntValue(false, 22)
		if q.err == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("RowOperationInvalidArg", func(t *testing.T) {
		invalid := NewPQLRowQuery("", sampleIndex, errors.New("invalid"))
		// invalid argument in pos 1
		q := sampleIndex.Union(invalid, b1)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		// invalid argument in pos 2
		q = sampleIndex.Intersect(b1, invalid)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		// invalid argument in pos 3
		q = sampleIndex.Intersect(b1, b2, invalid)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		// not enough rows supplied
		q = sampleIndex.Difference()
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		// not enough rows supplied
		q = sampleIndex.Intersect()
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}

		// not enough rows supplied
		q = sampleIndex.Xor(b1)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("Store", func(t *testing.T) {
		comparePQL(t,
			"Store(Row(collaboration=5),sample-field=10)",
			sampleField.Store(collabField.Row(5), 10))
		q := sampleField.Store(collabField.Row(5), nil)
		if q.Error() == nil {
			t.Fatalf("query error should be not nil")
		}
	})

	t.Run("Options", func(t *testing.T) {
		comparePQL(t,
			"Options(Row(collaboration=5),shards=[1,3])",
			sampleIndex.Options(collabField.Row(5),
				OptOptionsShards(1, 3),
			))
	})

	t.Run("BatchQuery", func(t *testing.T) {
		q := sampleIndex.BatchQuery()
		if q.Index() != sampleIndex {
			t.Fatalf("The correct index should be assigned")
		}
		q.Add(sampleField.Row(44))
		q.Add(sampleField.Row(10101))
		if q.Error() != nil {
			t.Fatalf("Error should be nil")
		}
		comparePQL(t, "Row(sample-field=44)Row(sample-field=10101)", q)

		q2 := sampleField.Row(nil)
		if q2.err == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("BatchQueryWithError", func(t *testing.T) {
		q := sampleIndex.BatchQuery()
		q.Add(NewPQLBaseQuery("", nil, errors.New("invalid")))
		if q.Error() == nil {
			t.Fatalf("The error must be set")
		}
	})

	t.Run("Count", func(t *testing.T) {
		q := projectIndex.Count(collabField.Row(42))
		comparePQL(t, "Count(Row(collaboration=42))", q)
	})

	t.Run("Range", func(t *testing.T) {
		start := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2000, time.February, 2, 3, 4, 0, 0, time.UTC)
		comparePQL(t,
			"Range(collaboration=10,1970-01-01T00:00,2000-02-02T03:04)",
			collabField.Range(10, start, end))

		comparePQL(t,
			"Range(collaboration='foo',1970-01-01T00:00,2000-02-02T03:04)",
			collabField.Range("foo", start, end))

		q := collabField.Range(nil, start, end)
		if q.err == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("RowRange", func(t *testing.T) {
		start := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(2000, time.February, 2, 3, 4, 0, 0, time.UTC)
		comparePQL(t,
			"Row(collaboration=10,from='1970-01-01T00:00',to='2000-02-02T03:04')",
			collabField.RowRange(10, start, end))

		comparePQL(t,
			"Row(collaboration='foo',from='1970-01-01T00:00',to='2000-02-02T03:04')",
			collabField.RowRange("foo", start, end))
		comparePQL(t,
			`Row(collaboration='bill\'s',from='1970-01-01T00:00',to='2000-02-02T03:04')`,
			collabField.RowRange("bill's", start, end))

		q := collabField.RowRange(nil, start, end)
		if q.err == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("Rows", func(t *testing.T) {
		comparePQL(t,
			"Rows(field='collaboration')",
			collabField.Rows())
	})

	t.Run("UnionRows", func(t *testing.T) {
		comparePQL(t,
			"UnionRows(Rows(field='collaboration'))",
			collabField.Rows().Union())
	})

	t.Run("Like", func(t *testing.T) {
		comparePQL(t,
			"Rows(field='collaboration',like='_')",
			collabField.Like("_"))
		comparePQL(t,
			`Rows(field='collaboration',like='_\\')`,
			collabField.Like(`_\`))
		comparePQL(t,
			`Rows(field='collaboration',like='_\'')`,
			collabField.Like(`_'`))
	})

	t.Run("RowPrevious", func(t *testing.T) {
		comparePQL(t,
			"Rows(field='collaboration',previous=42)",
			collabField.RowsPrevious(42))
		comparePQL(t,
			"Rows(field='collaboration',previous='forty-two')",
			collabField.RowsPrevious("forty-two"))
		comparePQL(t,
			`Rows(field='collaboration',previous='bill\'s')`,
			collabField.RowsPrevious("bill's"))
		q := collabField.RowsPrevious(1.2)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("RowLimit", func(t *testing.T) {
		comparePQL(t,
			"Rows(field='collaboration',limit=10)",
			collabField.RowsLimit(10))
		q := collabField.RowsLimit(-1)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("RowsColumn", func(t *testing.T) {
		comparePQL(t,
			"Rows(field='collaboration',column=1000)",
			collabField.RowsColumn(1000))
		comparePQL(t,
			"Rows(field='collaboration',column='one-thousand')",
			collabField.RowsColumn("one-thousand"))
		q := collabField.RowsColumn(1.2)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("RowsPreviousLimit", func(t *testing.T) {
		comparePQL(t,
			"Rows(field='collaboration',previous=42,limit=10)",
			collabField.RowsPreviousLimit(42, 10))
		comparePQL(t,
			"Rows(field='collaboration',previous='forty-two',limit=10)",
			collabField.RowsPreviousLimit("forty-two", 10))
		q := collabField.RowsPreviousLimit(1.2, 10)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		q = collabField.RowsPreviousLimit("forty-two", -1)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("RowsPreviousColumn", func(t *testing.T) {
		comparePQL(t,
			"Rows(field='collaboration',previous=42,column=1000)",
			collabField.RowsPreviousColumn(42, 1000))
		comparePQL(t,
			"Rows(field='collaboration',previous='forty-two',column='one-thousand')",
			collabField.RowsPreviousColumn("forty-two", "one-thousand"))
		q := collabField.RowsPreviousColumn(1.2, 1000)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		q = collabField.RowsPreviousColumn("forty-two", 1.2)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("All", func(t *testing.T) {
		comparePQL(t,
			"All()",
			projectIndex.All())
	})

	t.Run("Distinct", func(t *testing.T) {
		comparePQL(t,
			"Distinct(Row(collaboration!=null),index='project-index',field='collaboration')",
			collabField.Distinct())
	})

	t.Run("RowDistinct", func(t *testing.T) {
		comparePQL(t,
			"Distinct(Row(sample-field=44),index='project-index',field='collaboration')",
			collabField.RowDistinct(sampleField.Row(44)))
	})

	t.Run("RowLimitColumn", func(t *testing.T) {
		comparePQL(t,
			"Rows(field='collaboration',limit=10,column=1000)",
			collabField.RowsLimitColumn(10, 1000))
		comparePQL(t,
			"Rows(field='collaboration',limit=10,column='one-thousand')",
			collabField.RowsLimitColumn(10, "one-thousand"))
		q := collabField.RowsLimitColumn(10, 1.2)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		q = collabField.RowsLimitColumn(-1, 1000)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("RowsPreviousLimitColumn", func(t *testing.T) {
		comparePQL(t,
			"Rows(field='collaboration',previous=42,limit=10,column=1000)",
			collabField.RowsPreviousLimitColumn(42, 10, 1000))
		comparePQL(t,
			"Rows(field='collaboration',previous='forty-two',limit=10,column='one-thousand')",
			collabField.RowsPreviousLimitColumn("forty-two", 10, "one-thousand"))
		q := collabField.RowsPreviousLimitColumn(1.2, 10, 1000)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		q = collabField.RowsPreviousLimitColumn(42, -1, 1000)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		q = collabField.RowsPreviousLimitColumn(42, 10, 1.2)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("GroupBy", func(t *testing.T) {
		field := sampleIndex.Field("test")
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'))",
			sampleIndex.GroupBy(collabField.Rows()))
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),Rows(field='test'))",
			sampleIndex.GroupBy(collabField.Rows(), field.Rows()))
		q := sampleIndex.GroupBy()
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("GroupByLimit", func(t *testing.T) {
		field := sampleIndex.Field("test")
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),limit=10)",
			sampleIndex.GroupByLimit(10, collabField.Rows()))
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),Rows(field='test'),limit=10)",
			sampleIndex.GroupByLimit(10, collabField.Rows(), field.Rows()))
		q := sampleIndex.GroupByLimit(10)
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		q = sampleIndex.GroupByLimit(-1, collabField.Rows())
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("GroupByFilter", func(t *testing.T) {
		field := sampleIndex.Field("test")
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),filter=Row(test=5))",
			sampleIndex.GroupByFilter(field.Row(5), collabField.Rows()))
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),Rows(field='test'),filter=Row(test=5))",
			sampleIndex.GroupByFilter(field.Row(5), collabField.Rows(), field.Rows()))
		q := sampleIndex.GroupByFilter(field.Row(5))
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("GroupByLimitFilter", func(t *testing.T) {
		field := sampleIndex.Field("test")
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),limit=10,filter=Row(test=5))",
			sampleIndex.GroupByLimitFilter(10, field.Row(5), collabField.Rows()))
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),Rows(field='test'),limit=10,filter=Row(test=5))",
			sampleIndex.GroupByLimitFilter(10, field.Row(5), collabField.Rows(), field.Rows()))
		q := sampleIndex.GroupByLimitFilter(10, field.Row(5))
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
		q = sampleIndex.GroupByLimitFilter(-1, field.Row(5), collabField.Rows())
		if q.Error() == nil {
			t.Fatalf("should have failed")
		}
	})

	t.Run("GroupByBase", func(t *testing.T) {
		field := sampleIndex.Field("test")
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'))",
			sampleIndex.GroupByBase(
				OptGroupByBuilderRows(collabField.Rows()),
			),
		)
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),Rows(field='test'))",
			sampleIndex.GroupByBase(
				OptGroupByBuilderRows(collabField.Rows(), field.Rows()),
			),
		)

		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),limit=10)",
			sampleIndex.GroupByBase(
				OptGroupByBuilderLimit(10),
				OptGroupByBuilderRows(collabField.Rows()),
			),
		)
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),Rows(field='test'),limit=10)",
			sampleIndex.GroupByBase(
				OptGroupByBuilderLimit(10),
				OptGroupByBuilderRows(collabField.Rows(), field.Rows()),
			),
		)

		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),filter=Row(test=5))",
			sampleIndex.GroupByBase(
				OptGroupByBuilderFilter(field.Row(5)),
				OptGroupByBuilderRows(collabField.Rows()),
			),
		)
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),Rows(field='test'),filter=Row(test=5))",
			sampleIndex.GroupByBase(
				OptGroupByBuilderFilter(field.Row(5)),
				OptGroupByBuilderRows(collabField.Rows(), field.Rows()),
			),
		)

		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),limit=10,filter=Row(test=5))",
			sampleIndex.GroupByBase(
				OptGroupByBuilderLimit(10),
				OptGroupByBuilderFilter(field.Row(5)),
				OptGroupByBuilderRows(collabField.Rows()),
			),
		)
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),Rows(field='test'),limit=10,filter=Row(test=5))",
			sampleIndex.GroupByBase(
				OptGroupByBuilderLimit(10),
				OptGroupByBuilderFilter(field.Row(5)),
				OptGroupByBuilderRows(collabField.Rows(), field.Rows()),
			),
		)

		field2 := sampleIndex.Field("age")
		comparePQL(t,
			"GroupBy(Rows(field='collaboration'),Rows(field='test'),aggregate=Sum(Row(age=20),field='age'))",
			sampleIndex.GroupByBase(
				OptGroupByBuilderRows(collabField.Rows(), field.Rows()),
				OptGroupByBuilderAggregate(field2.Sum(field2.Row(20))),
			),
		)
	})

	t.Run("FieldOptions", func(t *testing.T) {
		field := sampleIndex.Field("foo", OptFieldKeys(true))
		if true != field.Opts().Keys() {
			t.Fatalf("field keys: %v != %v", true, field.Opts().Keys())
		}
	})

	t.Run("SetFieldOptions", func(t *testing.T) {
		field := sampleIndex.Field("set-field", OptFieldTypeSet(CacheTypeRanked, 9999))
		jsonString := field.options.String()
		targetString := `{"options":{"type":"set","cacheType":"ranked","cacheSize":9999}}`
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		compareFieldOptions(t,
			field.Options(),
			FieldTypeSet,
			clienttypes.TimeQuantumNone,
			CacheTypeRanked,
			9999,
			pql.NewDecimal(0, 0),
			pql.NewDecimal(0, 0),
			"",
			"",
			0)
	})

	t.Run("IntFieldOptions", func(t *testing.T) {
		field := sampleIndex.Field("int-field", OptFieldTypeInt(-10, 100))
		jsonString := field.options.String()
		targetString := `{"options":{"type":"int","min":-10,"max":100}}`
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		compareFieldOptions(t,
			field.Options(),
			FieldTypeInt,
			clienttypes.TimeQuantumNone,
			CacheTypeDefault,
			0,
			pql.NewDecimal(-10, 0),
			pql.NewDecimal(100, 0),
			"",
			"",
			0)

		field = sampleIndex.Field("int-field2", OptFieldTypeInt(-10))
		jsonString = field.options.String()
		targetString = fmt.Sprintf(`{"options":{"type":"int","min":-10,"max":%d}}`, math.MaxInt64)
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}

		compareFieldOptions(t,
			field.Options(),
			FieldTypeInt,
			clienttypes.TimeQuantumNone,
			CacheTypeDefault,
			0,
			pql.NewDecimal(-10, 0),
			pql.NewDecimal(math.MaxInt64, 0),
			"",
			"",
			0)
		field = sampleIndex.Field("int-field3", OptFieldTypeInt())
		jsonString = field.options.String()
		targetString = fmt.Sprintf(`{"options":{"type":"int","min":%d,"max":%d}}`, math.MinInt64, math.MaxInt64)
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		compareFieldOptions(t,
			field.Options(),
			FieldTypeInt,
			clienttypes.TimeQuantumNone,
			CacheTypeDefault,
			0,
			pql.NewDecimal(math.MinInt64, 0),
			pql.NewDecimal(math.MaxInt64, 0),
			"",
			"",
			0)

		field = sampleIndex.Field("int-field4", OptFieldTypeInt(), OptFieldForeignIndex("blerg"))
		jsonString = field.options.String()
		targetString = fmt.Sprintf(`{"options":{"type":"int","min":%d,"max":%d,"foreignIndex":"blerg"}}`, math.MinInt64, math.MaxInt64)
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		compareFieldOptions(t,
			field.Options(),
			FieldTypeInt,
			clienttypes.TimeQuantumNone,
			CacheTypeDefault,
			0,
			pql.NewDecimal(math.MinInt64, 0),
			pql.NewDecimal(math.MaxInt64, 0),
			"blerg",
			"",
			0)
	})

	t.Run("TimeFieldOptions", func(t *testing.T) {
		field := sampleIndex.Field("time-field", OptFieldTypeTime(clienttypes.TimeQuantumDayHour, true))
		if true != field.Opts().NoStandardView() {
			t.Fatalf("field noStandardView %v != %v", true, field.Opts().NoStandardView())
		}
		jsonString := field.options.String()
		targetString := `{"options":{"noStandardView":true,"type":"time","timeQuantum":"DH","ttl":"0s"}}`
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		compareFieldOptions(t,
			field.Options(),
			FieldTypeTime,
			clienttypes.TimeQuantumDayHour,
			CacheTypeDefault,
			0,
			pql.NewDecimal(0, 0),
			pql.NewDecimal(0, 0),
			"",
			"",
			0)
	})

	t.Run("TTLOptions", func(t *testing.T) {
		field := sampleIndex.Field("ttl-field", OptFieldTypeTime(clienttypes.TimeQuantumDayHour, true), OptFieldTTL(0))
		if true != field.Opts().NoStandardView() {
			t.Fatalf("field noStandardView %v != %v", true, field.Opts().NoStandardView())
		}
		jsonString := field.options.String()
		targetString := `{"options":{"noStandardView":true,"type":"time","timeQuantum":"DH","ttl":"0s"}}`
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		compareFieldOptions(t,
			field.Options(),
			FieldTypeTime,
			clienttypes.TimeQuantumDayHour,
			CacheTypeDefault,
			0,
			pql.NewDecimal(0, 0),
			pql.NewDecimal(0, 0),
			"",
			"",
			0)
	})

	t.Run("MutexFieldOptions", func(t *testing.T) {
		field := sampleIndex.Field("mutex-field", OptFieldTypeMutex(CacheTypeRanked, 9999))
		jsonString := field.options.String()
		targetString := `{"options":{"type":"mutex","cacheType":"ranked","cacheSize":9999}}`
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		compareFieldOptions(t,
			field.Options(),
			FieldTypeMutex,
			clienttypes.TimeQuantumNone,
			CacheTypeRanked,
			9999,
			pql.NewDecimal(0, 0),
			pql.NewDecimal(0, 0),
			"",
			"",
			0)
	})

	t.Run("BoolFieldOptions", func(t *testing.T) {
		field := sampleIndex.Field("bool-field", OptFieldTypeBool())
		jsonString := field.options.String()
		targetString := `{"options":{"type":"bool"}}`
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		compareFieldOptions(t,
			field.Options(),
			FieldTypeBool,
			clienttypes.TimeQuantumNone,
			CacheTypeDefault,
			0,
			pql.NewDecimal(0, 0),
			pql.NewDecimal(0, 0),
			"",
			"",
			0)
	})

	t.Run("DecimalFieldOptions", func(t *testing.T) {
		field := sampleIndex.Field("decimal-field", OptFieldTypeDecimal(3, pql.NewDecimal(7, 3), pql.NewDecimal(999, 3)))
		jsonString := field.options.String()
		targetString := `{"options":{"type":"decimal","scale":3,"max":0.999,"min":0.007}}`
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		compareFieldOptions(t,
			field.Options(),
			FieldTypeDecimal,
			clienttypes.TimeQuantumNone,
			CacheTypeDefault,
			0,
			pql.NewDecimal(7, 3),
			pql.NewDecimal(999, 3),
			"",
			"",
			0)
	})

	t.Run("DecimalFieldOptions", func(t *testing.T) {
		field := sampleIndex.Field("decimal-field", OptFieldTypeDecimal(3, pql.NewDecimal(7, 3), pql.NewDecimal(999, 3)))
		jsonString := field.options.String()
		targetString := `{"options":{"type":"decimal","scale":3,"max":0.999,"min":0.007}}`
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		compareFieldOptions(t,
			field.Options(),
			FieldTypeDecimal,
			clienttypes.TimeQuantumNone,
			CacheTypeDefault,
			0,
			pql.NewDecimal(7, 3),
			pql.NewDecimal(999, 3),
			"",
			"",
			0)
	})

	t.Run("TimestampFieldOptions", func(t *testing.T) {
		field := sampleIndex.Field("timestamp-field", OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds))

		/*jsonString := field.options.String()
		targetString := `{"options":{"type":"timestamp","timeUnit":"s","max":5000,"min":2}}`
		if sortedString(targetString) != sortedString(jsonString) {
			t.Fatalf("`%s` != `%s`", targetString, jsonString)
		}
		*/
		compareFieldOptions(t,
			field.Options(),
			FieldTypeTimestamp,
			clienttypes.TimeQuantumNone,
			CacheTypeDefault,
			0,
			pql.NewDecimal(MinTimestamp.UnixNano()/TimeUnitNanos(pilosa.TimeUnitSeconds), 0),
			pql.NewDecimal(MaxTimestamp.UnixNano()/TimeUnitNanos(pilosa.TimeUnitSeconds), 0),
			"",
			pilosa.TimeUnitSeconds,
			0)

	})

	t.Run("EncodeMapPanicsOnMarshalFailure", func(t *testing.T) {
		defer func() {
			_ = recover()
		}()
		m := map[string]interface{}{
			"foo": func() {},
		}
		encodeMap(m)
		t.Fatal("Should have panicked")
	})

	t.Run("FormatIDKey", func(t *testing.T) {
		testCase := [][]interface{}{
			{uint(42), "42", nil},
			{uint32(42), "42", nil},
			{uint64(42), "42", nil},
			{42, "42", nil},
			{int32(42), "42", nil},
			{int64(42), "42", nil},
			{"foo", `'foo'`, nil},
			{false, "", errors.New("error")},
		}
		for i, item := range testCase {
			s, err := formatIDKey(item[0])
			if item[2] != nil {
				if err == nil {
					t.Fatalf("Should have failed: %d", i)
				}
				continue
			}
			if item[1] != s {
				t.Fatalf("%s != %s", item[1], s)
			}
		}
	})
}

func comparePQL(t *testing.T, target string, q PQLQuery) {
	t.Helper()
	pql := q.Serialize().String()
	if target != pql {
		t.Fatalf("%s != %s", target, pql)
	}
}

func compareFieldOptions(t *testing.T, opts *FieldOptions, fieldType FieldType, timeQuantum clienttypes.TimeQuantum, cacheType CacheType, cacheSize int, min pql.Decimal, max pql.Decimal, foreignIndex string, timeUnit string, ttl time.Duration) {
	if fieldType != opts.Type() {
		t.Fatalf("%s != %s", fieldType, opts.Type())
	}
	if timeQuantum != opts.TimeQuantum() {
		t.Fatalf("%s != %s", timeQuantum, opts.TimeQuantum())
	}
	if cacheType != opts.CacheType() {
		t.Fatalf("%s != %s", cacheType, opts.CacheType())
	}
	if cacheSize != opts.CacheSize() {
		t.Fatalf("%d != %d", cacheSize, opts.CacheSize())
	}
	if !min.EqualTo(opts.Min()) {
		t.Fatalf("%v != %v", min, opts.Min())
	}
	if !max.EqualTo(opts.Max()) {
		t.Fatalf("%v != %v", max, opts.Max())
	}
	if foreignIndex != opts.ForeignIndex() {
		t.Fatalf("%s != %s", foreignIndex, opts.ForeignIndex())
	}
	if timeUnit != opts.TimeUnit() {
		t.Fatalf("%s != %s", timeUnit, opts.TimeUnit())
	}
	if ttl != opts.TTL() {
		t.Fatalf("%s != %s", ttl, opts.TTL())
	}
}

func sortedString(s string) string {
	arr := strings.Split(s, "")
	sort.Strings(arr)
	return strings.Join(arr, "")
}
