package client

import (
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/test"
)

func TestIngestAPIBatchAdd(t *testing.T) {
	t.Run("unkeyed", func(t *testing.T) {
		batch := NewIngestAPIBatch(nil, 10, logger.NopLogger, []*Field{
			{
				name:  "a",
				index: &Index{name: "idxname", options: &IndexOptions{}},
				options: &FieldOptions{
					fieldType: FieldTypeSet,
				},
			},
			{
				name:  "b",
				index: &Index{name: "idxname", options: &IndexOptions{}},
				options: &FieldOptions{
					fieldType: FieldTypeSet,
					keys:      true,
				},
			},
			{
				name:  "c",
				index: &Index{name: "idxname", options: &IndexOptions{}},
				options: &FieldOptions{
					fieldType: FieldTypeTime,
					keys:      true,
				},
			},
		})
		qt := QuantizedTime{}
		qt.Set(time.Date(2007, time.January, 1, 15, 0, 0, 0, time.UTC))
		err := batch.Add(Row{
			ID:     uint64(1),
			Values: []interface{}{uint64(2), "bkey", "ckey"},
			Time:   qt,
		})
		if err != nil {
			t.Fatalf("adding row to batch: %v", err)
		}

		if batch.records[1]["a"] != uint64(2) {
			t.Fatalf("unexpected batch.records: %+v", batch.records)
		}
		if batch.records[1]["b"] != "bkey" {
			t.Fatalf("unexpected batch.records: %+v", batch.records)
		}
		if batch.records[1]["c"].(map[string]interface{})["time"] != "2007-01-01T15:00:00Z" {
			t.Fatalf("unexpected batch.records: %+v", batch.records)
		}
		if batch.records[1]["c"].(map[string]interface{})["values"] != "ckey" {
			t.Fatalf("unexpected batch.records: %+v", batch.records)
		}

	})

	t.Run("keyed", func(t *testing.T) {
		batch := NewIngestAPIBatch(nil, 10, logger.NopLogger, []*Field{
			{
				name:  "a",
				index: &Index{name: "idxname", options: &IndexOptions{keys: true}},
				options: &FieldOptions{
					fieldType: FieldTypeSet,
				},
			},
			{
				name:  "b",
				index: &Index{name: "idxname", options: &IndexOptions{keys: true}},
				options: &FieldOptions{
					fieldType: FieldTypeSet,
					keys:      true,
				},
			},
			{
				name:  "c",
				index: &Index{name: "idxname", options: &IndexOptions{keys: true}},
				options: &FieldOptions{
					fieldType: FieldTypeTime,
					keys:      true,
				},
			},
		})
		qt := QuantizedTime{}
		qt.Set(time.Date(2007, time.January, 1, 15, 0, 0, 0, time.UTC))
		err := batch.Add(Row{
			ID:     "1",
			Values: []interface{}{uint64(2), "bkey", "ckey"},
			Time:   qt,
		})
		if err != nil {
			t.Fatalf("adding row to batch: %v", err)
		}

		if batch.recordsK["1"]["a"] != uint64(2) {
			t.Fatalf("unexpected batch.records: %+v", batch.recordsK)
		}
		if batch.recordsK["1"]["b"] != "bkey" {
			t.Fatalf("unexpected batch.records: %+v", batch.recordsK)
		}
		if batch.recordsK["1"]["c"].(map[string]interface{})["time"] != "2007-01-01T15:00:00Z" {
			t.Fatalf("unexpected batch.records: %+v", batch.recordsK)
		}
		if batch.recordsK["1"]["c"].(map[string]interface{})["values"] != "ckey" {
			t.Fatalf("unexpected batch.records: %+v", batch.recordsK)
		}

	})
}

func TestIngestAPIBatch(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	urls := make([]string, len(c.Nodes))
	for i, n := range c.Nodes {
		urls[i] = n.URL()
	}

	// Create a new client for the cluster
	cli, err := newClientFromAddresses(urls, &ClientOptions{})
	if err != nil {
		t.Fatalf("getting new client: %v", err)
	}
	defer cli.Close()

	cli.IngestSchema(map[string]interface{}{
		"index-name":       "test-1",
		"index-action":     "create",
		"primary-key-type": "uint",
		"field-action":     "create",
		"fields": []map[string]interface{}{
			{
				"field-name":    "astr",
				"field-type":    "string",
				"field-options": map[string]interface{}{},
			},
			{
				"field-name":    "bint",
				"field-type":    "int",
				"field-options": map[string]interface{}{},
			},
			{
				"field-name":    "cid",
				"field-type":    "id",
				"field-options": map[string]interface{}{},
			},
			{
				"field-name": "dtimestamp",
				"field-type": "timestamp",
				"field-options": map[string]interface{}{
					"unit": "s",
				},
			},
			{
				"field-name": "etime",
				"field-type": "string",
				"field-options": map[string]interface{}{
					"time-quantum": "YMD",
				},
			},
			{
				"field-name": "fdecimal",
				"field-type": "decimal",
				"field-options": map[string]interface{}{
					"scale": 3,
				},
			},
			{
				"field-name":    "gbool",
				"field-type":    "bool",
				"field-options": map[string]interface{}{},
			},
		},
	})

	schema, err := cli.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}
	index := schema.Index("test-1")
	defer cli.DeleteIndex(index)

	batch := NewIngestAPIBatch(cli, 10, logger.NopLogger, []*Field{
		{
			name:    "astr",
			index:   &Index{name: "test-1", options: &IndexOptions{}},
			options: &FieldOptions{fieldType: FieldTypeSet, keys: true},
		},
		{
			name:    "bint",
			options: &FieldOptions{fieldType: FieldTypeInt},
		},
		{
			name:    "cid",
			options: &FieldOptions{fieldType: FieldTypeSet, keys: false},
		},
		{
			name:    "dtimestamp",
			options: &FieldOptions{fieldType: FieldTypeTimestamp},
		},
		{
			name:    "etime",
			options: &FieldOptions{fieldType: FieldTypeTime, keys: true, timeQuantum: TimeQuantumYearMonthDay},
		},
		{
			name:    "fdecimal",
			options: &FieldOptions{fieldType: FieldTypeDecimal, scale: 3},
		},
		{
			name:    "gbool",
			options: &FieldOptions{fieldType: FieldTypeBool},
		},
	})

	qt0 := &QuantizedTime{}
	qt0.Set(time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC))
	if err := batch.Add(Row{
		ID:     uint64(7),
		Values: []interface{}{"a", -2, 9, 1287367623, "e", 1.2345, true},
		Time:   *qt0,
	}); err != nil {
		t.Fatalf("adding row: %v", err)
	}

	if err := batch.Import(); err != nil {
		t.Fatalf("importing row: %v", err)
	}

	if resp, err := cli.Query(NewPQLBaseQuery("Row(astr=a)", &Index{name: "test-1", options: &IndexOptions{}}, nil)); err != nil {
		t.Fatalf("querying: %v", err)
	} else if len(resp.Result().Row().Columns) != 1 || resp.Result().Row().Columns[0] != uint64(7) {
		t.Fatalf("unexpected Row(asr=a) result: %+v", resp.Result().Row().Columns)
	}

	if resp, err := cli.Query(NewPQLBaseQuery("Row(bint==-2)", &Index{name: "test-1", options: &IndexOptions{}}, nil)); err != nil {
		t.Fatalf("querying: %v", err)
	} else if len(resp.Result().Row().Columns) != 1 || resp.Result().Row().Columns[0] != uint64(7) {
		t.Fatalf("unexpected Row(asr=a) result: %+v", resp.Result().Row().Columns)
	}

	if resp, err := cli.Query(NewPQLBaseQuery("Row(cid=9)", &Index{name: "test-1", options: &IndexOptions{}}, nil)); err != nil {
		t.Fatalf("querying: %v", err)
	} else if len(resp.Result().Row().Columns) != 1 || resp.Result().Row().Columns[0] != uint64(7) {
		t.Fatalf("unexpected Row(asr=a) result: %+v", resp.Result().Row().Columns)
	}

	if resp, err := cli.Query(NewPQLBaseQuery("Row(dtimestamp=='2010-10-18T02:07:03Z')", &Index{name: "test-1", options: &IndexOptions{}}, nil)); err != nil {
		t.Fatalf("querying: %v", err)
	} else if len(resp.Result().Row().Columns) != 1 || resp.Result().Row().Columns[0] != uint64(7) {
		t.Fatalf("unexpected Row(asr=a) result: %+v", resp.Result().Row().Columns)
	}

	if resp, err := cli.Query(NewPQLBaseQuery("Row(etime=e, from='2010-01-01', to='2010-01-02')", &Index{name: "test-1", options: &IndexOptions{}}, nil)); err != nil {
		t.Fatalf("querying: %v", err)
	} else if len(resp.Result().Row().Columns) != 1 || resp.Result().Row().Columns[0] != uint64(7) {
		t.Fatalf("unexpected Row(asr=a) result: %+v", resp.Result().Row().Columns)
	}

	if resp, err := cli.Query(NewPQLBaseQuery("Row(fdecimal==1.234)", &Index{name: "test-1", options: &IndexOptions{}}, nil)); err != nil {
		t.Fatalf("querying: %v", err)
	} else if len(resp.Result().Row().Columns) != 1 || resp.Result().Row().Columns[0] != uint64(7) {
		t.Fatalf("unexpected Row(asr=a) result: %+v", resp.Result().Row().Columns)
	}

	if resp, err := cli.Query(NewPQLBaseQuery("Row(gbool=true)", &Index{name: "test-1", options: &IndexOptions{}}, nil)); err != nil {
		t.Fatalf("querying: %v", err)
	} else if len(resp.Result().Row().Columns) != 1 || resp.Result().Row().Columns[0] != uint64(7) {
		t.Fatalf("unexpected Row(asr=a) result: %+v", resp.Result().Row().Columns)
	}

}
