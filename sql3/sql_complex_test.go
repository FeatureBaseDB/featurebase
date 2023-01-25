// Copyright 2021 Molecula Corp. All rights reserved.
package sql3_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/parquet"
	"github.com/apache/arrow/go/v10/parquet/pqarrow"
	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/pql"
	sql_test "github.com/featurebasedb/featurebase/v3/sql3/test"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestPlanner_Misc(t *testing.T) {
	d, err := pql.ParseDecimal("12.345678")
	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, d.EqualTo(pql.NewDecimal(12345678, 6)))

	d, err = pql.FromFloat64WithScale(12.345678, 6)
	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, d.EqualTo(pql.NewDecimal(12345678, 6)))
}

func TestPlanner_SystemTableFanout(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	server := c.GetNode(0).Server

	t.Run("PerfCounters", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, server, `select * from fb_performance_counters`)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 15 {
			t.Fatal(fmt.Errorf("unexpected result set length"))
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString("nodeid"),
			wireQueryFieldString("namespace"),
			wireQueryFieldString("subsystem"),
			wireQueryFieldString("counter_name"),
			wireQueryFieldInt("value"),
			wireQueryFieldInt("counter_type"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("SystemTablesExecRequests", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `select * from fb_exec_requests`)
		if err != nil {
			t.Fatal(err)
		}

		if len(results) != 2 {
			t.Fatal(fmt.Errorf("unexpected result set length"))
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString("nodeid"),
			wireQueryFieldString("request_id"),
			wireQueryFieldString("user"),
			wireQueryFieldTimestamp("start_time"),
			wireQueryFieldTimestamp("end_time"),
			wireQueryFieldString("status"),
			wireQueryFieldString("wait_type"),
			wireQueryFieldInt("wait_time"),
			wireQueryFieldString("wait_resource"),
			wireQueryFieldInt("cpu_time"),
			wireQueryFieldInt("elapsed_time"),
			wireQueryFieldInt("reads"),
			wireQueryFieldInt("writes"),
			wireQueryFieldInt("logical_reads"),
			wireQueryFieldInt("row_count"),
			wireQueryFieldString("sql"),
			wireQueryFieldString("plan"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("SystemTablesExecRequestsAgg", func(t *testing.T) {
		_, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `select 
		count(request_id) as request_count,
		min(elapsed_time) as min_duration,
		max(elapsed_time) as max_duration,
		avg(elapsed_time) as avg_duration
	from 
		fb_exec_requests 
	where 
		status = 'complete';`)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("request_count"),
			wireQueryFieldInt("min_duration"),
			wireQueryFieldInt("max_duration"),
			wireQueryFieldDecimal("avg_duration", 4),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestPlanner_Show(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	index, err := c.GetHolder(0).CreateIndex(c.Idx("i"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := index.CreateField("f", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := index.CreateField("x", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	index2, err := c.GetHolder(0).CreateIndex(c.Idx("l"), "", pilosa.IndexOptions{TrackExistence: false})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := index2.CreateField("f", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := index2.CreateField("x", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	t.Run("SystemTablesInfo", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `select name, platform, platform_version, db_version, state, node_count, replica_count from fb_cluster_info`)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatal(fmt.Errorf("unexpected result set length"))
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString("name"),
			wireQueryFieldString("platform"),
			wireQueryFieldString("platform_version"),
			wireQueryFieldString("db_version"),
			wireQueryFieldString("state"),
			wireQueryFieldInt("node_count"),
			wireQueryFieldInt("replica_count"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("SystemTablesNode", func(t *testing.T) {
		_, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `select * from fb_cluster_nodes`)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString("id"),
			wireQueryFieldString("state"),
			wireQueryFieldString("uri"),
			wireQueryFieldString("grpc_uri"),
			wireQueryFieldBool("is_primary"),
			wireQueryFieldBool("space_used"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ShowTables", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `SHOW TABLES`)
		if err != nil {
			t.Fatal(err)
		}

		// we keep adding system tables on the fly so as
		// long as we get more than 0 tables, we're good
		if len(results) == 0 {
			t.Fatal(fmt.Errorf("unexpected result set length"))
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString("_id"),
			wireQueryFieldString("name"),
			wireQueryFieldString("owner"),
			wireQueryFieldString("updated_by"),
			wireQueryFieldTimestamp("created_at"),
			wireQueryFieldTimestamp("updated_at"),
			wireQueryFieldBool("keys"),
			wireQueryFieldInt("space_used"),
			wireQueryFieldString("description"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ShowCreateTable", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SHOW CREATE TABLE %i`, c))
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatal(fmt.Errorf("unexpected result set length: %d", len(results)))
		}

		if diff := cmp.Diff([][]interface{}{
			{string("create table testplannershowi (_id id, f int min 0 max 1000, x int min 0 max 1000);")},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString("ddl"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ShowCreateTableCacheTypes", func(t *testing.T) {
		_, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table iris1 (
			_id id,
			speciesid id cachetype ranked size 1000
			species string cachetype ranked size 1000
			speciesids idset cachetype ranked size 1000
			speciess stringset cachetype ranked size 1000
			speciesidsq idset timequantum 'YMD'
			speciessq stringset timequantum 'YMD'
			) keypartitions 12
		`)
		if err != nil {
			t.Fatal(err)
		}

		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `SHOW CREATE TABLE iris1`)
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 {
			t.Fatal(fmt.Errorf("unexpected result set length: %d", len(results)))
		}

		if diff := cmp.Diff([][]interface{}{
			{string("create table iris1 (_id id, speciesid id cachetype ranked size 1000, species string cachetype ranked size 1000, speciesids idset cachetype ranked size 1000, speciess stringset cachetype ranked size 1000, speciesidsq idset timequantum 'YMD', speciessq stringset timequantum 'YMD');")},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString("ddl"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ShowColumns", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SHOW COLUMNS FROM %i`, c))
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Fatal(fmt.Errorf("unexpected result set length: %d", len(results)))
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString("_id"),
			wireQueryFieldString("name"),
			wireQueryFieldString("type"),
			wireQueryFieldString("internal_type"),
			wireQueryFieldTimestamp("created_at"),
			wireQueryFieldBool("keys"),
			wireQueryFieldString("cache_type"),
			wireQueryFieldInt("cache_size"),
			wireQueryFieldInt("scale"),
			wireQueryFieldInt("min"),
			wireQueryFieldInt("max"),
			wireQueryFieldString("timeunit"),
			wireQueryFieldInt("epoch"),
			wireQueryFieldString("timequantum"),
			wireQueryFieldString("ttl"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ShowColumns2", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SHOW COLUMNS FROM %l`, c))
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 3 {
			t.Fatal(fmt.Errorf("unexpected result set length"))
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString("_id"),
			wireQueryFieldString("name"),
			wireQueryFieldString("type"),
			wireQueryFieldString("internal_type"),
			wireQueryFieldTimestamp("created_at"),
			wireQueryFieldBool("keys"),
			wireQueryFieldString("cache_type"),
			wireQueryFieldInt("cache_size"),
			wireQueryFieldInt("scale"),
			wireQueryFieldInt("min"),
			wireQueryFieldInt("max"),
			wireQueryFieldString("timeunit"),
			wireQueryFieldInt("epoch"),
			wireQueryFieldString("timequantum"),
			wireQueryFieldString("ttl"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ShowColumnsFromNotATable", func(t *testing.T) {
		_, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `SHOW COLUMNS FROM foo`)
		if err != nil {
			if err.Error() != "[1:19] table 'foo' not found" {
				t.Fatal(err)
			}
		}
	})
}

func TestPlanner_CoverCreateTable(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	server := c.GetNode(0).Server

	t.Run("Invalid", func(t *testing.T) {
		tableName := "invalidfieldcontraints"

		fields := []struct {
			name        string
			typ         string
			constraints string
			expErr      string
		}{
			{
				name:        "stringsetcolq",
				typ:         "stringset",
				constraints: "cachetype lru size 1000 timequantum 'YMD' ttl '24h'",
				expErr:      "[1:60] 'CACHETYPE' constraint conflicts with 'TIMEQUANTUM'",
			},
			{
				name:        "stringsetcolq",
				typ:         "stringset",
				constraints: "timequantum 'YMD' ttl '24h' cachetype ranked",
				expErr:      "[1:60] 'CACHETYPE' constraint conflicts with 'TIMEQUANTUM'",
			},
		}

		for i, fld := range fields {
			if fld.name == "" {
				t.Fatalf("field name at slice index %d is blank", i)
			}
			if fld.typ == "" {
				t.Fatalf("field type at slice index %d is blank", i)
			}

			// Build the create table statement based on the fields slice above.
			sql := "create table " + tableName + "_" + fld.name + " (_id id, "
			sql += fld.name + " " + fld.typ + " " + fld.constraints
			sql += `) keypartitions 12`

			// Run the create table statement.
			_, _, err := sql_test.MustQueryRows(t, server, sql)
			if assert.Error(t, err) {
				assert.Equal(t, fld.expErr, err.Error())
				// sql3.SQLErrConflictingColumnConstraint.Message
			}
		}
	})

	t.Run("Valid", func(t *testing.T) {
		tableName := "validfieldcontraints"

		fields := []struct {
			name        string
			typ         string
			constraints string
			expOptions  pilosa.FieldOptions
		}{
			{
				name: "_id",
				typ:  "id",
			},
			{
				name:        "intcol",
				typ:         "int",
				constraints: "min 100 max 10000",
				expOptions: pilosa.FieldOptions{
					Type: "int",
					Base: 100,
					Min:  pql.NewDecimal(100, 0),
					Max:  pql.NewDecimal(10000, 0),
				},
			},
			{
				name:        "boolcol",
				typ:         "bool",
				constraints: "",
				expOptions: pilosa.FieldOptions{
					Type: "bool",
				},
			},
			{
				name:        "timestampcol",
				typ:         "timestamp",
				constraints: "timeunit 'ms' epoch '2021-01-01T00:00:00Z'",
				expOptions: pilosa.FieldOptions{
					Base:     1609459200000,
					Type:     "timestamp",
					TimeUnit: "ms",
					Min:      pql.NewDecimal(-63745055999000, 0),
					Max:      pql.NewDecimal(251792841599000, 0),
				},
			},
			{
				name:        "decimalcol",
				typ:         "decimal(2)",
				constraints: "",
				expOptions: pilosa.FieldOptions{
					Type:  "decimal",
					Scale: 2,
					Min:   pql.NewDecimal(-9223372036854775808, 2),
					Max:   pql.NewDecimal(9223372036854775807, 2),
				},
			},
			{
				name:        "stringcol",
				typ:         "string",
				constraints: "cachetype ranked size 1000",
				expOptions: pilosa.FieldOptions{
					Type:      "mutex",
					Keys:      true,
					CacheType: "ranked",
					CacheSize: 1000,
				},
			},
			{
				name:        "stringsetcol",
				typ:         "stringset",
				constraints: "cachetype lru size 1000",
				expOptions: pilosa.FieldOptions{
					Type:      "set",
					Keys:      true,
					CacheType: "lru",
					CacheSize: 1000,
				},
			},
			{
				name:        "stringsetcolq",
				typ:         "stringset",
				constraints: "timequantum 'YMD' ttl '24h'",
				expOptions: pilosa.FieldOptions{
					Type:        "time",
					Keys:        true,
					CacheType:   "",
					CacheSize:   0,
					TimeQuantum: "YMD",
					TTL:         time.Duration(24 * time.Hour),
				},
			},
			{
				name:        "idcol",
				typ:         "id",
				constraints: "cachetype ranked size 1000",
				expOptions: pilosa.FieldOptions{
					Type:      "mutex",
					Keys:      false,
					CacheType: "ranked",
					CacheSize: 1000,
				},
			},
			{
				name:        "idsetcol",
				typ:         "idset",
				constraints: "cachetype lru",
				expOptions: pilosa.FieldOptions{
					Type:      "set",
					Keys:      false,
					CacheType: "lru",
					CacheSize: pilosa.DefaultCacheSize,
				},
			},
			{
				name:        "idsetcolsz",
				typ:         "idset",
				constraints: "cachetype lru size 1000",
				expOptions: pilosa.FieldOptions{
					Type:      "set",
					Keys:      false,
					CacheType: "lru",
					CacheSize: 1000,
				},
			},
			{
				name:        "idsetcolq",
				typ:         "idset",
				constraints: "timequantum 'YMD' ttl '24h'",
				expOptions: pilosa.FieldOptions{
					Type:        "time",
					Keys:        false,
					CacheType:   "",
					CacheSize:   0,
					TimeQuantum: "YMD",
					TTL:         time.Duration(24 * time.Hour),
				},
			},
		}

		// Build the create table statement based on the fields slice above.
		sql := "create table " + tableName + " ("
		fieldDefs := make([]string, len(fields))
		for i, fld := range fields {
			if fld.name == "" {
				t.Fatalf("field name at slice index %d is blank", i)
			}
			if fld.typ == "" {
				t.Fatalf("field type at slice index %d is blank", i)
			}
			fieldDefs[i] = fld.name + " " + fld.typ
			if fld.constraints != "" {
				fieldDefs[i] += " " + fld.constraints
			}
		}
		sql += strings.Join(fieldDefs, ", ")
		sql += `) keypartitions 12`

		// Run the create table statement.
		results, columns, err := sql_test.MustQueryRows(t, server, sql)
		assert.NoError(t, err)
		assert.Equal(t, [][]interface{}{}, results)
		assert.Equal(t, []*pilosa.WireQueryField{}, columns)

		// Ensure that the fields got created as expected.
		t.Run("EnsureFields", func(t *testing.T) {
			api := c.GetNode(0).API
			ctx := context.Background()

			schema, err := api.Schema(ctx, false)
			assert.NoError(t, err)
			// spew.Dump(schema)

			// Get the fields from the FeatureBase schema.
			// fbFields is a map of fieldName to FieldInfo.
			var fbFields map[string]*pilosa.FieldInfo
			var tableKeys bool
			for _, idx := range schema {
				if idx.Name == tableName {
					tableKeys = idx.Options.Keys
					fbFields = make(map[string]*pilosa.FieldInfo)
					for _, fld := range idx.Fields {
						fbFields[fld.Name] = fld
					}
				}
			}
			assert.NotNil(t, fbFields)

			// Ensure the schema field options match the expected options.
			for _, fld := range fields {
				t.Run(fmt.Sprintf("Field:%s", fld.name), func(t *testing.T) {
					// Field fmt.Sprintf(`_id`, c) isn't returned from FeatureBase in the schema,
					// but we do want to validate that its type is used to determine
					// whether or not the table is keyed.
					if fld.name == "_id" {
						switch fld.typ {
						case "id":
							assert.False(t, tableKeys)
						case "string":
							assert.True(t, tableKeys)
						default:
							t.Fatalf("invalid _id type: %s", fld.typ)
						}
						return
					}

					fbField, ok := fbFields[fld.name]
					assert.True(t, ok, "expected field: %s", fld.name)
					assert.Equal(t, fld.expOptions, fbField.Options)
				})
			}
		})
	})
}

func TestPlanner_CreateTable(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	server := c.GetNode(0).Server

	t.Run("CreateTableAllDataTypes", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, server, `create table allcoltypes (
			_id id,
			intcol int, 
			boolcol bool, 
			timestampcol timestamp, 
			decimalcol decimal(2), 
			stringcol string, 
			stringsetcol stringset, 
			idcol id, 
			idsetcol idset) keypartitions 12`)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([][]interface{}{}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("CreateTableAllDataTypesAgain", func(t *testing.T) {
		_, _, err := sql_test.MustQueryRows(t, server, `create table allcoltypes (
			_id id,
			intcol int, 
			boolcol bool, 
			timestampcol timestamp, 
			decimalcol decimal(2), 
			stringcol string, 
			stringsetcol stringset, 
			idcol id, 
			idsetcol idset) keypartitions 12`)
		if err == nil {
			t.Fatal("expected error")
		} else {
			if err.Error() != "[0:0] table 'allcoltypes' already exists" {
				t.Fatal(err)
			}
		}
	})

	t.Run("DropTable1", func(t *testing.T) {
		_, _, err := sql_test.MustQueryRows(t, server, `drop table allcoltypes`)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("CreateTableAllDataTypesAllConstraints", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, server, `create table allcoltypes (
			_id id,
			intcol int min 0 max 10000,
			boolcol bool,
			timestampcol timestamp timeunit 'ms' epoch '2010-01-01T00:00:00Z',
			decimalcol decimal(2),
			stringcol string cachetype ranked size 1000,
			stringsetcol stringset cachetype lru size 1000,
			stringsetcolq stringset timequantum 'YMD' ttl '24h',
			idcol id cachetype ranked size 1000,
			idsetcol idset cachetype lru,
			idsetcolsz idset cachetype lru size 1000,
			idsetcolq idset timequantum 'YMD' ttl '24h') keypartitions 12`)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([][]interface{}{}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ShowColumns1", func(t *testing.T) {
		_, columns, err := sql_test.MustQueryRows(t, server, `SHOW COLUMNS FROM allcoltypes`)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString("_id"),
			wireQueryFieldString("name"),
			wireQueryFieldString("type"),
			wireQueryFieldString("internal_type"),
			wireQueryFieldTimestamp("created_at"),
			wireQueryFieldBool("keys"),
			wireQueryFieldString("cache_type"),
			wireQueryFieldInt("cache_size"),
			wireQueryFieldInt("scale"),
			wireQueryFieldInt("min"),
			wireQueryFieldInt("max"),
			wireQueryFieldString("timeunit"),
			wireQueryFieldInt("epoch"),
			wireQueryFieldString("timequantum"),
			wireQueryFieldString("ttl"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("CreateTableDupeColumns", func(t *testing.T) {
		_, _, err := sql_test.MustQueryRows(t, server, `create table dupecols (
			_id id,
			_id int)`)
		if err == nil {
			t.Fatal("expected error")
		} else {
			if err.Error() != "[3:4] duplicate column '_id'" {
				t.Fatal(err)
			}
		}
	})

	t.Run("CreateTableMissingId", func(t *testing.T) {
		_, _, err := sql_test.MustQueryRows(t, server, `create table missingid (
			foo int)`)
		if err == nil {
			t.Fatal("expected error")
		} else {
			if err.Error() != "[1:1] _id column must be specified" {
				t.Fatal(err)
			}
		}
	})
}

func TestPlanner_AlterTable(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	index, err := c.GetHolder(0).CreateIndex(c.Idx("i"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := index.CreateField("f", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := index.CreateField("x", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	server := c.GetNode(0).Server

	t.Run("AlterTableDrop", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, server, fmt.Sprintf(`alter table %i drop column f`, c))
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([][]interface{}{}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("AlterTableAdd", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, server, fmt.Sprintf(`alter table %i add column f int`, c))
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([][]interface{}{}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("AlterTableRename", func(t *testing.T) {
		t.Skip("not yet implemented")
		results, columns, err := sql_test.MustQueryRows(t, server, fmt.Sprintf(`alter table %i rename column f to g`, c))
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([][]interface{}{}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestPlanner_DropTable(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	index, err := c.GetHolder(0).CreateIndex(c.Idx("i"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := index.CreateField("f", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := index.CreateField("x", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	t.Run("DropTable", func(t *testing.T) {
		_, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`DROP TABLE %i`, c))
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestPlanner_ExpressionsInSelectListParen(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex(c.Idx("j"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i0.CreateField("a", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("b", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	i1, err := c.GetHolder(0).CreateIndex(c.Idx("k"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i1.CreateField("x", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i1.CreateField("y", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("j"),
		Query: `
			Set(1, a=10)
			Set(1, b=100)
			Set(2, a=20)
			Set(2, b=200)
	`,
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("ParenOne", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT (a != b) = false, _id FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{bool(false), int64(1)},
			{bool(false), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldBool(""),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ParenTwo", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT (a != b) = (false), _id FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{bool(false), int64(1)},
			{bool(false), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldBool(""),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestPlanner_ExpressionsInSelectListLiterals(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex(c.Idx("j"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i0.CreateField("a", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("b", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("d", "", pilosa.OptFieldTypeDecimal(2)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("ts", "", pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, "s")); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("str", "", pilosa.OptFieldTypeMutex(pilosa.CacheTypeLRU, pilosa.DefaultCacheSize), pilosa.OptFieldKeys()); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("j"),
		Query: `
			Set(1, a=10)
			Set(1, b=100)
			Set(2, a=20)
			Set(2, b=200)
			Set(1, d=10.3)
			Set(1, ts='2022-02-22T22:22:22Z')
			Set(1, str='foo')
	`,
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("LiteralsBool", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT false = true, _id FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{bool(false), int64(1)},
			{bool(false), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldBool(""),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("LiteralsInt", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT 1 + 2, _id FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(3), int64(1)},
			{int64(3), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt(""),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("LiteralsID", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT _id + 2, _id FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(3), int64(1)},
			{int64(4), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt(""),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("LiteralsDecimal", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT d + 2.0, _id FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		opt := cmp.Comparer(func(x, y pql.Decimal) bool {
			return x.EqualTo(y)
		})

		if diff := cmp.Diff([][]interface{}{
			{pql.NewDecimal(1230, 2), int64(1)},
			{nil, int64(2)},
		}, results, opt); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldDecimal("", 2),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("LiteralsString", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT str || ' bar', _id FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{string("foo bar"), int64(1)},
			{nil, int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldString(""),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestPlanner_ExpressionsInSelectListCase(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex(c.Idx("j"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i0.CreateField("a", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("b", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("d", "", pilosa.OptFieldTypeDecimal(2)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("ts", "", pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, "s")); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("str", "", pilosa.OptFieldTypeMutex(pilosa.CacheTypeLRU, pilosa.DefaultCacheSize), pilosa.OptFieldKeys()); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("j"),
		Query: `
			Set(1, a=10)
			Set(1, b=100)
			Set(2, a=20)
			Set(2, b=200)
			Set(1, d=10.3)
			Set(1, ts='2022-02-22T22:22:22Z')
			Set(1, str='foo')
	`,
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("CaseWithBase", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT b, case b when 100 then 10 when 201 then 20 else 5 end, _id FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(100), int64(10), int64(1)},
			{int64(200), int64(5), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("b"),
			wireQueryFieldInt(""),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("CaseWithNoBase", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT b, case when b = 100 then 10 when b = 201 then 20 else 5 end, _id FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(100), int64(10), int64(1)},
			{int64(200), int64(5), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("b"),
			wireQueryFieldInt(""),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestPlanner_Select(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex(c.Idx("j"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i0.CreateField("a", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("b", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	i1, err := c.GetHolder(0).CreateIndex(c.Idx("k"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i1.CreateField("x", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i1.CreateField("y", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("j"),
		Query: `
			Set(1, a=10)
			Set(1, b=100)
			Set(2, a=20)
			Set(2, b=200)
	`,
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("UnqualifiedColumns", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT a, b, _id FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(10), int64(100), int64(1)},
			{int64(20), int64(200), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("QualifiedTableRef", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT bar.a, bar.b, bar._id FROM %j as bar`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(10), int64(100), int64(1)},
			{int64(20), int64(200), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("AliasedUnqualifiedColumns", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT a as foo, b as bar, _id as baz FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(10), int64(100), int64(1)},
			{int64(20), int64(200), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("foo"),
			wireQueryFieldInt("bar"),
			wireQueryFieldID("baz"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("QualifiedColumns", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT %j._id, %j.a, %j.b FROM %j`, c, c, c, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(1), int64(10), int64(100)},
			{int64(2), int64(20), int64(200)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldID("_id"),
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("UnqualifiedStar", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT * FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(1), int64(10), int64(100)},
			{int64(2), int64(20), int64(200)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldID("_id"),
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("QualifiedStar", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT %j.* FROM %j`, c, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(1), int64(10), int64(100)},
			{int64(2), int64(20), int64(200)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldID("_id"),
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("NoIdentifier", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT a, b FROM %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(10), int64(100)},
			{int64(20), int64(200)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		_, err := c.GetNode(0).Server.CompileExecutionPlan(context.Background(), fmt.Sprintf(`SELECT xyz FROM %j`, c))
		if err == nil || !strings.Contains(err.Error(), `column 'xyz' not found`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestPlanner_SelectOrderBy(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex(c.Idx("j"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i0.CreateField("a", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("b", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("j"),
		Query: `
			Set(1, a=10)
			Set(1, b=100)
			Set(2, a=20)
			Set(2, b=200)
	`,
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("OrderBy", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT a, b, _id FROM %j order by a desc`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(20), int64(200), int64(2)},
			{int64(10), int64(100), int64(1)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestPlanner_BulkInsert(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	_, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, "create table j (_id id, a int, b int)")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, "create table j1 (_id id, a int, b int)")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, "create table j2 (_id id, a int, b int)")
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table alltypes (
		_id id,
		id1 id,
		i1 int,
		ids1 idset,
		ss1 stringset,
		ts1 timestamp,
		s1 string,
		b1 bool,
		d1 decimal(2)
	)`)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("BulkBadMap", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0, 1 int, 2 int) from '/Users/bar/foo.csv';`)
		if err == nil || !strings.Contains(err.Error(), `expected type name, found ','`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkNoWith", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int) from '/Users/bar/foo.csv';`)
		if err == nil || !strings.Contains(err.Error(), ` expected WITH, found ';'`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkBadWith", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int) from '/Users/bar/foo.csv' WITH UNICORNS AND RAINBOWS;`)
		if err == nil || !strings.Contains(err.Error(), `expected BATCHSIZE, ROWSLIMIT, FORMAT, INPUT, ALLOW_MISSING_VALUES or HEADER_ROW, found UNICORNS`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkNoWithFormat", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int) from '/Users/bar/foo.csv' with batchsize 2;`)
		if err == nil || !strings.Contains(err.Error(), `format specifier expected`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkBadWithFormat", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int) from '/Users/bar/foo.csv' WITH FORMAT 'BLAH';`)
		if err == nil || !strings.Contains(err.Error(), `invalid format specifier 'BLAH'`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkNoWithInput", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int) from '/Users/bar/foo.csv' WITH FORMAT 'CSV';`)
		if err == nil || !strings.Contains(err.Error(), `input specifier expected`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkBadWithInput", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int) from '/Users/bar/foo.csv' WITH FORMAT 'CSV' INPUT 'WOOPWOOP';`)
		if err == nil || !strings.Contains(err.Error(), `invalid input specifier 'WOOPWOOP'`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkBadTable", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into foo (_id, a, b) map (0 id, 1 int, 2 int) from '/Users/bar/foo.csv' WITH FORMAT 'CSV' INPUT 'FILE';`)
		if err == nil || !strings.Contains(err.Error(), `table 'foo' not found`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkNoID", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (a, b) map (0 int, 1 int) from '/Users/bar/foo.csv' WITH FORMAT 'CSV' INPUT 'FILE';`)
		if err == nil || !strings.Contains(err.Error(), `insert column list must have '_id' column specified`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkNoNonID", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id) map (0 id) from '/Users/bar/foo.csv' WITH FORMAT 'CSV' INPUT 'FILE';`)
		if err == nil || !strings.Contains(err.Error(), `insert column list must have at least one non '_id' column specified`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkBadColumn", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, k, l) map (0 id, 1 int, 2 int) from '/Users/bar/foo.csv' WITH FORMAT 'CSV' INPUT 'FILE';`)
		if err == nil || !strings.Contains(err.Error(), `column 'k' not found`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkMapCountMismatch", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int) from '/Users/bar/foo.csv' WITH FORMAT 'CSV' INPUT 'FILE';`)
		if err == nil || !strings.Contains(err.Error(), `mismatch in the count of expressions and target columns`) {
			t.Fatalf("unexpected error: %v", err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int, 3 int) from '/Users/bar/foo.csv' WITH FORMAT 'CSV' INPUT 'FILE';`)
		if err == nil || !strings.Contains(err.Error(), `mismatch in the count of expressions and target columns`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkCSVFileNonExistent", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int) from '/Users/bar/foo.csv' WITH FORMAT 'CSV' INPUT 'FILE';`)
		if err == nil || !strings.Contains(err.Error(), `unable to read datasource '/Users/bar/foo.csv': file '/Users/bar/foo.csv' does not exist`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkCSVFileWithHeaderDefault", func(t *testing.T) {
		tmpfile, err := os.CreateTemp("", "BulkCSVFileWithHeaderDefault.*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())

		content := []byte("\"_id\",\"a\",\"b\"\n1,10,20\n2,11,21\n3,12,22\n4,13,23\n5,13,23\n6,13,23\n7,13,23\n8,13,23\n9,13,23\n10,13,23")

		if _, err := tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err := tmpfile.Close(); err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`bulk insert into j1 (_id, a, b) map (0 id, 1 int, 2 int) from '%s' WITH FORMAT 'CSV' INPUT 'FILE';`, tmpfile.Name()))
		if err == nil || !strings.Contains(err.Error(), `value '_id' cannot be converted to type 'id'`) {
			t.Fatalf("unexpected error: %v", err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`bulk insert into j1 (_id, a, b) map (0 id, 1 int, 2 int) from '%s' WITH FORMAT 'CSV' INPUT 'FILE' HEADER_ROW;`, tmpfile.Name()))
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BulkCSVBadMap", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int, 10 int) from x'1,10,20
		2,11,21
		3,12,22
		4,13,23
		5,13,23
		6,13,23
		7,13,23
		8,13,23
		9,13,23
		10,13,23' WITH FORMAT 'CSV' INPUT 'STREAM';`)
		if err == nil || !strings.Contains(err.Error(), `map index 10 out of range`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkCSVFileDefault", func(t *testing.T) {
		tmpfile, err := os.CreateTemp("", "BulkCSVFileDefault.*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())

		content := []byte("1,10,20\n2,11,21\n3,12,22\n4,13,23\n5,13,23\n6,13,23\n7,13,23\n8,13,23\n9,13,23\n10,13,23")

		if _, err := tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err := tmpfile.Close(); err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int) from '%s' WITH FORMAT 'CSV' INPUT 'FILE';`, tmpfile.Name()))
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BulkCSVFileNoColumns", func(t *testing.T) {
		tmpfile, err := os.CreateTemp("", "BulkCSVFileNoColumns.*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())

		content := []byte("1,10,20\n2,11,21\n3,12,22\n4,13,23\n5,13,23\n6,13,23\n7,13,23\n8,13,23\n9,13,23\n10,13,23")

		if _, err := tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err := tmpfile.Close(); err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`bulk insert into j map (0 id, 1 int, 2 int) from '%s' WITH FORMAT 'CSV' INPUT 'FILE';`, tmpfile.Name()))
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BulkCSVFileBadBatchSize", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int) from '/foo/bar' WITH FORMAT 'CSV' INPUT 'FILE' BATCHSIZE 0;`)
		if err == nil || !strings.Contains(err.Error(), `invalid batch size '0'`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkCSVFileRowsLimit", func(t *testing.T) {
		tmpfile, err := os.CreateTemp("", "BulkCSVFileDefault.*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())

		content := []byte("1,10,20\n2,11,21\n3,12,22\n4,13,23\n5,13,23\n6,13,23\n7,13,23\n8,13,23\n9,13,23\n10,13,23")

		if _, err := tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err := tmpfile.Close(); err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`bulk insert into j2 (_id, a, b) map (0 id, 1 int, 2 int) from '%s' WITH FORMAT 'CSV' INPUT 'FILE' ROWSLIMIT 2;`, tmpfile.Name()))
		if err != nil {
			t.Fatal(err)
		}

		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `SELECT count(*) from j2`)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt(""),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("BulkCSVBlobDefault", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, "bulk insert into j (_id, a, b) map (0 id, 1 int, 2 int) from x'1,10,20\n2,11,21\n3,12,22\n4,13,23\n5,13,23\n6,13,23\n7,13,23\n8,13,23\n9,13,23\n10,13,23' WITH FORMAT 'CSV' INPUT 'STREAM';")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BulkNDJsonBlobDefault", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map ('$._id' id, '$.a' int, '$.b' int) 
		from  x'{ "_id": 1, "a": 10, "b": 20  }
		{ "_id": 2, "a": 10, "b": 20  }
		{ "_id": 3, "a": 10, "b": 20  }
		{ "_id": 4, "a": 10, "b": 20  }
		{ "_id": 5, "a": 10, "b": 20  }
		{ "_id": 6, "a": 10, "b": 20  }
		{ "_id": 7, "a": 10, "b": 20  }
		{ "_id": 8, "a": 10, "b": 20  }
		{ "_id": 9, "a": 10, "b": 20  }
		{ "_id": 10, "a": 13, "b": 23  }'  WITH FORMAT 'NDJSON' INPUT 'STREAM';`)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BulkNDJsonBlobBadPath", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert into j (_id, a, b) map ('$._id' id, '$.a' int, '$.frobny' int) 
		from  x'{ "_id": 1, "a": 10, "b": 20  }
		{ "_id": 2, "a": 10, "b": 20  }
		{ "_id": 3, "a": 10, "b": 20  }
		{ "_id": 4, "a": 10, "b": 20  }
		{ "_id": 5, "a": 10, "b": 20  }
		{ "_id": 6, "a": 10, "b": 20  }
		{ "_id": 7, "a": 10, "b": 20  }
		{ "_id": 8, "a": 10, "b": 20  }
		{ "_id": 9, "a": 10, "b": 20  }
		{ "_id": 10, "a": 13, "b": 23  }'  WITH FORMAT 'NDJSON' INPUT 'STREAM';`)
		if err == nil || !strings.Contains(err.Error(), `unknown key frobny`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkNDJsonFileDefault", func(t *testing.T) {
		tmpfile, err := os.CreateTemp("", "BulkNDJsonFileDefault.*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())

		content := []byte(`{ "_id": 1, "a": 10, "b": 20  }
		{ "_id": 2, "a": 10, "b": 20  }`)

		if _, err := tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err := tmpfile.Close(); err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`bulk insert into j (_id, a, b) map ('$._id' id, '$.a' int, '$.b' int) from '%s' WITH FORMAT 'NDJSON' INPUT 'FILE';`, tmpfile.Name()))
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BulkNDJsonFileTransform", func(t *testing.T) {
		tmpfile, err := os.CreateTemp("", "BulkNDJsonFileTransform.*.csv")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())

		content := []byte(`{ "_id": 1, "a": 10, "b": 20  }
		{ "_id": 2, "a": 10, "b": 20  }`)

		if _, err := tmpfile.Write(content); err != nil {
			t.Fatal(err)
		}
		if err := tmpfile.Close(); err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`bulk insert into j (_id, a, b) map ('$._id' id, '$.a' int, '$.b' int) transform (@0, @1, @2) from '%s' WITH FORMAT 'NDJSON' INPUT 'FILE';`, tmpfile.Name()))
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BulkNDJsonAllTypes", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert 
		into alltypes (_id, id1, i1, ids1, ss1, ts1, s1, b1, d1) 
	
		map ('$._id' id, '$.id1' id, '$.i1' int, '$.ids1' idset, '$.ss1' stringset, '$.ts1' timestamp, '$.s1' string, '$.b1' bool, '$.d1' decimal(2))
	
	from 
		x'{ "_id": 1, "id1": 10, "i1": 11,  "ids1": [ 3, 4, 5 ], "ss1": [ "foo", "bar" ], "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": true, "d1": 11.34 }
		{ "_id": 2, "id1": 10, "i1": 11,  "ids1": [ 3, 4, 5 ], "ss1": [ "foo", "bar" ], "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": 0, "d1": 11.34 }
		{ "_id": 3, "id1": 10, "i1": 11,  "ids1": [ 3, 4, 5 ], "ss1": [ "foo", "bar" ], "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": 1, "d1": 11.34 }
		{ "_id": 4, "id1": 10, "i1": 11,  "ids1": 9, "ss1": "baz", "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": 1, "d1": 11.34 }' 
	with 
		format 'NDJSON' 
		input 'STREAM';`)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BulkNDJsonBadJsonPath", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert 
		into alltypes (_id, id1, i1, ids1, ss1, ts1, s1, b1, d1) 
	
		map ('$._id' id, '$.id1' id, '$.i1' int, '$.ids1' idset, '$.ss1' stringset, '$.ts1' timestamp, '$.s1' string, '$.blah' bool, '$.d1' decimal(2))
	
	from 
		x'{ "_id": 1, "id1": 10, "i1": 11,  "ids1": [ 3, 4, 5 ], "ss1": [ "foo", "bar" ], "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": true, "d1": 11.34 }
		{ "_id": 2, "id1": 10, "i1": 11,  "ids1": [ 3, 4, 5 ], "ss1": [ "foo", "bar" ], "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": 0, "d1": 11.34 }
		{ "_id": 3, "id1": 10, "i1": 11,  "ids1": [ 3, 4, 5 ], "ss1": [ "foo", "bar" ], "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": 1, "d1": 11.34 }
		{ "_id": 4, "id1": 10, "i1": 11,  "ids1": 9, "ss1": "baz", "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": 1, "d1": 11.34 }' 
	with 
		format 'NDJSON' 
		input 'STREAM';`)
		if err == nil || !strings.Contains(err.Error(), `unknown key blah`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkNDJsonBadJson", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert 
		into alltypes (_id, id1, i1, ids1, ss1, ts1, s1, b1, d1) 
	
		map ('$._id' id, '$.id1' id, '$.i1' int, '$.ids1' idset, '$.ss1' stringset, '$.ts1' timestamp, '$.s1' string, '$.b1' bool, '$.d1' decimal(2))
	
	from 
		x'{ "_id": 1, "id1": 10, "i1": 11,  "ids1": [ 3, 4, 5 ], "ss1": [ "foo", "bar" ], "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny" "b1": true, "d1": 11.34 }
		{ "_id": 2, "id1": 10, "i1": 11,  "ids1": [ 3, 4, 5 ], "ss1": [ "foo", "bar" ], "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": 0, "d1": 11.34 }
		{ "_id": 3, "id1": 10, "i1": 11,  "ids1": [ 3, 4, 5 ], "ss1": [ "foo", "bar" ], "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": 1, "d1": 11.34 }
		{ "_id": 4, "id1": 10, "i1": 11,  "ids1": 9, "ss1": "baz", "ts1": "2012-11-01T22:08:41+00:00", "s1": "frobny", "b1": 1, "d1": 11.34 }' 
	with 
		format 'NDJSON' 
		input 'STREAM';`)
		if err == nil || !strings.Contains(err.Error(), `: invalid character '"' after object key:value pair`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkInsertDecimals", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table iris (
			_id id,
			sepallength decimal(2),
			sepalwidth decimal(2),
			petallength decimal(2),
			petalwidth decimal(2),
			species string cachetype ranked size 1000
		) keypartitions 12;`)
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert
		into iris (_id, sepallength, sepalwidth, petallength, petalwidth, species)
		map('id' id,
		'sepalLength' DECIMAL,
		'sepalWidth' DECIMAL,
		'petalLength' DECIMAL,
		'petalWidth' DECIMAL,
		'species' STRING)
		from
		x'{"id": 1, "sepalLength": "5.1", "sepalWidth": "3.5", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
		{"id": 2, "sepalLength": "4.9", "sepalWidth": "3.0", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
		{"id": 3, "sepalLength": "4.7", "sepalWidth": "3.2", "petalLength": "1.3", "petalWidth": "0.2", "species": "setosa"}'
		with
			format 'NDJSON'
			input 'STREAM';`)
		if err == nil || !strings.Contains(err.Error(), `decimal scale expected`) {
			t.Fatalf("unexpected error: %v", err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert
		into iris (_id, sepallength, sepalwidth, petallength, petalwidth, species)
		map('id' id,
		'sepalLength' DECIMAL(2),
		'sepalWidth' DECIMAL(2),
		'petalLength' DECIMAL(2),
		'petalWidth' DECIMAL(2),
		'species' STRING)
		from
		x'{"id": 1, "sepalLength": "5.1", "sepalWidth": "3.5", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
		{"id": 2, "sepalLength": "4.9", "sepalWidth": "3.0", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
		{"id": 3, "sepalLength": "4.7", "sepalWidth": "3.2", "petalLength": "1.3", "petalWidth": "0.2", "species": "setosa"}'
		with
			format 'NDJSON'
			input 'STREAM';`)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BulkInsertDupeColumnPlusNullsInJson", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table dataviz (_id string, guid string, aba string,amount int, 
			audit_id id, bools bool, bools-exist bool, browser string, browser_version string, central_group string, device string, 
			error_description string, event_date_str string, event_epoch int, event_length int, event_type string, fidb string, 
			gt_status string, gt_type string, operating_system string, os_version string, transaction_id string, user_id id);`)
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert
		into dataviz (_id, aba, amount, audit_id, bools, bools-exist, browser, browser_version, central_group, device, error_description, event_date_str, event_epoch,event_length,event_type,fidb,gt_status,gt_type,_id,operating_system,os_version,transaction_id,user_id)
		map('guid' string,'aba' string, 'amount' int, 'audit_id' id, 'event_success' bool, 'db_success' bool, 'browser' string, 'browser_version' string, 'central_group' string, 'device' string, 'error_description' string, 'event_date_str' string, 'event_epoch' int, 'event_length' int, 'event_type' string, 'fidb' string, 'gt_status' string, 'gt_type' string, 'guid' string, 'operating_system' string, 'os_version' string, 'transaction_id' string, 'user_id' id)
		from
		x'{"event_type": "logon", "fidb": "Q2DB_5061", "guid": "70823998-fdf7-4e7a-bea9-a6b1b3538ed6", "aba": "314977104", "audit_id": 11848579444, "user_id": 601961, "central_group": "RETAIL USERS PROTECTED", "event_epoch": 1629548181, "event_date_str": "2021-08-21 07:00:00", "event_length": 523, "device": "Desktop", "browser": "Firefox", "browser_version": "67", "operating_system": "Windows", "os_version": "10", "event_success": true, "transaction_id": null, "amount": null, "gt_type": null, "gt_status": null, "error_description": "Success", "audit_action": "LogonUser", "postal_code": "97818", "zoneid": -1, "country": "US", "subdivision": "OR", "agg_allblocks": null, "agg_antiautomation": null, "agg_countryblocks": null, "agg_ratelimiting": null, "agg_threatfeedsexternal": null, "agg_threatfeedsinternal": null, "db_failure": null, "db_success": null, "product_or_inst_name": null, "sum_pfm_balances": null, "sum_pfm_accounts": null, "institution_count": null, "product_count": null}
		{"event_type": "logon", "fidb": "Q2DB_5061", "guid": "70823998-fdf7-4e7a-bea9-a6b1b3538ed6", "aba": "314977104", "audit_id": 11848581121, "user_id": 517782, "central_group": "RETAIL USERS PROTECTED", "event_epoch": 1629548202, "event_date_str": "2021-08-21 07:00:00", "event_length": 270, "device": "Mobile", "browser": "Mobile Safari", "browser_version": "14", "operating_system": "iOS", "os_version": "unknown", "event_success": true, "transaction_id": null, "amount": null, "gt_type": null, "gt_status": null, "error_description": "Success", "audit_action": "LogonUser", "postal_code": "78664", "zoneid": -1, "country": "US", "subdivision": "TX", "agg_allblocks": null, "agg_antiautomation": null, "agg_countryblocks": null, "agg_ratelimiting": null, "agg_threatfeedsexternal": null, "agg_threatfeedsinternal": null, "db_failure": null, "db_success": null, "product_or_inst_name": null, "sum_pfm_balances": null, "sum_pfm_accounts": null, "institution_count": null, "product_count": null}
		{"event_type": "gt_auth", "fidb": "Q2DB_3454", "guid": "fa307c33-2c30-4b7c-8b58-d60f87d3ddda", "aba": "107001481", "audit_id": 3653480727, "user_id": 545135, "central_group": "AFB New Relationship Retail", "event_epoch": 1629548207, "event_date_str": "2021-08-21 07:00:00", "event_length": 0, "device": "Mobile", "browser": "Mobile Safari", "browser_version": "14", "operating_system": "iOS", "os_version": "unknown", "event_success": true, "transaction_id": 2550927, "amount": 6000, "gt_type": "FundsTransfer", "gt_status": "Processed", "error_description": "Success", "audit_action": "AuthorizeFundsTransfer", "postal_code": "", "zoneid": 2, "country": "US", "subdivision": "", "agg_allblocks": null, "agg_antiautomation": null, "agg_countryblocks": null, "agg_ratelimiting": null, "agg_threatfeedsexternal": null, "agg_threatfeedsinternal": null, "db_failure": null, "db_success": null, "product_or_inst_name": null, "sum_pfm_balances": null, "sum_pfm_accounts": null, "institution_count": null, "product_count": null}'
			With
			format 'NDJSON'
			input 'STREAM';`)
		if err == nil || !strings.Contains(err.Error(), `duplicate column '_id'`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BulkInsertCSVStringIDSet", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table greg-test (
			_id STRING,
			id_col ID,
			string_col STRING cachetype ranked size 1000,
			int_col int,
			decimal_col DECIMAL(2),
			bool_col BOOL
			time_col TIMESTAMP,
			stringset_col STRINGSET,
			ideset_col IDSET
		);`)
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `BULK INSERT INTO greg-test (
			_id,
			id_col,
			string_col,
			int_col,
			decimal_col,
			bool_col,
			time_col,
			stringset_col,
			ideset_col)
			map (
			0 ID,
			1 STRING,
			2 INT,
			3 DECIMAL(2),
			4 BOOL,
			5 TIMESTAMP,
			6 STRINGSET,
			7 IDSET)
			transform(
			@1,
			@0,
			@1,
			@2,
			@3,
			@4,
			@5,
			@6,
			@7)
			FROM
			x'8924809397503602651,TEST,-123,1.12,0,2013-07-15T01:18:46Z,stringset1,1
			64575677503602651,TEST2,321,31.2,1,2014-07-15T01:18:46Z,stringset1,1
			8924809397503602651,TEST,-123,1.12,0,2013-07-15T01:18:46Z,stringset2,2'
			with
				BATCHSIZE 10000
				format 'CSV'
				input 'STREAM';`)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BulkInsertAllowMissingValues", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table greg-test-amv (
			_id STRING,
			id_col ID,
			string_col STRING cachetype ranked size 1000,
			int_col int,
			decimal_col DECIMAL(2),
			bool_col BOOL
			time_col TIMESTAMP,
			stringset_col STRINGSET,
			ideset_col IDSET
		);`)
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `BULK INSERT INTO greg-test-amv (
			_id,
			id_col,
			string_col,
			int_col,
			decimal_col,
			bool_col,
			time_col,
			stringset_col,
			ideset_col)
			map (
			'$.id_col' ID,
			'$.string_col' STRING,
			'$.int_col' INT,
			'$.decimal_col' DECIMAL(2),
			'$.bool_col' BOOL,
			'$.time_col' TIMESTAMP,
			'$.stringset_col' STRINGSET,
			'$.ideset_col' IDSET)
			transform(
			@1,
			@0,
			@1,
			@2,
			@3,
			@4,
			@5,
			@6,
			@7)
			FROM x'{"id_col": "3", "string_col": "TEST", "decimal_col": "1.12", "bool_col": false, "time_col": "2013-07-15T01:18:46Z", "stringset_col": "stringset1","ideset_col": 1}
			{"id_col": "4", "string_col": "TEST2", "decimal_col": "1.12", "bool_col": false, "time_col": "2013-07-15T01:18:46Z", "stringset_col": ["stringset1","stringset3"],"ideset_col": [1,2]}
			{"id_col": "5", "string_col": "TEST", "int_col": "321", "decimal_col": "12.1", "bool_col": 1, "time_col": "2014-07-15T01:18:46Z", "stringset_col": "stringset2","ideset_col": [1,3]}'
			with
				BATCHSIZE 10000
				format 'NDJSON'
				input 'STREAM'
				allow_missing_values;`)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("BulkInsertNDJSONStringIDSet", func(t *testing.T) {
		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table greg-test-01 (
			_id STRING,
			id_col ID,
			string_col STRING cachetype ranked size 1000,
			int_col int,
			decimal_col DECIMAL(2),
			bool_col BOOL
			time_col TIMESTAMP,
			stringset_col STRINGSET,
			ideset_col IDSET
		);`)
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `BULK INSERT INTO greg-test-01 (
			_id,
			id_col,
			string_col,
			int_col,
			decimal_col,
			bool_col,
			time_col,
			stringset_col,
			ideset_col)
			map (
			'id_col' ID,
			'string_col' STRING,
			'int_col' INT,
			'decimal_col' DECIMAL(2),
			'bool_col' BOOL,
			'time_col' TIMESTAMP,
			'stringset_col' STRINGSET,
			'ideset_col' IDSET)
			transform(
			@1,
			@0,
			@1,
			@2,
			@3,
			@4,
			@5,
			@6,
			@7)
			FROM '{"id_col": "3", "string_col": "TEST", "int_col": "-123", "decimal_col": "1.12", "bool_col": false, "time_col": "2013-07-15T01:18:46Z", "stringset_col": "stringset1","ideset_col": "1"}'
			with
				BATCHSIZE 10000
				format 'NDJSON'
				input 'STREAM';`)
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `BULK INSERT INTO greg-test-01 (
			_id,
			id_col,
			string_col,
			int_col,
			decimal_col,
			bool_col,
			time_col,
			stringset_col,
			ideset_col)
			map (
			'id_col' ID,
			'string_col' STRING,
			'int_col' INT,
			'decimal_col' DECIMAL(2),
			'bool_col' BOOL,
			'time_col' TIMESTAMP,
			'stringset_col' STRINGSET,
			'ideset_col' IDSET)
			transform(
			@1,
			@0,
			@1,
			@2,
			@3,
			@4,
			@5,
			@6,
			@7)
			FROM '{"id_col": "3", "string_col": "TEST", "int_col": "-123", "decimal_col": "1.12", "bool_col": false, "time_col": "2013-07-15T01:18:46Z", "stringset_col": "stringset1","ideset_col": ["1","2"]}'
			with
				BATCHSIZE 10000
				format 'NDJSON'
				input 'STREAM';`)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestPlanner_SelectSelectSource(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex(c.Idx("j"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i0.CreateField("a", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("b", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("j"),
		Query: `
			Set(1, a=10)
			Set(1, b=100)
			Set(2, a=20)
			Set(2, b=200)
	`,
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("ParenSource", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT a, b, _id FROM (select * from %j)`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(10), int64(100), int64(1)},
			{int64(20), int64(200), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ParenSourceWithAlias", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT foo.a, b, _id FROM (select * from %j) as foo`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(10), int64(100), int64(1)},
			{int64(20), int64(200), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestPlanner_In(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex(c.Idx("j"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i0.CreateField("a", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	i1, err := c.GetHolder(0).CreateIndex(c.Idx("k"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i1.CreateField("parentid", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i1.CreateField("x", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("j"),
		Query: `
			Set(1, a=10)
			Set(2, a=20)
			Set(3, a=30)
	`,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("k"),
		Query: `
			Set(1, parentid=1)
			Set(1, x=100)

			Set(2, parentid=1)
			Set(2, x=200)

			Set(3, parentid=2)
			Set(3, x=300)
	`,
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("Count", func(t *testing.T) {
		t.Skip("Need to add join conditions to get this to pass")
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT %j._id, %j.a, %k._id, %k.parentid, %k.x FROM %j INNER JOIN %k ON %j._id = %k.parentid`, c, c, c, c, c, c, c, c, c))
		// results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT COUNT(*) FROM %j INNER JOIN %k ON %j._id = %k.parentid`, c, c, c, c))
		// results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT a FROM %j where a = 20`, c)) //   SELECT COUNT(*) FROM %j INNER JOIN %k ON %j._id = %k.parentid
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("count"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	/*t.Run("Count", func(t *testing.T) {
		//results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT COUNT(*) FROM %j INNER JOIN %k ON %j._id = %k.parentid`, c, c, c, c))
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT COUNT(*) FROM %j where %j._id in (select distinct parentid from %k)`, c, c, c)) //   SELECT COUNT(*) FROM %j INNER JOIN %k ON %j._id = %k.parentid
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("count"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("CountWithParentCondition", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT COUNT(*) FROM %j where %j._id in (select distinct parentid from %k) and %j.a = 10`, c, c, c, c)) // SELECT COUNT(*) FROM %j INNER JOIN %k ON %j._id = %k.parentid WHERE %j.a = 10
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(1)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("count"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("CountWithParentAndChildCondition", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT COUNT(*) FROM %j where %j._id in (select distinct parentid from %k where x = 200) and %j.a = 10`, c, c, c, c)) // SELECT COUNT(*) FROM %j INNER JOIN %k ON %j._id = %k.parentid WHERE %j.a = 10
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(1)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("count"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})*/
}

func TestPlanner_Distinct(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex(c.Idx("j"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i0.CreateField("a", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	i1, err := c.GetHolder(0).CreateIndex(c.Idx("k"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i1.CreateField("parentid", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i1.CreateField("x", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("j"),
		Query: `
			Set(1, a=10)
			Set(2, a=20)
			Set(3, a=30)
	`,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("k"),
		Query: `
			Set(1, parentid=1)
			Set(1, x=100)

			Set(2, parentid=1)
			Set(2, x=200)

			Set(3, parentid=2)
			Set(3, x=300)
	`,
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("SelectDistinct_id", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT distinct _id from %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldID("_id"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("SelectDistinctNonId", func(t *testing.T) {
		t.Skip("WIP")
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`SELECT distinct parentid from %k`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldInt("parentid"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("SelectDistinctMultiple", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`select distinct _id, parentid from %k`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(1), int64(1)},
			{int64(2), int64(1)},
			{int64(3), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldID("_id"),
			wireQueryFieldInt("parentid"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestPlanner_SelectTop(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex(c.Idx("j"), "", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := i0.CreateField("a", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	if _, err := i0.CreateField("b", "", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: c.Idx("j"),
		Query: `
			Set(1, a=10)
			Set(2, a=20)
			Set(3, a=30)
			Set(1, b=100)
			Set(2, b=200)
			Set(3, b=300)
	`,
	}); err != nil {
		t.Fatal(err)
	}

	t.Run("SelectTopStar", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`select top(1) * from %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(1), int64(10), int64(100)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldID("_id"),
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("SelectTopNStar", func(t *testing.T) {
		results, columns, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`select topn(1) * from %j`, c))
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff([][]interface{}{
			{int64(1), int64(10), int64(100)},
			{int64(2), int64(20), int64(200)},
			{int64(3), int64(30), int64(300)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.WireQueryField{
			wireQueryFieldID("_id"),
			wireQueryFieldInt("a"),
			wireQueryFieldInt("b"),
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

// helpers

func wireQueryFieldID(name string) *pilosa.WireQueryField {
	return &pilosa.WireQueryField{
		Name:     dax.FieldName(name),
		Type:     dax.BaseTypeID,
		BaseType: dax.BaseTypeID,
	}
}

func wireQueryFieldBool(name string) *pilosa.WireQueryField {
	return &pilosa.WireQueryField{
		Name:     dax.FieldName(name),
		Type:     dax.BaseTypeBool,
		BaseType: dax.BaseTypeBool,
	}
}

func wireQueryFieldString(name string) *pilosa.WireQueryField {
	return &pilosa.WireQueryField{
		Name:     dax.FieldName(name),
		Type:     dax.BaseTypeString,
		BaseType: dax.BaseTypeString,
	}
}

func wireQueryFieldInt(name string) *pilosa.WireQueryField {
	return &pilosa.WireQueryField{
		Name:     dax.FieldName(name),
		Type:     dax.BaseTypeInt,
		BaseType: dax.BaseTypeInt,
	}
}

func wireQueryFieldTimestamp(name string) *pilosa.WireQueryField {
	return &pilosa.WireQueryField{
		Name:     dax.FieldName(name),
		Type:     dax.BaseTypeTimestamp,
		BaseType: dax.BaseTypeTimestamp,
	}
}

func wireQueryFieldDecimal(name string, scale int64) *pilosa.WireQueryField {
	return &pilosa.WireQueryField{
		Name:     dax.FieldName(name),
		Type:     fmt.Sprintf("%s(%d)", dax.BaseTypeDecimal, scale),
		BaseType: dax.BaseTypeDecimal,
		TypeInfo: map[string]interface{}{
			"scale": scale,
		},
	}
}

// This test verifies that data sent to all nodes shows up in the results
func TestPlanner_BulkInsert_FB1831(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	_, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table iris (_id id, sepallength decimal(2), sepalwidth decimal(2), petallength decimal(2), petalwidth decimal(2), species string cachetype ranked size 1000);`)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert
	into iris (_id, sepallength, sepalwidth, petallength, petalwidth, species)
	map('id' id,
	'sepalLength' DECIMAL(2),
	'sepalWidth' DECIMAL(2),
	'petalLength' DECIMAL(2),
	'petalWidth' DECIMAL(2),
	'species' STRING)
	from
	x'{"id": 1, "sepalLength": "5.1", "sepalWidth": "3.5", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
	{"id": 2, "sepalLength": "4.9", "sepalWidth": "3.0", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
	{"id": 3, "sepalLength": "4.7", "sepalWidth": "3.2", "petalLength": "1.3", "petalWidth": "0.2", "species": "setosa"}'
	with
		format 'NDJSON'
		input 'STREAM';`)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = sql_test.MustQueryRows(t, c.GetNode(1).Server, `bulk insert
	into iris (_id, sepallength, sepalwidth, petallength, petalwidth, species)
	map('id' id,
	'sepalLength' DECIMAL(2),
	'sepalWidth' DECIMAL(2),
	'petalLength' DECIMAL(2),
	'petalWidth' DECIMAL(2),
	'species' STRING)
	from
	x'{"id": 4, "sepalLength": "5.1", "sepalWidth": "3.5", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
	{"id": 5, "sepalLength": "4.9", "sepalWidth": "3.0", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
	{"id": 6, "sepalLength": "4.7", "sepalWidth": "3.2", "petalLength": "1.3", "petalWidth": "0.2", "species": "setosa"}'
	with
		format 'NDJSON'
		input 'STREAM';`)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = sql_test.MustQueryRows(t, c.GetNode(2).Server, `bulk insert
	into iris (_id, sepallength, sepalwidth, petallength, petalwidth, species)
	map('id' id,
	'sepalLength' DECIMAL(2),
	'sepalWidth' DECIMAL(2),
	'petalLength' DECIMAL(2),
	'petalWidth' DECIMAL(2),
	'species' STRING)
	from
	x'{"id": 7, "sepalLength": "5.1", "sepalWidth": "3.5", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
	{"id": 8, "sepalLength": "4.9", "sepalWidth": "3.0", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
	{"id": 9, "sepalLength": "4.7", "sepalWidth": "3.2", "petalLength": "1.3", "petalWidth": "0.2", "species": "setosa"}'
	with
		format 'NDJSON'
		input 'STREAM';`)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `bulk insert
	into iris (_id, sepallength, sepalwidth, petallength, petalwidth, species)
	map('id' id,
	'sepalLength' DECIMAL(2),
	'sepalWidth' DECIMAL(2),
	'petalLength' DECIMAL(2),
	'petalWidth' DECIMAL(2),
	'species' STRING)
	from
	x'{"id": 1048577, "sepalLength": "5.1", "sepalWidth": "3.5", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
	{"id": 2097153, "sepalLength": "4.9", "sepalWidth": "3.0", "petalLength": "1.4", "petalWidth": "0.2", "species": "setosa"}
	{"id": 3145729, "sepalLength": "4.7", "sepalWidth": "3.2", "petalLength": "1.3", "petalWidth": "0.2", "species": "setosa"}'
	with
		format 'NDJSON'
		input 'STREAM';`)
	if err != nil {
		t.Fatal(err)
	}
	results, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `select _id from iris`)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]int64, 0)
	for i := range results {
		got = append(got, results[i][0].(int64))
	}
	sort.Slice(got, func(i, j int) bool {
		return got[i] < got[j]
	})
	expected := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 1048577, 2097153, 3145729}
	if !reflect.DeepEqual(got, expected) {
		t.Fatal("Expecting to be equal")
	}
}

type tb struct {
	Name  string
	Type  arrow.DataType
	Value interface{}
}

func simpleParquetMaker(f *os.File, numRows int64, input []tb) error {
	mem := memory.NewGoAllocator()
	chunks := make([]arrow.Array, 0)
	for i := range input {
		switch input[i].Type {

		case arrow.PrimitiveTypes.Int64:
			idbuild := array.NewInt64Builder(mem)
			idbuild.AppendValues(input[i].Value.([]int64), nil)
			newChunk := idbuild.NewArray()
			chunks = append(chunks, newChunk)
		case arrow.PrimitiveTypes.Float64:
			fbuild := array.NewFloat64Builder(mem)
			fbuild.AppendValues(input[i].Value.([]float64), nil)
			newChunk := fbuild.NewArray()
			chunks = append(chunks, newChunk)
		case arrow.BinaryTypes.String:
			sbuild := array.NewStringBuilder(mem)
			sbuild.AppendValues(input[i].Value.([]string), nil)
			newChunk := sbuild.NewArray()
			chunks = append(chunks, newChunk)
		}
	}
	// make schema
	fields := make([]arrow.Field, len(input))
	for i := range input {
		fields[i].Name = input[i].Name
		fields[i].Type = input[i].Type
	}
	schema := arrow.NewSchema(fields, nil)
	rec := array.NewRecord(schema, chunks, numRows)
	table := array.NewTableFromRecords(schema, []arrow.Record{rec})
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	arrProps := pqarrow.DefaultWriterProps()
	err := pqarrow.WriteTable(table, f, 4096, props, arrProps)
	if err != nil {
		return err
	}
	return nil
}

func TestPlanner_BulkInsertParquet_File(t *testing.T) {
	// check that can pull parquet file from URL and load
	c := test.MustRunUnsharedCluster(t, 1)
	defer c.Close()

	_, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table j1 (_id ID, a INT, b DECIMAL(2), c STRING);`)
	if err != nil {
		t.Fatal(err)
	}
	tmpfile, err := os.CreateTemp("", "BulkParquetFile.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	// create a parquet file with all the example data
	err = simpleParquetMaker(tmpfile, 2, []tb{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Value: []int64{1, 2}},
		{Name: "int64V", Type: arrow.PrimitiveTypes.Int64, Value: []int64{42, 7}},
		{Name: "float64V", Type: arrow.PrimitiveTypes.Float64, Value: []float64{3.14159, 1.61803}},
		{Name: "stringV", Type: arrow.BinaryTypes.String, Value: []string{"pi", "goldenratio"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`bulk insert 
	into j1 (_id,a,b,c ) 
	map(
		'id' id, 
		'int64V' INT, 
		'float64V' DECIMAL(2), 
		'stringV' STRING) 
    from 
	   '%s' 
	   WITH FORMAT 'PARQUET' 
	   INPUT 'FILE';`, tmpfile.Name()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	results, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `select _id, a,c from j1`)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([][]interface{}{
		{int64(1), int64(42), "pi"},
		{int64(2), int64(7), "goldenratio"},
	}, results); diff != "" {
		t.Fatal(diff)
	}
	results, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, `select b from j1`)
	if err != nil {
		t.Fatal(err)
	}
	d, _ := pql.FromFloat64WithScale(3.14159, 2)
	if !pql.Decimal.EqualTo(d, results[0][0].(pql.Decimal)) {
		t.Fatal("Should be equal")
	}
}

func TestPlanner_BulkInsertParquet_URL(t *testing.T) {
	// check that can pull parquet file from URL and load
	c := test.MustRunUnsharedCluster(t, 1)
	defer c.Close()

	_, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table j2 (_id ID, a INT, b STRING);`)
	if err != nil {
		t.Fatal(err)
	}
	tmpfile, err := os.CreateTemp("", "BulkParquetFile.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	err = simpleParquetMaker(tmpfile, 1, []tb{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Value: []int64{1}},
		{Name: "int64V", Type: arrow.PrimitiveTypes.Int64, Value: []int64{42}},
		{Name: "stringV", Type: arrow.BinaryTypes.String, Value: []string{"pi"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// create a parquet file with all the example data
	mux := http.NewServeMux()
	ts := httptest.NewServer(mux)
	defer ts.Close()

	mux.HandleFunc("/static", func(w http.ResponseWriter, r *http.Request) {
		payload, _ := ioutil.ReadFile(tmpfile.Name())
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)
		w.Write(payload)
	})

	_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`bulk insert 
	into j2 (_id,a,b ) 
	map(
		'id' id, 
		'int64V' INT, 
		'stringV' STRING) 
    from 
	   '%s' 
	   WITH FORMAT 'PARQUET' 
	   INPUT 'URL';`, ts.URL+"/static"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	results, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `select _id, a,b from j2`)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([][]interface{}{
		{int64(1), int64(42), "pi"},
	}, results); diff != "" {
		t.Fatal(diff)
	}
}

func TestPlanner_BulkInsertParquet_FileTimeStamp(t *testing.T) {
	// check that timestamp string and mill formating is working
	c := test.MustRunUnsharedCluster(t, 1)
	defer c.Close()

	_, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `create table continuum (_id ID, created timestamp, updated timestamp);`)
	if err != nil {
		t.Fatal(err)
	}
	tmpfile, err := os.CreateTemp("", "BulkParquetFile.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	// create a parquet file with all the example data
	now := time.Now()
	err = simpleParquetMaker(tmpfile, 1, []tb{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Value: []int64{1}},
		{Name: "unixtime", Type: arrow.PrimitiveTypes.Int64, Value: []int64{now.UnixMilli()}},
		{Name: "stringtime", Type: arrow.BinaryTypes.String, Value: []string{now.Format(time.RFC3339)}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = sql_test.MustQueryRows(t, c.GetNode(0).Server, fmt.Sprintf(`bulk insert 
	into continuum (_id,created,updated ) 
	map(
		'id' id, 
		'unixtime' timestamp, 
		'stringtime' timestamp ) 
    from 
	   '%s' 
	   WITH FORMAT 'PARQUET' 
	   INPUT 'FILE';`, tmpfile.Name()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	results, _, err := sql_test.MustQueryRows(t, c.GetNode(0).Server, `select _id, created,updated from continuum`)
	if err != nil {
		t.Fatal(err)
	}
	row := results[0]
	if row[1] != row[2] {
		t.Fatalf("Not Equal %v == %v", row[1], row[2])
	}
}
