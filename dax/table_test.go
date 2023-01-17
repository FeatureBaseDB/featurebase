package dax_test

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	tableName := dax.TableName("foo")

	t.Run("StringKeys", func(t *testing.T) {
		// No PrimaryKeys.
		{
			tbl := &dax.Table{
				Name: tableName,
			}
			assert.False(t, tbl.StringKeys())
		}
		// One PrimaryKey (string).
		{
			tbl := &dax.Table{
				Name: tableName,
				Fields: []*dax.Field{
					{
						Name: dax.PrimaryKeyFieldName,
						Type: dax.BaseTypeString,
					},
					{
						Name: "stringField2",
						Type: dax.BaseTypeString,
					},
				},
			}
			assert.True(t, tbl.StringKeys())
		}
		// One PrimaryKey (non-string).
		{
			tbl := &dax.Table{
				Name: tableName,
				Fields: []*dax.Field{
					{
						Name: dax.PrimaryKeyFieldName,
						Type: dax.BaseTypeID,
					},
				},
			}
			assert.False(t, tbl.StringKeys())
		}
	})

	t.Run("Tables", func(t *testing.T) {
		tblA := &dax.Table{
			Name: "a",
		}
		tblZ := &dax.Table{
			Name: "z",
		}

		tables := []*dax.Table{tblZ, tblA}

		// Ensure tables starts out of order.
		assert.Equal(t, tblZ.Name, tables[0].Name)
		assert.Equal(t, tblA.Name, tables[1].Name)

		sort.Sort(dax.Tables(tables))

		// Ensure tables is ordered by name.
		assert.Equal(t, tblA.Name, tables[0].Name)
		assert.Equal(t, tblZ.Name, tables[1].Name)

	})

	t.Run("CreateSQL", func(t *testing.T) {
		tests := []struct {
			tbl    dax.Table
			expSQL string
		}{
			{
				tbl: dax.Table{
					Name: "just_a_table",
					Fields: []*dax.Field{
						{
							Name: "_id",
							Type: "id",
						},
					},
				},
				expSQL: "CREATE TABLE just_a_table (_id id) KEYPARTITIONS 0",
			},
			{
				tbl: dax.Table{
					Name: "all_field_types",
					Fields: []*dax.Field{
						{
							Name: "_id",
							Type: "string",
						},
						{
							Name: "an_id",
							Type: "id",
						},
						{
							Name: "a_string",
							Type: "string",
						},
						{
							Name: "an_id_set",
							Type: "idset",
						},
						{
							Name: "a_string_set",
							Type: "stringset",
						},
						{
							Name: "an_int",
							Type: "int",
						},
						{
							Name: "a_decimal",
							Type: "decimal",
						},
						{
							Name: "a_timestamp",
							Type: "timestamp",
						},
					},
				},
				expSQL: "CREATE TABLE all_field_types (_id string, an_id id, a_string string, an_id_set idset, a_string_set stringset, an_int int MIN 0 MAX 0, a_decimal decimal, a_timestamp timestamp) KEYPARTITIONS 0",
			},
			{
				tbl: dax.Table{
					Name: "all_field_types_with_options",
					Fields: []*dax.Field{
						{
							Name: "_id",
							Type: "string",
						},
						{
							Name: "an_id",
							Type: "id",
							Options: dax.FieldOptions{
								CacheType: "ranked",
								CacheSize: 500,
							},
						},
						{
							Name: "a_string",
							Type: "string",
							Options: dax.FieldOptions{
								CacheType: "ranked",
								CacheSize: 500,
							},
						},
						{
							Name: "an_id_set",
							Type: "idset",
							Options: dax.FieldOptions{
								CacheType: "ranked",
								CacheSize: 500,
							},
						},
						{
							Name: "a_string_set",
							Type: "stringset",
							Options: dax.FieldOptions{
								CacheType: "ranked",
								CacheSize: 500,
							},
						},
						{
							Name: "an_int",
							Type: "int",
							Options: dax.FieldOptions{
								Min: pql.NewDecimal(-100, 0),
								Max: pql.NewDecimal(200, 0),
							},
						},
						{
							Name:    "a_decimal",
							Type:    "decimal",
							Options: dax.FieldOptions{},
						},
						{
							Name: "a_timestamp",
							Type: "timestamp",
							Options: dax.FieldOptions{
								TimeUnit: "s",
								Epoch:    time.Date(2009, 11, 10, 23, 34, 56, 0, time.UTC),
							},
						},
					},
				},
				expSQL: "CREATE TABLE all_field_types_with_options (_id string, an_id id CACHETYPE ranked SIZE 500, a_string string CACHETYPE ranked SIZE 500, an_id_set idset CACHETYPE ranked SIZE 500, a_string_set stringset CACHETYPE ranked SIZE 500, an_int int MIN -100 MAX 200, a_decimal decimal, a_timestamp timestamp TIMEUNIT 's' EPOCH '2009-11-10T23:34:56Z') KEYPARTITIONS 0",
			},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
				assert.Equal(t, test.expSQL, test.tbl.CreateSQL())
			})
		}
	})

	t.Run("Table", func(t *testing.T) {
		// CleanStub makes sure that the portion of the dax.TableName that we
		// use in the pilosa.Index.Name is actually valid.
		t.Run("CleanStub", func(t *testing.T) {
			tests := []struct {
				name    dax.TableName
				expStub string
			}{
				{
					name:    "",
					expStub: "",
				},
				{
					name:    "foo",
					expStub: "foo",
				},
				{
					name:    "AbCdEfG1234",
					expStub: "abcdefg123",
				},
				{
					name:    "foo_bar_",
					expStub: "foobar",
				},
				{
					name:    "!!!!!!!",
					expStub: "",
				},
				{
					name:    "long1234567890",
					expStub: "long123456",
				},
				{
					name:    "&&&&&&&&&&&&&&valid_stuff",
					expStub: "validstuff",
				},
			}

			for i, test := range tests {
				t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
					n := dax.NewTable(test.name)
					assert.Empty(t, n.ID)
					n.CreateID()

					parts := strings.Split(string(n.ID), "_")
					assert.Equal(t, test.expStub, parts[0])
				})
			}
		})

		t.Run("RandomID", func(t *testing.T) {
			n := dax.NewTable(tableName)
			assert.Empty(t, n.ID)
			n.CreateID()
			assert.NotEmpty(t, n.ID)
			assert.Equal(t, tableName, n.Name)
		})

		t.Run("ToJSON", func(t *testing.T) {
			t.Run("WithID", func(t *testing.T) {
				n := dax.NewTable(tableName)
				n.CreateID()
				id := n.ID
				assert.NotEmpty(t, id)

				b, err := json.Marshal(n)
				assert.NoError(t, err)

				exp := fmt.Sprintf("{\"id\":\"%s\",\"name\":\"%s\",\"fields\":[],\"partitionN\":0}", id, tableName)
				assert.JSONEq(t, exp, string(b))
			})

			t.Run("WithoutID", func(t *testing.T) {
				n := dax.Table{
					Name: tableName,
				}
				id := n.ID
				assert.Empty(t, id)

				b, err := json.Marshal(n)
				assert.NoError(t, err)

				exp := fmt.Sprintf("{\"name\":\"%s\",\"fields\":null,\"partitionN\":0}", tableName)
				assert.JSONEq(t, exp, string(b))
			})
		})

		t.Run("FromJSON", func(t *testing.T) {
			tid := dax.TableID("0000000000abc123") // 11256099
			j := fmt.Sprintf("{\"id\":\"%s\",\"name\":\"foo\",\"fields\":[],\"partitionN\":0}", tid)

			tbl := &dax.Table{}
			err := json.Unmarshal([]byte(j), tbl)
			assert.NoError(t, err)
			assert.Equal(t, tid, tbl.ID)
		})
	})

	t.Run("QualifiedTable", func(t *testing.T) {
		orgID := dax.OrganizationID("acme")
		dbID := dax.DatabaseID("db1")

		t.Run("New", func(t *testing.T) {
			tbl := dax.NewTable(tableName)
			tbl.CreateID()
			qdbid := dax.QualifiedDatabaseID{
				OrganizationID: orgID,
				DatabaseID:     dbID,
			}
			qtbl := dax.NewQualifiedTable(qdbid, tbl)
			assert.NotEmpty(t, qtbl.ID)
			assert.Equal(t, tbl.ID, qtbl.ID)

			tq := qtbl.Qualifier()
			assert.Equal(t, qdbid.OrganizationID, tq.OrganizationID)
			assert.Equal(t, qdbid.DatabaseID, tq.DatabaseID)

			wrappedTable := qtbl.Table
			assert.Equal(t, tbl.Name, wrappedTable.Name)
			assert.Equal(t, tbl.ID, wrappedTable.ID)

			// Key.
			exp := dax.TableKey(fmt.Sprintf("%s%s%s%s%s%s%s",
				dax.PrefixTable,
				dax.TableKeyDelimiter,
				orgID,
				dax.TableKeyDelimiter,
				dbID,
				dax.TableKeyDelimiter,
				tbl.ID,
			))
			assert.Equal(t, exp, qtbl.Key())
		})

		t.Run("ToJSON", func(t *testing.T) {
			tbl := dax.NewTable(tableName)
			tbl.CreateID()
			qdbid := dax.QualifiedDatabaseID{
				OrganizationID: orgID,
				DatabaseID:     dbID,
			}
			qtbl := dax.NewQualifiedTable(qdbid, tbl)
			id := qtbl.ID

			b, err := json.Marshal(qtbl)
			assert.NoError(t, err)

			exp := fmt.Sprintf("{\"org-id\":\"%s\",\"db-id\":\"%s\",\"id\":\"%s\",\"name\":\"%s\",\"fields\":[],\"partitionN\":0}", orgID, dbID, id, tableName)
			assert.JSONEq(t, exp, string(b))
		})

		t.Run("FromJSON", func(t *testing.T) {
			tid := dax.TableID("0000000000abc123") // 11256099
			j := fmt.Sprintf("{\"org-id\":\"%s\",\"db-id\":\"%s\",\"id\":\"%s\",\"name\":\"foo\",\"fields\":[],\"partitionN\":0}", orgID, dbID, tid)

			qtbl := &dax.QualifiedTable{}
			err := json.Unmarshal([]byte(j), qtbl)
			assert.NoError(t, err)

			assert.Equal(t, tid, qtbl.ID)
			assert.Equal(t, orgID, qtbl.Qualifier().OrganizationID)
			assert.Equal(t, dbID, qtbl.Qualifier().DatabaseID)
		})
	})
}
