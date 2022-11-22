// Copyright 2021 Molecula Corp. All rights reserved.
package sql3_test

import (
	"fmt"
	"log"
	"sort"
	"testing"

	"github.com/molecula/featurebase/v3/dax"
	sql_test "github.com/molecula/featurebase/v3/sql3/test"
	"github.com/molecula/featurebase/v3/sql3/test/defs"
	"github.com/molecula/featurebase/v3/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_Execute(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	svr := c.GetNode(0).Server

	for i, test := range defs.TableTests {
		t.Run(test.Name(i), func(t *testing.T) {

			// Create a table with all field types.
			if test.HasTable() {
				_, _, err := sql_test.MustQueryRows(t, svr, test.CreateTable())
				assert.NoError(t, err)
			}

			if test.HasTable() && test.HasData() {
				// Populate fields with data.
				_, _, err := sql_test.MustQueryRows(t, svr, test.InsertInto(t))
				assert.NoError(t, err)
			}

			for i, sqltest := range test.SQLTests {
				t.Run(sqltest.Name(i), func(t *testing.T) {
					for _, sql := range sqltest.SQLs {
						t.Run(fmt.Sprintf("sql-%s", sql), func(t *testing.T) {
							log.Printf("SQL: %s", sql)
							rows, headers, err := sql_test.MustQueryRows(t, svr, sql)

							// Check expected error instead of results.
							if sqltest.ExpErr != "" {
								if assert.Error(t, err) {
									assert.Contains(t, err.Error(), sqltest.ExpErr)
								}
								return
							}

							require.NoError(t, err)

							// Check headers.
							assert.ElementsMatch(t, sqltest.ExpHdrs, headers)

							// make a map of column name to header index
							m := make(map[dax.FieldName]int)
							for i := range headers {
								m[headers[i].Name] = i
							}

							// Put the expRows in the same column order as the headers returned
							// by the query.
							exp := make([][]interface{}, len(sqltest.ExpRows))
							for i := range sqltest.ExpRows {
								exp[i] = make([]interface{}, len(headers))
								for j := range sqltest.ExpHdrs {
									targetIdx := m[sqltest.ExpHdrs[j].Name]
									assert.GreaterOrEqual(t, len(sqltest.ExpRows[i]), len(headers),
										"expected row set has fewer columns than returned headers")
									exp[i][targetIdx] = sqltest.ExpRows[i][j]
								}
							}

							if sqltest.SortStringKeys {
								sortStringKeys(rows)
							}

							switch sqltest.Compare {
							case defs.CompareExactOrdered:
								assert.Equal(t, len(sqltest.ExpRows), len(rows))
								assert.EqualValues(t, exp, rows)
							case defs.CompareExactUnordered:
								assert.Equal(t, len(sqltest.ExpRows), len(rows))
								assert.ElementsMatch(t, exp, rows)
							case defs.CompareIncludedIn:
								assert.Equal(t, sqltest.ExpRowCount, len(rows))
								for _, row := range rows {
									assert.Contains(t, exp, row)
								}
							}
						})
					}
				})
			}
		})
	}
}

// sortStringKeys goes through an entire set of rows, and for any []string it
// finds, it orders the elements. This is obviously only useful in tests, and
// only in cases where we expect the elements to match, but we don't care what
// order they're in. It's basically the equivalent of assert.ElementsMatch(),
// but the way we use that on rows doesn't recurse down into the field values
// within each row.
func sortStringKeys(in [][]interface{}) {
	for i := range in {
		for j := range in[i] {
			switch v := in[i][j].(type) {
			case []string:
				sort.Strings(v)
			}
		}
	}
}
