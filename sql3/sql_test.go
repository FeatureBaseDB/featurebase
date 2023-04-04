// Copyright 2021 Molecula Corp. All rights reserved.
package sql3_test

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	sql_test "github.com/featurebasedb/featurebase/v3/sql3/test"
	"github.com/featurebasedb/featurebase/v3/sql3/test/defs"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// add test names here to limit the tests to be run. Following sample
// will run only the 2 tests listed in the filter.
// var testsToRunFilter=[]string{"sql1testsgrouper", "sql1testsjoiner"}
var testsToRunFilter = []string{}

func isFilteredTest(s string) bool {
	for i := 0; i < len(testsToRunFilter); i++ {
		if testsToRunFilter[i] == s {
			return true
		}
	}
	return false
}

func TestSQL_Execute(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	svr := c.GetNode(0).Server

	for i, test := range defs.TableTests {
		if len(testsToRunFilter) > 0 && !isFilteredTest(test.Name(0)) {
			continue
		}

		t.Run(test.Name(i), func(t *testing.T) {

			// Create a table with all field types.
			if test.HasTable() {
				_, _, _, err := sql_test.MustQueryRows(t, nil, svr, test.CreateTable())
				assert.NoError(t, err)
			}

			if test.HasTable() && test.HasData() {
				// Populate fields with data.
				_, _, _, err := sql_test.MustQueryRows(t, nil, svr, test.InsertInto(t))
				assert.NoError(t, err)
			}

			for i, sqltest := range test.SQLTests {
				t.Run(sqltest.Name(i), func(t *testing.T) {
					for _, sql := range sqltest.SQLs {
						t.Run(fmt.Sprintf("sql-%s", sql), func(t *testing.T) {
							log.Printf("SQL: %s", sql)
							rows, headers, plan, err := sql_test.MustQueryRows(t, nil, svr, sql)

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

							// TODO(pok) - this will become increasingly problematic as result column headers
							// are not unique and can be empty
							// Put the expRows in the same column order as the headers returned
							// by the query.
							exp := make([][]interface{}, len(sqltest.ExpRows))
							for i := range sqltest.ExpRows {
								exp[i] = make([]interface{}, len(headers))
								for j := range sqltest.ExpHdrs {
									targetIdx := m[sqltest.ExpHdrs[j].Name]
									if sqltest.Compare != defs.ComparePartial {
										assert.GreaterOrEqual(t, len(sqltest.ExpRows[i]), len(headers),
											"expected row set has fewer columns than returned headers")
									}
									// if ExpRows[i] is short, that might be okay if we're doing a "partial"
									// compare.
									if len(sqltest.ExpRows[i]) > j {
										exp[i][targetIdx] = sqltest.ExpRows[i][j]
									}
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
							case defs.ComparePartial:
								assert.LessOrEqual(t, len(sqltest.ExpRows), len(rows))
								// Assert that every non-nil value in the row is found somewhere
								// in the corresponding expected row.
								for i, expRow := range exp {
									// have we found everything in this row yet?
									foundAll := false
									for _, row := range rows {
										maybeFound := true
										for k, exp := range expRow {
											if exp != nil {
												if k > len(row) || row[k] != exp {
													maybeFound = false
													break
												}
											}
										}
										if maybeFound {
											foundAll = true
											break
										}
									}
									if !foundAll {
										t.Errorf("expected row %d: couldn't find any result row matching all its values %#v", i, expRow)
									}
								}
							}

							if sqltest.PlanCheck != nil {
								err := sqltest.PlanCheck(plan)
								require.NoError(t, err)
							}
						})
					}
				})
			}
		})
	}

	if len(testsToRunFilter) > 0 {
		t.Log("WARNING: Only tests specified in the filter list were run. Don't forget to remove the filter before commiting.")
	}
}

func TestSQL_FilterCheck(t *testing.T) {
	if len(testsToRunFilter) > 0 {
		t.Error("An active SQL test filter is found. Test filters should be removed before checking-in the commit. Empty the filter by assigning testsToRunFilter={}")
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

func TestInternalError(t *testing.T) {
	e := sql3.NewErrInternal("foo")
	if !strings.Contains(e.Error(), "test.go") {
		t.Fatalf("internal error from *_test.go file should contain test.go string")
	}
}
