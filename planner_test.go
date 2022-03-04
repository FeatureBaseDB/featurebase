// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/test"
)

func TestPlanner_Count(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	index, err := c.GetHolder(0).CreateIndex("i", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	if _, err := index.CreateField("f", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := index.CreateField("x", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "i",
		Query: `
			Set(1, f=10)
			Set(2, f=10)
			Set(3, f=11)
			Set(4, f=12)
			Set(5, f=12)
			Set(6, f=13)

			Set(1, x=100)
			Set(2, x=200)
	`}); err != nil {
		t.Fatal(err)
	}

	t.Run("ALL", func(t *testing.T) {
		q := `SELECT COUNT(*) AS "count" FROM i`
		stmt, err := c.GetNode(0).Server.PlanSQL(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()

		var n int
		if err := stmt.QueryRowContext(context.Background()).Scan(&n); err != nil {
			t.Fatal(err)
		} else if got, want := n, 6; got != want {
			t.Fatalf("Scan()=%d, want %d", got, want)
		}
	})

	t.Run("WHERE", func(t *testing.T) {
		t.Run("EQ", func(t *testing.T) {
			q := `SELECT COUNT(*) AS "count" FROM i WHERE f = 10`
			stmt, err := c.GetNode(0).Server.PlanSQL(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()

			var n int
			if err := stmt.QueryRowContext(context.Background()).Scan(&n); err != nil {
				t.Fatal(err)
			} else if got, want := n, 2; got != want {
				t.Fatalf("Scan()=%d, want %d", got, want)
			}
		})
		t.Run("NE", func(t *testing.T) {
			q := `SELECT COUNT(*) AS "count" FROM i WHERE f != 10`
			stmt, err := c.GetNode(0).Server.PlanSQL(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()

			var n int
			if err := stmt.QueryRowContext(context.Background()).Scan(&n); err != nil {
				t.Fatal(err)
			} else if got, want := n, 4; got != want {
				t.Fatalf("Scan()=%d, want %d", got, want)
			}
		})
		t.Run("LT", func(t *testing.T) {
			q := `SELECT COUNT(*) AS "count" FROM i WHERE f < 12`
			stmt, err := c.GetNode(0).Server.PlanSQL(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()

			var n int
			if err := stmt.QueryRowContext(context.Background()).Scan(&n); err != nil {
				t.Fatal(err)
			} else if got, want := n, 3; got != want {
				t.Fatalf("Scan()=%d, want %d", got, want)
			}
		})
		t.Run("GT", func(t *testing.T) {
			q := `SELECT COUNT(*) AS "count" FROM i WHERE f > 12`
			stmt, err := c.GetNode(0).Server.PlanSQL(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()

			var n int
			if err := stmt.QueryRowContext(context.Background()).Scan(&n); err != nil {
				t.Fatal(err)
			} else if got, want := n, 1; got != want {
				t.Fatalf("Scan()=%d, want %d", got, want)
			}
		})

		t.Run("AND", func(t *testing.T) {
			q := `SELECT COUNT(*) AS "count" FROM i WHERE f = 10 AND x = 100`
			stmt, err := c.GetNode(0).Server.PlanSQL(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()

			var n int
			if err := stmt.QueryRowContext(context.Background()).Scan(&n); err != nil {
				t.Fatal(err)
			} else if got, want := n, 1; got != want {
				t.Fatalf("Scan()=%d, want %d", got, want)
			}
		})

		t.Run("OR", func(t *testing.T) {
			q := `SELECT COUNT(*) AS "count" FROM i WHERE f = 10 OR x = 200 OR f = 12`
			stmt, err := c.GetNode(0).Server.PlanSQL(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Close()

			var n int
			if err := stmt.QueryRowContext(context.Background()).Scan(&n); err != nil {
				t.Fatal(err)
			} else if got, want := n, 4; got != want {
				t.Fatalf("Scan()=%d, want %d", got, want)
			}
		})
	})
}

func TestPlanner_Select(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex("i0", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}
	defer i0.Close()

	if _, err := i0.CreateField("a", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("b", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	i1, err := c.GetHolder(0).CreateIndex("i1", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}
	defer i1.Close()

	if _, err := i1.CreateField("x", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i1.CreateField("y", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "i0",
		Query: `
			Set(1, a=10)
			Set(1, b=100)
			Set(2, a=20)
			Set(2, b=200)
	`}); err != nil {
		t.Fatal(err)
	}

	t.Run("UnqualifiedColumns", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT a, b, _id FROM i0`)
		if diff := cmp.Diff([][]interface{}{
			{int64(10), int64(100), int64(1)},
			{int64(20), int64(200), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "a", Type: "INT"},
			{Name: "b", Type: "INT"},
			{Name: "_id", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("QualifiedColumns", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT i0._id, i0.a, i0.b FROM i0`)
		if diff := cmp.Diff([][]interface{}{
			{int64(1), int64(10), int64(100)},
			{int64(2), int64(20), int64(200)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "_id", Type: "INT"},
			{Name: "a", Type: "INT"},
			{Name: "b", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("UnqualifiedStar", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT * FROM i0`)
		if diff := cmp.Diff([][]interface{}{
			{int64(1), int64(10), int64(100)},
			{int64(2), int64(20), int64(200)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "_id", Type: "INT"},
			{Name: "a", Type: "INT"},
			{Name: "b", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("QualifiedStar", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT i0.* FROM i0`)
		if diff := cmp.Diff([][]interface{}{
			{int64(1), int64(10), int64(100)},
			{int64(2), int64(20), int64(200)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "_id", Type: "INT"},
			{Name: "a", Type: "INT"},
			{Name: "b", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("NoIdentifier", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT a, b FROM i0`)
		if diff := cmp.Diff([][]interface{}{
			{int64(10), int64(100)},
			{int64(20), int64(200)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "a", Type: "INT"},
			{Name: "b", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		_, err := c.GetNode(0).Server.PlanSQL(context.Background(), `SELECT xyz FROM i0`)
		if err == nil || !strings.Contains(err.Error(), `xyz: field not found`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestPlanner_GroupBy(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex("i0", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}
	defer i0.Close()

	if _, err := i0.CreateField("x"); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("y", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i0.CreateField("z", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "i0",
		Query: `
			Set(1, x=10)
			Set(1, x=20)
			Set(1, y=100)
			Set(1, z=500)

			Set(2, x=10)
			Set(2, y=200)
			Set(2, z=500)

			Set(3, x=20)
			Set(3, z=600)
	`}); err != nil {
		t.Fatal(err)
	}

	t.Run("Count", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT COUNT(*), x FROM i0 GROUP BY x`)
		if diff := cmp.Diff([][]interface{}{
			{int64(2), int64(10)},
			{int64(2), int64(20)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "count", Type: "INT"},
			{Name: "x", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("DistinctCount", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT COUNT(DISTINCT z), x FROM i0 GROUP BY x`)
		if diff := cmp.Diff([][]interface{}{
			{int64(1), int64(10)},
			{int64(2), int64(20)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "count", Type: "INT"},
			{Name: "x", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("Sum", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT sum(y), x FROM i0 GROUP BY x`)
		if diff := cmp.Diff([][]interface{}{
			{int64(300), int64(10)},
			{int64(100), int64(20)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "sum", Type: "INT"},
			{Name: "x", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ReorderColumns", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT x, COUNT(*) FROM i0 GROUP BY x`)
		if diff := cmp.Diff([][]interface{}{
			{int64(10), int64(2)},
			{int64(20), int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "x", Type: "INT"},
			{Name: "count", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("NoResultColumn", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT COUNT(*) FROM i0 GROUP BY x`)
		if diff := cmp.Diff([][]interface{}{
			{int64(2)},
			{int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "count", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestPlanner_InnerJoin(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	i0, err := c.GetHolder(0).CreateIndex("i0", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}
	defer i0.Close()

	if _, err := i0.CreateField("a", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	i1, err := c.GetHolder(0).CreateIndex("i1", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}
	defer i1.Close()

	if _, err := i1.CreateField("parentid", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	} else if _, err := i1.CreateField("x", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	// Populate with data.
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "i0",
		Query: `
			Set(1, a=10)
			Set(2, a=20)
			Set(3, a=30)
	`}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "i1",
		Query: `
			Set(1, parentid=1)
			Set(1, x=100)
			
			Set(2, parentid=1)
			Set(2, x=200)
			
			Set(3, parentid=2)
			Set(3, x=300)
	`}); err != nil {
		t.Fatal(err)
	}

	t.Run("Count", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT COUNT(*) FROM i0 INNER JOIN i1 ON i0._id = i1.parentid`)
		if diff := cmp.Diff([][]interface{}{
			{int64(2)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "count", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("CountWithParentCondition", func(t *testing.T) {
		results, columns := mustQueryRows(t, c.GetNode(0).Server, `SELECT COUNT(*) FROM i0 INNER JOIN i1 ON i0._id = i1.parentid WHERE i0.a = 10`)
		if diff := cmp.Diff([][]interface{}{
			{int64(1)},
		}, results); diff != "" {
			t.Fatal(diff)
		}

		if diff := cmp.Diff([]*pilosa.StmtColumn{
			{Name: "count", Type: "INT"},
		}, columns); diff != "" {
			t.Fatal(diff)
		}
	})
}

func mustQueryRows(tb testing.TB, svr *pilosa.Server, q string) (results [][]interface{}, columns []*pilosa.StmtColumn) {
	tb.Helper()

	stmt, err := svr.PlanSQL(context.Background(), q)
	if err != nil {
		tb.Fatal(err)
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(context.Background())
	if err != nil {
		tb.Fatal(err)
	}

	results = make([][]interface{}, 0)
	for rows.Next() {
		result := make([]interface{}, len(rows.Columns()))

		// Create list of scan destination pointers.
		dsts := make([]interface{}, len(result))
		for i := range result {
			dsts[i] = &result[i]
		}

		if err := rows.Scan(dsts...); err != nil {
			tb.Fatal(err)
		}

		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		tb.Fatal(err)
	}

	return results, rows.Columns()
}
