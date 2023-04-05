package pilosa_test

import (
	"context"
	"testing"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	daxtest "github.com/featurebasedb/featurebase/v3/dax/test"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/stretchr/testify/assert"
)

// Ensure holder can handle an incoming directive.
func TestAPI_Directive(t *testing.T) {

	c := test.MustRunCluster(t, 1)
	defer c.Close()

	api := c.GetPrimary().API
	ctx := context.Background()

	qdbid := dax.NewQualifiedDatabaseID("acme", "db1")
	tbl1 := daxtest.TestQualifiedTableWithID(t, qdbid, "1", "tbl1", 12, false)
	tbl2 := daxtest.TestQualifiedTableWithID(t, qdbid, "2", "tbl2", 12, false)
	tbl3 := daxtest.TestQualifiedTableWithID(t, qdbid, "3", "tbl3", 12, false)

	t.Run("Schema", func(t *testing.T) {

		// Empty directive (and empty holder).
		{
			d := &dax.Directive{
				Method:  dax.DirectiveMethodFull,
				Version: 1,
			}
			err := api.ApplyDirective(ctx, d)
			assert.NoError(t, err)
			assertTablesMatch(t, []string{}, api.Holder().Indexes())
		}

		// Add a new table.
		{
			d := &dax.Directive{
				Method: dax.DirectiveMethodFull,
				Tables: []*dax.QualifiedTable{
					tbl1,
				},
				Version: 2,
			}
			err := api.ApplyDirective(ctx, d)
			assert.NoError(t, err)
			assertTablesMatch(t, []string{"tbl__acme__db1__1"}, api.Holder().Indexes())
		}

		// Add a new table, and keep the existing table.
		{
			d := &dax.Directive{
				Method: dax.DirectiveMethodFull,
				Tables: []*dax.QualifiedTable{
					tbl1,
					tbl2,
				},
				Version: 3,
			}
			err := api.ApplyDirective(ctx, d)
			assert.NoError(t, err)
			assertTablesMatch(t, []string{"tbl__acme__db1__1", "tbl__acme__db1__2"}, api.Holder().Indexes())
		}

		// Add a new table and remove one of the existing tables.
		{
			d := &dax.Directive{
				Method: dax.DirectiveMethodFull,
				Tables: []*dax.QualifiedTable{
					tbl2,
					tbl3,
				},
				Version: 4,
			}
			err := api.ApplyDirective(ctx, d)
			assert.NoError(t, err)
			assertTablesMatch(t, []string{"tbl__acme__db1__2", "tbl__acme__db1__3"}, api.Holder().Indexes())
		}
	})
}

// assertTablesMatch is a helper function which asserts that the list of index
// names in `actual` match those provided in `expected`.
func assertTablesMatch(t *testing.T, expected []string, actual []*pilosa.Index) {
	t.Helper()

	act := make([]string, len(actual))
	for i := range actual {
		act[i] = actual[i].Name()
	}
	assert.ElementsMatch(t, expected, act)
}
