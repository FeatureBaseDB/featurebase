package schemar_test

import (
	"context"
	"testing"

	"github.com/molecula/featurebase/v3/dax"
	daxtest "github.com/molecula/featurebase/v3/dax/test"
	"github.com/molecula/featurebase/v3/errors"
	"github.com/stretchr/testify/assert"
)

func TestSchemar(t *testing.T) {
	orgID := dax.OrganizationID("acme")
	dbID := dax.DatabaseID("db1")
	invalidTableID := dax.TableID("invalidID")
	tableName := dax.TableName("foo")
	tableName0 := dax.TableName("foo")
	tableName1 := dax.TableName("bar")
	tableID0 := "2"
	tableID1 := "1"
	partitionN := 12

	ctx := context.Background()
	qual := dax.NewTableQualifier(orgID, dbID)

	t.Run("NewSchemar", func(t *testing.T) {
		s, cleanup := daxtest.NewSchemar(t)
		defer cleanup()

		// Add new table.
		tbl := dax.NewTable(tableName)
		tbl.Fields = []*dax.Field{
			{
				Name: dax.PrimaryKeyFieldName,
				Type: dax.BaseTypeString,
			},
			{
				Name: "intField",
				Type: dax.BaseTypeInt,
			},
		}
		qtbl := dax.NewQualifiedTable(qual, tbl)
		qtbl.CreateID()
		assert.NoError(t, s.CreateTable(ctx, qtbl))

		// Try adding the table again.
		err := s.CreateTable(ctx, qtbl)
		if assert.Error(t, err) {
			assert.True(t, errors.Is(err, dax.ErrTableIDExists))
		}

		qtid := qtbl.QualifiedID()

		// Get the table.
		{
			tbl, err := s.Table(ctx, qtid)
			assert.NoError(t, err)
			assert.Equal(t, tableName, tbl.Name)
		}

		// Drop the table.
		{
			err := s.DropTable(ctx, qtid)
			assert.NoError(t, err)
		}

		// Drop invalid table.
		{
			iqtid := dax.NewQualifiedTableID(qual, invalidTableID)
			err := s.DropTable(ctx, iqtid)
			if assert.Error(t, err) {
				assert.True(t, errors.Is(err, dax.ErrTableIDDoesNotExist))
			}
		}
	})

	t.Run("GetTables", func(t *testing.T) {
		s, cleanup := daxtest.NewSchemar(t)
		defer cleanup()

		exp := []*dax.QualifiedTable{}
		tables, err := s.Tables(ctx, qual)
		assert.NoError(t, err)
		assert.Equal(t, exp, tables)

		qtbl0 := daxtest.TestQualifiedTableWithID(t, qual, tableID0, tableName0, partitionN, false)
		qtbl1 := daxtest.TestQualifiedTableWithID(t, qual, tableID1, tableName1, partitionN, false)

		// Add a couple of tables.
		assert.NoError(t, s.CreateTable(ctx, qtbl0))
		assert.NoError(t, s.CreateTable(ctx, qtbl1))

		exp = []*dax.QualifiedTable{
			qtbl1,
			qtbl0,
		}

		// All tables.
		tables, err = s.Tables(ctx, qual)
		assert.NoError(t, err)
		assert.Equal(t, exp, tables)

		// With a valid filter.
		tables, err = s.Tables(ctx, qual, qtbl0.ID)
		assert.NoError(t, err)
		assert.Equal(t, exp[1:], tables)

		// With an invalid filter.
		tables, err = s.Tables(ctx, qual, invalidTableID)
		assert.NoError(t, err)
		assert.Equal(t, exp[0:0], tables)

		// With both valid and invalid filters.
		tables, err = s.Tables(ctx, qual, qtbl0.ID, invalidTableID)
		assert.NoError(t, err)
		assert.Equal(t, exp[1:], tables)

		// With all valid filters.
		tables, err = s.Tables(ctx, qual, qtbl0.ID, qtbl1.ID)
		assert.NoError(t, err)
		assert.Equal(t, exp, tables)
	})
}
