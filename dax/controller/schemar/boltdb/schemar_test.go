package boltdb_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/schemar/boltdb"
	daxtest "github.com/featurebasedb/featurebase/v3/dax/test"
	testbolt "github.com/featurebasedb/featurebase/v3/dax/test/boltdb"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/stretchr/testify/assert"
)

func TestSchemar(t *testing.T) {
	orgID := dax.OrganizationID("acme")
	dbID := dax.DatabaseID("db1")
	dbName := dax.DatabaseName("dbname1")
	invalidTableID := dax.TableID("invalidID")
	tableName := dax.TableName("foo")
	tableName0 := dax.TableName("foo")
	tableName1 := dax.TableName("bar")
	tableID0 := "2"
	tableID1 := "1"
	partitionN := 12

	ctx := context.Background()
	qdbid := dax.NewQualifiedDatabaseID(orgID, dbID)

	qdb := &dax.QualifiedDatabase{
		OrganizationID: orgID,
		Database: dax.Database{
			ID:   dbID,
			Name: dbName,
		},
	}

	t.Run("NewSchemar", func(t *testing.T) {
		db := testbolt.MustOpenDB(t)
		defer testbolt.MustCloseDB(t, db)

		t.Cleanup(func() {
			testbolt.CleanupDB(t, db.Path())
		})

		// Initialize the buckets.
		assert.NoError(t, db.InitializeBuckets(boltdb.SchemarBuckets...))

		s := boltdb.NewSchemar(db, logger.NopLogger)

		tx, err := db.BeginTx(ctx, true)
		assert.NoError(t, err)
		defer tx.Rollback()

		// Create database.
		assert.NoError(t, s.CreateDatabase(tx, qdb))

		// Add new table.
		tbl := dax.NewTable(tableName)
		tbl.CreateID()
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
		qtbl := dax.NewQualifiedTable(qdbid, tbl)
		assert.NoError(t, s.CreateTable(tx, qtbl))

		// Try adding the table again.
		err = s.CreateTable(tx, qtbl)
		if assert.Error(t, err) {
			assert.True(t, errors.Is(err, dax.ErrTableIDExists))
		}

		qtid := qtbl.QualifiedID()

		// Get the table.
		{
			tbl, err := s.Table(tx, qtid)
			assert.NoError(t, err)
			assert.Equal(t, tableName, tbl.Name)
		}

		// Drop the table.
		assert.NoError(t, s.DropTable(tx, qtid))

		// Make sure the reverse-lookup (table by name) was dropped as well.
		{
			_, err := s.TableID(tx, qdbid, tableName)
			if assert.Error(t, err) {
				assert.True(t, errors.Is(err, dax.ErrTableNameDoesNotExist))
			}
		}

		// Try adding the table (i.e. the same table name) again.
		assert.NoError(t, s.CreateTable(tx, qtbl))

		// Drop the table again.
		assert.NoError(t, s.DropTable(tx, qtid))

		// Drop invalid table.
		{
			iqtid := dax.NewQualifiedTableID(qdbid, invalidTableID)
			err := s.DropTable(tx, iqtid)
			if assert.Error(t, err) {
				assert.True(t, errors.Is(err, dax.ErrTableIDDoesNotExist))
			}
		}
	})

	t.Run("GetTables", func(t *testing.T) {
		db := testbolt.MustOpenDB(t)
		defer testbolt.MustCloseDB(t, db)

		t.Cleanup(func() {
			testbolt.CleanupDB(t, db.Path())
		})

		// Initialize the buckets.
		assert.NoError(t, db.InitializeBuckets(boltdb.SchemarBuckets...))

		s := boltdb.NewSchemar(db, logger.NopLogger)

		tx, err := db.BeginTx(ctx, true)
		assert.NoError(t, err)
		defer tx.Rollback()

		// Create database.
		assert.NoError(t, s.CreateDatabase(tx, qdb))

		exp := []*dax.QualifiedTable{}
		tables, err := s.Tables(tx, qdbid)
		assert.NoError(t, err)
		assert.Equal(t, exp, tables)

		qtbl0 := daxtest.TestQualifiedTableWithID(t, qdbid, tableID0, tableName0, partitionN, false)
		qtbl1 := daxtest.TestQualifiedTableWithID(t, qdbid, tableID1, tableName1, partitionN, false)

		// Add a couple of tables.
		assert.NoError(t, s.CreateTable(tx, qtbl0))
		assert.NoError(t, s.CreateTable(tx, qtbl1))

		exp = []*dax.QualifiedTable{
			qtbl1,
			qtbl0,
		}

		// All tables.
		tables, err = s.Tables(tx, qdbid)
		assert.NoError(t, err)
		assert.Equal(t, exp, tables)

		// With a valid filter.
		tables, err = s.Tables(tx, qdbid, qtbl0.ID)
		assert.NoError(t, err)
		assert.Equal(t, exp[1:], tables)

		// With an invalid filter.
		tables, err = s.Tables(tx, qdbid, invalidTableID)
		assert.NoError(t, err)
		assert.Equal(t, exp[0:0], tables)

		// With both valid and invalid filters.
		tables, err = s.Tables(tx, qdbid, qtbl0.ID, invalidTableID)
		assert.NoError(t, err)
		assert.Equal(t, exp[1:], tables)

		// With all valid filters.
		tables, err = s.Tables(tx, qdbid, qtbl0.ID, qtbl1.ID)
		assert.NoError(t, err)
		assert.Equal(t, exp, tables)
	})

	t.Run("GetTablesAll", func(t *testing.T) {
		db := testbolt.MustOpenDB(t)
		defer testbolt.MustCloseDB(t, db)

		t.Cleanup(func() {
			testbolt.CleanupDB(t, db.Path())
		})

		// Initialize the buckets.
		assert.NoError(t, db.InitializeBuckets(boltdb.SchemarBuckets...))

		s := boltdb.NewSchemar(db, logger.NopLogger)

		tx, err := db.BeginTx(ctx, true)
		assert.NoError(t, err)
		defer tx.Rollback()

		qtbl0 := daxtest.TestQualifiedTableWithID(t, qdbid, tableID0, tableName0, partitionN, false)
		orgID2 := dax.OrganizationID("acme2")
		qdbid2 := dax.NewQualifiedDatabaseID(orgID2, dbID)
		tableID2 := "3"
		qtbl2 := daxtest.TestQualifiedTableWithID(t, qdbid2, tableID2, dax.TableName("two"), partitionN, false)

		// Create databases.
		assert.NoError(t, s.CreateDatabase(tx, qdb))
		qdb2 := &dax.QualifiedDatabase{
			OrganizationID: orgID2,
			Database: dax.Database{
				ID:   dbID,
				Name: dbName,
			},
		}
		assert.NoError(t, s.CreateDatabase(tx, qdb2))

		assert.NoError(t, s.CreateTable(tx, qtbl0))
		assert.NoError(t, s.CreateTable(tx, qtbl2))

		exp := []*dax.QualifiedTable{qtbl0, qtbl2}

		tables, err := s.Tables(tx, dax.QualifiedDatabaseID{})
		assert.NoError(t, err)
		assert.Equal(t, exp, tables)

		tables, err = s.Tables(tx, dax.QualifiedDatabaseID{OrganizationID: orgID2})
		assert.NoError(t, err)
		assert.Equal(t, []*dax.QualifiedTable{qtbl2}, tables)
	})
}
