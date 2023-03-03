package schemar_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/gobuffalo/pop/v6"
	"github.com/stretchr/testify/require"
)

const (
	orgID   = "orgid"
	orgID2  = "orgid2"
	dbID    = "blah"
	dbID2   = "blah2"
	dbID3   = "blah3"
	dbID4   = "blah4"
	dbName  = "haha"
	dbName2 = "haha2"
	tblName = "tbl"
)

var (
	qdbid = dax.QualifiedDatabaseID{OrganizationID: orgID, DatabaseID: dbID}
)

// TODO these tests can be generalized to share setup (getting
// transactor/tx), and then run the same tests against multiple
// underlying implementations

func TestSQLSchemar(t *testing.T) {
	// TODO: currently you must start w/ a clean test database
	// soda drop -e test; soda create -e test; soda migrate -e test
	conn, err := pop.Connect("test")
	require.NoError(t, err, "connecting")

	trans := sqldb.Transactor{Connection: conn}

	tx, err := trans.BeginTx(context.Background(), true)
	require.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	schemar := sqldb.NewSchemar(nil)

	err = schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID, Name: dbName}})
	require.NoError(t, err)

	// create 2nd db in same org
	err = schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID2, Name: dbName2}})
	require.NoError(t, err)

	// create 3rd db in new org
	schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID2,
			Database:       dax.Database{ID: dbID3, Name: dbName2}})
	require.NoError(t, err)

	err = schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{OrganizationID: orgID,
			Database: dax.Database{
				ID:   dbID,
				Name: dbName},
		})
	if !errors.Is(err, dax.ErrDatabaseIDExists) {
		t.Fatalf("got unexpected error creating DB that already exists: %v", err)
	}

	// make sure querying with empty org ID brings back all databases
	dbs, err := schemar.Databases(tx, "")
	require.NoError(t, err)
	require.Equal(t, 3, len(dbs))

	db, err := schemar.DatabaseByName(tx, orgID, dbName)
	require.NoError(t, err)
	require.EqualValues(t, dbID, db.ID)

	err = schemar.SetDatabaseOption(tx, qdbid, "workers_max", "4")
	require.NoError(t, err)

	err = schemar.SetDatabaseOption(tx, qdbid, "workers_min", "2")
	require.NoError(t, err)

	db, err = schemar.DatabaseByID(tx, qdbid)
	require.NoError(t, err)
	require.EqualValues(t, dbName, db.Name)
	require.EqualValues(t, 4, db.Options.WorkersMax)
	require.EqualValues(t, 2, db.Options.WorkersMin)

	dbs, err = schemar.Databases(tx, orgID)
	require.NoError(t, err)
	require.Equal(t, 2, len(dbs))
	require.EqualValues(t, orgID, dbs[0].OrganizationID)
	require.EqualValues(t, orgID, dbs[1].OrganizationID)

	// test create table
	qtbl := &dax.QualifiedTable{
		QualifiedDatabaseID: qdbid,
		Table: dax.Table{
			Name: tblName,
			Fields: []*dax.Field{{
				Name:    "_id",
				Type:    "string",
				Options: dax.FieldOptions{},
			}},
			PartitionN:  4,
			Description: "desc",
			Owner:       "own",
			UpdatedBy:   "me",
		},
	}

	_, err = qtbl.CreateID()
	require.NoError(t, err)
	err = schemar.CreateTable(tx, qtbl)
	require.NoError(t, err)

	// test create field
	err = schemar.CreateField(tx, qtbl.QualifiedID(), &dax.Field{Name: "age", Type: "int"})
	require.NoError(t, err)

	qtbl, err = schemar.Table(tx, qtbl.QualifiedID())
	require.NoError(t, err)

	require.Equal(t, 2, len(qtbl.Fields))

	var ageField *dax.Field

	for _, f := range qtbl.Fields {
		if f.Name == "age" {
			ageField = f
		}
	}

	require.NotNil(t, ageField)

	// drop field
	err = schemar.DropField(tx, qtbl.QualifiedID(), "age")
	require.NoError(t, err)

	// ensure field was dropped
	qtbl, err = schemar.Table(tx, qtbl.QualifiedID())
	require.NoError(t, err)
	require.Equal(t, 1, len(qtbl.Fields))

	if qtbl.Fields[0].Name != "_id" {
		t.Fatalf("unexpected field: %+v", qtbl.Fields[0])
	}

	tables, err := schemar.Tables(tx, qdbid)
	require.NoError(t, err)
	require.Equal(t, 1, len(tables))

	// TODO add test for Tables passing table ids
	tables, err = schemar.Tables(tx, qdbid, tables[0].ID)
	require.NoError(t, err)
	require.Equal(t, 1, len(tables))

	_, err = schemar.TableID(tx, qdbid, tblName)
	require.NoError(t, err)

	err = schemar.DropTable(tx, qtbl.QualifiedID())
	require.NoError(t, err)

	// make sure Table was deleted
	_, err = schemar.Table(tx, qtbl.QualifiedID())
	require.NotNil(t, err)

	err = schemar.DropDatabase(tx, qdbid)
	require.NoError(t, err)

	// make sure DB was deleted
	dbs, err = schemar.Databases(tx, orgID)
	require.NoError(t, err)
	require.Equal(t, 1, len(dbs))
	require.EqualValues(t, dbID2, dbs[0].Database.ID)

}

func TestCreateTableNoDBFails(t *testing.T) {
	_, tx, finish := setupSQLDBTx(t)
	defer finish()

	schemar := sqldb.NewSchemar(nil)
	qtbl := &dax.QualifiedTable{
		QualifiedDatabaseID: qdbid,
		Table: dax.Table{
			Name: tblName,
			Fields: []*dax.Field{{
				Name:    "_id",
				Type:    "string",
				Options: dax.FieldOptions{},
			}},
			PartitionN:  4,
			Description: "desc",
			Owner:       "own",
			UpdatedBy:   "me",
		},
	}

	qtbl.QualifiedDatabaseID = dax.QualifiedDatabaseID{OrganizationID: orgID, DatabaseID: dbID4}
	qtbl.ID = ""
	qtbl.CreateID()
	err := schemar.CreateTable(tx, qtbl)
	require.NotNil(t, err)
}

// Testing of failure cases needs to be done in separate tests because they abort the transaction.

// TODO: test create field with empty name fails
// if err := schemar.CreateField(tx, qtbl.QualifiedID(), &dax.Field{Name: "", Type: "string"}); err == nil {
// 	t.Fatal("should have errored")
// } else {
// 	t.Logf("Got expected error creating field with empty name: '%v'", err)
// }

// TODO: test create field with nonexistent table fails

func setupSQLDBTx(t *testing.T) (trans controller.Transactor, tx dax.Transaction, finish func()) {
	conn, err := pop.Connect("test")
	require.NoError(t, err, "connecting")

	trans = sqldb.Transactor{Connection: conn}

	tx, err = trans.BeginTx(context.Background(), true)
	require.NoError(t, err, "beginning transaction")

	finish = func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("committing: %v", err)
		}
	}
	return trans, tx, finish
}

func TestDropNonexistentField(t *testing.T) {
	trans, tx, _ := setupSQLDBTx(t)

	schemar := sqldb.NewSchemar(nil)

	err := schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID, Name: dbName}})
	require.NoError(t, err)

	qtbl := &dax.QualifiedTable{
		QualifiedDatabaseID: qdbid,
		Table: dax.Table{
			Name: tblName,
			Fields: []*dax.Field{
				{
					Name:    "_id",
					Type:    "string",
					Options: dax.FieldOptions{},
				},
				{
					Name:    "age",
					Type:    "int",
					Options: dax.FieldOptions{},
				},
			},
			PartitionN:  4,
			Description: "desc",
			Owner:       "own",
			UpdatedBy:   "me",
		},
	}

	fmt.Println("here")

	_, err = qtbl.CreateID()
	require.NoError(t, err)
	err = schemar.CreateTable(tx, qtbl)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	defer func() {
		tx, err = trans.BeginTx(context.Background(), true)
		if err != nil {
			t.Fatalf("Couldn't get tx to drop database: %v", err)
		}
		err = schemar.DropDatabase(tx, qdbid)
		if err != nil {
			t.Logf("dropping database: %v", err)
		}
	}()

	fmt.Println("here2")
	// Test Drop non-existent field fails with correct error
	tx, err = trans.BeginTx(context.Background(), true)
	require.NoError(t, err)
	err = schemar.DropField(tx, qtbl.QualifiedID(), "unknownField")
	require.True(t, errors.Is(err, dax.ErrFieldDoesNotExist))
	tx.Rollback()

	// Test Lookup non-existent table fails with correct error (by name)
	tx, err = trans.BeginTx(context.Background(), true)
	require.NoError(t, err)
	_, err = schemar.Table(tx, dax.QualifiedTableID{QualifiedDatabaseID: qdbid, Name: "humbug"})
	require.True(t, errors.Is(err, dax.ErrTableNameDoesNotExist))
	tx.Rollback()
	fmt.Println("here4")

	// Test Lookup non-existent table fails with correct error (by name)
	tx, err = trans.BeginTx(context.Background(), true)
	require.NoError(t, err)
	_, err = schemar.Table(tx, dax.QualifiedTableID{QualifiedDatabaseID: qdbid, ID: "bumhug", Name: "humbug"})
	require.True(t, errors.Is(err, dax.ErrTableIDDoesNotExist))
	tx.Rollback()

	// Test Lookup non-existent tableID fails with correct error (by name)
	tx, err = trans.BeginTx(context.Background(), true)
	require.NoError(t, err)
	_, err = schemar.TableID(tx, qdbid, "humbug")
	require.True(t, errors.Is(err, dax.ErrTableNameDoesNotExist))
	tx.Rollback()

	fmt.Println("here5")
	// Test Create existing field fails
	// tx, err = trans.BeginTx(context.Background(), true)
	// require.NoError(t, err)
	// err = schemar.CreateField(tx, qtbl.QualifiedID(), &dax.Field{Name: "age", Type: "int", Options: dax.FieldOptions{}})
	// fmt.Println("isING")
	// val := errors.Is(err, dax.ErrFieldExists)
	// fmt.Println("requireING")
	// require.True(t, val)
	// fmt.Println("uh require complete?")
	// tx.Rollback()

	// fmt.Println("here6")
	// // Test Create field empty name fails
	// tx, err = trans.BeginTx(context.Background(), true)
	// require.NoError(t, err)
	// err = schemar.CreateField(tx, qtbl.QualifiedID(), &dax.Field{Name: "", Type: "int", Options: dax.FieldOptions{}})
	// require.True(t, errors.Is(err, cschemar.ErrCodeFieldNameInvalid))
	// tx.Rollback()

}
