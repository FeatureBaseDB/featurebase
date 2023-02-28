package schemar_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/gobuffalo/pop/v6"
	"github.com/stretchr/testify/assert"
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

// TODO these tests can be generalized to share setup (getting
// transactor/tx), and then run the same tests against multiple
// underlying implementations

func TestSQLSchemar(t *testing.T) {
	// TODO: currently you must start w/ a clean test database
	// soda drop -e test; soda create -e test; soda migrate -e test
	conn, err := pop.Connect("test")
	assert.NoError(t, err, "connecting")

	trans := sqldb.Transactor{Connection: conn}

	tx, err := trans.BeginTx(context.Background(), true)
	assert.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	schemar := &sqldb.Schemar{}

	err = schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID, Name: dbName}})
	assert.NoError(t, err)

	// create 2nd db in same org
	err = schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID2, Name: dbName2}})
	assert.NoError(t, err)

	// create 3rd db in new org
	schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID2,
			Database:       dax.Database{ID: dbID3, Name: dbName2}})
	assert.NoError(t, err)

	qdbid := dax.QualifiedDatabaseID{OrganizationID: orgID, DatabaseID: dbID}

	err = schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{OrganizationID: orgID,
			Database: dax.Database{
				ID:   dbID,
				Name: dbName},
		})
	if !errors.Is(err, dax.ErrDatabaseIDExists) {
		t.Fatalf("got unexpected error creating DB that already exists: %v", err)
	}

	db, err := schemar.DatabaseByName(tx, orgID, dbName)
	assert.NoError(t, err)
	assert.EqualValues(t, dbID, db.ID)

	err = schemar.SetDatabaseOption(tx, qdbid, "workers_max", "4")
	assert.NoError(t, err)

	err = schemar.SetDatabaseOption(tx, qdbid, "workers_min", "2")
	assert.NoError(t, err)

	db, err = schemar.DatabaseByID(tx, qdbid)
	assert.NoError(t, err)
	assert.EqualValues(t, dbName, db.Name)
	assert.EqualValues(t, 4, db.Options.WorkersMax)
	assert.EqualValues(t, 2, db.Options.WorkersMin)

	dbs, err := schemar.Databases(tx, orgID)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dbs))
	assert.EqualValues(t, orgID, dbs[0].OrganizationID)
	assert.EqualValues(t, orgID, dbs[1].OrganizationID)

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
	assert.NoError(t, err)
	err = schemar.CreateTable(tx, qtbl)
	assert.NoError(t, err)

	// test create field
	err = schemar.CreateField(tx, qtbl.QualifiedID(), &dax.Field{Name: "age", Type: "int"})
	assert.NoError(t, err)

	qtbl, err = schemar.Table(tx, qtbl.QualifiedID())
	assert.NoError(t, err)

	assert.Equal(t, 2, len(qtbl.Fields))

	var ageField *dax.Field

	for _, f := range qtbl.Fields {
		if f.Name == "age" {
			ageField = f
		}
	}

	assert.NotNil(t, ageField)

	// drop field
	err = schemar.DropField(tx, qtbl.QualifiedID(), "age")
	assert.NoError(t, err)

	// ensure field was dropped
	qtbl, err = schemar.Table(tx, qtbl.QualifiedID())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(qtbl.Fields))

	if qtbl.Fields[0].Name != "_id" {
		t.Fatalf("unexpected field: %+v", qtbl.Fields[0])
	}

	tables, err := schemar.Tables(tx, qdbid)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tables))

	_, err = schemar.TableID(tx, qdbid, tblName)
	assert.NoError(t, err)

	err = schemar.DropTable(tx, qtbl.QualifiedID())
	assert.NoError(t, err)

	// make sure Table was deleted
	_, err = schemar.Table(tx, qtbl.QualifiedID())
	assert.NotNil(t, err)

	err = schemar.DropDatabase(tx, qdbid)
	assert.NoError(t, err)

	// make sure DB was deleted
	dbs, err = schemar.Databases(tx, orgID)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dbs))
	assert.EqualValues(t, dbID2, dbs[0].Database.ID)

}

func TestCreateTableNoDBFails(t *testing.T) {
	conn, err := pop.Connect("test")
	assert.NoError(t, err, "connecting")

	trans := sqldb.Transactor{Connection: conn}

	tx, err := trans.BeginTx(context.Background(), true)
	assert.NoError(t, err, "beginning transaction")

	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("committing: %v", err)
		}
	}()

	schemar := &sqldb.Schemar{}
	qdbid := dax.QualifiedDatabaseID{OrganizationID: orgID, DatabaseID: dbID}
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
	err = schemar.CreateTable(tx, qtbl)
	assert.NotNil(t, err)
}

// Testing of failure cases needs to be done in separate tests because they abort the transaction.

// TODO: test create field with empty name fails
// if err := schemar.CreateField(tx, qtbl.QualifiedID(), &dax.Field{Name: "", Type: "string"}); err == nil {
// 	t.Fatal("should have errored")
// } else {
// 	t.Logf("Got expected error creating field with empty name: '%v'", err)
// }

// TODO: test create field with nonexistent table fails
// TODO: test drop non-existent field
