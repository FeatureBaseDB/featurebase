package schemar_test

import (
	"context"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller/sqldb"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/gobuffalo/pop/v6"
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
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	trans := sqldb.Transactor{Connection: conn}

	tx, err := trans.BeginTx(context.Background(), true)
	if err != nil {
		t.Fatalf("getting transaction: %v", err)
	}
	defer func() {
		err := tx.Rollback()
		if err != nil {
			t.Logf("committing: %v", err)
		}
	}()

	schemar := &sqldb.Schemar{}

	if err := schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID, Name: dbName}}); err != nil {
		t.Fatal(err)
	}
	// create 2nd db in same org
	if err := schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID,
			Database:       dax.Database{ID: dbID2, Name: dbName2}}); err != nil {
		t.Fatal(err)
	}
	// create 3rd db in new org
	if err := schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{
			OrganizationID: orgID2,
			Database:       dax.Database{ID: dbID3, Name: dbName2}}); err != nil {
		t.Fatal(err)
	}

	qdbid := dax.QualifiedDatabaseID{OrganizationID: orgID, DatabaseID: dbID}

	if err := schemar.CreateDatabase(tx,
		&dax.QualifiedDatabase{OrganizationID: orgID,
			Database: dax.Database{
				ID:   dbID,
				Name: dbName},
		}); err == nil {
		t.Fatal("expected error, but got none")
	} else if !errors.Is(err, dax.ErrDatabaseIDExists) {
		t.Fatalf("got unexpected error creating DB that already exists: %v", err)
	}

	db, err := schemar.DatabaseByName(tx, orgID, dbName)
	if err != nil {
		t.Fatalf("getting database by name: %v", err)
	}
	if db.ID != dbID {
		t.Fatalf("returned db has wrong name")
	}

	err = schemar.SetDatabaseOption(tx, qdbid, "workers_max", "4")
	if err != nil {
		t.Fatalf("setting workers max: %v", err)
	}
	err = schemar.SetDatabaseOption(tx, qdbid, "workers_min", "2")
	if err != nil {
		t.Fatalf("setting workers min: %v", err)
	}

	db, err = schemar.DatabaseByID(tx, qdbid)
	if err != nil {
		t.Fatalf("getting db by id: %v", err)
	}
	if db.Name != dbName {
		t.Fatalf("returned db with wrong name")
	}
	if db.Options.WorkersMax != 4 || db.Options.WorkersMin != 2 {
		t.Fatalf("unexpected values for options: %+v", db)
	}

	dbs, err := schemar.Databases(tx, orgID)
	if err != nil {
		t.Fatalf("getting databases: %v", err)
	}
	if len(dbs) != 2 {
		t.Fatalf("unexpected number of databases returned: %+v", dbs)
	}
	if dbs[0].OrganizationID != orgID || dbs[1].OrganizationID != orgID {
		t.Fatalf("unexpected orgID from a returned database: %+v", dbs)
	}

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

	if _, err := qtbl.CreateID(); err != nil {
		t.Fatalf("creating table ID: %v", err)
	}
	if err := schemar.CreateTable(tx, qtbl); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	// test create field
	if err := schemar.CreateField(tx, qtbl.QualifiedID(), &dax.Field{Name: "age", Type: "int"}); err != nil {
		t.Fatalf("creating field")
	}
	qtbl, err = schemar.Table(tx, qtbl.QualifiedID())
	if err != nil {
		t.Fatalf("getting table: %v", err)
	}

	if len(qtbl.Fields) != 2 {
		t.Fatalf("unexpected fields: %+v", qtbl.Fields)
	}

	var ageField *dax.Field

	for _, f := range qtbl.Fields {
		if f.Name == "age" {
			ageField = f
		}
	}

	if ageField == nil {
		t.Fatalf("did not get age field: %+v", qtbl)
	}

	// drop field
	if err := schemar.DropField(tx, qtbl.QualifiedID(), "age"); err != nil {
		t.Fatalf("deleting age field: %v", err)
	}

	// ensure field was dropped
	qtbl, err = schemar.Table(tx, qtbl.QualifiedID())
	if err != nil {
		t.Fatalf("getting table: %v", err)
	}
	if len(qtbl.Fields) != 1 {
		t.Fatalf("unexpected number of fields: %+v", qtbl.Fields)
	}
	if qtbl.Fields[0].Name != "_id" {
		t.Fatalf("unexpected field: %+v", qtbl.Fields[0])
	}

	tables, err := schemar.Tables(tx, qdbid)
	if err != nil {
		t.Fatalf("getting tables: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("getting tables: %+v", tables)
	}

	if _, err := schemar.TableID(tx, qdbid, tblName); err != nil {
		t.Fatalf("looking up table ID: %v", err)
	}

	if err := schemar.DropTable(tx, qtbl.QualifiedID()); err != nil {
		t.Fatalf("dropping table: %v", err)
	}

	// make sure Table was deleted
	_, err = schemar.Table(tx, qtbl.QualifiedID())
	if err == nil {
		t.Fatalf("shouldn't be able to find table")
	} else {
		t.Logf("INFO: from fetching nonexistent table: '%v'\n", err)
	}

	if err := schemar.DropDatabase(tx, qdbid); err != nil {
		t.Fatal(err)
	}

	// make sure DB was deleted
	if dbs, err := schemar.Databases(tx, orgID); err != nil {
		t.Fatalf("getting databases: %v", err)
	} else if len(dbs) != 1 {
		t.Fatalf("got len(dbs) != 1: %v", dbs)
	} else if dbs[0].Database.ID != dbID2 {
		t.Fatalf("unexpected database: %v", dbs[0])
	}
}

func TestCreateTableNoDBFails(t *testing.T) {
	conn, err := pop.Connect("test")
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	trans := sqldb.Transactor{Connection: conn}

	tx, err := trans.BeginTx(context.Background(), true)
	if err != nil {
		t.Fatalf("getting transaction: %v", err)
	}
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
	if err := schemar.CreateTable(tx, qtbl); err == nil {
		t.Fatalf("should have errored creating table pointing to non-existent DB")
	}

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
