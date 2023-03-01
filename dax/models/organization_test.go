package models_test

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/gobuffalo/pop/v6"
	"github.com/stretchr/testify/assert"
)

func TestOrganization(t *testing.T) {
	c, err := pop.Connect("test")
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}

	tx, err := c.NewTransaction()
	assert.NoError(t, err, "getting transaction")

	defer func() {
		err := tx.TX.Rollback()
		if err != nil {
			t.Logf("rolling back: %v", err)
		}
	}()

	o := &models.Organization{
		ID: "orgid",
	}
	if err := tx.Create(o); err != nil {
		t.Fatal(err)
	}

	db := &models.Database{Organization: o, ID: "dbid"}
	if err := tx.Create(db); err != nil {
		t.Fatal(err)
	}

	tab := &models.Table{Name: "mytable1", Database: db, Description: "blah", PartitionN: 2, Owner: "zowner", ID: "tableID"}
	if err := tx.Create(tab); err != nil {
		t.Fatal(err)
	}

	tab2 := &models.Table{Name: "mytable2", Database: db, Description: "blah2", PartitionN: 4, Owner: "zowner2", ID: "tableID2"}
	if err := tx.Create(tab2); err != nil {
		t.Fatal(err)
	}

	col1 := &models.Column{Name: "me", Type: "string", TableID: tab.ID, Constraints: "size 200"}
	if err := tx.Create(col1); err != nil {
		t.Fatal(err)
	}

	o2 := &models.Organization{}
	if err := tx.Eager().Find(o2, o.ID); err != nil {
		t.Fatal(err)
	}

	if len(o2.Databases) < 1 {
		t.Fail()
	}

	tx.Eager().Find(&(o2.Databases[0]), o2.Databases[0].ID)
}
