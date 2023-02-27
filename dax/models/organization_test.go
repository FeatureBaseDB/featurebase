package models_test

import (
	"fmt"
	"testing"

	"github.com/featurebasedb/featurebase/v3/dax/models"
	"github.com/gobuffalo/pop/v6"
)

func TestOrganization(t *testing.T) {
	c, err := pop.Connect("development")
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}

	o := &models.Organization{}
	if err := c.Create(o); err != nil {
		t.Fatal(err)
	}

	db := &models.Database{Organization: o}
	if err := c.Create(db); err != nil {
		t.Fatal(err)
	}

	tab := &models.Table{Name: "mytable1", Database: db, Description: "blah", PartitionN: 2, Owner: "zowner"}
	if err := c.Create(tab); err != nil {
		t.Fatal(err)
	}

	tab2 := &models.Table{Name: "mytable2", Database: db, Description: "blah2", PartitionN: 4, Owner: "zowner2"}
	if err := c.Create(tab2); err != nil {
		t.Fatal(err)
	}

	col1 := &models.Column{Name: "me", Type: "string", TableID: tab.ID, Constraints: "size 200"}
	if err := c.Create(col1); err != nil {
		t.Fatal(err)
	}

	o2 := &models.Organization{}
	if err := c.Eager().Find(o2, o.ID); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%s\n", o2)

	if len(o2.Databases) < 1 {
		t.Fail()
	}

	c.Eager().Find(&(o2.Databases[0]), o2.Databases[0].ID)

	fmt.Printf("%s\n", o2)

}
