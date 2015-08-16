package db_test

import (
	"testing"

	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/util"
)

func TestCluster(t *testing.T) {
	c := db.NewCluster()
	d := c.GetOrCreateDatabase("main")

	f := d.GetOrCreateFrame("general")
	sl := d.GetOrCreateSlice(0)
	d.GetOrCreateFragment(f, sl, util.Id())
}
