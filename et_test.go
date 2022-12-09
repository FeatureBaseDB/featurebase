package pilosa_test

import (
	"testing"

	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/featurebasedb/featurebase/v3/vprint"
)

func TestEtcd_StartStop(t *testing.T) {
	c := test.MustRunUnsharedCluster(t, 1)
	err := c.Close()
	vprint.VV("DONE %v", err)
}
