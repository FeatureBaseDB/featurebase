package pilosa_test

import (
	"testing"

	"github.com/molecula/featurebase/v3/test"
	"github.com/molecula/featurebase/v3/vprint"
)

func TestEtcd_StartStop(t *testing.T) {
	c := test.MustRunUnsharedCluster(t, 1)
	err := c.Close()
	vprint.VV("DONE %v", err)
}
