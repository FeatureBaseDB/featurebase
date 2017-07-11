package pilosa_test

import (
	"testing"

	"github.com/pilosa/pilosa"
)

func Test_NewConfig(t *testing.T) {
	x := pilosa.NewConfig()

	// Check for bind addres in cluster hosts
	if err := x.Validate(); err != pilosa.ErrConfigHosts {
		t.Fatal(err)
	}

	x.Cluster.Type = "http"
}
