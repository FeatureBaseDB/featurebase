package plugins

import (
	"github.com/pilosa/tanimoto"
	"github.com/pilosa/pilosa"
)

func init() {
	pilosa.RegisterPlugin("Tanimoto", tanimoto.NewTanimotoPlugin)
}