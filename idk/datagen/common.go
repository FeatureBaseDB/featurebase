package datagen

import (
	"github.com/molecula/featurebase/v3/idk"
)

// SourceGenerator is an interface for anything which can generate
// data by providing one or more idk.Source. It also contains
// a method for getting information about the supported sources.
// Info() - return information about the generator along with
//          a list of supported types
// Sources() - provided a string key and configuration, returns
// a list of primary key fields and a list of sources.
type SourceGenerator interface {
	Info() string
	Sources(string, SourceGeneratorConfig) ([]string, []idk.Source, error)
	Config() SourceGeneratorConfig
}

// SourceGeneratorConfig provides configuration values used by
// implementations of the SourceGenerator interface.
type SourceGeneratorConfig struct {
	StartFrom    uint64
	EndAt        uint64
	Concurrency  int
	Seed         int64
	CustomConfig string
}

// Seedable is an interface representing anything for which
// a seed can be provided.
type Seedable interface {
	Seed(int64)
}
