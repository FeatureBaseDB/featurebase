package test

import (
	"strings"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

// Executor represents a test wrapper for pilosa.Executor.
type Executor struct {
	*pilosa.Executor
}

// NewExecutor returns a new instance of Executor.
// The executor always matches the uri of the first cluster node.
func NewExecutor(holder *pilosa.Holder, cluster *pilosa.Cluster) *Executor {
	e := &Executor{Executor: pilosa.NewExecutor(nil)}
	e.Holder = holder
	e.Cluster = cluster
	e.URI = cluster.Nodes[0].URI
	return e
}

// MustParse parses s into a PQL query. Panic on error.
func MustParse(s string) *pql.Query {
	q, err := pql.NewParser(strings.NewReader(s)).Parse()
	if err != nil {
		panic(err)
	}
	return q
}
