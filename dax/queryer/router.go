package queryer

import (
	"github.com/molecula/featurebase/v3/dax"
)

type Router interface {
	Importer(addr dax.Address) Importer
}

// Ensure type implements interface.
var _ Router = &NopRouter{}

// NopRouter is a no-op implementation of the Router interface.
type NopRouter struct{}

func NewNopRouter() *NopRouter {
	return &NopRouter{}
}

func (d *NopRouter) Importer(addr dax.Address) Importer {
	return nil
}
