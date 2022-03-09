package alpha

import (
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/dax/queryer"
	"github.com/molecula/featurebase/v3/errors"
	featurebaseserver "github.com/molecula/featurebase/v3/server"
)

// Ensure type implements interface.
var _ queryer.Router = (*Router)(nil)

type Router struct {
	computers map[dax.Address]*featurebaseserver.Command
}

func NewRouter() *Router {
	return &Router{
		computers: make(map[dax.Address]*featurebaseserver.Command),
	}
}

func (r *Router) AddCmd(addr dax.Address, cmd *featurebaseserver.Command) error {
	if cmd == nil {
		return errors.New(errors.ErrUncoded, "cannot add nil cmd to director")
	}
	r.computers[addr] = cmd
	return nil
}

func (r *Router) Importer(addr dax.Address) queryer.Importer {
	if cmd, found := r.computers[addr]; found {
		return queryer.NewFeatureBaseImporter(cmd.API)
	}
	return nil
}
