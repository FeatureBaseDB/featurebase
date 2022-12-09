// Package alpha contains inter-service implemenations of interfaces.
package alpha

import (
	"context"
	"encoding/json"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/mds/controller"
	"github.com/featurebasedb/featurebase/v3/errors"
	featurebaseserver "github.com/featurebasedb/featurebase/v3/server"
)

// Ensure type implements interface.
var _ controller.Director = (*Director)(nil)

// Director is a direct, service-to-service implementation of the Director
// interface.
type Director struct {
	computers map[dax.Address]*featurebaseserver.Command
}

func NewDirector() *Director {
	return &Director{
		computers: make(map[dax.Address]*featurebaseserver.Command),
	}
}

func (d *Director) AddCmd(addr dax.Address, cmd *featurebaseserver.Command) error {
	if cmd == nil {
		return errors.New(errors.ErrUncoded, "cannot add nil cmd to director")
	}
	d.computers[addr] = cmd
	return nil
}

func (d *Director) api(addr dax.Address) (*featurebase.API, error) {
	cmd, found := d.computers[addr]
	if !found {
		// Address not registered with the Director.
		return nil, errors.New(errors.ErrUncoded, "cmd not registered with director")
	}

	api := cmd.API
	if api == nil {
		// Command does not have an API.
		return nil, errors.New(errors.ErrUncoded, "cmd does not have an api")
	}

	return api, nil
}

func (d *Director) SendDirective(ctx context.Context, dir *dax.Directive) error {
	api, err := d.api(dir.Address)
	if err != nil {
		return errors.Wrap(err, "getting api from director")
	}

	ndir, err := marshalUnmarshal(dir)
	if err != nil {
		return errors.Wrap(err, "marshalUnmarshal")
	}

	return api.Directive(ctx, ndir)
}

func (d *Director) SendSnapshotShardDataRequest(ctx context.Context, req *dax.SnapshotShardDataRequest) error {
	api, err := d.api(req.Address)
	if err != nil {
		return errors.Wrap(err, "getting api from director")
	}

	nreq, err := marshalUnmarshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalUnmarshal")
	}

	return api.SnapshotShardData(ctx, nreq)
}

func (d *Director) SendSnapshotTableKeysRequest(ctx context.Context, req *dax.SnapshotTableKeysRequest) error {
	api, err := d.api(req.Address)
	if err != nil {
		return errors.Wrap(err, "getting api from director")
	}

	nreq, err := marshalUnmarshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalUnmarshal")
	}

	return api.SnapshotTableKeys(ctx, nreq)
}

func (d *Director) SendSnapshotFieldKeysRequest(ctx context.Context, req *dax.SnapshotFieldKeysRequest) error {
	api, err := d.api(req.Address)
	if err != nil {
		return errors.Wrap(err, "getting api from director")
	}

	nreq, err := marshalUnmarshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalUnmarshal")
	}

	return api.SnapshotFieldKeys(ctx, nreq)
}

// marshalUnmarshal simply marshals anything to json, and then
// unmarshals it. This might seem a bit silly. The reason it exists is
// to exercise the same encode/decode logic that we'd need to if we
// were traversing the network, and guarantee that we aren't sharing
// pointers across API boundaries.
func marshalUnmarshal[K any](a K) (K, error) {
	var newA K
	abytes, err := json.Marshal(a)
	if err != nil {
		return newA, errors.Wrap(err, "marshaling directive")
	}
	if err := json.Unmarshal(abytes, &newA); err != nil {
		return newA, errors.Wrap(err, "unmarshaling directive")
	}
	return newA, nil
}
