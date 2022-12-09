package service

import (
	"net/http"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/mds"
	mdshttp "github.com/featurebasedb/featurebase/v3/dax/mds/http"
	"github.com/featurebasedb/featurebase/v3/errors"
	fbnet "github.com/featurebasedb/featurebase/v3/net"
)

// Ensure type implements interface.
var _ dax.Service = (*mdsService)(nil)

type mdsService struct {
	uri *fbnet.URI
	mds *mds.MDS
}

func New(uri *fbnet.URI, mds *mds.MDS) *mdsService {
	return &mdsService{
		uri: uri,
		mds: mds,
	}
}

func (m *mdsService) Start() error {
	// Start mds service.
	if err := m.mds.Start(); err != nil {
		return errors.Wrap(err, "starting mds")
	}
	return nil
}

func (m *mdsService) Stop() error {
	return m.mds.Stop()
}

func (m *mdsService) Address() dax.Address {
	return dax.Address(m.uri.HostPort() + "/" + dax.ServicePrefixMDS)
}

func (m *mdsService) HTTPHandler() http.Handler {
	return mdshttp.Handler(m.mds)
}
