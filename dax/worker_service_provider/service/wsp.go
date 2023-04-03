package service

import (
	"net/http"

	"github.com/featurebasedb/featurebase/v3/dax"
	controllerclient "github.com/featurebasedb/featurebase/v3/dax/controller/client"
	"github.com/featurebasedb/featurebase/v3/dax/worker_service_provider"
	wsphttp "github.com/featurebasedb/featurebase/v3/dax/worker_service_provider/http"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	fbnet "github.com/featurebasedb/featurebase/v3/net"
)

// Ensure type implements interface.
var _ dax.Service = (*wspService)(nil)

type wspService struct {
	uri    *fbnet.URI
	wsp    *worker_service_provider.WSP
	logger logger.Logger
}

func New(uri *fbnet.URI, wsp *worker_service_provider.WSP, logger logger.Logger) *wspService {
	return &wspService{
		uri:    uri,
		wsp:    wsp,
		logger: logger.WithPrefix("WorkerSvcProvider: "),
	}
}

func (w *wspService) Start() error {
	// Start wsp service.
	if err := w.wsp.Start(); err != nil {
		return errors.Wrap(err, "starting wsp")
	}
	return nil
}

func (w *wspService) Stop() error {
	return nil
}

func (w *wspService) Address() dax.Address {
	return dax.Address(w.uri.HostPort() + "/" + dax.ServicePrefixWSP)
}

func (w *wspService) HTTPHandler() http.Handler {
	return wsphttp.Handler(w.wsp)
}

func (w *wspService) SetController(addr dax.Address) error {
	controllercli := controllerclient.New(addr, w.logger)
	w.wsp.SetController(controllercli)
	return nil
}
