package service

import (
	"net/http"

	"github.com/featurebasedb/featurebase/v3/dax"
	mdsclient "github.com/featurebasedb/featurebase/v3/dax/mds/client"
	"github.com/featurebasedb/featurebase/v3/dax/queryer"
	queryerhttp "github.com/featurebasedb/featurebase/v3/dax/queryer/http"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
	fbnet "github.com/featurebasedb/featurebase/v3/net"
)

// Ensure type implements interface.
var _ dax.Service = (*queryerService)(nil)

type queryerService struct {
	uri     *fbnet.URI
	queryer *queryer.Queryer
	logger  logger.Logger
}

func New(uri *fbnet.URI, queryer *queryer.Queryer, logger logger.Logger) *queryerService {
	return &queryerService{
		uri:     uri,
		queryer: queryer,
		logger:  logger,
	}
}

func (q *queryerService) Start() error {
	// Start queryer service.
	if err := q.queryer.Start(); err != nil {
		return errors.Wrap(err, "starting queryer")
	}
	return nil
}

func (q *queryerService) Stop() error {
	return nil
}

func (q *queryerService) Address() dax.Address {
	return dax.Address(q.uri.HostPort() + "/" + dax.ServicePrefixQueryer)
}

func (q *queryerService) HTTPHandler() http.Handler {
	return queryerhttp.Handler(q.queryer)
}

func (q *queryerService) SetMDS(addr dax.Address) error {
	mdscli := mdsclient.New(addr, q.logger)
	q.queryer.SetController(mdscli)
	return nil
}
