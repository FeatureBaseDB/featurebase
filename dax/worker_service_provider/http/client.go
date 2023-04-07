package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/controller"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

const (
	defaultScheme = "http"
)

var _ controller.WorkerServiceProvider = (*Client)(nil)

// Client is an HTTP client that operates on the WorkerServiceProvider.
type Client struct {
	address    dax.Address
	httpClient *http.Client
	logger     logger.Logger
}

// NewClient returns a new instance of Client.
func NewClient(address dax.Address, logger logger.Logger) controller.WorkerServiceProvider {
	return &Client{
		address: address,
		logger:  logger,
		httpClient: &http.Client{
			Timeout: time.Second * 30,
		},
	}
}

// Health returns true if the client address returns status OK at its /health
// endpoint.
func (c *Client) Health() bool {
	url := fmt.Sprintf("%s/health", c.address.WithScheme(defaultScheme))

	if resp, err := c.httpClient.Get(url); err != nil {
		return false
	} else if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return false
	}

	return true
}

func (c *Client) ClaimService(ctx context.Context, svc *dax.WorkerService) error {
	postBody, err := json.Marshal(svc)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	buf := bytes.NewBuffer(postBody)

	resp, err := c.httpClient.Post(fmt.Sprintf("%s/claim", c.address), "application/json", buf)
	if err != nil {
		return errors.Wrap(err, "making post /claim request")
	}
	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	return nil

}

func (c *Client) UpdateService(ctx context.Context, svc *dax.WorkerService) error {
	postBody, err := json.Marshal(svc)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	buf := bytes.NewBuffer(postBody)

	resp, err := c.httpClient.Post(fmt.Sprintf("%s/update", c.address), "application/json", buf)
	if err != nil {
		return errors.Wrap(err, "making post /update request")
	}
	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}
	return nil

}

func (c *Client) DropService(ctx context.Context, svc *dax.WorkerService) error {
	postBody, err := json.Marshal(svc)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	buf := bytes.NewBuffer(postBody)

	resp, err := c.httpClient.Post(fmt.Sprintf("%s/drop", c.address), "application/json", buf)
	if err != nil {
		return errors.Wrap(err, "making post /drop request")
	}
	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}
	return nil

}
