// Package client is an HTTP client for the Queryer.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

const (
	defaultScheme = "http"
)

// Client is an HTTP client that operates on the Controller endpoints exposed by
// the main Controller service.
type Client struct {
	address dax.Address
	logger  logger.Logger
}

// New returns a new instance of Client.
func New(address dax.Address, logger logger.Logger) *Client {
	return &Client{
		address: address,
		logger:  logger,
	}
}

// Health returns true if the client address returns status OK at its /health
// endpoint.
func (c *Client) Health() bool {
	url := fmt.Sprintf("%s/health", c.address.WithScheme(defaultScheme))

	if resp, err := http.Get(url); err != nil {
		return false
	} else if resp.StatusCode != http.StatusOK {
		return false
	}

	return true
}

func (c *Client) QuerySQL(ctx context.Context, qdbid dax.QualifiedDatabaseID, sql io.Reader) (*featurebase.WireQueryResponse, error) {
	url := fmt.Sprintf("%s/databases/%s/sql", c.address.WithScheme(defaultScheme), qdbid.DatabaseID)
	if qdbid.DatabaseID == "" {
		url = fmt.Sprintf("%s/sql", c.address.WithScheme(defaultScheme))
	}

	client := &http.Client{
		Timeout: time.Second * 30,
	}

	// Post the request.
	c.logger.Debugf("POST query sql request: url: %s", url)
	req, err := http.NewRequest(http.MethodPost, url, sql)
	if err != nil {
		return nil, errors.Wrap(err, "creating new post request")
	}
	req.Header.Add("Content-Type", "text/plain")
	req.Header.Add("OrganizationID", string(qdbid.OrganizationID))

	var resp *http.Response
	if resp, err = client.Do(req); err != nil {
		return nil, errors.Wrap(err, "executing post request")
	}

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var wireResp *featurebase.WireQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&wireResp); err != nil {
		return nil, errors.Wrap(err, "reading response body")
	}

	return wireResp, nil
}
