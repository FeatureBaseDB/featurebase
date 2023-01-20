// Package client is an HTTP client for MDS.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	queryerhttp "github.com/featurebasedb/featurebase/v3/dax/queryer/http"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

const (
	defaultScheme = "http"
)

// Client is an HTTP client that operates on the MDS endpoints exposed by the
// main MDS service.
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

func (c *Client) QuerySQL(ctx context.Context, qdbid dax.QualifiedDatabaseID, sql string) (*featurebase.WireQueryResponse, error) {
	url := fmt.Sprintf("%s/sql", c.address.WithScheme(defaultScheme))

	req := &queryerhttp.SQLRequest{
		OrganizationID: qdbid.OrganizationID,
		DatabaseID:     qdbid.DatabaseID,
		SQL:            sql,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	c.logger.Debugf("POST query sql request: url: %s", url)
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return nil, errors.Wrap(err, "posting query sql request")
	}
	defer resp.Body.Close()

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

func (c *Client) QueryPQL(ctx context.Context, qdbid dax.QualifiedDatabaseID, table dax.TableName, pql string) (*featurebase.WireQueryResponse, error) {
	url := fmt.Sprintf("%s/query", c.address.WithScheme(defaultScheme))

	req := &queryerhttp.QueryRequest{
		OrganizationID: qdbid.OrganizationID,
		DatabaseID:     qdbid.DatabaseID,
		Table:          table,
		PQL:            pql,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	c.logger.Debugf("POST query pql request: url: %s", url)
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return nil, errors.Wrap(err, "posting query pql request")
	}
	defer resp.Body.Close()

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
