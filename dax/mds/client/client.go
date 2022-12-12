// Package client is an HTTP client for MDS.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/mds/controller"
	mdshttp "github.com/featurebasedb/featurebase/v3/dax/mds/http"
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

// TODO(tlt): collapse Table into this
func (c *Client) TableByID(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	return c.Table(ctx, qtid)
}

// TODO(tlt): collapse TableID into this
func (c *Client) TableByName(ctx context.Context, qual dax.TableQualifier, tname dax.TableName) (*dax.QualifiedTable, error) {
	qtid, err := c.TableID(ctx, qual, tname)
	if err != nil {
		return nil, errors.Wrap(err, "getting table id")
	}
	return c.Table(ctx, qtid)
}

func (c *Client) Table(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	url := fmt.Sprintf("%s/table", c.address.WithScheme(defaultScheme))

	// Encode the request.
	postBody, err := json.Marshal(qtid)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	c.logger.Debugf("POST table request: url: %s", url)
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return nil, errors.Wrap(err, "posting table request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var qtable *dax.QualifiedTable
	if err := json.NewDecoder(resp.Body).Decode(&qtable); err != nil {
		return nil, errors.Wrap(err, "reading response body")
	}

	return qtable, nil
}

func (c *Client) TableID(ctx context.Context, qual dax.TableQualifier, name dax.TableName) (dax.QualifiedTableID, error) {
	url := fmt.Sprintf("%s/table-id", c.address.WithScheme(defaultScheme))

	dflt := dax.QualifiedTableID{}

	req := dax.QualifiedTableID{
		TableQualifier: qual,
		Name:           name,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return dflt, errors.Wrap(err, "marshalling post request")
	}
	requestBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", requestBody)
	if err != nil {
		return dflt, errors.Wrap(err, "posting table-id request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return dflt, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var qtid dax.QualifiedTableID
	if err := json.NewDecoder(resp.Body).Decode(&qtid); err != nil {
		return dflt, errors.Wrap(err, "reading response body")
	}

	return qtid, nil
}

func (c *Client) Tables(ctx context.Context, qual dax.TableQualifier, ids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	url := fmt.Sprintf("%s/tables", c.address.WithScheme(defaultScheme))

	req := mdshttp.TablesRequest{
		OrganizationID: qual.OrganizationID,
		DatabaseID:     qual.DatabaseID,
		TableIDs:       ids,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return nil, errors.Wrap(err, "posting tables request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var qtables []*dax.QualifiedTable
	if err := json.NewDecoder(resp.Body).Decode(&qtables); err != nil {
		return nil, errors.Wrap(err, "reading response body")
	}

	return qtables, nil
}

func (c *Client) CreateTable(ctx context.Context, qtbl *dax.QualifiedTable) error {
	url := fmt.Sprintf("%s/create-table", c.address.WithScheme(defaultScheme))

	// Encode the request.
	postBody, err := json.Marshal(qtbl)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting create table request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (c *Client) DropTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	url := fmt.Sprintf("%s/drop-table", c.address.WithScheme(defaultScheme))

	// Encode the request.
	postBody, err := json.Marshal(qtid)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting drop table request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (c *Client) CreateField(ctx context.Context, qtid dax.QualifiedTableID, fld *dax.Field) error {
	url := fmt.Sprintf("%s/create-field", c.address.WithScheme(defaultScheme))

	req := mdshttp.CreateFieldRequest{
		TableKey: qtid.Key(),
		Field:    fld,
	}
	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting create field request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (c *Client) DropField(ctx context.Context, qtid dax.QualifiedTableID, fldName dax.FieldName) error {
	url := fmt.Sprintf("%s/drop-field", c.address.WithScheme(defaultScheme))

	// Encode the request.
	req := mdshttp.DropFieldRequest{
		Table: qtid,
		Field: fldName,
	}

	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting drop field request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (c *Client) IngestShard(ctx context.Context, qtid dax.QualifiedTableID, shard dax.ShardNum) (dax.Address, error) {
	url := fmt.Sprintf("%s/ingest-shard", c.address.WithScheme(defaultScheme))

	var host dax.Address

	req := &mdshttp.IngestShardRequest{
		Table: qtid,
		Shard: shard,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return host, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return host, errors.Wrap(err, "posting ingest-shard request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return host, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var isr *mdshttp.IngestShardResponse
	if err := json.NewDecoder(resp.Body).Decode(&isr); err != nil {
		return host, errors.Wrap(err, "reading response body")
	}

	return isr.Address, nil
}

func (c *Client) IngestPartition(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum) (dax.Address, error) {
	url := fmt.Sprintf("%s/ingest-partition", c.address.WithScheme(defaultScheme))

	var host dax.Address

	req := &mdshttp.IngestPartitionRequest{
		Table:     qtid,
		Partition: partition,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return host, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return host, errors.Wrap(err, "posting ingest-partition request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return host, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var isr *mdshttp.IngestPartitionResponse
	if err := json.NewDecoder(resp.Body).Decode(&isr); err != nil {
		return host, errors.Wrap(err, "reading response body")
	}

	return isr.Address, nil
}

func (c *Client) ComputeNodes(ctx context.Context, qtid dax.QualifiedTableID, shards ...dax.ShardNum) ([]dax.ComputeNode, error) {
	url := fmt.Sprintf("%s/compute-nodes", c.address.WithScheme(defaultScheme))
	c.logger.Debugf("ComputeNodes url: %s", url)

	var nodes []dax.ComputeNode

	req := &mdshttp.ComputeNodesRequest{
		Table:  qtid,
		Shards: shards,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return nodes, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return nodes, errors.Wrap(err, "posting compute-nodes request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nodes, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var cnr *mdshttp.ComputeNodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&cnr); err != nil {
		return nodes, errors.Wrap(err, "reading response body")
	}

	return cnr.ComputeNodes, nil
}

func (c *Client) TranslateNodes(ctx context.Context, qtid dax.QualifiedTableID, partitions ...dax.PartitionNum) ([]dax.TranslateNode, error) {
	url := fmt.Sprintf("%s/translate-nodes", c.address.WithScheme(defaultScheme))
	c.logger.Debugf("TranslateNodes url: %s", url)

	var nodes []dax.TranslateNode

	req := &mdshttp.TranslateNodesRequest{
		Table:      qtid,
		Partitions: partitions,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return nodes, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return nodes, errors.Wrap(err, "posting translate-nodes request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nodes, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var cnr *mdshttp.TranslateNodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&cnr); err != nil {
		return nodes, errors.Wrap(err, "reading response body")
	}

	return cnr.TranslateNodes, nil
}

func (c *Client) RegisterNode(ctx context.Context, node *dax.Node) error {
	url := fmt.Sprintf("%s/register-node", c.address.WithScheme(defaultScheme))
	c.logger.Debugf("RegisterNode url: %s", url)

	req := &mdshttp.RegisterNodeRequest{
		Address:   node.Address,
		RoleTypes: node.RoleTypes,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting translate-nodes request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (c *Client) CheckInNode(ctx context.Context, node *dax.Node) error {
	url := fmt.Sprintf("%s/check-in-node", c.address.WithScheme(defaultScheme))
	c.logger.Debugf("CheckInNode url: %s", url)

	req := &mdshttp.CheckInNodeRequest{
		Address:   node.Address,
		RoleTypes: node.RoleTypes,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting translate-nodes request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (c *Client) SnapshotTable(ctx context.Context, qtid dax.QualifiedTableID) error {
	url := fmt.Sprintf("%s/snapshot", c.address.WithScheme(defaultScheme))
	c.logger.Debugf("Snapshot url: %s", url)

	// Encode the request.
	postBody, err := json.Marshal(qtid)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := http.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting translate-nodes request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}
