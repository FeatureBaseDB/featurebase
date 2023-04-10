// Package client is an HTTP client for Controller.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/dax/computer"
	controllerhttp "github.com/featurebasedb/featurebase/v3/dax/controller/http"
	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/logger"
)

const (
	defaultScheme = "http"
)

// Ensure type implements interface.
var _ computer.Registrar = (*Client)(nil)
var _ dax.Schemar = (*Client)(nil)

// Client is an HTTP client that operates on the Controller endpoints exposed by
// the main Controller service.
type Client struct {
	address    dax.Address
	httpClient *http.Client
	logger     logger.Logger
}

// New returns a new instance of Client.
func New(address dax.Address, logger logger.Logger) *Client {
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

func (c *Client) CreateDatabase(ctx context.Context, qdb *dax.QualifiedDatabase) error {
	url := fmt.Sprintf("%s/create-database", c.address.WithScheme(defaultScheme))

	// Encode the request.
	postBody, err := json.Marshal(qdb)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting create database request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) DropDatabase(ctx context.Context, qdbid dax.QualifiedDatabaseID) error {
	url := fmt.Sprintf("%s/drop-database", c.address.WithScheme(defaultScheme))

	// Encode the request.
	postBody, err := json.Marshal(qdbid)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting drop database request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) DatabaseByID(ctx context.Context, qdbid dax.QualifiedDatabaseID) (*dax.QualifiedDatabase, error) {
	url := fmt.Sprintf("%s/database-by-id", c.address.WithScheme(defaultScheme))

	// Encode the request.
	postBody, err := json.Marshal(qdbid)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	c.logger.Debugf("POST database-by-id request: url: %s", url)
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return nil, errors.Wrap(err, "posting database-by-id request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	var qdb *dax.QualifiedDatabase
	if err := json.NewDecoder(resp.Body).Decode(&qdb); err != nil {
		return nil, errors.Wrap(err, "reading response body")
	}

	return qdb, nil
}

func (c *Client) DatabaseByName(ctx context.Context, orgID dax.OrganizationID, name dax.DatabaseName) (*dax.QualifiedDatabase, error) {
	url := fmt.Sprintf("%s/database-by-name", c.address.WithScheme(defaultScheme))

	req := &controllerhttp.DatabaseByNameRequest{
		OrganizationID: orgID,
		Name:           name,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	c.logger.Debugf("POST database-by-name request: url: %s", url)
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return nil, errors.Wrap(err, "posting database-by-name request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	var qdb *dax.QualifiedDatabase
	if err := json.NewDecoder(resp.Body).Decode(&qdb); err != nil {
		return nil, errors.Wrap(err, "reading response body")
	}

	return qdb, nil
}

func (c *Client) Databases(ctx context.Context, orgID dax.OrganizationID, ids ...dax.DatabaseID) ([]*dax.QualifiedDatabase, error) {
	url := fmt.Sprintf("%s/databases", c.address.WithScheme(defaultScheme))

	req := &controllerhttp.DatabasesRequest{
		OrganizationID: orgID,
		DatabaseIDs:    ids,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	c.logger.Debugf("POST databases request: url: %s", url)
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return nil, errors.Wrap(err, "posting databases request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	var qdbs []*dax.QualifiedDatabase
	if err := json.NewDecoder(resp.Body).Decode(&qdbs); err != nil {
		return nil, errors.Wrap(err, "reading response body")
	}

	return qdbs, nil
}

func (c *Client) SetDatabaseOption(ctx context.Context, qdbid dax.QualifiedDatabaseID, option string, value string) error {
	url := fmt.Sprintf("%s/database/options", c.address.WithScheme(defaultScheme))

	req := &controllerhttp.DatabaseOptionRequest{
		QualifiedDatabaseID: qdbid,
		Option:              option,
		Value:               value,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	request, err := http.NewRequest(http.MethodPatch, url, responseBody)
	if err != nil {
		return errors.Wrap(err, "creating http request")
	}
	request.Header.Set("Content-Type", "application/json")

	// Post the request as PATCH.
	c.logger.Debugf("PATCH database/option request: url: %s", url)
	resp, err := c.httpClient.Do(request)
	if err != nil {
		return errors.Wrap(err, "posting database/option request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	return nil
}

// TODO(tlt): collapse Table into this
func (c *Client) TableByID(ctx context.Context, qtid dax.QualifiedTableID) (*dax.QualifiedTable, error) {
	return c.Table(ctx, qtid)
}

// TODO(tlt): collapse TableID into this
func (c *Client) TableByName(ctx context.Context, qdbid dax.QualifiedDatabaseID, tname dax.TableName) (*dax.QualifiedTable, error) {
	qtid, err := c.TableID(ctx, qdbid, tname)
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
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return nil, errors.Wrap(err, "posting table request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	var qtable *dax.QualifiedTable
	if err := json.NewDecoder(resp.Body).Decode(&qtable); err != nil {
		return nil, errors.Wrap(err, "reading response body")
	}

	return qtable, nil
}

func (c *Client) TableID(ctx context.Context, qdbid dax.QualifiedDatabaseID, name dax.TableName) (dax.QualifiedTableID, error) {
	url := fmt.Sprintf("%s/table-id", c.address.WithScheme(defaultScheme))

	dflt := dax.QualifiedTableID{}

	req := dax.QualifiedTableID{
		QualifiedDatabaseID: qdbid,
		Name:                name,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return dflt, errors.Wrap(err, "marshalling post request")
	}
	requestBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := c.httpClient.Post(url, "application/json", requestBody)
	if err != nil {
		return dflt, errors.Wrap(err, "posting table-id request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return dflt, errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	var qtid dax.QualifiedTableID
	if err := json.NewDecoder(resp.Body).Decode(&qtid); err != nil {
		return dflt, errors.Wrap(err, "reading response body")
	}

	return qtid, nil
}

func (c *Client) Tables(ctx context.Context, qdbid dax.QualifiedDatabaseID, ids ...dax.TableID) ([]*dax.QualifiedTable, error) {
	url := fmt.Sprintf("%s/tables", c.address.WithScheme(defaultScheme))

	req := controllerhttp.TablesRequest{
		OrganizationID: qdbid.OrganizationID,
		DatabaseID:     qdbid.DatabaseID,
		TableIDs:       ids,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return nil, errors.Wrap(err, "posting tables request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Wrapf(errors.UnmarshalJSON(resp.Body), "Status code: %d", resp.StatusCode)
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
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting create table request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
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
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting drop table request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) CreateField(ctx context.Context, qtid dax.QualifiedTableID, fld *dax.Field) error {
	url := fmt.Sprintf("%s/create-field", c.address.WithScheme(defaultScheme))

	req := controllerhttp.CreateFieldRequest{
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
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting create field request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) DropField(ctx context.Context, qtid dax.QualifiedTableID, fldName dax.FieldName) error {
	url := fmt.Sprintf("%s/drop-field", c.address.WithScheme(defaultScheme))

	// Encode the request.
	req := controllerhttp.DropFieldRequest{
		Table: qtid,
		Field: fldName,
	}

	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting drop field request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) IngestShard(ctx context.Context, qtid dax.QualifiedTableID, shard dax.ShardNum) (dax.Address, error) {
	url := fmt.Sprintf("%s/ingest-shard", c.address.WithScheme(defaultScheme))

	var host dax.Address

	req := &controllerhttp.IngestShardRequest{
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
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return host, errors.Wrap(err, "posting ingest-shard request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return host, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var isr *controllerhttp.IngestShardResponse
	if err := json.NewDecoder(resp.Body).Decode(&isr); err != nil {
		return host, errors.Wrap(err, "reading response body")
	}

	return isr.Address, nil
}

func (c *Client) IngestPartition(ctx context.Context, qtid dax.QualifiedTableID, partition dax.PartitionNum) (dax.Address, error) {
	url := fmt.Sprintf("%s/ingest-partition", c.address.WithScheme(defaultScheme))

	var host dax.Address

	req := &controllerhttp.IngestPartitionRequest{
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
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return host, errors.Wrap(err, "posting ingest-partition request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return host, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var isr *controllerhttp.IngestPartitionResponse
	if err := json.NewDecoder(resp.Body).Decode(&isr); err != nil {
		return host, errors.Wrap(err, "reading response body")
	}

	return isr.Address, nil
}

func (c *Client) ComputeNodes(ctx context.Context, qtid dax.QualifiedTableID, shards ...dax.ShardNum) ([]dax.ComputeNode, error) {
	url := fmt.Sprintf("%s/compute-nodes", c.address.WithScheme(defaultScheme))
	c.logger.Debugf("ComputeNodes url: %s", url)

	var nodes []dax.ComputeNode

	req := &controllerhttp.ComputeNodesRequest{
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
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return nodes, errors.Wrap(err, "posting compute-nodes request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nodes, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var cnr *controllerhttp.ComputeNodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&cnr); err != nil {
		return nodes, errors.Wrap(err, "reading response body")
	}

	return cnr.ComputeNodes, nil
}

func (c *Client) TranslateNodes(ctx context.Context, qtid dax.QualifiedTableID, partitions ...dax.PartitionNum) ([]dax.TranslateNode, error) {
	url := fmt.Sprintf("%s/translate-nodes", c.address.WithScheme(defaultScheme))
	c.logger.Debugf("TranslateNodes url: %s", url)

	var nodes []dax.TranslateNode

	req := &controllerhttp.TranslateNodesRequest{
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
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return nodes, errors.Wrap(err, "posting translate-nodes request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nodes, errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	var cnr *controllerhttp.TranslateNodesResponse
	if err := json.NewDecoder(resp.Body).Decode(&cnr); err != nil {
		return nodes, errors.Wrap(err, "reading response body")
	}

	return cnr.TranslateNodes, nil
}

func (c *Client) RegisterNode(ctx context.Context, node *dax.Node) error {
	url := fmt.Sprintf("%s/register-node", c.address.WithScheme(defaultScheme))
	c.logger.Debugf("RegisterNode: %s, url: %s", node.Address, url)

	req := &dax.Node{
		Address:      node.Address,
		RoleTypes:    node.RoleTypes,
		HasDirective: node.HasDirective,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting translate-nodes request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "registration request to %s status code: %d", url, resp.StatusCode)
	}

	return nil
}

func (c *Client) CheckInNode(ctx context.Context, node *dax.Node) error {
	url := fmt.Sprintf("%s/check-in-node", c.address.WithScheme(defaultScheme))
	c.logger.Debugf("CheckInNode url: %s", url)

	req := &dax.Node{
		Address:      node.Address,
		RoleTypes:    node.RoleTypes,
		HasDirective: node.HasDirective,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return errors.Wrap(err, "posting translate-nodes request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
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
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
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

func (c *Client) WorkerCount(ctx context.Context, qdbid dax.QualifiedDatabaseID) (int, error) {
	baseEndpoint := c.address.WithScheme(defaultScheme)
	url := fmt.Sprintf("%s/worker-count", baseEndpoint)

	// Encode the request.
	postBody, err := json.Marshal(qdbid)
	if err != nil {
		return 0, errors.Wrap(err, "marshalling post request")
	}
	responseBody := bytes.NewBuffer(postBody)

	// Post the request.
	c.logger.Debugf("POST database-by-id request: url: %s", url)
	resp, err := c.httpClient.Post(url, "application/json", responseBody)
	if err != nil {
		return 0, errors.Wrap(err, "posting database-by-id request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, errors.Wrapf(errors.UnmarshalJSON(resp.Body), "status code: %d", resp.StatusCode)
	}

	var workers int
	if err := json.NewDecoder(resp.Body).Decode(&workers); err != nil {
		return 0, errors.Wrap(err, "reading response body")
	}

	return workers, nil
}
