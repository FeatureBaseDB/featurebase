// Copyright 2021 Molecula Corp. All rights reserved.
// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.
package client

import (
	"bytes"
	"crypto/tls"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto" //nolint:staticcheck
	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/client/types"
	fbproto "github.com/molecula/featurebase/v3/encoding/proto" // TODO use this everywhere and get rid of proto import
	"github.com/molecula/featurebase/v3/logger"
	pnet "github.com/molecula/featurebase/v3/net"
	"github.com/molecula/featurebase/v3/pb"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/stats"
	"github.com/molecula/featurebase/v3/vprint"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// PQLVersion is the version of PQL expected by the client
const PQLVersion = "1.0"

// DefaultShardWidth is used if an index doesn't have it defined.
const DefaultShardWidth = pilosa.ShardWidth

// Client is the HTTP client for Pilosa server.
type Client struct {
	cluster            *Cluster
	client             *http.Client
	logger             logger.Logger
	primaryURI         *pnet.URI
	primaryLock        *sync.RWMutex
	manualFragmentNode *fragmentNode
	manualServerURI    *pnet.URI
	tracer             opentracing.Tracer
	Stats              stats.StatsClient
	// An exponential backoff algorithm retries requests exponentially (if an HTTP request fails),
	// increasing the waiting time between retries up to a maximum backoff time.
	maxBackoff time.Duration
	maxRetries int

	nat map[pnet.URI]pnet.URI

	shardNodes shardNodes
	tick       *time.Ticker
	done       chan struct{}

	// pathPrefix is prepended to every URL path. This is used, for example,
	// when running a compute nodes as a sub-service of the featurebase command.
	// In that case, a path might look like `localhost:8080/compute/schema`,
	// where `/compute` is the pathPrefix.
	pathPrefix string

	AuthToken string
}

func (c *Client) getURIsForShard(index string, shard uint64) ([]*pnet.URI, error) {
	uris, ok := c.shardNodes.Get(index, shard)
	if ok {
		return uris, nil
	}
	fragmentNodes, err := c.fetchFragmentNodes(index, shard)
	if err != nil {
		return nil, errors.Wrap(err, "trying to look up nodes for shard")
	}
	uris = make([]*pnet.URI, 0, len(fragmentNodes))
	for _, fn := range fragmentNodes {
		uris = append(uris, fn.URI())
	}
	c.shardNodes.Put(index, shard, uris)
	return uris, nil
}

func (c *Client) runChangeDetection() {
	for {
		select {
		case <-c.tick.C:
			c.detectClusterChanges()
		case <-c.done:
			return
		}
	}
}

func (c *Client) Close() error {
	c.tick.Stop()
	close(c.done)
	return nil
}

// detectClusterChanges chooses a random index and shard from the
// shardNodes cache and deletes it. It then looks it up from Pilosa to
// see if it still matches, and if not it drops the whole cache.
func (c *Client) detectClusterChanges() {
	c.shardNodes.mu.Lock()
	needsUnlock := true
	// we rely on Go's random map iteration order to get a random
	// element. If it doesn't end up being random, it shouldn't
	// actually matter.
	for index, shardMap := range c.shardNodes.data {
		for shard, uris := range shardMap {
			delete(shardMap, shard)
			c.shardNodes.data[index] = shardMap
			c.shardNodes.mu.Unlock()
			needsUnlock = false
			newURIs, err := c.getURIsForShard(index, shard) // refetch URIs from server.
			if err != nil {
				c.logger.Errorf("problem invalidating shard node cache: %v", err)
				return
			}
			if len(uris) != len(newURIs) {
				c.logger.Printf("invalidating shard node cache old: %v, new: %v", uris, newURIs)
				c.shardNodes.Invalidate()
				return
			}
			for i := range uris {
				u1, u2 := uris[i], newURIs[i]
				if *u1 != *u2 {
					c.logger.Printf("invalidating shard node cache, uri mismatch at %d old: %v, new: %v", i, uris, newURIs)
					c.shardNodes.Invalidate()
					return
				}
			}
			break
		}
		break
	}
	if needsUnlock {
		c.shardNodes.mu.Unlock()
	}
}

// DefaultClient creates a client with the default address and options.
func DefaultClient() *Client {
	return newClientWithCluster(NewClusterWithHost(pnet.DefaultURI()), nil)
}

func newClientFromAddresses(addresses []string, options *ClientOptions) (*Client, error) {
	uris := make([]*pnet.URI, len(addresses))
	for i, address := range addresses {
		uri, err := pnet.NewURIFromAddress(address)
		if err != nil {
			return nil, err
		}
		uris[i] = uri
	}
	cluster := NewClusterWithHost(uris...)
	client := newClientWithCluster(cluster, options)
	return client, nil
}

func newClientWithCluster(cluster *Cluster, options *ClientOptions) *Client {
	client := newClientWithOptions(options)
	client.cluster = cluster
	return client
}

func newClientWithURI(uri *pnet.URI, options *ClientOptions) *Client {
	client := newClientWithOptions(options)
	if options.manualServerAddress {
		fNode := newFragmentNodeFromURI(uri)
		client.manualFragmentNode = &fNode
		client.manualServerURI = uri
		client.cluster = NewClusterWithHost()
	}
	client.cluster = NewClusterWithHost(uri)
	return client
}

func newClientWithOptions(options *ClientOptions) *Client {
	if options == nil {
		options = &ClientOptions{}
	}
	options = options.withDefaults()

	c := &Client{
		client:      newHTTPClient(options.withDefaults()),
		logger:      logger.NewStandardLogger(os.Stderr),
		primaryLock: &sync.RWMutex{},

		shardNodes: newShardNodes(),
		tick:       time.NewTicker(time.Minute),
		done:       make(chan struct{}),

		nat: options.nat,

		pathPrefix: options.pathPrefix,
	}

	if options.tracer == nil {
		c.tracer = NoopTracer{}
	} else {
		c.tracer = options.tracer
	}
	if options.stats == nil {
		c.Stats = stats.NopStatsClient
	} else {
		c.Stats = options.stats
	}

	c.maxRetries = *options.retries
	c.maxBackoff = 2 * time.Minute
	go c.runChangeDetection()
	return c
}

// NewClient creates a client with the given address, URI, or cluster and options.
func NewClient(addrURIOrCluster interface{}, options ...ClientOption) (*Client, error) {
	var cluster *Cluster
	clientOptions := &ClientOptions{
		nat: make(map[pnet.URI]pnet.URI),
	}
	err := clientOptions.addOptions(options...)
	if err != nil {
		return nil, err
	}

	switch u := addrURIOrCluster.(type) {
	case string:
		uri, err := pnet.NewURIFromAddress(u)
		if err != nil {
			return nil, err
		}
		return newClientWithURI(uri, clientOptions), nil
	case []string:
		if len(u) == 1 {
			uri, err := pnet.NewURIFromAddress(u[0])
			if err != nil {
				return nil, err
			}
			return newClientWithURI(uri, clientOptions), nil
		} else if clientOptions.manualServerAddress {
			return nil, ErrSingleServerAddressRequired
		}
		return newClientFromAddresses(u, clientOptions)
	case *pnet.URI:
		uriCopy := *u
		return newClientWithURI(&uriCopy, clientOptions), nil
	case []*pnet.URI:
		if len(u) == 1 {
			uriCopy := *u[0]
			return newClientWithURI(&uriCopy, clientOptions), nil
		} else if clientOptions.manualServerAddress {
			return nil, ErrSingleServerAddressRequired
		}
		cluster = NewClusterWithHost(u...)
	case *Cluster:
		cluster = u
	case nil:
		cluster = NewClusterWithHost()
	default:
		return nil, ErrAddrURIClusterExpected
	}

	return newClientWithCluster(cluster, clientOptions), nil
}

// prefix is a helper function which allows us to provide a pathPrefix value as
// "compute" instead of "/compute".
func (c *Client) prefix() string {
	if c.pathPrefix == "" {
		return ""
	}
	return "/" + c.pathPrefix
}

// Query runs the given query against the server with the given options.
// Pass nil for default options.
func (c *Client) Query(query PQLQuery, options ...interface{}) (*QueryResponse, error) {
	span := c.tracer.StartSpan("Client.Query")
	defer span.Finish()

	if err := query.Error(); err != nil {
		return nil, err
	}
	queryOptions := &QueryOptions{}
	err := queryOptions.addOptions(options...)
	if err != nil {
		return nil, err
	}
	serializedQuery := query.Serialize()
	reqData, err := makeRequestData(serializedQuery.String(), queryOptions)
	if err != nil {
		return nil, errors.Wrap(err, "making request data")
	}
	path := fmt.Sprintf("/index/%s/query", query.Index().name)
	_, respData, err := c.HTTPRequest("POST", path, reqData, c.augmentHeaders(defaultProtobufHeaders()))
	if err != nil {
		return nil, err
	}
	iqr := &pb.QueryResponse{}
	err = proto.Unmarshal(respData, iqr)
	if err != nil {
		return nil, err
	}
	queryResponse, err := newQueryResponseFromInternal(iqr)
	if err != nil {
		return nil, err
	}
	return queryResponse, nil
}

// CreateIndex creates an index on the server using the given Index struct.
func (c *Client) CreateIndex(index *Index) error {
	span := c.tracer.StartSpan("Client.CreateIndex")
	defer span.Finish()

	data := []byte(index.options.String())
	path := fmt.Sprintf("/index/%s", index.name)
	status, body, err := c.HTTPRequest("POST", path, data, c.augmentHeaders(nil))
	if err != nil {
		return errors.Wrapf(err, "creating index: %s", index.name)
	}
	var resp struct {
		CreatedAt int64 `json:"createdAt,omitempty"`
	}
	if err := json.Unmarshal(body, &resp); err == nil && resp.CreatedAt != 0 {
		index.createdAt = resp.CreatedAt
	}

	if status == http.StatusConflict {
		return ErrIndexExists
	}
	return nil
}

// CreateField creates a field on the server using the given Field struct.
func (c *Client) CreateField(field *Field) error {
	span := c.tracer.StartSpan("Client.CreateField")
	defer span.Finish()

	data := []byte(field.options.String())
	path := fmt.Sprintf("/index/%s/field/%s", field.index.name, field.name)
	status, body, err := c.HTTPRequest("POST", path, data, c.augmentHeaders(nil))
	if err != nil {
		return errors.Wrapf(err, "creating field: %s in index: %s", field.name, field.index.name)
	}
	var resp struct {
		CreatedAt int64 `json:"createdAt,omitempty"`
	}
	if err := json.Unmarshal(body, &resp); err == nil && resp.CreatedAt != 0 {
		field.createdAt = resp.CreatedAt
	}

	if status == http.StatusConflict {
		return ErrFieldExists
	}
	return nil
}

// EnsureIndex creates an index on the server if it does not exist.
func (c *Client) EnsureIndex(index *Index) error {
	err := c.CreateIndex(index)
	if err == ErrIndexExists {
		return nil
	}
	return errors.Wrap(err, "creating index")
}

func (c *Client) SyncIndex(index *Index) error {
	err := c.EnsureIndex(index)
	if err != nil {
		return errors.Wrapf(err, "ensuring index exists")
	}

	for name, field := range index.fields {
		if name == "_exists" {
			continue
		}
		err = c.EnsureField(field)
		if err != nil {
			return errors.Wrapf(err, "ensuring field")
		}
	}

	return nil
}

// EnsureField creates a field on the server if it doesn't exists.
func (c *Client) EnsureField(field *Field) error {
	err := c.CreateField(field)
	if err == ErrFieldExists {
		return nil
	}
	return err
}

// DeleteIndex deletes an index on the server.
func (c *Client) DeleteIndex(index *Index) error {
	if index != nil {
		return c.DeleteIndexByName(index.Name())
	}
	return nil
}

// DeleteIndexByName deletes the named index on the server.
func (c *Client) DeleteIndexByName(index string) error {
	span := c.tracer.StartSpan("Client.DeleteIndex")
	defer span.Finish()

	path := fmt.Sprintf("/index/%s", index)
	_, _, err := c.HTTPRequest("DELETE", path, nil, c.augmentHeaders(nil))
	return err
}

// DeleteField deletes a field on the server.
func (c *Client) DeleteField(field *Field) error {
	span := c.tracer.StartSpan("Client.DeleteField")
	defer span.Finish()

	path := fmt.Sprintf("/index/%s/field/%s", field.index.name, field.name)
	_, _, err := c.HTTPRequest("DELETE", path, nil, c.augmentHeaders(nil))
	return err
}

// SyncSchema updates a schema with the indexes and fields on the server and
// creates the indexes and fields in the schema on the server side.
// This function does not delete indexes and the fields on the server side nor in the schema.
func (c *Client) SyncSchema(schema *Schema) error {
	span := c.tracer.StartSpan("Client.SyncSchema")
	defer span.Finish()
	schema.mu.Lock()
	defer schema.mu.Unlock()
	serverSchema, err := c.Schema()
	if err != nil {
		return err
	}
	serverSchema.mu.RLock()
	defer serverSchema.mu.RUnlock()

	return c.syncSchema(schema, serverSchema)
}

func (c *Client) syncSchema(schema *Schema, serverSchema *Schema) error {
	var err error

	// find out local - remote schema
	diffSchema := schema.diff(serverSchema)
	// create the indexes and fields which doesn't exist on the server side
	for indexName, index := range diffSchema.indexes {
		if _, ok := serverSchema.indexes[indexName]; !ok {
			err = c.EnsureIndex(index)
			if err != nil {
				return errors.Wrap(err, "ensuring index")
			}
		}
		for name, field := range index.fields {
			if name == "_exists" {
				continue
			}
			err = c.EnsureField(field)
			if err != nil {
				return errors.Wrapf(err, "ensuring field")
			}
		}
	}

	// find out remote - local schema
	diffSchema = serverSchema.diff(schema)
	for indexName, index := range diffSchema.indexes {
		if localIndex, ok := schema.indexes[indexName]; !ok {
			schema.indexes[indexName] = index
		} else {
			for fieldName, field := range index.fields {
				localIndex.fields[fieldName] = field
			}
		}
	}

	return nil
}

// Schema returns the indexes and fields on the server.
func (c *Client) Schema() (*Schema, error) {
	span := c.tracer.StartSpan("Client.Schema")
	defer span.Finish()

	var indexes []SchemaIndex
	indexes, err := c.readSchema()
	if err != nil {
		return nil, err
	}
	schema := NewSchema()
	for _, indexInfo := range indexes {
		index := schema.indexWithOptions(indexInfo.Name, indexInfo.CreatedAt, indexInfo.ShardWidth, indexInfo.Options.asIndexOptions())

		for _, fieldInfo := range indexInfo.Fields {
			index.fieldWithOptions(fieldInfo.Name, fieldInfo.CreatedAt, fieldInfo.Options.asFieldOptions())
		}
	}
	return schema, nil
}

// Import imports data for a single shard using the regular import
// endpoint rather than import-roaring. This is good for e.g. mutex or
// bool fields where import-roaring is not supported.
func (c *Client) Import(field *Field, shard uint64, vals, ids []uint64, clear bool) error {
	path, data, err := c.EncodeImport(field, shard, vals, ids, clear)
	if err != nil {
		return errors.Wrap(err, "encoding import request")
	}
	err = c.DoImport(field.index.Name(), shard, path, data)
	return errors.Wrap(err, "doing import")
}

// EncodeImport computes the HTTP path and payload for an import
// request. It is typically followed by a call to DoImport.
func (c *Client) EncodeImport(field *Field, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error) {
	msg := &pb.ImportRequest{
		Index:          field.index.Name(),
		IndexCreatedAt: field.index.CreatedAt(),
		Field:          field.Name(),
		FieldCreatedAt: field.CreatedAt(),
		Shard:          shard,
		RowIDs:         vals,
		ColumnIDs:      ids,
	}
	data, err = proto.Marshal(msg)
	if err != nil {
		return "", nil, errors.Wrap(err, "marshaling Import to protobuf")
	}
	path = fmt.Sprintf("/index/%s/field/%s/import?clear=%s&ignoreKeyCheck=true", field.index.Name(), field.Name(), strconv.FormatBool(clear))
	return path, data, nil
}

// DoImport takes a path and data payload (normally from EncodeImport
// or EncodeImportValues), logs the import, finds all nodes which own
// this shard, and concurrently imports to those nodes.
func (c *Client) DoImport(index string, shard uint64, path string, data []byte) error {
	// Unlike ImportRoaring, Pilosa does not forward requests to the
	// .../import endpoint to all replicas, so we must do that
	// here. Yes this is odd. To make it worse, if a ../import request
	// was made with keys that needed to be translated server side,
	// Pilosa would handle sending the translated data to all the
	// appropriate nodes and replicas.

	uris, err := c.getURIsForShard(index, shard)
	if err != nil {
		return errors.Wrap(err, "getting uris")
	}

	eg := errgroup.Group{}
	for _, uri := range uris {
		uri := uri
		eg.Go(func() error {
			return c.importData(uri, path, data)
		})
	}
	return errors.Wrap(eg.Wait(), "importing to nodes")
}

// EncodeImportValues computes the HTTP path and payload for an
// import-values request. It is typically followed by a call to
// DoImportValues.
func (c *Client) EncodeImportValues(field *Field, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error) {
	msg := &pb.ImportValueRequest{
		Index:          field.index.Name(),
		IndexCreatedAt: field.index.CreatedAt(),
		Field:          field.Name(),
		FieldCreatedAt: field.CreatedAt(),
		Shard:          shard,
		ColumnIDs:      ids,
		Values:         vals,
	}
	data, err = proto.Marshal(msg)
	if err != nil {
		return "", nil, errors.Wrap(err, "marshaling ImportValue to protobuf")
	}
	path = fmt.Sprintf("/index/%s/field/%s/import?clear=%s&ignoreKeyCheck=true", field.index.Name(), field.Name(), strconv.FormatBool(clear))
	return path, data, nil
}

// ImportValues takes the given integer values and column ids (which
// must all be in the given shard) and imports them into the given
// index,field,shard on all nodes which should hold that shard. It
// assumes that the ids have been translated from keys if necessary
// and so tells Pilosa to ignore checking if the index uses column
// keys. ImportValues wraps EncodeImportValues and DoImportValues —
// these are broken out and exported so that performance conscious
// users can re-use the same vals and ids byte buffers for local
// encoding, while performing the imports concurrently.
func (c *Client) ImportValues(field *Field, shard uint64, vals []int64, ids []uint64, clear bool) error {
	path, data, err := c.EncodeImportValues(field, shard, vals, ids, clear)
	if err != nil {
		return errors.Wrap(err, "encoding import-values request")
	}
	err = c.DoImportValues(field.index.Name(), shard, path, data)
	return errors.Wrap(err, "doing import values")
}

// DoImportValues is deprecated. Use DoImport.
func (c *Client) DoImportValues(index string, shard uint64, path string, data []byte) error {
	return c.DoImport(index, shard, path, data)
}

func (c *Client) fetchFragmentNodes(indexName string, shard uint64) ([]fragmentNode, error) {
	if c.manualFragmentNode != nil {
		return []fragmentNode{*c.manualFragmentNode}, nil
	}
	path := fmt.Sprintf("/internal/fragment/nodes?shard=%d&index=%s", shard, indexName)
	_, body, err := c.HTTPRequest("GET", path, []byte{}, c.augmentHeaders(nil))
	if err != nil {
		return nil, err
	}
	fragmentNodes := []fragmentNode{}
	var fragmentNodeURIs []fragmentNodeRoot
	err = json.Unmarshal(body, &fragmentNodeURIs)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling fragment node URIs")
	}
	for _, nodeURI := range fragmentNodeURIs {
		fragmentNodes = append(fragmentNodes, nodeURI.URI)
	}
	return fragmentNodes, nil
}

func (c *Client) fetchPrimaryNode() (fragmentNode, error) {
	if c.manualFragmentNode != nil {
		return *c.manualFragmentNode, nil
	}
	status, err := c.Status()
	if err != nil {
		return fragmentNode{}, err
	}
	for _, node := range status.Nodes {
		if node.IsPrimary {
			nodeURI := node.URI.URI().Translate(c.nat)
			return fragmentNode{ //nolint:gosimple
				Scheme: nodeURI.Scheme,
				Host:   nodeURI.Host,
				Port:   nodeURI.Port,
			}, nil
		}
	}
	return fragmentNode{}, errors.New("Primary node not found")
}

func (c *Client) importData(uri *pnet.URI, path string, data []byte) error {
	if status, _, err := c.doRequest(uri, "POST", path, c.augmentHeaders(defaultProtobufHeaders()), data); err != nil {
		return errors.Wrapf(err, "import to %s", uri.HostPort())
	} else if status == http.StatusPreconditionFailed {
		return ErrPreconditionFailed
	}

	return nil
}

// ImportRoaringShard imports into the shard-transactional endpoint.
func (c *Client) ImportRoaringShard(index string, shard uint64, request *pilosa.ImportRoaringShardRequest) error {
	uris, err := c.getURIsForShard(index, shard)
	if err != nil {
		return errors.Wrap(err, "getting URIs for import")
	}

	data, err := fbproto.DefaultSerializer.Marshal(request)
	if err != nil {
		return errors.Wrap(err, "marshaling")
	}
	eg := errgroup.Group{}
	for _, uri := range uris {
		uri := uri
		eg.Go(func() error {
			return c.importData(uri, fmt.Sprintf("/index/%s/shard/%d/import-roaring", index, shard), data)
		})
	}
	err = eg.Wait()
	return errors.Wrap(err, "importing")
}

// ImportRoaringBitmap can import pre-made bitmaps for a number of
// different views into the given field/shard. If the view name in the
// map is an empty string, the standard view will be used.
func (c *Client) ImportRoaringBitmap(field *Field, shard uint64, views map[string]*roaring.Bitmap, clear bool) error {
	uris, err := c.getURIsForShard(field.index.Name(), shard)
	if err != nil {
		return errors.Wrap(err, "getting URIs for import")
	}
	err = c.importRoaringBitmap(uris[0], field, shard, views, &ImportOptions{clear: clear})
	return errors.Wrap(err, "importing bitmap")
}

func (c *Client) importRoaringBitmap(uri *pnet.URI, field *Field, shard uint64, views viewImports, options *ImportOptions) error {
	protoViews := []*pb.ImportRoaringRequestView{}
	for name, bmp := range views {
		buf := &bytes.Buffer{}
		_, err := bmp.WriteTo(buf)
		if err != nil {
			return errors.Wrap(err, "marshalling bitmap")
		}
		protoViews = append(protoViews, &pb.ImportRoaringRequestView{
			Name: name,
			Data: buf.Bytes(),
		})
	}
	params := url.Values{}
	params.Add("clear", strconv.FormatBool(options.clear))
	path := makeRoaringImportPath(field, shard, params)
	req := &pb.ImportRoaringRequest{
		Clear:          options.clear,
		Views:          protoViews,
		IndexCreatedAt: field.index.CreatedAt(),
		FieldCreatedAt: field.CreatedAt(),
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	header := c.augmentHeaders(defaultProtobufHeaders())
	status, _, err := c.doRequest(uri, "POST", path, header, data)
	if err != nil {
		return errors.Wrapf(err, "roaring import to %s, status: %d", uri.HostPort(), status)
	}
	if status == http.StatusPreconditionFailed {
		return ErrPreconditionFailed
	}

	return nil
}

// ExportField exports columns for a field.
func (c *Client) ExportField(field *Field) (io.Reader, error) {
	span := c.tracer.StartSpan("Client.ExportField")
	defer span.Finish()

	var shardsMax map[string]uint64
	var err error

	status, err := c.Status()
	if err != nil {
		return nil, err
	}
	shardsMax, err = c.shardsMax()
	if err != nil {
		return nil, err
	}
	status.indexMaxShard = shardsMax
	shardURIs, err := c.statusToNodeShardsForIndex(status, field.index.Name())
	if err != nil {
		return nil, err
	}

	return newExportReader(c, shardURIs, field), nil
}

// Info returns the server's configuration/host information.
func (c *Client) Info() (Info, error) {
	span := c.tracer.StartSpan("Client.Info")
	defer span.Finish()

	path := "/info"
	_, data, err := c.HTTPRequest("GET", path, nil, c.augmentHeaders(nil))
	if err != nil {
		return Info{}, errors.Wrap(err, "requesting /info")
	}
	info := Info{}
	err = json.Unmarshal(data, &info)
	if err != nil {
		return Info{}, errors.Wrap(err, "unmarshaling /info data")
	}
	return info, nil
}

// Status returns the server's status.
func (c *Client) Status() (Status, error) {
	span := c.tracer.StartSpan("Client.Status")
	defer span.Finish()

	path := "/status"
	_, data, err := c.HTTPRequest("GET", path, nil, nil)
	if err != nil {
		return Status{}, errors.Wrap(err, "requesting /status")
	}
	status := Status{}
	err = json.Unmarshal(data, &status)
	if err != nil {
		return Status{}, errors.Wrap(err, "unmarshaling /status data")
	}
	return status, nil
}

func (c *Client) readSchema() ([]SchemaIndex, error) {
	path := "/schema"
	_, data, err := c.HTTPRequest("GET", path, nil, c.augmentHeaders(nil))
	if err != nil {
		return nil, errors.Wrap(err, "requesting /schema")
	}
	schemaInfo := SchemaInfo{}
	err = json.Unmarshal(data, &schemaInfo)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling /schema data")
	}
	return schemaInfo.Indexes, nil
}

func (c *Client) shardsMax() (map[string]uint64, error) {
	path := "/internal/shards/max"
	_, data, err := c.HTTPRequest("GET", path, nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "requesting /internal/shards/max")
	}
	m := map[string]map[string]uint64{}
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling /internal/shards/max data")
	}
	return m["standard"], nil
}

// HTTPRequest sends an HTTP request to the Pilosa server (used by idk)
func (c *Client) HTTPRequest(method string, path string, data []byte, headers map[string]string) (status int, body []byte, err error) {
	span := c.tracer.StartSpan("Client.HTTPRequest")

	status, body, err = c.httpRequest(method, path, data, headers, false)
	span.Finish()
	return
}

// httpRequest makes a request to the cluster - use this when you want the
// client to choose a host, and it doesn't matter if the request goes to a
// specific host
func (c *Client) httpRequest(method string, path string, data []byte, headers map[string]string, usePrimary bool) (int, []byte, error) {
	if data == nil {
		data = []byte{}
	}

	var (
		status int
		body   []byte
		err    error
	)
	// try request on host, if it fails, try again on primary
	for i := 0; i <= 1; i++ {
		host, herr := c.host(usePrimary)
		if herr != nil {
			return status, nil, errors.Wrapf(herr, "getting host, previous err: %v", err)
		}
		// doRequest implements expotential backoff
		status, body, err = c.doRequest(host, method, path, c.augmentHeaders(headers), data)
		// conditions when primary should not be tried
		pathCheck := "/status"
		if err == nil || usePrimary || path == pathCheck {
			break
		}

		usePrimary = true
	}

	if err != nil {
		err = errors.Wrap(err, ErrHTTPRequest.Error())
	}

	return status, body, err
}

// host returns the first URI that applies, in this order:
// - a non-nil manualServerURI
// - primary URI (if usePrimary = true)
// - the next host from the node list (round-robin)
func (c *Client) host(usePrimary bool) (*pnet.URI, error) {
	if c.manualServerURI != nil {
		return c.manualServerURI, nil
	}
	var host *pnet.URI
	if usePrimary {
		c.primaryLock.RLock()
		host = c.primaryURI
		c.primaryLock.RUnlock()
		if host == nil {
			c.primaryLock.Lock()
			if c.primaryURI == nil {
				node, err := c.fetchPrimaryNode()
				if err != nil {
					c.primaryLock.Unlock()
					return nil, errors.Wrap(err, "fetching primary node")
				}
				addr := fmt.Sprintf("%s://%s:%d", node.Scheme, node.Host, node.Port)
				if host, err = pnet.NewURIFromAddress(addr); err != nil {
					return nil, errors.Wrapf(err, "parsing primary node URL: %s", addr)
				}
			} else {
				host = c.primaryURI
			}
			c.primaryURI = host
			c.primaryLock.Unlock()
		}
	} else {
		// get a host from the cluster
		host = c.cluster.Host()
		if host == nil {
			return nil, ErrEmptyCluster
		}
	}
	return host, nil
}

// doRequest creates and performs an http request.
func (c *Client) doRequest(host *pnet.URI, method, path string, headers map[string]string, data []byte) (int, []byte, error) {
	var (
		req       *http.Request
		resp      *http.Response
		err       error
		sleepTime time.Duration
		rand      = rand.New(rand.NewSource(time.Now().UnixNano()))
	)

	// We have to add the service prefix to the path here (where applicable)
	// because the pnet.URI type doesn't support the path portion of an address.
	// Where needed, we already have a service prefix set on the Client.
	path = c.prefix() + path

	for retry := 0; ; {
		if req, err = buildRequest(host, method, path, headers, data); err != nil {
			return 0, nil, errors.Wrap(err, "building request")
		}
		if resp, err = c.client.Do(req); err != nil {
			return 0, nil, errors.Wrap(err, "sending request")
		}
		if warning := resp.Header.Get("warning"); warning != "" {
			c.logger.Warnf(warning)
		}

		buf := bytes.NewBuffer(make([]byte, 0, 1+resp.ContentLength))
		_, err = buf.ReadFrom(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return resp.StatusCode, nil, errors.Wrap(err, "reading response body")
		}
		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			// [200, 300): OK
			return resp.StatusCode, buf.Bytes(), nil

		case resp.StatusCode == 409:
			// 409 Conflict
			return resp.StatusCode, buf.Bytes(), nil

		case resp.StatusCode == 412:
			// 412 Precondition Failed
			return resp.StatusCode, buf.Bytes(), nil

		case resp.StatusCode == 429:
			// 429 Too Many Requests
			// A Retry-After header might be included to this response indicating how long to wait before making a new request.
			if ms, _ := strconv.Atoi(resp.Header.Get("Retry-After")); ms > 0 {
				sleepTime = time.Duration(ms) * time.Millisecond
			} else {
				sleepTime = time.Duration(1<<uint(retry))*time.Second + time.Duration(rand.Intn(1000))*time.Millisecond
			}

		case resp.StatusCode >= 400 && resp.StatusCode < 500:
			// don't retry any 400 level errors
			return resp.StatusCode, nil, errors.New(strings.TrimSpace(buf.String()))

		case resp.StatusCode == 503:
			// This indicates that Pilosa is not ready to service this request,
			// typically during startup. In this case, it's ok to give Pilosa
			// some time and try again.
			sleepTime = time.Duration(1<<uint(retry))*time.Second + time.Duration(rand.Intn(1000))*time.Millisecond

		default:
			err = errors.Errorf("server error (%d) %s: %s", resp.StatusCode, resp.Status, strings.TrimSpace(buf.String()))
			sleepTime = time.Duration(1<<uint(retry))*time.Second + time.Duration(rand.Intn(1000))*time.Millisecond
		}

		// Retry logic
		if retry >= c.maxRetries {
			// If the error here is nil, we still want to return an error because
			// we've hit the max retries limit. If an error exists, wrap it.
			errMsg := fmt.Sprintf("max retries (%d) exceeded", c.maxRetries)
			if err == nil {
				return resp.StatusCode, nil, errors.New(errMsg)
			}
			return resp.StatusCode, nil, errors.Wrap(err, errMsg)
		}
		// The client can continue retrying after it has reached the maxBackoff time.
		if sleepTime > c.maxBackoff {
			return resp.StatusCode, nil, errors.Wrapf(err, "max backoff (%s) time exceeded", c.maxBackoff)
		}
		retry++
		c.logger.Errorf("request failed with: '%v' status: %d, retrying %d after %v ", err, resp.StatusCode, retry, sleepTime)
		time.Sleep(sleepTime)
	}
	// Unreachable code
}

// statusToNodeShardsForIndex finds the hosts which contains shards for the given index
func (c *Client) statusToNodeShardsForIndex(status Status, indexName string) (map[uint64]*pnet.URI, error) {
	result := make(map[uint64]*pnet.URI)
	if maxShard, ok := status.indexMaxShard[indexName]; ok {
		for shard := 0; shard <= int(maxShard); shard++ {
			fragmentNodes, err := c.fetchFragmentNodes(indexName, uint64(shard))
			if err != nil {
				return nil, err
			}
			if len(fragmentNodes) == 0 {
				return nil, ErrNoFragmentNodes
			}
			node := fragmentNodes[0]
			uri := &pnet.URI{
				Host:   node.Host,
				Port:   node.Port,
				Scheme: node.Scheme,
			}

			result[uint64(shard)] = uri
		}
	} else {
		return nil, ErrNoShard
	}
	return result, nil
}

func (c *Client) augmentHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		headers = map[string]string{}
	}

	// TODO: move the following block to NewClient once cluster-resize support branch is merged.
	version := strings.TrimPrefix(Version, "v")

	headers["User-Agent"] = fmt.Sprintf("pilosa/client/%s", version)
	if c.AuthToken != "" {
		headers["Authorization"] = c.AuthToken
	}
	return headers
}

// FindFieldKeys looks up the IDs associated with specified keys in a field.
// If a key does not exist, the result will not include it.
func (c *Client) FindFieldKeys(field *Field, keys ...string) (map[string]uint64, error) {
	path := fmt.Sprintf("/internal/translate/field/%s/%s/keys/find", field.index.name, field.name)

	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}

	headers := c.augmentHeaders(map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})

	status, body, err := c.HTTPRequest(http.MethodPost, path, reqData, headers)
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	if status != http.StatusOK {
		return nil, errors.Errorf("find field keys request failed (status: %d): %q", status, string(body))
	}

	var result map[string]uint64
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling response")
	}

	return result, nil
}

// CreateFieldKeys looks up the IDs associated with specified keys in a field.
// If a key does not exist, it will be created.
func (c *Client) CreateFieldKeys(field *Field, keys ...string) (map[string]uint64, error) {
	path := fmt.Sprintf("/internal/translate/field/%s/%s/keys/create", field.index.name, field.name)

	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}

	headers := c.augmentHeaders(map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})

	status, body, err := c.httpRequest(http.MethodPost, path, reqData, headers, field.options.foreignIndex == "")
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	if status != http.StatusOK {
		return nil, errors.Errorf("find field keys request failed (status: %d): %q", status, string(body))
	}

	var result map[string]uint64
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling response")
	}

	return result, nil
}

// FindIndexKeys looks up the IDs associated with specified column keys in an index.
// If a key does not exist, the result will not include it.
func (c *Client) FindIndexKeys(idx *Index, keys ...string) (map[string]uint64, error) {
	path := fmt.Sprintf("/internal/translate/index/%s/keys/find", idx.name)

	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}

	headers := c.augmentHeaders(map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})

	status, body, err := c.HTTPRequest(http.MethodPost, path, reqData, headers)
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	if status != http.StatusOK {
		return nil, errors.Errorf("find field keys request failed (status: %d): %q", status, string(body))
	}

	var result map[string]uint64
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling response")
	}

	return result, nil
}

// CreateIndexKeys looks up the IDs associated with specified column keys in an index.
// If a key does not exist, it will be created.
func (c *Client) CreateIndexKeys(idx *Index, keys ...string) (map[string]uint64, error) {
	path := fmt.Sprintf("/internal/translate/index/%s/keys/create", idx.name)

	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}

	headers := c.augmentHeaders(map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
	})

	status, body, err := c.HTTPRequest(http.MethodPost, path, reqData, headers)
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	if status != http.StatusOK {
		return nil, errors.Errorf("find field keys request failed (status: %d): %q", status, string(body))
	}

	var result map[string]uint64
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling response")
	}

	return result, nil
}

type TransactionResponse struct {
	Transaction *pilosa.Transaction `json:"transaction,omitempty"`
	Error       string              `json:"error,omitempty"`
}

// StartTransaction tries to start a new transaction in Pilosa. It
// will continue trying until at least requestTimeout time has
// passed. If it fails due to an exclusive transaction already
// existing, it will return that transaction along with a non-nil
// error.
func (c *Client) StartTransaction(id string, timeout time.Duration, exclusive bool, requestTimeout time.Duration) (*pilosa.Transaction, error) {
	return c.startTransaction(id, timeout, exclusive, time.Now().Add(requestTimeout))
}

func (c *Client) startTransaction(id string, timeout time.Duration, exclusive bool, deadline time.Time) (*pilosa.Transaction, error) {
	trns := pilosa.Transaction{
		ID:        id,
		Timeout:   timeout,
		Exclusive: exclusive,
	}
	bod, err := json.Marshal(&trns)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling transaction")
	}

	path := "/transaction"
	status, data, err := c.httpRequest("POST", path, bod, c.augmentHeaders(defaultJSONHeaders()), true)
	if status == http.StatusConflict && time.Now().Before(deadline) {
		// if we're getting StatusConflict after all the usual timeouts/retries, keep retrying until the deadline
		time.Sleep(time.Second)
		return c.startTransaction(id, timeout, exclusive, deadline)
	}
	if err != nil {
		return nil, err
	}

	tr := &TransactionResponse{}
	uerr := json.Unmarshal(data, &tr)
	if uerr != nil {
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal failed after")
		}
		return nil, errors.Wrap(uerr, "couldn't decode body")
	}

	if tr.Error != "" {
		err = errors.New(tr.Error)
	}

	return tr.Transaction, err
}

func (c *Client) FinishTransaction(id string) (*pilosa.Transaction, error) {
	path := fmt.Sprintf("/transaction/%s/finish", id)
	_, data, err := c.httpRequest("POST", path, nil, c.augmentHeaders(defaultJSONHeaders()), true)
	if err != nil && len(data) == 0 {
		return nil, err
	}

	tr := &TransactionResponse{}
	uerr := json.Unmarshal(data, &tr)
	if uerr != nil {
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal failed after")
		}
		return nil, errors.Wrap(uerr, "couldn't decode body")
	}

	if tr.Error != "" {
		err = errors.New(tr.Error)
	}

	return tr.Transaction, err
}

func (c *Client) Transactions() (map[string]*pilosa.Transaction, error) {
	path := "/transactions"
	_, respData, err := c.httpRequest("GET", path, nil, c.augmentHeaders(defaultJSONHeaders()), true)
	if err != nil {
		return nil, errors.Wrap(err, "getting transactions")
	}

	trnsMap := make(map[string]*pilosa.Transaction)
	err = json.Unmarshal(respData, &trnsMap)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling transactions")
	}
	return trnsMap, nil
}

func (c *Client) GetTransaction(id string) (*pilosa.Transaction, error) {
	path := fmt.Sprintf("/transaction/%s", id)
	_, data, err := c.httpRequest("GET", path, nil, c.augmentHeaders(defaultJSONHeaders()), true)
	if err != nil {
		return nil, err
	}

	tr := &TransactionResponse{}
	uerr := json.Unmarshal(data, &tr)
	if uerr != nil {
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal failed after")
		}
		return nil, errors.Wrap(uerr, "couldn't decode body")
	}

	if tr.Error != "" {
		err = errors.New(tr.Error)
	}

	return tr.Transaction, err
}

func defaultProtobufHeaders() map[string]string {
	return map[string]string{
		"Content-Type": "application/x-protobuf",
		"Accept":       "application/x-protobuf",
		"PQL-Version":  PQLVersion,
	}
}

func defaultJSONHeaders() map[string]string {
	return map[string]string{
		"Content-Type": "application/json",
		"Accept":       "application/json",
		"PQL-Version":  PQLVersion,
	}
}

func buildRequest(host *pnet.URI, method, path string, headers map[string]string, data []byte) (*http.Request, error) {
	request, err := http.NewRequest(method, host.Normalize()+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		request.Header.Set(k, v)
	}

	return request, nil
}

func newHTTPClient(options *ClientOptions) *http.Client {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: options.ConnectTimeout,
		}).Dial,
		TLSClientConfig:     options.TLSConfig,
		MaxIdleConnsPerHost: options.PoolSizePerRoute,
		MaxIdleConns:        options.TotalPoolSize,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   options.SocketTimeout,
	}
}

func makeRequestData(query string, options *QueryOptions) ([]byte, error) {
	request := &pb.QueryRequest{
		Query:  query,
		Shards: options.Shards,
	}
	r, err := proto.Marshal(request)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling request to protobuf")
	}
	return r, nil
}

func makeRoaringImportPath(field *Field, shard uint64, params url.Values) string {
	return fmt.Sprintf("/index/%s/field/%s/import-roaring/%d?%s",
		field.index.name, field.name, shard, params.Encode())
}

type viewImports map[string]*roaring.Bitmap

// ClientOptions control the properties of client connection to the server.
type ClientOptions struct {
	SocketTimeout       time.Duration
	ConnectTimeout      time.Duration
	PoolSizePerRoute    int
	TotalPoolSize       int
	TLSConfig           *tls.Config
	manualServerAddress bool
	tracer              opentracing.Tracer
	retries             *int
	stats               stats.StatsClient
	nat                 map[pnet.URI]pnet.URI
	pathPrefix          string
}

func (co *ClientOptions) addOptions(options ...ClientOption) error {
	for _, option := range options {
		err := option(co)
		if err != nil {
			return err
		}
	}
	return nil
}

// ClientOption is used when creating a PilosaClient struct.
type ClientOption func(options *ClientOptions) error

// OptClientSocketTimeout is the maximum idle socket time in nanoseconds
func OptClientSocketTimeout(timeout time.Duration) ClientOption {
	return func(options *ClientOptions) error {
		options.SocketTimeout = timeout
		return nil
	}
}

// OptClientConnectTimeout is the maximum time to connect in nanoseconds.
func OptClientConnectTimeout(timeout time.Duration) ClientOption {
	return func(options *ClientOptions) error {
		options.ConnectTimeout = timeout
		return nil
	}
}

// OptClientPoolSizePerRoute is the maximum number of active connections in the pool to a host.
func OptClientPoolSizePerRoute(size int) ClientOption {
	return func(options *ClientOptions) error {
		options.PoolSizePerRoute = size
		return nil
	}
}

// OptClientTotalPoolSize is the maximum number of connections in the pool.
func OptClientTotalPoolSize(size int) ClientOption {
	return func(options *ClientOptions) error {
		options.TotalPoolSize = size
		return nil
	}
}

// OptClientTLSConfig contains the TLS configuration.
func OptClientTLSConfig(config *tls.Config) ClientOption {
	return func(options *ClientOptions) error {
		options.TLSConfig = config
		return nil
	}
}

// OptClientManualServerAddress forces the client use only the manual server address
func OptClientManualServerAddress(enabled bool) ClientOption {
	return func(options *ClientOptions) error {
		options.manualServerAddress = enabled
		return nil
	}
}

// OptClientTracer sets the Open Tracing tracer
// See: https://opentracing.io
func OptClientTracer(tracer opentracing.Tracer) ClientOption {
	return func(options *ClientOptions) error {
		options.tracer = tracer
		return nil
	}
}

// OptClientRetries sets the number of retries on HTTP request failures.
func OptClientRetries(retries int) ClientOption {
	return func(options *ClientOptions) error {
		if retries < 0 {
			return errors.New("retries must be non-negative")
		}
		options.retries = &retries
		return nil
	}
}

// OptClientStatsClient sets a stats client, such as Prometheus
func OptClientStatsClient(stats stats.StatsClient) ClientOption {
	return func(options *ClientOptions) error {
		options.stats = stats
		return nil
	}
}

// OptClientNAT sets a NAT map used to translate the advertised URI to something
// else (for example, when accessing pilosa running in docker).
func OptClientNAT(nat map[string]string) ClientOption {
	return func(options *ClientOptions) error {
		// covert the strings to URIs
		m := make(map[pnet.URI]pnet.URI)
		for k, v := range nat {
			if kuri, err := pnet.NewURIFromAddress(k); err != nil {
				return errors.Wrapf(err, "converting string to URI: %s", k)
			} else if vuri, err := pnet.NewURIFromAddress(v); err != nil {
				return errors.Wrapf(err, "converting string to URI: %s", v)
			} else {
				m[*kuri] = *vuri
			}
		}
		options.nat = m
		return nil
	}
}

// OptClientPathPrefix sets the http path prefix.
func OptClientPathPrefix(prefix string) ClientOption {
	return func(options *ClientOptions) error {
		options.pathPrefix = prefix
		return nil
	}
}

func (co *ClientOptions) withDefaults() (updated *ClientOptions) {
	// copy options so the original is not updated
	updated = &ClientOptions{}
	*updated = *co
	// impose defaults
	if updated.SocketTimeout <= 0 {
		updated.SocketTimeout = time.Second * 300
	}
	if updated.ConnectTimeout <= 0 {
		updated.ConnectTimeout = time.Second * 60
	}
	if updated.PoolSizePerRoute <= 0 {
		updated.PoolSizePerRoute = 50
	}
	if updated.TotalPoolSize <= 0 {
		updated.TotalPoolSize = 500
	}
	if updated.TLSConfig == nil {
		updated.TLSConfig = &tls.Config{}
	}
	if updated.retries == nil {
		retries := 2
		updated.retries = &retries
	}
	return
}

// QueryOptions contains options to customize the Query function.
type QueryOptions struct {
	// Shards restricts query to a subset of shards. Queries all shards if nil.
	Shards []uint64
}

func (qo *QueryOptions) addOptions(options ...interface{}) error {
	for i, option := range options {
		switch o := option.(type) {
		case nil:
			if i != 0 {
				return ErrInvalidQueryOption
			}
			continue
		case *QueryOptions:
			if i != 0 {
				return ErrInvalidQueryOption
			}
			*qo = *o
		case QueryOption:
			err := o(qo)
			if err != nil {
				return err
			}
		default:
			return ErrInvalidQueryOption
		}
	}
	return nil
}

// QueryOption is used when using options with a client.Query,
type QueryOption func(options *QueryOptions) error

// OptQueryShards restricts the set of shards on which a query operates.
func OptQueryShards(shards ...uint64) QueryOption {
	return func(options *QueryOptions) error {
		options.Shards = append(options.Shards, shards...)
		return nil
	}
}

// ImportOptions are the options for controlling the importer
type ImportOptions struct {
	threadCount int
	batchSize   int
	wantRoaring *bool
	clear       bool
	skipSort    bool
}

// ImportOption is used when running imports.
type ImportOption func(options *ImportOptions) error

// OptImportThreadCount is the number of goroutines allocated for import.
func OptImportThreadCount(count int) ImportOption {
	return func(options *ImportOptions) error {
		options.threadCount = count
		return nil
	}
}

// OptImportBatchSize is the number of records read before importing them.
func OptImportBatchSize(batchSize int) ImportOption {
	return func(options *ImportOptions) error {
		options.batchSize = batchSize
		return nil
	}
}

// OptImportClear sets clear import, which clears bits instead of setting them.
func OptImportClear(clear bool) ImportOption {
	return func(options *ImportOptions) error {
		options.clear = clear
		return nil
	}
}

// OptImportRoaring enables importing using roaring bitmaps which is more performant.
func OptImportRoaring(enable bool) ImportOption {
	return func(options *ImportOptions) error {
		options.wantRoaring = &enable
		return nil
	}
}

// OptImportSort tells the importer whether or not to sort batches of records, on
// by default. Sorting imposes some performance cost, especially on data that's
// already sorted, but dramatically improves performance in pathological
// cases. It is enabled by default because the pathological cases are awful,
// and the performance hit is comparatively small, but the performance cost can
// be significant if you know your data is sorted.
func OptImportSort(sorting bool) ImportOption {
	return func(options *ImportOptions) error {
		// skipSort is expressed negatively because we want to
		// keep sorting enabled by default, so the zero value should
		// be that default behavior. The client option expresses it
		// positively because that's easier for API users.
		options.skipSort = !sorting
		return nil
	}
}

type fragmentNodeRoot struct {
	URI fragmentNode `json:"uri"`
}

type fragmentNode struct {
	Scheme string `json:"scheme"`
	Host   string `json:"host"`
	Port   uint16 `json:"port"`
}

func newFragmentNodeFromURI(uri *pnet.URI) fragmentNode {
	return fragmentNode{
		Scheme: uri.Scheme,
		Host:   uri.Host,
		Port:   uri.Port,
	}
}

func (node fragmentNode) URI() *pnet.URI {
	return &pnet.URI{
		Scheme: node.Scheme,
		Host:   node.Host,
		Port:   node.Port,
	}
}

// Info contains the configuration/host information from a Pilosa server.
type Info struct {
	ShardWidth       uint64 `json:"shardWidth"`       // width of each shard
	Memory           uint64 `json:"memory"`           // approximate host physical memory
	CPUType          string `json:"cpuType"`          // "brand name string" from cpuid
	CPUPhysicalCores int    `json:"CPUPhysicalCores"` // physical cores (cpuid)
	CPULogicalCores  int    `json:"CPULogicalCores"`  // logical cores cpuid
	CPUMHz           uint64 `json:"CPUMHz"`           // estimated clock speed
}

// Status contains the status information from a Pilosa server.
type Status struct {
	Nodes         []StatusNode `json:"nodes"`
	State         string       `json:"state"`
	LocalID       string       `json:"localID"`
	indexMaxShard map[string]uint64
}

// StatusNode contains information about a node in the cluster.
type StatusNode struct {
	ID        string    `json:"id"`
	URI       StatusURI `json:"uri"`
	IsPrimary bool      `json:"isPrimary"`
}

// StatusURI contains node information.
type StatusURI struct {
	Scheme string `json:"scheme"`
	Host   string `json:"host"`
	Port   uint16 `json:"port"`
}

// URI returns the StatusURI as a URI.
func (s StatusURI) URI() pnet.URI {
	return pnet.URI{
		Scheme: s.Scheme,
		Host:   s.Host,
		Port:   s.Port,
	}
}

// SchemaInfo contains the indexes.
type SchemaInfo struct {
	Indexes []SchemaIndex `json:"indexes"`
}

// SchemaIndex contains index information.
type SchemaIndex struct {
	Name       string        `json:"name"`
	CreatedAt  int64         `json:"createdAt,omitempty"`
	Options    SchemaOptions `json:"options"`
	Fields     []SchemaField `json:"fields"`
	Shards     []uint64      `json:"shards"`
	ShardWidth uint64        `json:"shardWidth"`
}

// SchemaField contains field information.
type SchemaField struct {
	Name      string        `json:"name"`
	CreatedAt int64         `json:"createdAt,omitempty"`
	Options   SchemaOptions `json:"options"`
}

// SchemaOptions contains options for a field or an index.
type SchemaOptions struct {
	FieldType      FieldType     `json:"type"`
	CacheType      string        `json:"cacheType"`
	CacheSize      uint          `json:"cacheSize"`
	TimeQuantum    string        `json:"timeQuantum"`
	TTL            time.Duration `json:"ttl"`
	Min            pql.Decimal   `json:"min"`
	Max            pql.Decimal   `json:"max"`
	Scale          int64         `json:"scale"`
	Keys           bool          `json:"keys"`
	NoStandardView bool          `json:"noStandardView"`
	TrackExistence bool          `json:"trackExistence"`
	TimeUnit       string        `json:"timeUnit"`
	Base           int64         `json:"base"`
	Epoch          time.Time     `json:"epoch"`
	ForeignIndex   string        `json:"foreignIndex"`
}

func (so SchemaOptions) asIndexOptions() *IndexOptions {
	return &IndexOptions{
		keys:              so.Keys,
		keysSet:           true,
		trackExistence:    so.TrackExistence,
		trackExistenceSet: true,
	}
}

func (so SchemaOptions) asFieldOptions() *FieldOptions {
	return &FieldOptions{
		fieldType:      so.FieldType,
		cacheSize:      int(so.CacheSize),
		cacheType:      CacheType(so.CacheType),
		timeQuantum:    types.TimeQuantum(so.TimeQuantum),
		ttl:            so.TTL,
		min:            so.Min,
		max:            so.Max,
		scale:          so.Scale,
		keys:           so.Keys,
		noStandardView: so.NoStandardView,
		timeUnit:       so.TimeUnit,
		base:           so.Base,
		epoch:          so.Epoch,
		foreignIndex:   so.ForeignIndex,
	}
}

type exportReader struct {
	client       *Client
	shardURIs    map[uint64]*pnet.URI
	field        *Field
	body         []byte
	bodyIndex    int
	currentShard uint64
	shardCount   uint64
}

func newExportReader(client *Client, shardURIs map[uint64]*pnet.URI, field *Field) *exportReader {
	return &exportReader{
		client:     client,
		shardURIs:  shardURIs,
		field:      field,
		shardCount: uint64(len(shardURIs)),
	}
}

// Read updates the passed array with the exported CSV data and returns the number of bytes read
func (r *exportReader) Read(p []byte) (n int, err error) {
	if r.currentShard >= r.shardCount {
		err = io.EOF
		return
	}
	if r.body == nil {
		uri := r.shardURIs[r.currentShard]
		headers := map[string]string{
			"Accept": "text/csv",
		}
		path := fmt.Sprintf("%s/export?index=%s&field=%s&shard=%d",
			r.client.prefix(), r.field.index.Name(), r.field.Name(), r.currentShard)
		_, respData, err := r.client.doRequest(uri, "GET", path, headers, nil)
		if err != nil {
			return 0, errors.Wrap(err, "doing export request")
		}
		r.body = respData
		r.bodyIndex = 0
	}
	n = copy(p, r.body[r.bodyIndex:])
	r.bodyIndex += n
	if n >= len(r.body) {
		r.body = nil
		r.currentShard++
	}
	return
}

// SetAuthToken sets the Client.AuthToken value. We needed this to be a method
// in order to satisfy the SchemaManager interface.
func (c *Client) SetAuthToken(token string) {
	c.AuthToken = token
}

// ApplyDataframeChangeset will create or append the existing dataframe
// if creating, the provided schema will be used
// if appending, the provided schema will be used to validate against
// Only one shard can be written to at a time so slight care must be taken
// Server side locking will prevent writting conflict
func (c *Client) ApplyDataframeChangeset(indexName string, cr *pilosa.ChangesetRequest, shard uint64) (map[string]interface{}, error) {
	path := fmt.Sprintf("/index/%s/dataframe/%d", indexName, shard)

	headers := c.augmentHeaders(map[string]string{
		"Content-Type": "application/octet-stream",
		"Accept":       "application/json",
	})

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(cr)
	if err != nil {
		return nil, errors.Wrap(err, "encoding changesetrequest")
	}
	uris, err := c.getURIsForShard(indexName, shard)
	if err != nil {
		return nil, errors.Wrap(err, "getting URIs for import")
	}

	eg := errgroup.Group{}
	data := buffer.Bytes()
	for _, uri := range uris {
		uri := uri
		eg.Go(func() error {
			vprint.VV("SENDING: %v", uri)
			status, body, err := c.doRequest(uri, "POST", path, headers, data)
			if err != nil {
				return errors.Wrap(err, "executing request")
			}
			if status != http.StatusOK {
				return errors.Errorf("find dataframe request failed (status: %d): %q", status, string(body))
			}
			return nil
		})
	}
	err = eg.Wait()

	//	status, body, err := c.HTTPRequest(http.MethodPost, path, buffer.Bytes(), headers)
	/*
		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling response")
		}
	*/

	return nil, err
}
