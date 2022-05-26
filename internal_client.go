// Copyright 2022 Molecula Corp. All rights reserved.
package pilosa

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/molecula/featurebase/v3/authn"
	"github.com/molecula/featurebase/v3/ingest"
	"github.com/molecula/featurebase/v3/logger"
	pnet "github.com/molecula/featurebase/v3/net"
	"github.com/molecula/featurebase/v3/topology"
	"github.com/molecula/featurebase/v3/tracing"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

// InternalClient represents a client to the Pilosa cluster.
type InternalClient struct {
	defaultURI *pnet.URI
	serializer Serializer

	log logger.Logger

	// The client to use for HTTP communication.
	httpClient      *http.Client
	retryableClient *retryablehttp.Client
	// the local node's API, used for operations that we can short-circuit that way
	api *API

	// secret Key for auth across nodes
	secretKey string
}

// NewInternalClient returns a new instance of InternalClient to connect to host.
// If api is non-nil, the client uses it for some same-host operations instead
// of going through http.
func NewInternalClient(host string, remoteClient *http.Client, opts ...InternalClientOption) (*InternalClient, error) {
	if host == "" {
		return nil, ErrHostRequired
	}

	uri, err := pnet.NewURIFromAddress(host)
	if err != nil {
		return nil, errors.Wrap(err, "getting URI")
	}

	client := NewInternalClientFromURI(uri, remoteClient, opts...)
	return client, nil
}

type InternalClientOption func(c *InternalClient)

func WithSerializer(s Serializer) InternalClientOption {
	return func(c *InternalClient) {
		c.serializer = s
	}
}

// WithSecretKey adds the secretKey used for inter-node communication when auth
// is enabled
func WithSecretKey(secretKey string) InternalClientOption {
	return func(c *InternalClient) {
		c.secretKey = secretKey
	}
}

// WithClientRetryPeriod is the max amount of total time the client will
// retry failed requests using exponential backoff.
func WithClientRetryPeriod(period time.Duration) InternalClientOption {
	min := time.Millisecond * 100

	// do some math to figure out how many attempts we need to get our
	// total sleep time close to the period
	attempts := math.Log2(float64(period)) - math.Log2(float64(min))
	attempts += 0.3 // mmmm, fudge
	if attempts < 1 {
		attempts = 1
	}

	return func(c *InternalClient) {
		rc := retryablehttp.NewClient()
		rc.HTTPClient = c.httpClient
		rc.RetryWaitMin = min
		rc.RetryMax = int(attempts)
		rc.CheckRetry = retryWith400Policy
		c.retryableClient = rc
	}
}

func WithClientLogger(log logger.Logger) InternalClientOption {
	return func(c *InternalClient) {
		c.log = log
	}
}

func noRetryPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	return false, nil
}

// retryWith400Policy wraps retryablehttp's default retry policy to
// also retry on 4XX errors which *should* be client errors and
// therefore useless to retry, but we have some incorrect status codes.
// TODO: fix the incorrect status codes so we can get rid of this.
func retryWith400Policy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if resp != nil && resp.StatusCode >= 400 {
		return true, nil
	}
	return retryablehttp.DefaultRetryPolicy(ctx, resp, err)
}

func NewInternalClientFromURI(defaultURI *pnet.URI, remoteClient *http.Client, opts ...InternalClientOption) *InternalClient {
	ic := &InternalClient{
		defaultURI: defaultURI,
		httpClient: remoteClient,
		log:        logger.NewStandardLogger(os.Stderr),
	}

	for _, opt := range opts {
		opt(ic)
	}

	if ic.retryableClient == nil {
		rc := retryablehttp.NewClient()
		rc.HTTPClient = ic.httpClient
		rc.CheckRetry = noRetryPolicy
		rc.Logger = logger.NopLogger
		ic.retryableClient = rc
	}
	return ic
}

// AddAuthToken checks in a couple spots for our authorization token and
// adds it to the Authorization Header in the request if it finds it. It does the
// same for refresh tokens as well.
func AddAuthToken(ctx context.Context, header *http.Header) {
	var access, refresh string
	if token, ok := ctx.Value(authn.ContextValueAccessToken).(string); ok {
		// the AccessToken value should be prefixed with "Bearer"
		access = token
	}
	if token, ok := ctx.Value(authn.ContextValueRefreshToken).(string); ok {
		refresh = token
	}

	// not combining these ifs so we don't call ctx.Value unless we have to
	if access == "" || refresh == "" {
		if uinfo := ctx.Value("userinfo"); uinfo != nil {
			if access == "" {
				// UserInfo.Token is not prefixed with "Bearer"
				access = "Bearer " + uinfo.(*authn.UserInfo).Token
			}
			if refresh == "" {
				refresh = uinfo.(*authn.UserInfo).RefreshToken
			}
		}
	}

	// set ogIP to request for remote calls
	if ogIP, ok := ctx.Value(OriginalIPHeader).(string); ok && ogIP != "" {
		header.Set(OriginalIPHeader, ogIP)
	}

	header.Set("Authorization", access)
	header.Set(authn.RefreshHeaderName, refresh)
}

// MaxShardByIndex returns the number of shards on a server by index.
func (c *InternalClient) MaxShardByIndex(ctx context.Context) (map[string]uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.MaxShardByIndex")
	defer span.Finish()
	return c.maxShardByIndex(ctx)
}

// maxShardByIndex returns the number of shards on a server by index.
func (c *InternalClient) maxShardByIndex(ctx context.Context) (map[string]uint64, error) {
	// Execute request against the host.
	u := uriPathToURL(c.defaultURI, "/internal/shards/max")

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rsp getShardsMaxResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}

	return rsp.Standard, nil
}

// AvailableShards returns a list of shards for an index.
func (c *InternalClient) AvailableShards(ctx context.Context, indexName string) ([]uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.AvailableShards")
	defer span.Finish()

	// Execute request against the host.
	u := uriPathToURL(c.defaultURI, path.Join("/internal/index", indexName, "/shards"))

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rsp getIndexAvailableShardsResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return rsp.Shards, nil
}

// SchemaNode returns all index and field schema information from the specified
// node.
func (c *InternalClient) SchemaNode(ctx context.Context, uri *pnet.URI, views bool) ([]*IndexInfo, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Schema")
	defer span.Finish()

	// TODO: /?views parameter will be ignored, till we implement schemator!
	// Execute request against the host.
	u := uri.Path(fmt.Sprintf("/schema?views=%v", views))

	// Build request.
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rsp getSchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return rsp.Indexes, nil
}

// Schema returns all index and field schema information.
func (c *InternalClient) Schema(ctx context.Context) ([]*IndexInfo, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Schema")
	defer span.Finish()

	// Execute request against the host.
	u := c.defaultURI.Path("/schema")

	// Build request.
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rsp getSchemaResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return rsp.Indexes, nil
}

// IngestSchema uses the new schema ingest endpoint. It returns a
// map from index names to fields created within them; note that if the
// entire index was created, the list of fields is empty. The intended
// usage is cleaning up after creating the indexes, so if you create the
// index, you don't need to delete the fields, but if you created fields
// within an existing index, you should delete those fields but not the
// whole index.
func (c *InternalClient) IngestSchema(ctx context.Context, uri *pnet.URI, buf []byte) (created map[string][]string, err error) {
	if uri == nil {
		uri = c.defaultURI
	}
	u := uri.Path("/internal/schema")
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	resp, err := c.executeRequest(req.WithContext(ctx), giveRawResponse(true))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()
	buf, err = ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		if err != nil {
			return nil, errors.Wrapf(err, "bad status '%s' and err reading body", resp.Status)
		}
		var msg string
		// try to decode a JSON response
		var sr successResponse
		qr := &QueryResponse{}
		if err = json.Unmarshal(buf, &sr); err == nil {
			msg = sr.Error.Error()
		} else if err := c.serializer.Unmarshal(buf, qr); err == nil {
			msg = qr.Err.Error()
		} else {
			msg = string(buf)
		}
		return nil, errors.Errorf("against %s %s: '%s'", req.URL.String(), resp.Status, msg)
	}
	// this is the err from ioutil.ReadAll, but in the case where resp.StatusCode
	// was 2xx, so we don't have a bad status.
	if err != nil {
		return nil, errors.Wrapf(err, "error reading response body")
	}
	if err = json.Unmarshal(buf, &created); err != nil {
		return nil, errors.Wrapf(err, "error interpreting response body")
	}
	return created, nil
}

// IngestOperations uses the new ingest endpoint for ingest data
func (c *InternalClient) IngestOperations(ctx context.Context, uri *pnet.URI, indexName string, buf []byte) error {
	if uri == nil {
		uri = c.defaultURI
	}
	u := uri.Path(fmt.Sprintf("/internal/ingest/%s", indexName))
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("unexpected status code: %s", resp.Status)
	}
	return nil
}

// IngestNodeOperations uses the internal/protobuf ingest endpoint for ingest data
func (c *InternalClient) IngestNodeOperations(ctx context.Context, uri *pnet.URI, indexName string, ireq *ingest.ShardedRequest) error {
	if uri == nil {
		uri = c.defaultURI
	}
	u := uri.Path(fmt.Sprintf("/internal/ingest/%s/node", indexName))

	buf, err := c.serializer.Marshal(ireq)
	if err != nil {
		return errors.Wrap(err, "marshalling")
	}
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("unexpected status code: %s", resp.Status)
	}
	return nil
}

// MutexCheck uses the mutex-check endpoint to request mutex collision data
// from a single node. It produces per-shard results, and does not translate
// them.
func (c *InternalClient) MutexCheck(ctx context.Context, uri *pnet.URI, indexName string, fieldName string, details bool, limit int) (map[uint64]map[uint64][]uint64, error) {
	if uri == nil {
		uri = c.defaultURI
	}
	// This is not actually a "Path", but reworking this to support queries
	// is messier than I have resources to pursue just now.
	u := uri.Path(fmt.Sprintf("/internal/index/%s/field/%s/mutex-check?details=%t&limit=%d", indexName, fieldName, details, limit))
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected status code: %s", resp.Status)
	}
	var out map[uint64]map[uint64][]uint64
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&out)
	return out, err
}

func (c *InternalClient) PostSchema(ctx context.Context, uri *pnet.URI, s *Schema, remote bool) error {
	u := uri.Path(fmt.Sprintf("/schema?remote=%v", remote))
	buf, err := json.Marshal(s)
	if err != nil {
		return errors.Wrap(err, "marshalling schema")
	}
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return errors.Errorf("unexpected status code: %s", resp.Status)
	}
	return nil
}

// CreateIndex creates a new index on the server.
func (c *InternalClient) CreateIndex(ctx context.Context, index string, opt IndexOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.CreateIndex")
	defer span.Finish()

	// Get the primary node. Schema changes must go through
	// primary to avoid weird race conditions.
	nodes, err := c.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("getting nodes: %s", err)
	}
	coord := getPrimaryNode(nodes)
	if coord == nil {
		return fmt.Errorf("could not find the primary node")
	}

	// Encode query request.
	buf, err := json.Marshal(&postIndexRequest{
		Options: opt,
	})
	if err != nil {
		return errors.Wrap(err, "encoding request")
	}

	// Create URL & HTTP request.
	u := uriPathToURL(&coord.URI, fmt.Sprintf("/index/%s", index))
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusConflict {
			return ErrIndexExists
		}
		return err
	}
	return errors.Wrap(resp.Body.Close(), "closing response body")
}

// FragmentNodes returns a list of nodes that own a shard.
func (c *InternalClient) FragmentNodes(ctx context.Context, index string, shard uint64) ([]*topology.Node, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FragmentNodes")
	defer span.Finish()

	// Execute request against the host.
	u := uriPathToURL(c.defaultURI, "/internal/fragment/nodes")
	u.RawQuery = (url.Values{"index": {index}, "shard": {strconv.FormatUint(shard, 10)}}).Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var a []*topology.Node
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return a, nil
}

// Nodes returns a list of all nodes.
func (c *InternalClient) Nodes(ctx context.Context) ([]*topology.Node, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Nodes")
	defer span.Finish()

	// Execute request against the host.
	u := uriPathToURL(c.defaultURI, "/internal/nodes")

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var a []*topology.Node
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return a, nil
}

// Query executes query against the index.
func (c *InternalClient) Query(ctx context.Context, index string, queryRequest *QueryRequest) (*QueryResponse, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Query")
	defer span.Finish()
	return c.QueryNode(ctx, c.defaultURI, index, queryRequest)
}

// QueryNode executes query against the index, sending the request to the node specified.
func (c *InternalClient) QueryNode(ctx context.Context, uri *pnet.URI, index string, queryRequest *QueryRequest) (*QueryResponse, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "QueryNode")
	defer span.Finish()

	if index == "" {
		return nil, ErrIndexRequired
	} else if queryRequest.Query == "" {
		return nil, ErrQueryRequired
	}
	buf, err := c.serializer.Marshal(queryRequest)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling queryRequest")
	}

	// Create HTTP request.
	u := uri.Path(fmt.Sprintf("/index/%s/query", index))
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	AddAuthToken(ctx, &req.Header)

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("X-Pilosa-Row", "roaring")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrapf(err, "'%s'", queryRequest.Query)
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	qresp := &QueryResponse{}
	if err := c.serializer.Unmarshal(body, qresp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	} else if qresp.Err != nil {
		return nil, qresp.Err
	}

	return qresp, nil
}

func getPrimaryNode(nodes []*topology.Node) *topology.Node {
	for _, node := range nodes {
		if node.IsPrimary {
			return node
		}
	}
	return nil
}

func (c *InternalClient) EnsureIndex(ctx context.Context, name string, options IndexOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.EnsureIndex")
	defer span.Finish()

	err := c.CreateIndex(ctx, name, options)
	if err == nil || errors.Cause(err) == ErrIndexExists {
		return nil
	}
	return err
}

func (c *InternalClient) EnsureField(ctx context.Context, indexName string, fieldName string) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.EnsureField")
	defer span.Finish()
	return c.EnsureFieldWithOptions(ctx, indexName, fieldName, FieldOptions{})
}

func (c *InternalClient) EnsureFieldWithOptions(ctx context.Context, indexName string, fieldName string, opt FieldOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.EnsureFieldWithOptions")
	defer span.Finish()
	err := c.CreateFieldWithOptions(ctx, indexName, fieldName, opt)
	if err == nil || errors.Cause(err) == ErrFieldExists {
		return nil
	}
	return err
}

// importNode sends a pre-marshaled import request to a node.
func (c *InternalClient) importNode(ctx context.Context, node *topology.Node, index, field string, buf []byte, opts *ImportOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.importNode")
	defer span.Finish()

	// Create URL & HTTP request.
	path := fmt.Sprintf("/index/%s/field/%s/import", index, field)
	u := nodePathToURL(node, path)

	vals := url.Values{}
	if opts.Clear {
		vals.Set("clear", "true")
	}
	if opts.IgnoreKeyCheck {
		vals.Set("ignoreKeyCheck", "true")
	}
	url := fmt.Sprintf("%s?%s", u.String(), vals.Encode())

	req, err := http.NewRequest("POST", url, bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("X-Pilosa-Row", "roaring")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "reading")
	}

	var isresp ImportResponse
	if err := c.serializer.Unmarshal(body, &isresp); err != nil {
		return fmt.Errorf("unmarshal import response: %s", err)
	} else if s := isresp.Err; s != "" {
		return errors.New(s)
	}

	return nil
}

// importHelper is an experiment to see whether SonarCloud's code duplication
// complaints make sense to address in this context, given the impracticality
// of refactoring ImportRequest/ImportValueRequest right now. The process
// function exists because we would use either api.ImportValueWithTx or
// api.ImportWithTx, passing it the actual underlying-type of req, but doing
// that in here with a type switch seems messy. Similarly, index/field/shard
// exist because we can't access those members of the two slightly different
// structs.
func (c *InternalClient) importHelper(ctx context.Context, req Message, process func() error, index string, field string, shard uint64, options *ImportOptions) error {
	// If we don't actually know what shards we're sending to, and we have
	// a local API and a qcx, we'll have a process function that uses the local
	// API. Otherwise, even if we have an API
	var nodes []*topology.Node
	var err error
	if shard != ^uint64(0) {
		// we need a list of nodes specific to this shard.
		nodes, err = c.FragmentNodes(ctx, index, shard)
		if err != nil {
			return errors.Errorf("shard nodes: %s", err)
		}
	} else {
		// We don't know what shard to use, any shard is fine, local host
		// is better if available.
		if process != nil {
			// skip the HTTP round-trip if we can.
			err = process()
			// Note that Wrap(nil, ...) is still nil.
			return errors.Wrap(err, "local import")
		}
		// get the complete list of nodes, so if we have an API, we can
		// pick our local node and probably avoid actually sending the http
		// request over the wire, even though we still have to go through
		// the http interface.
		nodes, err = c.Nodes(ctx)
		if err != nil {
			return errors.Wrap(err, "getting nodes")
		}
	}

	// "us" is a usable local node if any, "them" is every node that we need
	// to process which isn't that node. We start out with us == nil and
	// them = the whole set of nodes.
	var us *topology.Node
	var them []*topology.Node = nodes

	// If we have an API, we know what node we are. Even if we don't have
	// a Qcx, we still care, because looping back to the local node will
	// be faster than going to another node.
	if c.api != nil {
		myID := c.api.NodeID()
		for i, node := range nodes {
			if myID == node.ID {
				// swap our node into the first position
				nodes[i], nodes[0] = nodes[0], nodes[i]
				// If we have a qcx, we'll treat our node even MORE
				// specially.
				if process != nil {
					us, them = nodes[0], nodes[1:]
				}
				break
			}
		}
	}

	// If we had a valid API and Qcx, and shard was ^0, we'd have handled
	// it previously. So if we get here, we don't have both a Qcx and an API,
	// but we might have an API, in which case we'll have shuffled our node
	// into the first position. Otherwise we're just taking whatever the first
	// node is.
	if shard == ^uint64(0) {
		them = them[:1]
	}

	// We handle remote nodes first, for two distinct reasons. One is that
	// the local API ImportWithTx is allowed to modify its inputs, and if we
	// ran that before serializing, we'd get corrupt data serialized.
	// The other is that if we were to hold a write lock that started with
	// that import and ended when we hit the end of our Qcx, we wouldn't want
	// to hold it during all our requests to the remote nodes.
	if len(them) > 0 {
		buf, err := c.serializer.Marshal(req)
		if err != nil {
			return errors.Errorf("marshal import request: %s", err)
		}
		// We process remote nodes first so we won't be actually holding our
		// write lock yet, in theory. This doesn't actually matter yet, but is
		// helpful for future planned refactoring.
		for _, node := range them {
			if err = c.importNode(ctx, node, index, field, buf, options); err != nil {
				return errors.Wrap(err, "remote import")
			}
		}
	}

	// Write to the local node if we have one.
	if us != nil {
		// WARNING: ImportWithTx can alter its inputs. However, we can
		// only ever do this once, and if we're going to need a marshalled
		// form, we already made it.
		if err = process(); err != nil {
			return errors.Wrap(err, "local import after remote imports")
		}
	}
	return nil
}

// Import imports values using an ImportRequest, whether or not it's keyed.
// It may modify the contents of req.
//
// If a request comes in with Shard -1, it will be sent to only one node,
// which will translate if necessary, split into shards, and loop back
// through this for each sub-request. If a request uses record keys,
// it will be set to use shard = -1 unconditionally, because we know
// that it has to be translated and possibly reshuffled. Value keys
// don't override the shard.
//
// If we get a non-nil qcx, and have an associated API, we'll use that API
// directly for the local shard.
func (c *InternalClient) Import(ctx context.Context, qcx *Qcx, req *ImportRequest, options *ImportOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Import")
	defer span.Finish()

	if req.ColumnKeys != nil {
		req.Shard = ^uint64(0)
	}
	var process func() error
	if c.api != nil && qcx != nil {
		process = func() error {
			return c.api.ImportWithTx(ctx, qcx, req, options)
		}
	}
	return c.importHelper(ctx, req, process, req.Index, req.Field, req.Shard, options)
}

// ImportValue imports values using an ImportValueRequest, whether or not it's
// keyed. It may modify the contents of req.
//
// If a request comes in with Shard -1, it will be sent to only one node,
// which will translate if necessary, split into shards, and loop back
// through this for each sub-request. If a request uses record keys,
// it will be set to use shard = -1 unconditionally, because we know
// that it has to be translated and possibly reshuffled. Value keys
// don't override the shard.
//
// If we get a non-nil qcx, and have an associated API, we'll use that API
// directly for the local shard.
func (c *InternalClient) ImportValue(ctx context.Context, qcx *Qcx, req *ImportValueRequest, options *ImportOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Import")
	defer span.Finish()

	if req.ColumnKeys != nil {
		req.Shard = ^uint64(0)
	}
	var process func() error
	if c.api != nil && qcx != nil {
		process = func() error {
			return c.api.ImportValueWithTx(ctx, qcx, req, options)
		}
	}
	return c.importHelper(ctx, req, process, req.Index, req.Field, req.Shard, options)
}

// ImportRoaring does fast import of raw bits in roaring format (pilosa or
// official format, see API.ImportRoaring).
func (c *InternalClient) ImportRoaring(ctx context.Context, uri *pnet.URI, index, field string, shard uint64, remote bool, req *ImportRoaringRequest) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ImportRoaring")
	defer span.Finish()

	if index == "" {
		return ErrIndexRequired
	} else if field == "" {
		return ErrFieldRequired
	}
	if uri == nil {
		uri = c.defaultURI
	}

	vals := url.Values{}
	vals.Set("remote", strconv.FormatBool(remote))
	url := fmt.Sprintf("%s/index/%s/field/%s/import-roaring/%d?%s", uri, index, field, shard, vals.Encode())

	// Marshal data to protobuf.
	data, err := c.serializer.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshal import request")
	}

	// Generate HTTP request.
	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Accept", "application/x-protobuf")
	httpReq.Header.Set("X-Pilosa-Row", "roaring")
	httpReq.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &httpReq.Header)

	// Execute request against the host.
	resp, err := c.executeRequest(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	rbody := &ImportResponse{}
	err = dec.Decode(rbody)
	// Decode can return EOF when no error occurred. helpful!
	if err != nil && err != io.EOF {
		return errors.Wrap(err, "decoding response body")
	}
	if rbody.Err != "" {
		return errors.Wrap(errors.New(rbody.Err), "importing roaring")
	}
	return nil
}

// ExportCSV bulk exports data for a single shard from a host to CSV format.
func (c *InternalClient) ExportCSV(ctx context.Context, index, field string, shard uint64, w io.Writer) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ExportCSV")
	defer span.Finish()

	if index == "" {
		return ErrIndexRequired
	} else if field == "" {
		return ErrFieldRequired
	}

	// Retrieve a list of nodes that own the shard.
	nodes, err := c.FragmentNodes(ctx, index, shard)
	if err != nil {
		return fmt.Errorf("shard nodes: %s", err)
	}

	// Attempt nodes in random order.
	var e error
	for _, i := range rand.Perm(len(nodes)) {
		node := nodes[i]

		if err := c.exportNodeCSV(ctx, node, index, field, shard, w); err != nil {
			e = fmt.Errorf("export node: host=%s, err=%s", node.URI, err)
			continue
		} else {
			return nil
		}
	}

	return e
}

// exportNode copies a CSV export from a node to w.
func (c *InternalClient) exportNodeCSV(ctx context.Context, node *topology.Node, index, field string, shard uint64, w io.Writer) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.exportNodeCSV")
	defer span.Finish()

	// Create URL.
	u := nodePathToURL(node, "/export")
	u.RawQuery = url.Values{
		"index": {index},
		"field": {field},
		"shard": {strconv.FormatUint(shard, 10)},
	}.Encode()

	// Generate HTTP request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Accept", "text/csv")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Copy body to writer.
	if _, err := io.Copy(w, resp.Body); err != nil {
		return errors.Wrap(err, "copying")
	}

	return nil
}

// RetrieveShardFromURI returns a ReadCloser which contains the data of the
// specified shard from the specified node. Caller *must* close the returned
// ReadCloser or risk leaking goroutines/tcp connections.
func (c *InternalClient) RetrieveShardFromURI(ctx context.Context, index, field, view string, shard uint64, uri pnet.URI) (io.ReadCloser, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.RetrieveShardFromURI")
	defer span.Finish()

	node := &topology.Node{
		URI: uri,
	}

	u := nodePathToURL(node, "/internal/fragment/data")
	u.RawQuery = url.Values{
		"index": {index},
		"field": {field},
		"view":  {view},
		"shard": {strconv.FormatUint(shard, 10)},
	}.Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, ErrFragmentNotFound
		}
		return nil, err
	}

	return resp.Body, nil
}

func (c *InternalClient) CreateField(ctx context.Context, index, field string) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.CreateField")
	defer span.Finish()
	return c.CreateFieldWithOptions(ctx, index, field, FieldOptions{})
}

// CreateFieldWithOptions creates a new field on the server.
func (c *InternalClient) CreateFieldWithOptions(ctx context.Context, index, field string, opt FieldOptions) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.CreateFieldWithOptions")
	defer span.Finish()

	if index == "" {
		return ErrIndexRequired
	}

	// convert FieldOptions to fieldOptions
	//
	// TODO this kind of sucks because it's one more place that needs
	// changes when we change anything with field options (and there
	// are a lot of places already). It's not clear to me that this is
	// providing a lot of value, but I think this kind of validation
	// should probably happen in the field anyway??
	fieldOpt := fieldOptions{
		Type: opt.Type,
	}
	switch fieldOpt.Type {
	case FieldTypeSet, FieldTypeMutex:
		fieldOpt.CacheType = &opt.CacheType
		fieldOpt.CacheSize = &opt.CacheSize
		fieldOpt.Keys = &opt.Keys
	case FieldTypeInt:
		fieldOpt.Min = &opt.Min
		fieldOpt.Max = &opt.Max
	case FieldTypeTime:
		fieldOpt.TimeQuantum = &opt.TimeQuantum
		ttlString := opt.TTL.String()
		fieldOpt.TTL = &ttlString
	case FieldTypeBool:
		// pass
	case FieldTypeDecimal:
		fieldOpt.Min = &opt.Min
		fieldOpt.Max = &opt.Max
		fieldOpt.Scale = &opt.Scale
	default:
		fieldOpt.Type = DefaultFieldType
		fieldOpt.Keys = &opt.Keys
	}

	// TODO: remove buf completely? (depends on whether importer needs to create specific field types)
	// Encode query request.
	buf, err := json.Marshal(&postFieldRequest{
		Options: fieldOpt,
	})
	if err != nil {
		return errors.Wrap(err, "marshaling")
	}

	// Get the primary node. Schema changes must go through
	// primary to avoid weird race conditions.
	nodes, err := c.Nodes(ctx)
	if err != nil {
		return fmt.Errorf("getting nodes: %s", err)
	}
	coord := getPrimaryNode(nodes)
	if coord == nil {
		return fmt.Errorf("could not find the primary node")
	}

	// Create URL & HTTP request.
	u := uriPathToURL(&coord.URI, fmt.Sprintf("/index/%s/field/%s", index, field))
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusConflict {
			return ErrFieldExists
		}
		return err
	}

	return errors.Wrap(resp.Body.Close(), "closing response body")
}

// FragmentBlocks returns a list of block checksums for a fragment on a host.
// Only returns blocks which contain data.
func (c *InternalClient) FragmentBlocks(ctx context.Context, uri *pnet.URI, index, field, view string, shard uint64) ([]FragmentBlock, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FragmentBlocks")
	defer span.Finish()

	if uri == nil {
		uri = c.defaultURI
	}
	u := uriPathToURL(uri, "/internal/fragment/blocks")
	u.RawQuery = url.Values{
		"index": {index},
		"field": {field},
		"view":  {view},
		"shard": {strconv.FormatUint(shard, 10)},
	}.Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		// Return the appropriate error.
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, ErrFragmentNotFound
		}
		return nil, err
	}
	defer resp.Body.Close()

	// Decode response object.
	var rsp getFragmentBlocksResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, errors.Wrap(err, "decoding")
	}
	return rsp.Blocks, nil
}

// BlockData returns row/column id pairs for a block.
func (c *InternalClient) BlockData(ctx context.Context, uri *pnet.URI, index, field, view string, shard uint64, block int) ([]uint64, []uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.BlockData")
	defer span.Finish()

	if uri == nil {
		panic("need to pass a URI to BlockData")
	}
	buf, err := c.serializer.Marshal(&BlockDataRequest{
		Index: index,
		Field: field,
		View:  view,
		Shard: shard,
		Block: uint64(block),
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "marshaling")
	}

	u := uriPathToURL(uri, "/internal/fragment/block/data")
	req, err := http.NewRequest("GET", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, nil, errors.Wrap(err, "creating request")
	}
	req.Header.Set("Content-Type", "application/protobuf")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Accept", "application/protobuf")
	req.Header.Set("X-Pilosa-Row", "roaring")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	defer resp.Body.Close()

	// Decode response object.
	var rsp BlockDataResponse
	if body, err := ioutil.ReadAll(resp.Body); err != nil {
		return nil, nil, errors.Wrap(err, "reading")
	} else if err := c.serializer.Unmarshal(body, &rsp); err != nil {
		return nil, nil, errors.Wrap(err, "unmarshalling")
	}
	return rsp.RowIDs, rsp.ColumnIDs, nil
}

// SendMessage posts a message synchronously.
func (c *InternalClient) SendMessage(ctx context.Context, uri *pnet.URI, msg []byte) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.SendMessage")
	defer span.Finish()

	u := uriPathToURL(uri, "/internal/cluster/message")
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(msg))
	if err != nil {
		return errors.Wrap(err, "making new request")
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Connection", "keep-alive")
	if c.secretKey != "" {
		req.Header.Set("X-Feature-Key", c.secretKey)
	}

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return errors.Wrap(err, "executing request")
	}
	defer resp.Body.Close()
	_, err = io.Copy(ioutil.Discard, resp.Body)
	return errors.Wrap(err, "draining SendMessage response body")
}

// TranslateKeysNode function is mainly called to translate keys from primary node.
// If primary node returns 404 error the function wraps it with ErrTranslatingKeyNotFound.
func (c *InternalClient) TranslateKeysNode(ctx context.Context, uri *pnet.URI, index, field string, keys []string, writable bool) ([]uint64, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "TranslateKeysNode")
	defer span.Finish()

	if index == "" {
		return nil, ErrIndexRequired
	}

	buf, err := c.serializer.Marshal(&TranslateKeysRequest{
		Index:       index,
		Field:       field,
		Keys:        keys,
		NotWritable: !writable,
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshaling TranslateKeysRequest")
	}

	// Create HTTP request.
	u := uri.Path("/internal/translate/keys")
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("X-Pilosa-Row", "roaring")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, errors.Wrap(ErrTranslatingKeyNotFound, err.Error())
		}
		return nil, err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	tkresp := &TranslateKeysResponse{}
	if err := c.serializer.Unmarshal(body, tkresp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	}
	return tkresp.IDs, nil
}

// TranslateIDsNode sends an id translation request to a specific node.
func (c *InternalClient) TranslateIDsNode(ctx context.Context, uri *pnet.URI, index, field string, ids []uint64) ([]string, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "TranslateIDsNode")
	defer span.Finish()

	if index == "" {
		return nil, ErrIndexRequired
	}

	buf, err := c.serializer.Marshal(&TranslateIDsRequest{
		Index: index,
		Field: field,
		IDs:   ids,
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshaling TranslateIDsRequest")
	}

	// Create HTTP request.
	u := uri.Path("/internal/translate/ids")
	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("X-Pilosa-Row", "roaring")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	tkresp := &TranslateIDsResponse{}
	if err := c.serializer.Unmarshal(body, tkresp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	}
	return tkresp.Keys, nil
}

// GetPastQueries retrieves the query history log for the specified node.
func (c *InternalClient) GetPastQueries(ctx context.Context, uri *pnet.URI) ([]PastQueryStatus, error) {
	u := uri.Path("/query-history?remote=true")
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Execute request against the host.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading")
	}

	queries := make([]PastQueryStatus, 100)
	if err := json.Unmarshal(body, &queries); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	}
	return queries, nil
}

func (c *InternalClient) FindIndexKeysNode(ctx context.Context, uri *pnet.URI, index string, keys ...string) (transMap map[string]uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FindIndexKeysNode")
	defer span.Finish()

	// Create HTTP request.
	u := uriPathToURL(uri, fmt.Sprintf("/internal/translate/index/%s/keys/find", index))
	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(reqData))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	// Apply headers.
	req.Header.Set("Content-Length", strconv.Itoa(len(reqData)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Send the request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing response body")
		}
	}()

	// Read the response body.
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}

	// Decode the translations.
	transMap = make(map[string]uint64, len(keys))
	err = json.Unmarshal(result, &transMap)
	if err != nil {
		return nil, errors.Wrap(err, "json decoding")
	}

	return transMap, nil
}

func (c *InternalClient) FindFieldKeysNode(ctx context.Context, uri *pnet.URI, index string, field string, keys ...string) (transMap map[string]uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FindFieldKeysNode")
	defer span.Finish()

	// Create HTTP request.
	u := uriPathToURL(uri, fmt.Sprintf("/internal/translate/field/%s/%s/keys/find", index, field))
	q := u.Query()
	q.Add("remote", "true")
	u.RawQuery = q.Encode()
	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(reqData))
	// Apply headers.
	req.Header.Set("Content-Length", strconv.Itoa(len(reqData)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Send the request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing response body")
		}
	}()

	// Read the response body.
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}

	// Decode the translations.
	transMap = make(map[string]uint64, len(keys))
	err = json.Unmarshal(result, &transMap)
	if err != nil {
		return nil, errors.Wrap(err, "json decoding")
	}

	return transMap, nil
}

func (c *InternalClient) CreateIndexKeysNode(ctx context.Context, uri *pnet.URI, index string, keys ...string) (transMap map[string]uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.CreateIndexKeysNode")
	defer span.Finish()

	// Create HTTP request.
	u := uriPathToURL(uri, fmt.Sprintf("/internal/translate/index/%s/keys/create", index))
	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(reqData))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	// Apply headers.
	req.Header.Set("Content-Length", strconv.Itoa(len(reqData)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Send the request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing response body")
		}
	}()

	// Read the response body.
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}

	// Decode the translations.
	transMap = make(map[string]uint64, len(keys))
	err = json.Unmarshal(result, &transMap)
	if err != nil {
		return nil, errors.Wrap(err, "json decoding")
	}

	return transMap, nil
}

func (c *InternalClient) CreateFieldKeysNode(ctx context.Context, uri *pnet.URI, index string, field string, keys ...string) (transMap map[string]uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.CreateFieldKeysNode")
	defer span.Finish()

	// Create HTTP request.
	u := uriPathToURL(uri, fmt.Sprintf("/internal/translate/field/%s/%s/keys/create", index, field))
	q := u.Query()
	q.Add("remote", "true")
	u.RawQuery = q.Encode()
	reqData, err := json.Marshal(keys)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling request")
	}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(reqData))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	// Apply headers.
	req.Header.Set("Content-Length", strconv.Itoa(len(reqData)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Send the request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing response body")
		}
	}()

	// Read the response body.
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}

	// Decode the translations.
	transMap = make(map[string]uint64, len(keys))
	err = json.Unmarshal(result, &transMap)
	if err != nil {
		return nil, errors.Wrap(err, "json decoding")
	}

	return transMap, nil
}

func (c *InternalClient) MatchFieldKeysNode(ctx context.Context, uri *pnet.URI, index string, field string, like string) (matches []uint64, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.MatchFieldKeysNode")
	defer span.Finish()

	// Create HTTP request.
	u := uriPathToURL(uri, fmt.Sprintf("/internal/translate/field/%s/%s/keys/like", index, field))
	req, err := http.NewRequest("POST", u.String(), strings.NewReader(like))
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	// Apply headers.
	req.Header.Set("Content-Length", strconv.Itoa(len(like)))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Send the request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		cerr := resp.Body.Close()
		if cerr != nil && err == nil {
			err = errors.Wrap(cerr, "closing response body")
		}
	}()

	// Read the response body.
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "reading response")
	}

	// Decode the translations.
	err = json.Unmarshal(result, &matches)
	if err != nil {
		return nil, errors.Wrap(err, "json decoding")
	}

	return matches, nil
}

func (c *InternalClient) Transactions(ctx context.Context) (map[string]*Transaction, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Transactions")
	defer span.Finish()

	u := uriPathToURL(c.defaultURI, "/transactions")
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating transactions request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	trnsMap := make(map[string]*Transaction)
	err = json.NewDecoder(resp.Body).Decode(&trnsMap)
	return trnsMap, errors.Wrap(err, "json decoding")
}

func (c *InternalClient) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool) (*Transaction, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.StartTransaction")
	defer span.Finish()
	buf, err := json.Marshal(&Transaction{
		ID:        id,
		Timeout:   timeout,
		Exclusive: exclusive,
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshalling payload")
	}
	// We're using the defaultURI here because this is only used by
	// tests, and we want to test requests against all hosts. A robust
	// client implementation would ensure that these requests go to
	// the primary.
	u := uriPathToURL(c.defaultURI, "/transaction/"+id)
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, errors.Wrap(err, "creating post transaction request")
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	resp, err := c.executeRequest(req.WithContext(ctx), giveRawResponse(true))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	tr := &TransactionResponse{}
	err = json.NewDecoder(resp.Body).Decode(tr)
	if err != nil {
		return nil, errors.Wrap(err, "decoding response")
	}
	if resp.StatusCode == 409 {
		err = ErrTransactionExclusive
	} else if tr.Error != "" {
		err = errors.New(tr.Error)
	}
	return tr.Transaction, err
}

func (c *InternalClient) FinishTransaction(ctx context.Context, id string) (*Transaction, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FinishTransaction")
	defer span.Finish()

	u := uriPathToURL(c.defaultURI, "/transaction/"+id+"/finish")
	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating finish transaction request")
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	resp, err := c.executeRequest(req.WithContext(ctx), giveRawResponse(true))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	tr := &TransactionResponse{}
	err = json.NewDecoder(resp.Body).Decode(tr)
	if err != nil {
		return nil, errors.Wrap(err, "decoding response")
	}

	if tr.Error != "" {
		err = errors.New(tr.Error)
	}
	return tr.Transaction, err
}

func (c *InternalClient) GetTransaction(ctx context.Context, id string) (*Transaction, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.GetTransaction")
	defer span.Finish()

	// We're using the defaultURI here because this is only used by
	// tests, and we want to test requests against all hosts. A robust
	// client implementation would ensure that these requests go to
	// the primary.
	u := uriPathToURL(c.defaultURI, "/transaction/"+id)
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating get transaction request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	resp, err := c.executeRequest(req.WithContext(ctx), giveRawResponse(true))
	if err != nil {
		return nil, errors.Wrap(err, "executing request")
	}
	defer func() {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	tr := &TransactionResponse{}
	err = json.NewDecoder(resp.Body).Decode(tr)
	if err != nil {
		return nil, errors.Wrap(err, "decoding response")
	}

	if tr.Error != "" {
		err = errors.New(tr.Error)
	}
	return tr.Transaction, err
}

type executeOpts struct {
	// giveRawResponse instructs executeRequest not to process the
	// respStatusCode and try to extract errors or whatever.
	giveRawResponse bool
	// forwardAuthHeader instructs executeRequest not to follow redirects
	forwardAuthHeader bool
}

type executeRequestOption func(*executeOpts)

func giveRawResponse(b bool) executeRequestOption {
	return func(eo *executeOpts) {
		eo.giveRawResponse = b
	}
}
func forwardAuthHeader(b bool) executeRequestOption {
	return func(eo *executeOpts) {
		eo.forwardAuthHeader = b
	}
}

// executeRequest executes the given request and checks the Response. For
// responses with non-2XX status, the body is read and closed, and an error is
// returned. If the error is nil, the caller must ensure that the response body
// is closed.
func (c *InternalClient) executeRequest(req *http.Request, opts ...executeRequestOption) (*http.Response, error) {
	return c.executeRetryableRequest(&retryablehttp.Request{Request: req}, opts...)
}

func (c *InternalClient) executeRetryableRequest(req *retryablehttp.Request, opts ...executeRequestOption) (*http.Response, error) {
	tracing.GlobalTracer.InjectHTTPHeaders(req.Request)
	req.Close = false
	eo := &executeOpts{}
	for _, opt := range opts {
		opt(eo)
	}

	var resp *http.Response
	var err error
	if eo.forwardAuthHeader {
		rc := retryablehttp.NewClient()
		rc.HTTPClient = &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) > 0 {
					access, refresh := getTokens(via[0])
					req.Header.Set("Authorization", "Bearer "+access)
					req.Header.Set(authn.RefreshHeaderName, refresh)
				}
				return nil
			},
		}
		rc.CheckRetry = retryWith400Policy
		rc.Logger = logger.NopLogger

		resp, err = rc.Do(req)
	} else {
		resp, err = c.retryableClient.Do(req)
	}

	return c.handleResponse(req.Request, eo, resp, err)
}

func (c *InternalClient) handleResponse(req *http.Request, eo *executeOpts, resp *http.Response, err error) (*http.Response, error) {
	if err != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return nil, errors.Wrap(err, "getting response")
	}
	if eo.giveRawResponse {
		return resp, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return resp, errors.Wrapf(err, "bad status '%s' and err reading body", resp.Status)
		}
		var msg string
		// try to decode a JSON response
		var sr successResponse
		qr := &QueryResponse{}
		if err = json.Unmarshal(buf, &sr); err == nil {
			msg = sr.Error.Error()
		} else if err := c.serializer.Unmarshal(buf, qr); err == nil {
			msg = qr.Err.Error()
		} else {
			msg = string(buf)
		}
		return resp, errors.Errorf("against %s %s: '%s'", req.URL.String(), resp.Status, msg)
	}
	return resp, nil
}

// Bit represents the intersection of a row and a column. It can be specified by
// integer ids or string keys.
type Bit struct {
	RowID     uint64
	ColumnID  uint64
	RowKey    string
	ColumnKey string
	Timestamp int64
}

// Bits is a slice of Bit.
type Bits []Bit

func (p Bits) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p Bits) Len() int      { return len(p) }

func (p Bits) Less(i, j int) bool {
	if p[i].RowID == p[j].RowID {
		if p[i].ColumnID < p[j].ColumnID {
			return p[i].Timestamp < p[j].Timestamp
		}
		return p[i].ColumnID < p[j].ColumnID
	}
	return p[i].RowID < p[j].RowID
}

// HasRowKeys returns true if any values use a row key.
func (p Bits) HasRowKeys() bool {
	for i := range p {
		if p[i].RowKey != "" {
			return true
		}
	}
	return false
}

// HasColumnKeys returns true if any values use a column key.
func (p Bits) HasColumnKeys() bool {
	for i := range p {
		if p[i].ColumnKey != "" {
			return true
		}
	}
	return false
}

// RowIDs returns a slice of all the row IDs.
func (p Bits) RowIDs() []uint64 {
	if p.HasRowKeys() {
		return nil
	}
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].RowID
	}
	return other
}

// ColumnIDs returns a slice of all the column IDs.
func (p Bits) ColumnIDs() []uint64 {
	if p.HasColumnKeys() {
		return nil
	}
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].ColumnID
	}
	return other
}

// RowKeys returns a slice of all the row keys.
func (p Bits) RowKeys() []string {
	if !p.HasRowKeys() {
		return nil
	}
	other := make([]string, len(p))
	for i := range p {
		other[i] = p[i].RowKey
	}
	return other
}

// ColumnKeys returns a slice of all the column keys.
func (p Bits) ColumnKeys() []string {
	if !p.HasColumnKeys() {
		return nil
	}
	other := make([]string, len(p))
	for i := range p {
		other[i] = p[i].ColumnKey
	}
	return other
}

// Timestamps returns a slice of all the timestamps.
func (p Bits) Timestamps() []int64 {
	other := make([]int64, len(p))
	for i := range p {
		other[i] = p[i].Timestamp
	}
	return other
}

// GroupByShard returns a map of bits by shard.
func (p Bits) GroupByShard() map[uint64][]Bit {
	m := make(map[uint64][]Bit)
	for _, bit := range p {
		shard := bit.ColumnID / ShardWidth
		m[shard] = append(m[shard], bit)
	}

	for shard, bits := range m {
		sort.Sort(Bits(bits))
		m[shard] = bits
	}

	return m
}

// FieldValue represents the value for a column within a
// range-encoded field.
type FieldValue struct {
	ColumnID  uint64
	ColumnKey string
	Value     int64
}

// FieldValues represents a slice of field values.
type FieldValues []FieldValue

func (p FieldValues) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p FieldValues) Len() int      { return len(p) }

func (p FieldValues) Less(i, j int) bool {
	return p[i].ColumnID < p[j].ColumnID
}

// HasColumnKeys returns true if any values use a column key.
func (p FieldValues) HasColumnKeys() bool {
	for i := range p {
		if p[i].ColumnKey != "" {
			return true
		}
	}
	return false
}

// ColumnIDs returns a slice of all the column IDs.
func (p FieldValues) ColumnIDs() []uint64 {
	if p.HasColumnKeys() {
		return nil
	}
	other := make([]uint64, len(p))
	for i := range p {
		other[i] = p[i].ColumnID
	}
	return other
}

// ColumnKeys returns a slice of all the column keys.
func (p FieldValues) ColumnKeys() []string {
	if !p.HasColumnKeys() {
		return nil
	}
	other := make([]string, len(p))
	for i := range p {
		other[i] = p[i].ColumnKey
	}
	return other
}

// Values returns a slice of all the values.
func (p FieldValues) Values() []int64 {
	other := make([]int64, len(p))
	for i := range p {
		other[i] = p[i].Value
	}
	return other
}

// GroupByShard returns a map of field values by shard.
func (p FieldValues) GroupByShard() map[uint64][]FieldValue {
	m := make(map[uint64][]FieldValue)
	for _, val := range p {
		shard := val.ColumnID / ShardWidth
		m[shard] = append(m[shard], val)
	}

	for shard, vals := range m {
		sort.Sort(FieldValues(vals))
		m[shard] = vals
	}

	return m
}

// BitsByPos is a slice of bits sorted row then column.
type BitsByPos []Bit

func (p BitsByPos) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p BitsByPos) Len() int      { return len(p) }
func (p BitsByPos) Less(i, j int) bool {
	p0, p1 := pos(p[i].RowID, p[i].ColumnID), pos(p[j].RowID, p[j].ColumnID)
	if p0 == p1 {
		return p[i].Timestamp < p[j].Timestamp
	}
	return p0 < p1
}

func uriPathToURL(uri *pnet.URI, path string) url.URL {
	return url.URL{
		Scheme: uri.Scheme,
		Host:   uri.HostPort(),
		Path:   path,
	}
}

func nodePathToURL(node *topology.Node, path string) url.URL {
	return url.URL{
		Scheme: node.URI.Scheme,
		Host:   node.URI.HostPort(),
		Path:   path,
	}
}

// RetrieveTranslatePartitionFromURI returns a ReadCloser which contains the data of the
// specified translate partition from the specified node. Caller *must* close the returned
// ReadCloser or risk leaking goroutines/tcp connections.
func (c *InternalClient) RetrieveTranslatePartitionFromURI(ctx context.Context, index string, partition int, uri pnet.URI) (io.ReadCloser, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.RetrieveTranslatePartitionFromURI")
	defer span.Finish()

	node := &topology.Node{
		URI: uri,
	}

	u := nodePathToURL(node, "/internal/translate/data")
	u.RawQuery = url.Values{
		"index":     {index},
		"partition": {strconv.FormatInt(int64(partition), 10)},
	}.Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, ErrFragmentNotFound
		}
		return nil, err
	}

	return resp.Body, nil
}
func (c *InternalClient) ImportIndexKeys(ctx context.Context, uri *pnet.URI, index string, partitionID int, remote bool, readerFunc func() (io.Reader, error)) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ImportIndexKeys")
	defer span.Finish()

	if index == "" {
		return ErrIndexRequired
	}

	if uri == nil {
		uri = c.defaultURI
	}

	vals := url.Values{}
	vals.Set("remote", strconv.FormatBool(remote))
	url := fmt.Sprintf("%s/internal/translate/index/%s/%d", uri, index, partitionID)

	// Generate HTTP request.
	httpReq, err := retryablehttp.NewRequest("POST", url, readerFunc)
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	httpReq.Header.Set("User-Agent", "pilosa/"+Version)

	AddAuthToken(ctx, &httpReq.Header)

	// Execute request against the host.
	resp, err := c.executeRetryableRequest(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (c *InternalClient) ImportFieldKeys(ctx context.Context, uri *pnet.URI, index, field string, remote bool, readerFunc func() (io.Reader, error)) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ImportFieldKeys")
	defer span.Finish()

	if index == "" {
		return ErrIndexRequired
	}

	if uri == nil {
		uri = c.defaultURI
	}

	vals := url.Values{}
	vals.Set("remote", strconv.FormatBool(remote))
	url := fmt.Sprintf("%s/internal/translate/field/%s/%s", uri, index, field)

	// Generate HTTP request.
	httpReq, err := retryablehttp.NewRequest("POST", url, readerFunc)
	if err != nil {
		return errors.Wrap(err, "creating request")
	}
	httpReq.Header.Set("User-Agent", "pilosa/"+Version)

	AddAuthToken(ctx, &httpReq.Header)

	// Execute request against the host.
	resp, err := c.executeRetryableRequest(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// ShardReader returns a reader that provides a snapshot of the current shard RBF data.
func (c *InternalClient) ShardReader(ctx context.Context, index string, shard uint64) (io.ReadCloser, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.ShardReader")
	defer span.Finish()

	// Execute request against the host.
	u := fmt.Sprintf("%s/internal/index/%s/shard/%d/snapshot", c.defaultURI, index, shard)

	// Build request.
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/octet-stream")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// IDAllocDataReader returns a reader that provides a snapshot of ID allocation data.
func (c *InternalClient) IDAllocDataReader(ctx context.Context) (io.ReadCloser, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.IDAllocDataReader")
	defer span.Finish()

	// Build request.
	req, err := http.NewRequest("GET", c.defaultURI.String()+"/internal/idalloc/data", nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/octet-stream")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *InternalClient) IDAllocDataWriter(ctx context.Context, f io.Reader, primary *topology.Node) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.IDAllocDataWriter")
	defer span.Finish()

	u := primary.URI.Path("/internal/idalloc/restore")

	// Build request.
	req, err := http.NewRequest("POST", u, f)
	if err != nil {
		return errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/octet-stream")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	_, err = c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return err
	}
	return err
}

// IndexTranslateDataReader returns a reader that provides a snapshot of
// translation data for a partition in an index.
func (c *InternalClient) IndexTranslateDataReader(ctx context.Context, index string, partitionID int) (io.ReadCloser, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.IndexTranslateDataReader")
	defer span.Finish()

	// Execute request against the host.
	u := fmt.Sprintf("%s/internal/translate/data?index=%s&partition=%d", c.defaultURI, url.QueryEscape(index), partitionID)

	// Build request.
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/octet-stream")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx), forwardAuthHeader(true))
	if resp != nil && resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrTranslateStoreNotFound
	} else if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// FieldTranslateDataReader returns a reader that provides a snapshot of
// translation data for a field.
func (c *InternalClient) FieldTranslateDataReader(ctx context.Context, index, field string) (io.ReadCloser, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.FieldTranslateDataReader")
	defer span.Finish()

	// Execute request against the host.
	u := fmt.Sprintf("%s/internal/translate/data?index=%s&field=%s", c.defaultURI, url.QueryEscape(index), url.QueryEscape(field))

	// Build request.
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/octet-stream")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if resp != nil && resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrTranslateStoreNotFound
	} else if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Status returns pilosa cluster state as a string ("NORMAL", "DEGRADED", "DOWN", "RESIZING", ...)
func (c *InternalClient) Status(ctx context.Context) (string, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.Status")
	defer span.Finish()

	// Execute request against the host.
	u := c.defaultURI.Path("/status")

	// Build request.
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return "", errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var rsp getStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return "", fmt.Errorf("json decode: %s", err)
	}
	return rsp.State, nil
}

func (c *InternalClient) PartitionNodes(ctx context.Context, partitionID int) ([]*topology.Node, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "InternalClient.PartitionNodes")
	defer span.Finish()

	// Execute request against the host.
	u := uriPathToURL(c.defaultURI, "/internal/partition/nodes")
	u.RawQuery = (url.Values{"partition": {strconv.FormatInt(int64(partitionID), 10)}}).Encode()

	// Build request.
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	AddAuthToken(ctx, &req.Header)

	// Execute request.
	resp, err := c.executeRequest(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var a []*topology.Node
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return a, nil
}

func (c *InternalClient) SetInternalAPI(api *API) {
	c.api = api
}

func (c *InternalClient) OAuthConfig() (rsp oauth2.Config, err error) {
	u := uriPathToURL(c.defaultURI, "/internal/oauth-config")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return rsp, errors.Wrap(err, "creating request")
	}

	req.Header.Set("User-Agent", "pilosa/"+Version)
	req.Header.Set("Accept", "application/json")
	resp, err := c.executeRequest(req)
	if err != nil {
		return rsp, fmt.Errorf("getting config: %w", err)
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return rsp, fmt.Errorf("json decode: %s", err)
	}

	return rsp, nil
}
