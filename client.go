package pilosa

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa/internal"
)

// Client represents a client to the Pilosa cluster.
type Client struct {
	host string

	// The client to use for HTTP communication.
	// Defaults to the http.DefaultClient.
	HTTPClient *http.Client
}

// NewClient returns a new instance of Client to connect to host.
func NewClient(host string) (*Client, error) {
	if host == "" {
		return nil, ErrHostRequired
	}

	return &Client{
		host:       host,
		HTTPClient: http.DefaultClient,
	}, nil
}

// Host returns the host the client was initialized with.
func (c *Client) Host() string { return c.host }

// SliceNodes returns a list of nodes that own a slice.
func (c *Client) SliceNodes(slice uint64) ([]*Node, error) {
	// Execute request against the host.
	u := url.URL{
		Scheme:   "http",
		Host:     c.host,
		Path:     "/slices/nodes",
		RawQuery: (url.Values{"slice": {strconv.FormatUint(slice, 10)}}).Encode(),
	}
	resp, err := c.HTTPClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var a []*Node
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}

	return a, nil
}

// Import bulk imports bits for a single slice to a host.
func (c *Client) Import(db, frame string, slice uint64, bits []Bit) error {
	if db == "" {
		return ErrDatabaseRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	// Retrieve a list of nodes that own the slice.
	nodes, err := c.SliceNodes(slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Separate bitmap and profile IDs to reduce allocations.
	bitmapIDs := Bits(bits).BitmapIDs()
	profileIDs := Bits(bits).ProfileIDs()

	// Marshal bits to protobufs.
	buf, err := proto.Marshal(&internal.ImportRequest{
		DB:         proto.String(db),
		Frame:      proto.String(frame),
		Slice:      proto.Uint64(slice),
		BitmapIDs:  bitmapIDs,
		ProfileIDs: profileIDs,
	})
	if err != nil {
		return fmt.Errorf("marshal import request: %s", err)
	}

	// Import to each node.
	for _, node := range nodes {
		if err := c.importNode(node, buf); err != nil {
			return fmt.Errorf("import node: host=%s, err=%s", node.Host, err)
		}
	}

	return nil
}

// importNode sends a pre-marshaled import request to a node.
func (c *Client) importNode(node *Node, buf []byte) error {
	// Create URL & HTTP request.
	u := url.URL{Scheme: "http", Host: node.Host, Path: "/import"}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	} else if resp.StatusCode != http.StatusOK {
		return errors.New(string(body))
	}

	var isresp internal.ImportResponse
	if err := proto.Unmarshal(body, &isresp); err != nil {
		return fmt.Errorf("unmarshal import response: %s", err)
	} else if s := isresp.GetErr(); s != "" {
		return errors.New(s)
	}

	return nil
}

// Bit represents the location of a single bit.
type Bit struct {
	BitmapID  uint64
	ProfileID uint64
}

// Bits represents a slice of bits.
type Bits []Bit

// BitmapIDs returns a slice of all the bitmap IDs.
func (a Bits) BitmapIDs() []uint64 {
	other := make([]uint64, len(a))
	for i := range a {
		other[i] = a[i].BitmapID
	}
	return other
}

// ProfileIDs returns a slice of all the profile IDs.
func (a Bits) ProfileIDs() []uint64 {
	other := make([]uint64, len(a))
	for i := range a {
		other[i] = a[i].ProfileID
	}
	return other
}

// GroupBySlice returns a map of bits by slice.
func (a Bits) GroupBySlice() map[uint64][]Bit {
	m := make(map[uint64][]Bit)
	for _, bit := range a {
		slice := bit.ProfileID / SliceWidth
		m[slice] = append(m[slice], bit)
	}
	return m
}
