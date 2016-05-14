package pilosa

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"

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

// SliceN returns the number of slices on a server.
func (c *Client) SliceN() (uint64, error) {
	// Execute request against the host.
	u := url.URL{
		Scheme: "http",
		Host:   c.host,
		Path:   "/slices/max",
	}
	resp, err := c.HTTPClient.Get(u.String())
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var rsp sliceMaxResponse
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return 0, fmt.Errorf("json decode: %s", err)
	}

	return rsp.SliceMax, nil
}

// Schema returns all database and frame schema information.
func (c *Client) Schema() ([]*DBInfo, error) {
	// Execute request against the host.
	u := url.URL{
		Scheme: "http",
		Host:   c.host,
		Path:   "/schema",
	}
	resp, err := c.HTTPClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rsp getSchemaResponse
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, fmt.Errorf("json decode: %s", err)
	}
	return rsp.DBs, nil
}

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

// ExecuteQuery executes query against db on the server.
func (c *Client) ExecuteQuery(db, query string, allowRedirect bool) (result interface{}, err error) {
	if db == "" {
		return nil, ErrDatabaseRequired
	} else if query == "" {
		return nil, ErrQueryRequired
	}

	// Encode query request.
	buf, err := proto.Marshal(&internal.QueryRequest{
		DB:     proto.String(db),
		Query:  proto.String(query),
		Remote: proto.Bool(!allowRedirect),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal: %s", err)
	}

	// Create URL & HTTP request.
	u := url.URL{Scheme: "http", Host: c.host, Path: "/query"}
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")

	// Execute request against the host.
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read body and unmarshal response.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		return nil, errors.New(string(body))
	}

	var qresp internal.QueryResponse
	if err := proto.Unmarshal(body, &qresp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %s", err)
	} else if s := qresp.GetErr(); s != "" {
		return nil, errors.New(s)
	}

	return nil, nil
}

// Import bulk imports bits for a single slice to a host.
func (c *Client) Import(db, frame string, slice uint64, bits []Bit) error {
	if db == "" {
		return ErrDatabaseRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	buf, err := ImportPayload(db, frame, slice, bits)
	if err != nil {
		return fmt.Errorf("Error Creating Payload: %s", err)
	}

	// Retrieve a list of nodes that own the slice.
	nodes, err := c.SliceNodes(slice)

	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Import to each node.
	for _, node := range nodes {
		if err := c.importNode(node, buf); err != nil {
			return fmt.Errorf("import node: host=%s, err=%s", node.Host, err)
		}
	}

	return nil
}

func ImportPayload(db, frame string, slice uint64, bits []Bit) ([]byte, error) {
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
		return nil, fmt.Errorf("marshal import request: %s", err)
	}
	return buf, nil
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

// BackupTo backs up an entire frame from a cluster to w.
func (c *Client) BackupTo(w io.Writer, db, frame string) error {
	if db == "" {
		return ErrDatabaseRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	// Create tar writer around writer.
	tw := tar.NewWriter(w)

	// Find the maximum number of slices.
	sliceN, err := c.SliceN()
	if err != nil {
		return fmt.Errorf("slice n: %s", err)
	}

	// Backup every slice to the tar file.
	for i := uint64(0); i <= sliceN; i++ {
		if err := c.backupSliceTo(tw, db, frame, i); err != nil {
			return err
		}
	}

	// Close tar file.
	if err := tw.Close(); err != nil {
		return err
	}

	return nil
}

// backupSliceTo backs up a single slice to tw.
func (c *Client) backupSliceTo(tw *tar.Writer, db, frame string, slice uint64) error {
	// Return error if unable to backup from any slice.
	r, err := c.BackupSlice(db, frame, slice)
	if err != nil {
		return fmt.Errorf("backup slice: slice=%d, err=%s", slice, err)
	} else if r == nil {
		return nil
	}
	defer r.Close()

	// Read entire buffer to determine file size.
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	} else if err := r.Close(); err != nil {
		return err
	}

	// Write slice file header.
	if err := tw.WriteHeader(&tar.Header{
		Name:    strconv.FormatUint(slice, 10),
		Mode:    0666,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}); err != nil {
		return err
	}

	// Write buffer to file.
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("write buffer: %s", err)
	}

	return nil
}

// BackupSlice retrieves a streaming backup from a single slice.
// This function tries slice owners until one succeeds.
func (c *Client) BackupSlice(db, frame string, slice uint64) (io.ReadCloser, error) {
	// Retrieve a list of nodes that own the slice.
	nodes, err := c.SliceNodes(slice)
	if err != nil {
		return nil, fmt.Errorf("slice nodes: %s", err)
	}

	// Try to backup slice from each one until successful.
	for _, i := range rand.Perm(len(nodes)) {
		r, err := c.backupSliceNode(db, frame, slice, nodes[i])
		if err == nil {
			return r, nil // successfully attached
		} else if err == ErrFragmentNotFound {
			return nil, nil // slice doesn't exist
		} else if err != nil {
			log.Println(err)
			continue
		}
	}

	return nil, fmt.Errorf("unable to connect to any owner")
}

func (c *Client) backupSliceNode(db, frame string, slice uint64, node *Node) (io.ReadCloser, error) {
	u := url.URL{
		Scheme: "http",
		Host:   node.Host,
		Path:   "/fragment/data",
		RawQuery: url.Values{
			"db":    {db},
			"frame": {frame},
			"slice": {strconv.FormatUint(slice, 10)},
		}.Encode(),
	}
	resp, err := c.HTTPClient.Get(u.String())
	if err != nil {
		return nil, err
	}

	// Return error if status is not OK.
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrFragmentNotFound
	} else if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected backup status code: host=%s, code=%d", node.Host, resp.StatusCode)
	}

	return resp.Body, nil
}

// RestoreFrom restores a frame from a backup file to an entire cluster.
func (c *Client) RestoreFrom(r io.Reader, db, frame string) error {
	if db == "" {
		return ErrDatabaseRequired
	} else if frame == "" {
		return ErrFrameRequired
	}

	// Create tar reader around input.
	tr := tar.NewReader(r)

	// Process each file.
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// Parse slice from entry name.
		slice, err := strconv.ParseUint(hdr.Name, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid backup entry: %s", hdr.Name)
		}

		// Read file into buffer.
		var buf bytes.Buffer
		if _, err := io.CopyN(&buf, tr, hdr.Size); err != nil {
			return err
		}

		// Restore file to all nodes that own it.
		if err := c.restoreSliceFrom(buf.Bytes(), db, frame, slice); err != nil {
			return err
		}
	}
}

// restoreSliceFrom restores a single slice to all owning nodes.
func (c *Client) restoreSliceFrom(buf []byte, db, frame string, slice uint64) error {
	// Retrieve a list of nodes that own the slice.
	nodes, err := c.SliceNodes(slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Restore slice to each owner.
	for _, node := range nodes {
		u := url.URL{
			Scheme: "http",
			Host:   node.Host,
			Path:   "/fragment/data",
			RawQuery: url.Values{
				"db":    {db},
				"frame": {frame},
				"slice": {strconv.FormatUint(slice, 10)},
			}.Encode(),
		}
		resp, err := c.HTTPClient.Post(u.String(), "application/octet-stream", bytes.NewReader(buf))
		if err != nil {
			return err
		}
		resp.Body.Close()

		// Return error if response not OK.
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: host=%s, code=%d", node.Host, resp.StatusCode)
		}
	}

	return nil
}

// RestoreFrame restores an entire frame from a host in another cluster.
func (c *Client) RestoreFrame(host, db, frame string) error {
	u := url.URL{
		Scheme: "http",
		Host:   c.Host(),
		Path:   "/frame/restore",
		RawQuery: url.Values{
			"host":  {host},
			"db":    {db},
			"frame": {frame},
		}.Encode(),
	}
	resp, err := c.HTTPClient.Post(u.String(), "application/octet-stream", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()

	// Return error if response not OK.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: host=%s, code=%d", host, resp.StatusCode)
	}

	return nil
}

// FragmentBlocks returns a list of block checksums for a fragment on a host.
// Only returns blocks which contain data.
func (c *Client) FragmentBlocks(db, frame string, slice uint64) ([]FragmentBlock, error) {
	u := url.URL{
		Scheme: "http",
		Host:   c.host,
		Path:   "/fragment/blocks",
		RawQuery: url.Values{
			"db":    {db},
			"frame": {frame},
			"slice": {strconv.FormatUint(slice, 10)},
		}.Encode(),
	}
	resp, err := c.HTTPClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Return error if status is not OK.
	switch resp.StatusCode {
	case http.StatusOK: // ok
	case http.StatusNotFound:
		return nil, ErrFragmentNotFound
	default:
		return nil, fmt.Errorf("unexpected status: code=%d", resp.StatusCode)
	}

	// Decode response object.
	var rsp getFragmentBlocksResponse
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return nil, err
	}
	return rsp.Blocks, nil
}

// BlockData returns bitmap/profile id pairs for a block.
func (c *Client) BlockData(db, frame string, slice uint64, block int) ([]uint64, []uint64, error) {
	buf, err := proto.Marshal(&internal.BlockDataRequest{
		DB:    proto.String(db),
		Frame: proto.String(frame),
		Slice: proto.Uint64(slice),
		Block: proto.Uint64(uint64(block)),
	})
	if err != nil {
		return nil, nil, err
	}

	u := url.URL{Scheme: "http", Host: c.host, Path: "/fragment/block/data"}
	req, err := http.NewRequest("GET", u.String(), bytes.NewReader(buf))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/protobuf")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	req.Header.Set("Accept", "application/protobuf")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	// Return error if status is not OK.
	switch resp.StatusCode {
	case http.StatusOK: // fallthrough
	case http.StatusNotFound:
		return nil, nil, nil
	default:
		return nil, nil, fmt.Errorf("unexpected status: code=%d", resp.StatusCode)
	}

	// Decode response object.
	var rsp internal.BlockDataResponse
	if body, err := ioutil.ReadAll(resp.Body); err != nil {
		return nil, nil, err
	} else if err := proto.Unmarshal(body, &rsp); err != nil {
		return nil, nil, err
	}
	return rsp.BitmapIDs, rsp.ProfileIDs, nil
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
