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
	// Retrieve a list of nodes that own the slice.
	nodes, err := c.SliceNodes(slice)
	if err != nil {
		return fmt.Errorf("slice nodes: %s", err)
	}

	// Try to backup slice from each one until successful.
	var data []byte
	for _, i := range rand.Perm(len(nodes)) {
		buf, err := c.backupSliceNode(db, frame, slice, nodes[i])
		if err == nil {
			data = buf
			break // backup successful
		} else if err == ErrFragmentNotFound {
			return nil // slice doesn't exist
		} else if err != nil {
			log.Println(err)
			continue
		}
	}

	// Return error if unable to backup from any slice.
	if data == nil {
		return fmt.Errorf("unable to backup slice %d", slice)
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

func (c *Client) backupSliceNode(db, frame string, slice uint64, node *Node) ([]byte, error) {
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
	defer resp.Body.Close()

	// Return error if status is not OK.
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrFragmentNotFound
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected backup status code: host=%s, code=%d", node.Host, resp.StatusCode)
	}

	return ioutil.ReadAll(resp.Body)
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
