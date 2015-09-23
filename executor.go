package pilosa

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa/internal"
	"github.com/umbel/pilosa/pql"
)

// DefaultFrame is the frame used if one is not specified.
const DefaultFrame = "general"

// ErrDatabaseRequired is returned when no database is specified.
var ErrDatabaseRequired = errors.New("database required")

// Executor recursively executes calls in a PQL query across all slices.
type Executor struct {
	index *Index

	// Local hostname.
	Host string

	// Cluster configuration
	Cluster *Cluster

	// Client used for remote HTTP requests.
	HTTPClient *http.Client
}

// NewExecutor returns a new instance of Executor.
func NewExecutor(index *Index) *Executor {
	return &Executor{
		index:      index,
		HTTPClient: http.DefaultClient,
	}
}

// Index returns the index that the executor runs against.
func (e *Executor) Index() *Index { return e.index }

// Execute executes a PQL query.
func (e *Executor) Execute(db string, q *pql.Query, slices []uint64) (interface{}, error) {
	// Verify that a database is set.
	if db == "" {
		return nil, ErrDatabaseRequired
	}

	// If slices aren't specified, then include all of them.
	if len(slices) == 0 {
		// Round up the number of slices.
		sliceN := (e.index.SliceN() % uint64(len(e.Cluster.Nodes))) + uint64(len(e.Cluster.Nodes))

		// Generate a slices of all slices.
		slices = make([]uint64, sliceN+1)
		for i := range slices {
			slices[i] = uint64(i)
		}
	}

	return e.executeCall(db, q.Root, slices)
}

// executeCall executes a call.
func (e *Executor) executeCall(db string, c pql.Call, slices []uint64) (interface{}, error) {
	switch c := c.(type) {
	case pql.BitmapCall:
		return e.executeBitmapCall(db, c, slices)
	case *pql.Count:
		return e.executeCount(db, c, slices)
	case *pql.TopN:
		return e.executeTopN(db, c, slices)
	default:
		panic("unreachable")
	}
}

// executeBitmapCall executes a call that returns a bitmap.
func (e *Executor) executeBitmapCall(db string, c pql.BitmapCall, slices []uint64) (*Bitmap, error) {
	other := NewBitmap()
	for node, nodeSlices := range e.slicesByNode(slices) {
		// Execute locally if the hostname matches.
		if node.Host == e.Host {
			for _, slice := range nodeSlices {
				bm, err := e.executeBitmapCallSlice(db, c, slice)
				if err != nil {
					return nil, err
				}
				other.Merge(bm)
			}
			continue
		}

		// Otherwise execute remotely.
		res, err := e.exec(node, db, &pql.Query{Root: c}, nodeSlices)
		if err != nil {
			return nil, err
		}
		other.Merge(res.(*Bitmap))
	}
	return other, nil
}

// executeBitmapCallSlice executes a bitmap call for a single slice.
func (e *Executor) executeBitmapCallSlice(db string, c pql.BitmapCall, slice uint64) (*Bitmap, error) {
	switch c := c.(type) {
	case *pql.Difference:
		return e.executeDifferenceSlice(db, c, slice)
	case *pql.Get:
		return e.executeGetSlice(db, c, slice)
	case *pql.Intersect:
		return e.executeIntersectSlice(db, c, slice)
	case *pql.Range:
		return e.executeRangeSlice(db, c, slice)
	case *pql.Union:
		return e.executeUnionSlice(db, c, slice)
	default:
		panic("unreachable")
	}
}

// executeTopN executes a top-n() call.
func (e *Executor) executeTopN(db string, c *pql.TopN, slices []uint64) ([]Pair, error) {
	panic("FIXME: calculate top n from each slice")
}

// executeDifferenceSlice executes a difference() call for a local slice.
func (e *Executor) executeDifferenceSlice(db string, c *pql.Difference, slice uint64) (*Bitmap, error) {
	var other *Bitmap
	for i, input := range c.Inputs {
		bm, err := e.executeBitmapCallSlice(db, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = bm
		} else {
			other = other.Difference(bm)
		}
	}
	return other, nil
}

func (e *Executor) executeGetSlice(db string, c *pql.Get, slice uint64) (*Bitmap, error) {
	frame := c.Frame
	if frame == "" {
		frame = DefaultFrame
	}

	f := e.Index().Fragment(db, frame, slice)
	return f.Bitmap(c.ID), nil
}

// executeIntersectSlice executes a intersect() call for a local slice.
func (e *Executor) executeIntersectSlice(db string, c *pql.Intersect, slice uint64) (*Bitmap, error) {
	var other *Bitmap
	for i, input := range c.Inputs {
		bm, err := e.executeBitmapCallSlice(db, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = bm
		} else {
			other = other.Intersect(bm)
		}
	}
	return other, nil
}

// executeRangeSlice executes a range() call for a local slice.
func (e *Executor) executeRangeSlice(db string, c *pql.Range, slice uint64) (*Bitmap, error) {
	panic("FIXME")
}

// executeUnionSlice executes a union() call for a local slice.
func (e *Executor) executeUnionSlice(db string, c *pql.Union, slice uint64) (*Bitmap, error) {
	var other *Bitmap
	for i, input := range c.Inputs {
		bm, err := e.executeBitmapCallSlice(db, input, slice)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			other = bm
		} else {
			other = other.Union(bm)
		}
	}
	return other, nil
}

// executeCount executes a count() call.
func (e *Executor) executeCount(db string, c *pql.Count, slices []uint64) (uint64, error) {
	var n uint64
	for node, nodeSlices := range e.slicesByNode(slices) {
		// Execute locally if the hostname matches.
		if node.Host == e.Host {
			for _, slice := range nodeSlices {
				bm, err := e.executeBitmapCallSlice(db, c.Input, slice)
				if err != nil {
					return 0, err
				}
				n += bm.Count()
			}
			continue
		}

		// Otherwise execute remotely.
		res, err := e.exec(node, db, &pql.Query{Root: c}, nodeSlices)
		if err != nil {
			return 0, err
		}
		n += res.(uint64)
	}
	return n, nil
}

// exec executes a PQL query remotely for a set of slices on a node.
func (e *Executor) exec(node *Node, db string, q *pql.Query, slices []uint64) (result interface{}, err error) {
	// Encode request object.
	buf, err := proto.Marshal(&internal.QueryRequest{
		DB:     proto.String(db),
		Query:  proto.String(q.String()),
		Slices: slices,
	})
	if err != nil {
		return nil, err
	}

	// Create HTTP request.
	req, err := http.NewRequest("POST", (&url.URL{
		Scheme: "http",
		Host:   node.Host,
		Path:   "/query",
	}).String(), bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	// Require protobuf encoding.
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("Content-Type", "application/x-protobuf")

	// Send request to remote node.
	resp, err := e.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read response into buffer.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Check status code.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status: code=%d, err=%s", resp.StatusCode, body)
	}

	// Decode response object.
	var pb internal.QueryResponse
	if err := proto.Unmarshal(body, &pb); err != nil {
		return nil, err
	}

	// Return an error, if specified on response.
	if err := decodeError(pb.GetErr()); err != nil {
		return nil, err
	}

	// Return appropriate data for the query.
	switch q.Root.(type) {
	case pql.BitmapCall:
		return decodeBitmap(pb.GetBitmap()), nil
	case *pql.TopN:
		return decodePairs(pb.GetPairs()), nil
	case *pql.Count:
		return pb.GetN(), nil
	default:
		panic(fmt.Sprintf("invalid node for remote exec: %T", q.Root))
	}
}

// slicesByNode returns a mapping of nodes to slices.
//
// NOTE: Currently the only primary node is used.
func (e *Executor) slicesByNode(slices []uint64) map[*Node][]uint64 {
	m := make(map[*Node][]uint64)
	for _, slice := range slices {
		nodes := e.Cluster.SliceNodes(slice)

		node := nodes[0]
		m[node] = append(m[node], slice)
	}
	return m
}

// decodeError returns an error representation of s if s is non-blank.
// Returns nil if s is blank.
func decodeError(s string) error {
	if s == "" {
		return nil
	}
	return errors.New(s)
}
