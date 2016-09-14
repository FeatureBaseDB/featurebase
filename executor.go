package pilosa

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa/internal"
	"github.com/umbel/pilosa/pql"
)

// DefaultFrame is the frame used if one is not specified.
const DefaultFrame = "general"

// Executor recursively executes calls in a PQL query across all slices.
type Executor struct {
	Index *Index

	// Local hostname & cluster configuration.
	Host    string
	Cluster *Cluster

	// Client used for remote HTTP requests.
	HTTPClient *http.Client
}

// NewExecutor returns a new instance of Executor.
func NewExecutor() *Executor {
	return &Executor{
		HTTPClient: http.DefaultClient,
	}
}

// Execute executes a PQL query.
func (e *Executor) Execute(db string, q *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error) {
	// Verify that a database is set.
	if db == "" {
		return nil, ErrDatabaseRequired
	}

	// Default options.
	if opt == nil {
		opt = &ExecOptions{}
	}

	// If slices aren't specified, then include all of them.
	if len(slices) == 0 {
		// Round up the number of slices.
		sliceN := e.Index.SliceN()
		sliceN += (sliceN % uint64(len(e.Cluster.Nodes))) + uint64(len(e.Cluster.Nodes))

		// Generate a slices of all slices.
		slices = make([]uint64, sliceN+1)
		for i := range slices {
			slices[i] = uint64(i)
		}
	}

	// Optimize handling for bulk attribute insertion.
	if hasOnlySetBitmapAttrs(q.Calls) {
		return e.executeBulkSetBitmapAttrs(db, q.Calls, opt)
	}

	// Execute each call serially.
	results := make([]interface{}, 0, len(q.Calls))
	for _, call := range q.Calls {
		v, err := e.executeCall(db, call, slices, opt)
		if err != nil {
			return nil, err
		}
		results = append(results, v)
	}
	return results, nil
}

// executeCall executes a call.
func (e *Executor) executeCall(db string, c pql.Call, slices []uint64, opt *ExecOptions) (interface{}, error) {
	switch c := c.(type) {
	case pql.BitmapCall:
		return e.executeBitmapCall(db, c, slices, opt)
	case *pql.ClearBit:
		return e.executeClearBit(db, c, opt)
	case *pql.Count:
		return e.executeCount(db, c, slices, opt)
	case *pql.Profile:
		return e.executeProfile(db, c, opt)
	case *pql.SetBit:
		return e.executeSetBit(db, c, opt)
	case *pql.SetBitmapAttrs:
		return nil, e.executeSetBitmapAttrs(db, c, opt)
	case *pql.SetProfileAttrs:
		return nil, e.executeSetProfileAttrs(db, c, opt)
	case *pql.TopN:
		return e.executeTopN(db, c, slices, opt)
	default:
		panic("unreachable")
	}
}

// executeBitmapCall executes a call that returns a bitmap.
func (e *Executor) executeBitmapCall(db string, c pql.BitmapCall, slices []uint64, opt *ExecOptions) (*Bitmap, error) {
	other := NewBitmap()
	for node, nodeSlices := range e.slicesByNode(db, slices) {
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
		res, err := e.exec(node, db, &pql.Query{Calls: pql.Calls{c}}, nodeSlices, opt)
		if err != nil {
			return nil, err
		}
		other.Merge(res[0].(*Bitmap))
	}

	// Attach bitmap attributes for Bitmap() calls.
	if c, ok := c.(*pql.Bitmap); ok {
		fr := e.Index.Frame(db, c.Frame)
		if fr != nil {
			attrs, err := fr.BitmapAttrStore().Attrs(c.ID)
			if err != nil {
				return nil, err
			}
			other.Attrs = attrs
		}
	}

	return other, nil
}

// executeBitmapCallSlice executes a bitmap call for a single slice.
func (e *Executor) executeBitmapCallSlice(db string, c pql.BitmapCall, slice uint64) (*Bitmap, error) {
	switch c := c.(type) {
	case *pql.Bitmap:
		return e.executeBitmapSlice(db, c, slice)
	case *pql.Difference:
		return e.executeDifferenceSlice(db, c, slice)
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

// executeTopN executes a TopN() call.
// This first performs the TopN() to determine the top results and then
// requeries to retrieve the full counts for each of the top results.
func (e *Executor) executeTopN(db string, c *pql.TopN, slices []uint64, opt *ExecOptions) ([]Pair, error) {
	// Execute original query.
	pairs, err := e.executeTopNSlices(db, c, slices, opt)
	if err != nil {
		return nil, err
	}

	// If this call is against specific ids, or we didn't get results,
	// or we are part of a larger distributed query then don't refetch.
	if len(pairs) == 0 || len(c.BitmapIDs) > 0 || opt.Remote {
		return pairs, nil
	}

	// Only the original caller should refetch the full counts.
	other := *c
	other.N = 0
	other.BitmapIDs = Pairs(pairs).Keys()
	sort.Sort(uint64Slice(other.BitmapIDs))

	return e.executeTopNSlices(db, &other, slices, opt)
}

func (e *Executor) executeTopNSlices(db string, c *pql.TopN, slices []uint64, opt *ExecOptions) ([]Pair, error) {
	slicesByNode := e.slicesByNode(db, slices)

	type resp struct {
		pairs []Pair
		err   error
	}
	ch := make(chan resp, len(slicesByNode))

	for node, nodeSlices := range slicesByNode {
		go func(node *Node, nodeSlices []uint64) {
			// Execute locally if the hostname matches.
			if node.Host == e.Host {
				pairs, err := e.executeTopNSlicesLocal(db, c, nodeSlices)
				ch <- resp{pairs: pairs, err: err}
				return
			}

			// Otherwise execute remotely.
			res, err := e.exec(node, db, &pql.Query{Calls: pql.Calls{c}}, nodeSlices, opt)
			if err != nil {
				ch <- resp{err: err}
				return
			}
			ch <- resp{pairs: res[0].([]Pair)}
		}(node, nodeSlices)
	}

	// Collect results.
	var results []Pair
	for range slicesByNode {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		results = Pairs(results).Add(r.pairs)
	}

	// Sort final merged results.
	sort.Sort(Pairs(results))

	// Only keep the top n after sorting.
	if c.N > 0 && len(results) > c.N {
		results = results[0:c.N]
	}

	return results, nil
}

func (e *Executor) executeTopNSlicesLocal(db string, c *pql.TopN, slices []uint64) ([]Pair, error) {
	type resp struct {
		pairs []Pair
		err   error
	}
	ch := make(chan resp, len(slices))

	// Execute TopN() in parallel across slices.
	for _, slice := range slices {
		go func(slice uint64) {
			pairs, err := e.executeTopNSlice(db, c, slice)
			ch <- resp{pairs: pairs, err: err}
		}(slice)
	}

	// Collect results.
	var results []Pair
	for range slices {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}
		results = Pairs(results).Add(r.pairs)
	}

	return results, nil
}

// executeTopNSlice executes a TopN call for a single slice.
func (e *Executor) executeTopNSlice(db string, c *pql.TopN, slice uint64) ([]Pair, error) {
	// Retrieve bitmap used to intersect.
	var src *Bitmap
	if c.Src != nil {
		bm, err := e.executeBitmapCallSlice(db, c.Src, slice)
		if err != nil {
			return nil, err
		}
		src = bm
	}

	// Set default frame.
	frame := c.Frame
	if frame == "" {
		frame = DefaultFrame
	}

	f := e.Index.Fragment(db, frame, slice)
	if f == nil {
		return nil, nil
	}

	return f.Top(TopOptions{
		N:            c.N,
		Src:          src,
		BitmapIDs:    c.BitmapIDs,
		FilterField:  c.Field,
		FilterValues: c.Filters,
	})
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
	other.InvalidateCount()
	return other, nil
}

func (e *Executor) executeBitmapSlice(db string, c *pql.Bitmap, slice uint64) (*Bitmap, error) {
	frame := c.Frame
	if frame == "" {
		frame = DefaultFrame
	}

	f := e.Index.Fragment(db, frame, slice)
	if f == nil {
		return NewBitmap(), nil
	}
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
	other.InvalidateCount()
	return other, nil
}

// executeRangeSlice executes a range() call for a local slice.
func (e *Executor) executeRangeSlice(db string, c *pql.Range, slice uint64) (*Bitmap, error) {
	frame := c.Frame
	if frame == "" {
		frame = DefaultFrame
	}

	f := e.Index.Fragment(db, frame, slice)
	if f == nil {
		return NewBitmap(), nil
	}
	return f.Range(c.ID, c.StartTime, c.EndTime), nil
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
	other.InvalidateCount()
	return other, nil
}

// executeCount executes a count() call.
func (e *Executor) executeCount(db string, c *pql.Count, slices []uint64, opt *ExecOptions) (uint64, error) {
	var n uint64
	for node, nodeSlices := range e.slicesByNode(db, slices) {
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
		res, err := e.exec(node, db, &pql.Query{Calls: pql.Calls{c}}, nodeSlices, opt)
		if err != nil {
			return 0, err
		}
		n += res[0].(uint64)
	}
	return n, nil
}

// executeProfile executes a Profile() call.
// This call only executes locally since the profile attibutes are stored locally.
func (e *Executor) executeProfile(db string, c *pql.Profile, opt *ExecOptions) (*Profile, error) {
	panic("FIXME: impl: e.Index.ProfileAttr(c.ID)")
}

// executeClearBit executes a ClearBit() call.
func (e *Executor) executeClearBit(db string, c *pql.ClearBit, opt *ExecOptions) (bool, error) {
	slice := c.ProfileID / SliceWidth
	ret := false
	for _, node := range e.Cluster.FragmentNodes(db, slice) {
		// Update locally if host matches.
		if node.Host == e.Host {
			f, err := e.Index.CreateFragmentIfNotExists(db, c.Frame, slice)
			if err != nil {
				return false, fmt.Errorf("fragment: %s", err)
			}
			val, err := f.ClearBit(c.ID, c.ProfileID)
			if err != nil {
				return false, err
			}
			if val {
				ret = true
			}
			continue
		}

		// Forward call to remote node otherwise.
		if res, err := e.exec(node, db, &pql.Query{Calls: pql.Calls{c}}, nil, opt); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeSetBit executes a SetBit() call.
func (e *Executor) executeSetBit(db string, c *pql.SetBit, opt *ExecOptions) (bool, error) {
	slice := c.ProfileID / SliceWidth
	ret := false

	for _, node := range e.Cluster.FragmentNodes(db, slice) {
		// Update locally if host matches.
		if node.Host == e.Host {
			f, err := e.Index.CreateFragmentIfNotExists(db, c.Frame, slice)
			if err != nil {
				return false, fmt.Errorf("fragment: %s", err)
			}
			val, err := f.SetBit(c.ID, c.ProfileID, opt.Timestamp, opt.Quantum)
			if err != nil {
				return false, err
			}
			if val {
				ret = true
			}
			continue
		}

		// Do not forward call if this is already being forwarded.
		if opt.Remote {
			continue
		}

		// Forward call to remote node otherwise.
		if res, err := e.exec(node, db, &pql.Query{Calls: pql.Calls{c}}, nil, opt); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeSetBitmapAttrs executes a SetBitmapAttrs() call.
func (e *Executor) executeSetBitmapAttrs(db string, c *pql.SetBitmapAttrs, opt *ExecOptions) error {
	// Retrieve frame.
	frame, err := e.Index.CreateFrameIfNotExists(db, c.Frame)
	if err != nil {
		return err
	}

	// Set attributes.
	if err := frame.BitmapAttrStore().SetAttrs(c.ID, c.Attrs); err != nil {
		return err
	}

	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.Nodes).FilterHost(e.Host)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.exec(node, db, &pql.Query{Calls: pql.Calls{c}}, nil, opt)
			resp <- err
		}(node)
	}

	// Return first error.
	for range nodes {
		if err := <-resp; err != nil {
			return err
		}
	}

	return nil
}

// executeBulkSetBitmapAttrs executes a set of SetBitmapAttrs() calls.
func (e *Executor) executeBulkSetBitmapAttrs(db string, calls pql.Calls, opt *ExecOptions) ([]interface{}, error) {
	// Collect attributes by frame/id.
	m := make(map[string]map[uint64]map[string]interface{})
	for _, call := range calls {
		c := call.(*pql.SetBitmapAttrs)

		// Create frame group, if not exists.
		frameMap := m[c.Frame]
		if frameMap == nil {
			frameMap = make(map[uint64]map[string]interface{})
			m[c.Frame] = frameMap
		}

		// Set or merge attributes.
		attr := frameMap[c.ID]
		if attr == nil {
			frameMap[c.ID] = cloneAttrs(c.Attrs)
		} else {
			for k, v := range c.Attrs {
				attr[k] = v
			}
		}
	}

	// Bulk insert attributes by frame.
	for name, frameMap := range m {
		// Retrieve frame.
		frame, err := e.Index.CreateFrameIfNotExists(db, name)
		if err != nil {
			return nil, err
		}

		// Set attributes.
		if err := frame.BitmapAttrStore().SetBulkAttrs(frameMap); err != nil {
			return nil, err
		}
	}

	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return make([]interface{}, len(calls)), nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.Nodes).FilterHost(e.Host)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.exec(node, db, &pql.Query{Calls: calls}, nil, opt)
			resp <- err
		}(node)
	}

	// Return first error.
	for range nodes {
		if err := <-resp; err != nil {
			return nil, err
		}
	}

	// Return a set of nil responses to match the non-optimized return.
	return make([]interface{}, len(calls)), nil
}

// executeSetProfileAttrs executes a SetProfileAttrs() call.
func (e *Executor) executeSetProfileAttrs(db string, c *pql.SetProfileAttrs, opt *ExecOptions) error {
	// Retrieve database.
	d, err := e.Index.CreateDBIfNotExists(db)
	if err != nil {
		return err
	}

	// Set attributes.
	if err := d.ProfileAttrStore().SetAttrs(c.ID, c.Attrs); err != nil {
		return err
	}

	// Do not forward call if this is already being forwarded.
	if opt.Remote {
		return nil
	}

	// Execute on remote nodes in parallel.
	nodes := Nodes(e.Cluster.Nodes).FilterHost(e.Host)
	resp := make(chan error, len(nodes))
	for _, node := range nodes {
		go func(node *Node) {
			_, err := e.exec(node, db, &pql.Query{Calls: pql.Calls{c}}, nil, opt)
			resp <- err
		}(node)
	}

	// Return first error.
	for range nodes {
		if err := <-resp; err != nil {
			return err
		}
	}

	return nil
}

// exec executes a PQL query remotely for a set of slices on a node.
func (e *Executor) exec(node *Node, db string, q *pql.Query, slices []uint64, opt *ExecOptions) (results []interface{}, err error) {
	// Encode request object.
	pbreq := &internal.QueryRequest{
		DB:      proto.String(db),
		Query:   proto.String(q.String()),
		Slices:  slices,
		Quantum: proto.Uint32(uint32(opt.Quantum)),
		Remote:  proto.Bool(true),
	}
	if opt.Timestamp != nil {
		pbreq.Timestamp = proto.Int64(opt.Timestamp.UnixNano())
	}
	buf, err := proto.Marshal(pbreq)
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
	results = make([]interface{}, len(q.Calls))
	for i, call := range q.Calls {
		var v interface{}
		var err error

		switch call.(type) {
		case pql.BitmapCall:
			v, err = decodeBitmap(pb.Results[i].GetBitmap()), nil
		case *pql.TopN:
			v, err = decodePairs(pb.Results[i].GetPairs()), nil
		case *pql.Count:
			v, err = pb.Results[i].GetN(), nil
		case *pql.SetBit:
			v, err = pb.Results[i].GetChanged(), nil
		case *pql.SetBitmapAttrs:
		case *pql.SetProfileAttrs:
		default:
			panic(fmt.Sprintf("invalid node for remote exec: %T", call))
		}
		if err != nil {
			return nil, err
		}

		results[i] = v
	}
	return results, nil
}

// slicesByNode returns a mapping of nodes to slices.
//
// NOTE: Currently the only primary node is used.
func (e *Executor) slicesByNode(db string, slices []uint64) map[*Node][]uint64 {
	m := make(map[*Node][]uint64)
	for _, slice := range slices {
		nodes := e.Cluster.FragmentNodes(db, slice)

		node := nodes[0]
		m[node] = append(m[node], slice)
	}
	return m
}

// ExecOptions represents an execution context for a single Execute() call.
type ExecOptions struct {
	Timestamp *time.Time
	Quantum   TimeQuantum
	Remote    bool
}

// decodeError returns an error representation of s if s is non-blank.
// Returns nil if s is blank.
func decodeError(s string) error {
	if s == "" {
		return nil
	}
	return errors.New(s)
}

// hasOnlySetBitmapAttrs returns true if calls only contains SetBitmapAttrs() calls.
func hasOnlySetBitmapAttrs(calls pql.Calls) bool {
	if len(calls) == 0 {
		return false
	}

	for _, call := range calls {
		if _, ok := call.(*pql.SetBitmapAttrs); !ok {
			return false
		}
	}
	return true
}
