package pilosa

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
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
func (e *Executor) Execute(ctx context.Context, db string, q *pql.Query, slices []uint64, opt *ExecOptions) ([]interface{}, error) {
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
		if needsSlices(q.Calls) {
			// Round up the number of slices.
			maxSlice := e.Index.DB(db).MaxSlice()

			// Generate a slices of all slices.
			slices = make([]uint64, maxSlice+1)
			for i := range slices {
				slices[i] = uint64(i)
			}
		}
	}

	// Optimize handling for bulk attribute insertion.
	if hasOnlySetBitmapAttrs(q.Calls) {
		return e.executeBulkSetBitmapAttrs(ctx, db, q.Calls, opt)
	}

	// Execute each call serially.
	results := make([]interface{}, 0, len(q.Calls))
	for _, call := range q.Calls {
		v, err := e.executeCall(ctx, db, call, slices, opt)
		if err != nil {
			return nil, err
		}
		results = append(results, v)
	}
	return results, nil
}

// executeCall executes a call.
func (e *Executor) executeCall(ctx context.Context, db string, c pql.Call, slices []uint64, opt *ExecOptions) (interface{}, error) {
	switch c := c.(type) {
	case pql.BitmapCall:
		return e.executeBitmapCall(ctx, db, c, slices, opt)
	case *pql.ClearBit:
		return e.executeClearBit(ctx, db, c, opt)
	case *pql.Count:
		return e.executeCount(ctx, db, c, slices, opt)
	case *pql.Profile:
		return e.executeProfile(ctx, db, c, opt)
	case *pql.SetBit:
		return e.executeSetBit(ctx, db, c, opt)
	case *pql.SetBitmapAttrs:
		return nil, e.executeSetBitmapAttrs(ctx, db, c, opt)
	case *pql.SetProfileAttrs:
		return nil, e.executeSetProfileAttrs(ctx, db, c, opt)
	case *pql.TopN:
		return e.executeTopN(ctx, db, c, slices, opt)
	default:
		panic("unreachable")
	}
}

// executeBitmapCall executes a call that returns a bitmap.
func (e *Executor) executeBitmapCall(ctx context.Context, db string, c pql.BitmapCall, slices []uint64, opt *ExecOptions) (*Bitmap, error) {
	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		return e.executeBitmapCallSlice(ctx, db, c, slice)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(*Bitmap)
		if other == nil {
			other = NewBitmap()
		}
		other.Merge(v.(*Bitmap))
		return other
	}

	other, err := e.mapReduce(ctx, db, slices, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}

	// Attach bitmap attributes for Bitmap() calls.
	bm, _ := other.(*Bitmap)
	if c, ok := c.(*pql.Bitmap); ok {
		fr := e.Index.Frame(db, c.Frame)
		if fr != nil {
			attrs, err := fr.BitmapAttrStore().Attrs(c.ID)
			if err != nil {
				return nil, err
			}
			bm.Attrs = attrs
		}
	}

	return bm, nil
}

// executeBitmapCallSlice executes a bitmap call for a single slice.
func (e *Executor) executeBitmapCallSlice(ctx context.Context, db string, c pql.BitmapCall, slice uint64) (*Bitmap, error) {
	switch c := c.(type) {
	case *pql.Bitmap:
		return e.executeBitmapSlice(ctx, db, c, slice)
	case *pql.Difference:
		return e.executeDifferenceSlice(ctx, db, c, slice)
	case *pql.Intersect:
		return e.executeIntersectSlice(ctx, db, c, slice)
	case *pql.Range:
		return e.executeRangeSlice(ctx, db, c, slice)
	case *pql.Union:
		return e.executeUnionSlice(ctx, db, c, slice)
	default:
		panic("unreachable")
	}
}

// executeTopN executes a TopN() call.
// This first performs the TopN() to determine the top results and then
// requeries to retrieve the full counts for each of the top results.
func (e *Executor) executeTopN(ctx context.Context, db string, c *pql.TopN, slices []uint64, opt *ExecOptions) ([]Pair, error) {
	// Execute original query.
	pairs, err := e.executeTopNSlices(ctx, db, c, slices, opt)
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

	trimmedList, err := e.executeTopNSlices(ctx, db, &other, slices, opt)
	if err != nil {
		return nil, err
	}

	if c.N != 0 && int(c.N) < len(trimmedList) {
		trimmedList = trimmedList[0:c.N]
	}
	return trimmedList, nil

}

func (e *Executor) executeTopNSlices(ctx context.Context, db string, c *pql.TopN, slices []uint64, opt *ExecOptions) ([]Pair, error) {
	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		return e.executeTopNSlice(ctx, db, c, slice)
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.([]Pair)
		return Pairs(other).Add(v.([]Pair))
	}

	other, err := e.mapReduce(ctx, db, slices, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}
	results, _ := other.([]Pair)

	// Sort final merged results.
	sort.Sort(Pairs(results))

	return results, nil
}

// executeTopNSlice executes a TopN call for a single slice.
func (e *Executor) executeTopNSlice(ctx context.Context, db string, c *pql.TopN, slice uint64) ([]Pair, error) {
	// Retrieve bitmap used to intersect.
	var src *Bitmap
	if c.Src != nil {
		bm, err := e.executeBitmapCallSlice(ctx, db, c.Src, slice)
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
func (e *Executor) executeDifferenceSlice(ctx context.Context, db string, c *pql.Difference, slice uint64) (*Bitmap, error) {
	var other *Bitmap
	for i, input := range c.Inputs {
		bm, err := e.executeBitmapCallSlice(ctx, db, input, slice)
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

func (e *Executor) executeBitmapSlice(ctx context.Context, db string, c *pql.Bitmap, slice uint64) (*Bitmap, error) {
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
func (e *Executor) executeIntersectSlice(ctx context.Context, db string, c *pql.Intersect, slice uint64) (*Bitmap, error) {
	other := &Bitmap{}
	for i, input := range c.Inputs {
		bm, err := e.executeBitmapCallSlice(ctx, db, input, slice)
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
func (e *Executor) executeRangeSlice(ctx context.Context, db string, c *pql.Range, slice uint64) (*Bitmap, error) {
	frame := c.Frame
	if frame == "" {
		frame = DefaultFrame
	}

	// Retrieve base frame.
	f := e.Index.Frame(db, frame)
	if f == nil {
		return &Bitmap{}, nil
	}

	// If no quantum exists then return an empty bitmap.
	q := f.TimeQuantum()
	if q == "" {
		return &Bitmap{}, nil
	}

	// Union bitmaps across all time-based subframes.
	bm := &Bitmap{}
	for _, subframe := range FramesByTimeRange(frame, c.StartTime, c.EndTime, q) {
		f := e.Index.Fragment(db, subframe, slice)
		if f == nil {
			continue
		}
		bm = bm.Union(f.Bitmap(c.ID))
	}
	return bm, nil
}

// executeUnionSlice executes a union() call for a local slice.
func (e *Executor) executeUnionSlice(ctx context.Context, db string, c *pql.Union, slice uint64) (*Bitmap, error) {
	other := &Bitmap{}
	for i, input := range c.Inputs {
		bm, err := e.executeBitmapCallSlice(ctx, db, input, slice)
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
func (e *Executor) executeCount(ctx context.Context, db string, c *pql.Count, slices []uint64, opt *ExecOptions) (uint64, error) {
	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		bm, err := e.executeBitmapCallSlice(ctx, db, c.Input, slice)
		if err != nil {
			return 0, err
		}
		return bm.Count(), nil
	}

	// Merge returned results at coordinating node.
	reduceFn := func(prev, v interface{}) interface{} {
		other, _ := prev.(uint64)
		return other + v.(uint64)
	}

	result, err := e.mapReduce(ctx, db, slices, c, opt, mapFn, reduceFn)
	if err != nil {
		return 0, err
	}
	n, _ := result.(uint64)

	return n, nil
}

// executeProfile executes a Profile() call.
// This call only executes locally since the profile attibutes are stored locally.
func (e *Executor) executeProfile(ctx context.Context, db string, c *pql.Profile, opt *ExecOptions) (*Profile, error) {
	panic("FIXME: impl: e.Index.ProfileAttr(c.ID)")
}

// executeClearBit executes a ClearBit() call.
func (e *Executor) executeClearBit(ctx context.Context, db string, c *pql.ClearBit, opt *ExecOptions) (bool, error) {
	slice := c.ProfileID / SliceWidth
	ret := false
	for _, node := range e.Cluster.FragmentNodes(db, slice) {
		// Update locally if host matches.
		if node.Host == e.Host {
			f := e.Index.Fragment(db, c.Frame, slice)
			if f == nil {
				return false, nil
			}

			val, err := f.ClearBit(c.ID, c.ProfileID)
			if err != nil {
				return false, err
			} else if val {
				ret = true
			}
			continue
		}
		// Do not forward call if this is already being forwarded.
		if opt.Remote {
			continue
		}

		// Forward call to remote node otherwise.
		if res, err := e.exec(ctx, node, db, &pql.Query{Calls: pql.Calls{c}}, nil, opt); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeSetBit executes a SetBit() call.
func (e *Executor) executeSetBit(ctx context.Context, db string, c *pql.SetBit, opt *ExecOptions) (bool, error) {
	slice := c.ProfileID / SliceWidth
	ret := false

	for _, node := range e.Cluster.FragmentNodes(db, slice) {
		// Update locally if host matches.
		if node.Host == e.Host {
			db, err := e.Index.CreateDBIfNotExists(db)
			if err != nil {
				return false, fmt.Errorf("db: %s", err)
			}
			val, err := db.SetBit(c.Frame, c.ID, c.ProfileID, opt.Timestamp)
			if err != nil {
				return false, err
			} else if val {
				ret = true
			}
			continue
		}

		// Do not forward call if this is already being forwarded.
		if opt.Remote {
			continue
		}

		// Forward call to remote node otherwise.
		if res, err := e.exec(ctx, node, db, &pql.Query{Calls: pql.Calls{c}}, nil, opt); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeSetBitmapAttrs executes a SetBitmapAttrs() call.
func (e *Executor) executeSetBitmapAttrs(ctx context.Context, db string, c *pql.SetBitmapAttrs, opt *ExecOptions) error {
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
			_, err := e.exec(ctx, node, db, &pql.Query{Calls: pql.Calls{c}}, nil, opt)
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
func (e *Executor) executeBulkSetBitmapAttrs(ctx context.Context, db string, calls pql.Calls, opt *ExecOptions) ([]interface{}, error) {
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
			_, err := e.exec(ctx, node, db, &pql.Query{Calls: calls}, nil, opt)
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
func (e *Executor) executeSetProfileAttrs(ctx context.Context, db string, c *pql.SetProfileAttrs, opt *ExecOptions) error {
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
			_, err := e.exec(ctx, node, db, &pql.Query{Calls: pql.Calls{c}}, nil, opt)
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
func (e *Executor) exec(ctx context.Context, node *Node, db string, q *pql.Query, slices []uint64, opt *ExecOptions) (results []interface{}, err error) {
	// Encode request object.
	pbreq := &internal.QueryRequest{
		DB:     db,
		Query:  q.String(),
		Slices: slices,
		Remote: true,
	}
	if opt.Timestamp != nil {
		pbreq.Timestamp = opt.Timestamp.UnixNano()
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
	if err := decodeError(pb.Err); err != nil {
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
			v, err = pb.Results[i].N, nil
		case *pql.SetBit:
			v, err = pb.Results[i].Changed, nil
		case *pql.ClearBit:
			v, err = pb.Results[i].Changed, nil
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
// Returns errSliceUnavailable if a slice cannot be allocated to a node.
func (e *Executor) slicesByNode(nodes []*Node, db string, slices []uint64) (map[*Node][]uint64, error) {
	m := make(map[*Node][]uint64)

loop:
	for _, slice := range slices {
		for _, node := range e.Cluster.FragmentNodes(db, slice) {
			if Nodes(nodes).Contains(node) {
				m[node] = append(m[node], slice)
				continue loop
			}
		}
		return nil, errSliceUnavailable
	}
	return m, nil
}

// mapReduce maps and reduces data across the cluster.
//
// If a mapping of slices to a node fails then the slices are resplit across
// secondary nodes and retried. This continues to occur until all nodes are exhausted.
func (e *Executor) mapReduce(ctx context.Context, db string, slices []uint64, c pql.Call, opt *ExecOptions, mapFn mapFunc, reduceFn reduceFunc) (interface{}, error) {
	ch := make(chan mapResponse, 0)

	// Wrap context with a cancel to kill goroutines on exit.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// If this is the coordinating node then start with all nodes in the cluster.
	//
	// However, if this request is being sent from the coordinator then all
	// processing should be done locally so we start with just the local node.
	var nodes []*Node
	if !opt.Remote {
		nodes = Nodes(e.Cluster.Nodes).Clone()
	} else {
		nodes = []*Node{e.Cluster.NodeByHost(e.Host)}
	}

	// Start mapping across all primary owners.
	if err := e.mapper(ctx, ch, nodes, db, slices, c, opt, mapFn, reduceFn); err != nil {
		return nil, err
	}

	// Iterate over all map responses and reduce.
	var result interface{}
	var maxSlice int
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp := <-ch:
			// On error retry against remaining nodes. If an error returns then
			// the context will cancel and cause all open goroutines to return.
			if resp.err != nil {
				// Filter out unavailable nodes.
				nodes = Nodes(nodes).Filter(resp.node)

				// Begin mapper against secondary nodes.
				if err := e.mapper(ctx, ch, nodes, db, resp.slices, c, opt, mapFn, reduceFn); err == errSliceUnavailable {
					return nil, resp.err
				} else if err != nil {
					return nil, err
				}
				continue
			}

			// Reduce value.
			result = reduceFn(result, resp.result)

			// If all slices have been processed then return.
			maxSlice += len(resp.slices)
			if maxSlice >= len(slices) {
				return result, nil
			}
		}
	}
}

func (e *Executor) mapper(ctx context.Context, ch chan mapResponse, nodes []*Node, db string, slices []uint64, c pql.Call, opt *ExecOptions, mapFn mapFunc, reduceFn reduceFunc) error {
	// Group slices together by nodes.
	m, err := e.slicesByNode(nodes, db, slices)
	if err != nil {
		return err
	}

	// Execute each node in a separate goroutine.
	for n, nodeSlices := range m {
		go func(n *Node, nodeSlices []uint64) {
			resp := mapResponse{node: n, slices: nodeSlices}

			// Send local slices to mapper, otherwise remote exec.
			if n.Host == e.Host {
				resp.result, resp.err = e.mapperLocal(ctx, nodeSlices, mapFn, reduceFn)
			} else if !opt.Remote {
				results, err := e.exec(ctx, n, db, &pql.Query{Calls: pql.Calls{c}}, nodeSlices, opt)
				if len(results) > 0 {
					resp.result = results[0]
				}
				resp.err = err
			}

			// Return response to the channel.
			select {
			case <-ctx.Done():
			case ch <- resp:
			}
		}(n, nodeSlices)
	}

	return nil
}

// mapperLocal performs map & reduce entirely on the local node.
func (e *Executor) mapperLocal(ctx context.Context, slices []uint64, mapFn mapFunc, reduceFn reduceFunc) (interface{}, error) {
	ch := make(chan mapResponse, len(slices))

	for _, slice := range slices {
		go func(slice uint64) {
			result, err := mapFn(slice)

			// Return response to the channel.
			select {
			case <-ctx.Done():
			case ch <- mapResponse{result: result, err: err}:
			}
		}(slice)
	}

	// Reduce results
	var maxSlice int
	var result interface{}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp := <-ch:
			if resp.err != nil {
				return nil, resp.err
			}
			result = reduceFn(result, resp.result)
			maxSlice++
		}

		// Exit once all slices are processed.
		if maxSlice == len(slices) {
			return result, nil
		}
	}
}

// errSliceUnavailable is a marker error if no nodes are available.
var errSliceUnavailable = errors.New("slice unavailable")

type mapFunc func(slice uint64) (interface{}, error)

type reduceFunc func(prev, v interface{}) interface{}

type mapResponse struct {
	node   *Node
	slices []uint64

	result interface{}
	err    error
}

// ExecOptions represents an execution context for a single Execute() call.
type ExecOptions struct {
	Timestamp *time.Time
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

func needsSlices(calls pql.Calls) bool {
	if len(calls) == 0 {
		return false
	}

	for _, call := range calls {
		if _, ok := call.(pql.BitmapCall); ok {
			return true
		} else if _, ok := call.(*pql.Count); ok {
			return true
		} else if _, ok := call.(*pql.TopN); ok {
			return true
		}

	}
	return false
}
