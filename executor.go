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
func (e *Executor) executeCall(ctx context.Context, db string, c *pql.Call, slices []uint64, opt *ExecOptions) (interface{}, error) {
	// Special handling for mutation and top-n calls.
	switch c.Name {
	case "ClearBit":
		return e.executeClearBit(ctx, db, c, opt)
	case "Count":
		return e.executeCount(ctx, db, c, slices, opt)
	case "Profile":
		return e.executeProfile(ctx, db, c, opt)
	case "SetBit":
		return e.executeSetBit(ctx, db, c, opt)
	case "SetBitmapAttrs":
		return nil, e.executeSetBitmapAttrs(ctx, db, c, opt)
	case "SetProfileAttrs":
		return nil, e.executeSetProfileAttrs(ctx, db, c, opt)
	case "TopN":
		return e.executeTopN(ctx, db, c, slices, opt)
	default:
		return e.executeBitmapCall(ctx, db, c, slices, opt)
	}
}

// executeBitmapCall executes a call that returns a bitmap.
func (e *Executor) executeBitmapCall(ctx context.Context, db string, c *pql.Call, slices []uint64, opt *ExecOptions) (*Bitmap, error) {
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
	if c.Name == "Bitmap" {
		frame, _ := c.Args["frame"].(string)

		fr := e.Index.Frame(db, frame)
		if fr != nil {
			rowLabel := fr.RowLabel()
			rowID, _ := c.Args[rowLabel].(uint64)

			attrs, err := fr.BitmapAttrStore().Attrs(rowID)
			if err != nil {
				return nil, err
			}
			bm.Attrs = attrs
		}
	}

	return bm, nil
}

// executeBitmapCallSlice executes a bitmap call for a single slice.
func (e *Executor) executeBitmapCallSlice(ctx context.Context, db string, c *pql.Call, slice uint64) (*Bitmap, error) {
	switch c.Name {
	case "Bitmap":
		return e.executeBitmapSlice(ctx, db, c, slice)
	case "Difference":
		return e.executeDifferenceSlice(ctx, db, c, slice)
	case "Intersect":
		return e.executeIntersectSlice(ctx, db, c, slice)
	case "Range":
		return e.executeRangeSlice(ctx, db, c, slice)
	case "Union":
		return e.executeUnionSlice(ctx, db, c, slice)
	default:
		return nil, fmt.Errorf("unknown call: %s", c.Name)
	}
}

// executeTopN executes a TopN() call.
// This first performs the TopN() to determine the top results and then
// requeries to retrieve the full counts for each of the top results.
func (e *Executor) executeTopN(ctx context.Context, db string, c *pql.Call, slices []uint64, opt *ExecOptions) ([]Pair, error) {
	bitmapIDs, _ := c.Args["ids"].([]uint64)
	var n uint64
	if nval, ok := c.Args["n"]; ok {
		n = nval.(uint64)
	}

	// Execute original query.
	pairs, err := e.executeTopNSlices(ctx, db, c, slices, opt)
	if err != nil {
		return nil, err
	}

	// If this call is against specific ids, or we didn't get results,
	// or we are part of a larger distributed query then don't refetch.
	if len(pairs) == 0 || len(bitmapIDs) > 0 || opt.Remote {
		return pairs, nil
	}
	// Only the original caller should refetch the full counts.
	other := c.Clone()

	// Double the size of n for other calls in order to...
	// TODO: travis review
	other.Args["n"] = len(bitmapIDs) * 2

	ids := Pairs(pairs).Keys()
	sort.Sort(uint64Slice(ids))
	other.Args["ids"] = ids

	trimmedList, err := e.executeTopNSlices(ctx, db, other, slices, opt)
	if err != nil {
		return nil, err
	}

	if n != 0 && int(n) < len(trimmedList) {
		trimmedList = trimmedList[0:n]
	}
	return trimmedList, nil
}

func (e *Executor) executeTopNSlices(ctx context.Context, db string, c *pql.Call, slices []uint64, opt *ExecOptions) ([]Pair, error) {
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
func (e *Executor) executeTopNSlice(ctx context.Context, db string, c *pql.Call, slice uint64) ([]Pair, error) {
	frame, _ := c.Args["frame"].(string)
	n, _ := c.Args["n"].(uint64)
	field, _ := c.Args["field"].(string)
	bitmapIDs, _ := c.Args["ids"].([]uint64)
	filters, _ := c.Args["filters"].([]interface{})

	// Retrieve bitmap used to intersect.
	var src *Bitmap
	if len(c.Children) == 1 {
		bm, err := e.executeBitmapCallSlice(ctx, db, c.Children[0], slice)
		if err != nil {
			return nil, err
		}
		src = bm
	} else if len(c.Children) > 1 {
		return nil, errors.New("TopN() can only have one input bitmap")
	}

	// Set default frame.
	if frame == "" {
		frame = DefaultFrame
	}

	f := e.Index.Fragment(db, frame, slice)
	if f == nil {
		return nil, nil
	}

	return f.Top(TopOptions{
		N:            int(n),
		Src:          src,
		BitmapIDs:    bitmapIDs,
		FilterField:  field,
		FilterValues: filters,
	})
}

// executeDifferenceSlice executes a difference() call for a local slice.
func (e *Executor) executeDifferenceSlice(ctx context.Context, db string, c *pql.Call, slice uint64) (*Bitmap, error) {
	var other *Bitmap
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Difference query is currently not supported")
	}
	for i, input := range c.Children {
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

func (e *Executor) executeBitmapSlice(ctx context.Context, db string, c *pql.Call, slice uint64) (*Bitmap, error) {
	frame, _ := c.Args["frame"].(string)
	if frame == "" {
		frame = DefaultFrame
	}

	f := e.Index.Frame(db, frame)
	if f == nil {
		return nil, ErrFrameNotFound
	}
	rowLabel := f.RowLabel()

	rowID, ok := c.Args[rowLabel].(uint64)
	if !ok {
		return nil, fmt.Errorf("Bitmap() field required: %s", rowLabel)
	}

	frag := e.Index.Fragment(db, frame, slice)
	if frag == nil {
		return NewBitmap(), nil
	}
	return frag.Bitmap(rowID), nil
}

// executeIntersectSlice executes a intersect() call for a local slice.
func (e *Executor) executeIntersectSlice(ctx context.Context, db string, c *pql.Call, slice uint64) (*Bitmap, error) {
	var other *Bitmap
	if len(c.Children) == 0 {
		return nil, fmt.Errorf("empty Intersect query is currently not supported")
	}
	for i, input := range c.Children {
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
func (e *Executor) executeRangeSlice(ctx context.Context, db string, c *pql.Call, slice uint64) (*Bitmap, error) {
	// Parse frame, use default if unset.
	frame, _ := c.Args["frame"].(string)
	if frame == "" {
		frame = DefaultFrame
	}

	// Retrieve base frame.
	f := e.Index.Frame(db, frame)
	if f == nil {
		return nil, ErrFrameNotFound
	}
	rowLabel := f.RowLabel()

	// Read row id.
	rowID, _ := c.Args[rowLabel].(uint64)

	// Parse start time.
	startTimeStr, ok := c.Args["start"].(string)
	if !ok {
		return nil, errors.New("Range() start time required")
	}
	startTime, err := time.Parse(TimeFormat, startTimeStr)
	if err != nil {
		return nil, errors.New("cannot parse Range() start time")
	}

	// Parse end time.
	endTimeStr, _ := c.Args["end"].(string)
	if !ok {
		return nil, errors.New("Range() end time required")
	}
	endTime, err := time.Parse(TimeFormat, endTimeStr)
	if err != nil {
		return nil, errors.New("cannot parse Range() end time")
	}

	// If no quantum exists then return an empty bitmap.
	q := f.TimeQuantum()
	if q == "" {
		return &Bitmap{}, nil
	}

	// Union bitmaps across all time-based subframes.
	bm := &Bitmap{}
	for _, subframe := range FramesByTimeRange(frame, startTime, endTime, q) {
		f := e.Index.Fragment(db, subframe, slice)
		if f == nil {
			continue
		}
		bm = bm.Union(f.Bitmap(rowID))
	}
	return bm, nil
}

// executeUnionSlice executes a union() call for a local slice.
func (e *Executor) executeUnionSlice(ctx context.Context, db string, c *pql.Call, slice uint64) (*Bitmap, error) {
	other := NewBitmap()
	for i, input := range c.Children {
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
func (e *Executor) executeCount(ctx context.Context, db string, c *pql.Call, slices []uint64, opt *ExecOptions) (uint64, error) {
	if len(c.Children) == 0 {
		return 0, errors.New("Count() requires an input bitmap")
	} else if len(c.Children) > 1 {
		return 0, errors.New("Count() only accepts a single bitmap input")
	}

	// Execute calls in bulk on each remote node and merge.
	mapFn := func(slice uint64) (interface{}, error) {
		bm, err := e.executeBitmapCallSlice(ctx, db, c.Children[0], slice)
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
func (e *Executor) executeProfile(ctx context.Context, db string, c *pql.Call, opt *ExecOptions) (*Profile, error) {
	panic("FIXME: impl: e.Index.ProfileAttr(c.ID)")
}

// executeClearBit executes a ClearBit() call.
func (e *Executor) executeClearBit(ctx context.Context, db string, c *pql.Call, opt *ExecOptions) (bool, error) {
	frame, ok := c.Args["frame"].(string)
	if !ok {
		return false, errors.New("ClearBit() frame required")
	}

	// Lookup column label.
	d := e.Index.DB(db)
	if d == nil {
		return false, nil
	}
	columnLabel := d.ColumnLabel()

	// Lookup row label.
	f := e.Index.Frame(db, frame)
	if f == nil {
		return false, nil
	}
	rowLabel := f.RowLabel()

	// Read row & column ids.
	rowID, ok := c.Args[rowLabel].(uint64)
	if !ok {
		return false, fmt.Errorf("ClearBit() field required: %s", rowLabel)
	}

	colID, ok := c.Args[columnLabel].(uint64)
	if !ok {
		return false, fmt.Errorf("ClearBit() field required: %s", columnLabel)
	}

	slice := colID / SliceWidth
	ret := false
	for _, node := range e.Cluster.FragmentNodes(db, slice) {
		// Update locally if host matches.
		if node.Host == e.Host {
			frag := e.Index.Fragment(db, frame, slice)
			if frag == nil {
				return false, nil
			}

			val, err := frag.ClearBit(rowID, colID)
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
		if res, err := e.exec(ctx, node, db, &pql.Query{Calls: []*pql.Call{c}}, nil, opt); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeSetBit executes a SetBit() call.
func (e *Executor) executeSetBit(ctx context.Context, db string, c *pql.Call, opt *ExecOptions) (bool, error) {
	frame, ok := c.Args["frame"].(string)
	if !ok {
		return false, errors.New("SetBit() field required: frame")
	}

	// Retrieve frame.
	d := e.Index.DB(db)
	if d == nil {
		return false, ErrFrameNotFound
	}
	f := d.Frame(frame)
	if f == nil {
		return false, ErrFrameNotFound
	}

	// Retrieve labels.
	columnLabel := d.ColumnLabel()
	rowLabel := f.RowLabel()

	// Read fields using labels.
	rowID, ok := c.Args[rowLabel].(uint64)
	if !ok {
		return false, fmt.Errorf("SetBit() field required: %s", rowLabel)
	}

	colID, ok := c.Args[columnLabel].(uint64)
	if !ok {
		return false, fmt.Errorf("SetBit() field required: %s", columnLabel)
	}

	var timestamp *time.Time
	sTimestamp, ok := c.Args["timestamp"].(string)
	if ok {
		t, err := time.Parse(TimeFormat, sTimestamp)
		if err != nil {
			return false, fmt.Errorf("invalid date: %s", sTimestamp)
		}
		timestamp = &t
	}

	slice := colID / SliceWidth
	ret := false

	for _, node := range e.Cluster.FragmentNodes(db, slice) {
		// Update locally if host matches.
		if node.Host == e.Host {
			d := e.Index.DB(db)
			if d == nil {
				return false, ErrDatabaseNotFound
			}

			val, err := d.SetBit(frame, rowID, colID, timestamp)
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
		if res, err := e.exec(ctx, node, db, &pql.Query{Calls: []*pql.Call{c}}, nil, opt); err != nil {
			return false, err
		} else {
			ret = res[0].(bool)
		}
	}
	return ret, nil
}

// executeSetBitmapAttrs executes a SetBitmapAttrs() call.
func (e *Executor) executeSetBitmapAttrs(ctx context.Context, db string, c *pql.Call, opt *ExecOptions) error {
	frameName, ok := c.Args["frame"].(string)
	if !ok {
		return errors.New("SetBitmapAttrs() frame required")
	}

	// Retrieve frame.
	frame := e.Index.Frame(db, frameName)
	if frame == nil {
		return ErrFrameNotFound
	}
	rowLabel := frame.RowLabel()

	// Parse labels.
	rowID, ok := c.Args[rowLabel].(uint64)
	if !ok {
		return fmt.Errorf("SetBitmapAttrs() field required: %s", rowLabel)
	}

	// Copy args and remove reserved fields.
	attrs := pql.CopyArgs(c.Args)
	delete(attrs, "frame")
	delete(attrs, rowLabel)

	// Set attributes.
	if err := frame.BitmapAttrStore().SetAttrs(rowID, attrs); err != nil {
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
			_, err := e.exec(ctx, node, db, &pql.Query{Calls: []*pql.Call{c}}, nil, opt)
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
func (e *Executor) executeBulkSetBitmapAttrs(ctx context.Context, db string, calls []*pql.Call, opt *ExecOptions) ([]interface{}, error) {
	// Collect attributes by frame/id.
	m := make(map[string]map[uint64]map[string]interface{})
	for _, c := range calls {
		frame, ok := c.Args["frame"].(string)
		if !ok {
			return nil, errors.New("SetBitmapAttrs() frame required")
		}

		// Retrieve frame.
		f := e.Index.Frame(db, frame)
		if f == nil {
			return nil, ErrFrameNotFound
		}
		rowLabel := f.RowLabel()

		rowID, ok := c.Args[rowLabel].(uint64)
		if !ok {
			return nil, fmt.Errorf("SetBitmapAttrs() field required: %s", rowLabel)
		}

		// Copy args and remove reserved fields.
		attrs := pql.CopyArgs(c.Args)
		delete(attrs, "frame")
		delete(attrs, rowLabel)

		// Create frame group, if not exists.
		frameMap := m[frame]
		if frameMap == nil {
			frameMap = make(map[uint64]map[string]interface{})
			m[frame] = frameMap
		}

		// Set or merge attributes.
		attr := frameMap[rowID]
		if attr == nil {
			frameMap[rowID] = cloneAttrs(attrs)
		} else {
			for k, v := range attrs {
				attr[k] = v
			}
		}
	}

	// Bulk insert attributes by frame.
	for name, frameMap := range m {
		// Retrieve frame.
		frame := e.Index.Frame(db, name)
		if frame == nil {
			return nil, ErrFrameNotFound
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
func (e *Executor) executeSetProfileAttrs(ctx context.Context, db string, c *pql.Call, opt *ExecOptions) error {
	id, ok := c.Args["id"].(uint64)
	if !ok {
		return errors.New("SetProfileAttrs() id required")
	}

	// Copy args and remove reserved fields.
	attrs := pql.CopyArgs(c.Args)
	delete(attrs, "id")

	// Retrieve database.
	d := e.Index.DB(db)
	if d == nil {
		return ErrDatabaseNotFound
	}

	// Set attributes.
	if err := d.ProfileAttrStore().SetAttrs(id, attrs); err != nil {
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
			_, err := e.exec(ctx, node, db, &pql.Query{Calls: []*pql.Call{c}}, nil, opt)
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

		switch call.Name {
		case "TopN":
			v, err = decodePairs(pb.Results[i].GetPairs()), nil
		case "Count":
			v, err = pb.Results[i].N, nil
		case "SetBit":
			v, err = pb.Results[i].Changed, nil
		case "ClearBit":
			v, err = pb.Results[i].Changed, nil
		case "SetBitmapAttrs":
		case "SetProfileAttrs":
		default:
			v, err = decodeBitmap(pb.Results[i].GetBitmap()), nil
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
func (e *Executor) mapReduce(ctx context.Context, db string, slices []uint64, c *pql.Call, opt *ExecOptions, mapFn mapFunc, reduceFn reduceFunc) (interface{}, error) {
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

func (e *Executor) mapper(ctx context.Context, ch chan mapResponse, nodes []*Node, db string, slices []uint64, c *pql.Call, opt *ExecOptions, mapFn mapFunc, reduceFn reduceFunc) error {
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

				results, err := e.exec(ctx, n, db, &pql.Query{Calls: []*pql.Call{c}}, nodeSlices, opt)
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
	Remote bool
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
func hasOnlySetBitmapAttrs(calls []*pql.Call) bool {
	if len(calls) == 0 {
		return false
	}

	for _, call := range calls {
		if call.Name != "SetBitmapAttrs" {
			return false
		}
	}
	return true
}

func needsSlices(calls []*pql.Call) bool {
	if len(calls) == 0 {
		return false
	}
	for _, call := range calls {
		switch call.Name {
		case "ClearBit", "Profile", "SetBit", "SetBitmapAttrs", "SetProfileAttrs":
			continue
		case "Count", "TopN":
			return true
		// default catches Bitmap calls
		default:
			return true
		}
	}
	return false
}
