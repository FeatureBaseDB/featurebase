package query

import (
	"encoding/gob"
	"errors"
	"fmt"
	log "github.com/cihub/seelog"
	"math/rand"
	"pilosa/db"
	"pilosa/index"
	"pilosa/util"
	"time"

	"github.com/davecgh/go-spew/spew"
)

type PortableQueryStep interface {
	GetId() *util.GUID
	GetLocation() *db.Location
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// ERRORS
///////////////////////////////////////////////////////////////////////////////////////////////////

// Invalid Frame
type InvalidFrame struct {
	Db    string
	Frame string
	Retry bool
}

func NewInvalidFrame(db string, frame string) *InvalidFrame {
	return &InvalidFrame{db, frame, false}
}

func (self *InvalidFrame) Error() string {
	return fmt.Sprintf("Invalid Frame: %s:%s", self.Db, self.Frame)
}

// Fragment Not Found
type FragmentNotFound struct {
	Db    string
	Frame string
	Slice int
	Retry bool
}

func NewFragmentNotFound(db string, frame string, slice int) *FragmentNotFound {
	return &FragmentNotFound{db, frame, slice, true}
}

func (self *FragmentNotFound) Error() string {
	return fmt.Sprintf("Fragment Not Found: %s:%s:%d", self.Db, self.Frame, self.Slice)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// BASE
///////////////////////////////////////////////////////////////////////////////////////////////////

type BaseQueryStep struct {
	Id          *util.GUID
	Operation   string
	Location    *db.Location
	Destination *db.Location
}

func (self *BaseQueryStep) GetId() *util.GUID {
	log.Trace("BaseQueryStep", *self.Id)
	return self.Id
}
func (self *BaseQueryStep) GetLocation() *db.Location {
	log.Trace("BaseQueryStep.GetLocation", *self.Location)
	return self.Location
}

func (self *BaseQueryStep) LocIsDest() bool {
	log.Trace("BaseQueryStep.LocIsDest")
	if self.Location.ProcessId == self.Destination.ProcessId &&
		self.Location.FragmentId == self.Destination.FragmentId {
		log.Trace("BaseQueryStep.LocIsDest Return true")
		return true
	}
	log.Trace("BaseQueryStep.LocIsDest Return false")
	return false
}

type BaseQueryResult struct {
	Id   *util.GUID
	Data interface{}
}

func (self *BaseQueryResult) ResultId() *util.GUID {
	log.Trace("BaseQueryStep.ResultId", self)

	return self.Id
}

func (self *BaseQueryResult) ResultData() interface{} {
	log.Trace("BaseQueryStep.ResultData", self)
	return self.Data
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// COUNT
///////////////////////////////////////////////////////////////////////////////////////////////////
type CountQueryStep struct {
	*BaseQueryStep
	Input *util.GUID
}

type CountQueryResult struct {
	*BaseQueryResult
}

// QueryTree for COUNT queries
type CountQueryTree struct {
	subquery QueryTree
	location *db.Location
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *CountQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("CountQueryTree.getLocation", d, qt)
	return qt.subquery.getLocation(d)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// TOP-N
///////////////////////////////////////////////////////////////////////////////////////////////////
type TopNQueryStep struct {
	*BaseQueryStep
	Input   *util.GUID
	Filters []uint64
	N       int
	Frame   string
}

type TopNQueryResult struct {
	*BaseQueryResult
}

// QueryTree for TOP-N queries
type TopNQueryTree struct {
	subquery QueryTree
	location *db.Location
	Filters  []uint64
	N        int
	Frame    string
	Slice    int
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *TopNQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("TopNQueryTree.getLocation", d, qt)
	var err error
	if qt.location == nil {
		frame := d.GetOrCreateFrame(qt.Frame)
		slice := d.GetOrCreateSlice(qt.Slice)
		fragment, err := d.GetFragmentForFrameSlice(frame, slice)
		if err != nil {
			log.Warn("GetFragmentForFrameSliceFailed TopNQueryTree", frame, slice)
			return nil, err
		}
		qt.location = fragment.GetLocation()
		return qt.location, nil
	}
	return qt.location, err
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// UNION
///////////////////////////////////////////////////////////////////////////////////////////////////
type UnionQueryStep struct {
	*BaseQueryStep
	Inputs []*util.GUID
}

type UnionQueryResult struct {
	*BaseQueryResult
}

// QueryTree for UNION queries
type UnionQueryTree struct {
	subqueries []QueryTree
	location   *db.Location
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *UnionQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("UnionQueryTree.getLocation", d, qt)
	var err error
	if qt.location == nil {
		subqueryLength := len(qt.subqueries)
		if subqueryLength > 0 {
			locationIndex := rand.Intn(subqueryLength)
			subquery := qt.subqueries[locationIndex]
			qt.location, err = subquery.getLocation(d)
		}
	}
	return qt.location, err
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// INTERSECT
///////////////////////////////////////////////////////////////////////////////////////////////////
type IntersectQueryStep struct {
	*BaseQueryStep
	Inputs []*util.GUID
}

type IntersectQueryResult struct {
	*BaseQueryResult
}

// QueryTree for UNION queries
type IntersectQueryTree struct {
	subqueries []QueryTree
	location   *db.Location
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *IntersectQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("IntersectQueryTree.getLocation", d, qt)
	var err error
	if qt.location == nil {
		subqueryLength := len(qt.subqueries)
		if subqueryLength > 0 {
			locationIndex := rand.Intn(subqueryLength)
			subquery := qt.subqueries[locationIndex]
			qt.location, err = subquery.getLocation(d)
		}
	}
	return qt.location, err
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// DIFFERENCE
///////////////////////////////////////////////////////////////////////////////////////////////////
type DifferenceQueryStep struct {
	*BaseQueryStep
	Inputs []*util.GUID
}

type DifferenceQueryResult struct {
	*BaseQueryResult
}

// QueryTree for UNION queries
type DifferenceQueryTree struct {
	subqueries []QueryTree
	location   *db.Location
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *DifferenceQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("DifferenceQueryTree.getLocation", d, qt)
	var err error
	if qt.location == nil {
		subqueryLength := len(qt.subqueries)
		if subqueryLength > 0 {
			locationIndex := rand.Intn(subqueryLength)
			subquery := qt.subqueries[locationIndex]
			qt.location, err = subquery.getLocation(d)
		}
	}
	return qt.location, err
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// CAT
///////////////////////////////////////////////////////////////////////////////////////////////////
type CatQueryStep struct {
	*BaseQueryStep
	Inputs []*util.GUID
	N      int
}
type Appendable interface {
	Append(subq QueryTree)
}

type CatQueryResult struct {
	*BaseQueryResult
}

// QueryTree for CAT queries
type CatQueryTree struct {
	subqueries []QueryTree
	location   *db.Location
	N          int
}

func (qt *CatQueryTree) Append(subtree QueryTree) {
	log.Trace("CatQueryTree.Append", qt, subtree)
	qt.subqueries = append(qt.subqueries, subtree)
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *CatQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("CatQueryTree.getLocation", d)
	var err error
	if qt.location == nil {
		subqueryLength := len(qt.subqueries)
		if subqueryLength > 0 {
			locationIndex := rand.Intn(subqueryLength)
			subquery := qt.subqueries[locationIndex]
			qt.location, err = subquery.getLocation(d)
		}
	}
	return qt.location, err
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// ALL
///////////////////////////////////////////////////////////////////////////////////////////////////

// QueryTree for GET queries
type AllQueryTree struct {
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *AllQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("AllQueryTree.getLocation", d)
	return nil, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// GET
///////////////////////////////////////////////////////////////////////////////////////////////////
type GetQueryStep struct {
	*BaseQueryStep
	Bitmap *db.Bitmap
	Slice  int
}

type GetQueryResult struct {
	*BaseQueryResult
}

// QueryTree for GET queries
type GetQueryTree struct {
	bitmap *db.Bitmap
	slice  int
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *GetQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("GetQueryTree.getLocation", d)
	slice := d.GetOrCreateSlice(qt.slice) // TODO: this should probably be just GetSlice (no create)
	fragment, err := d.GetFragmentForBitmap(slice, qt.bitmap)
	if err != nil {
		log.Warn("GetFragmenForBitmapFailed GetQueryTree", slice)
		return nil, err
	}
	return fragment.GetLocation(), nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// SET
///////////////////////////////////////////////////////////////////////////////////////////////////
type SetQueryStep struct {
	*BaseQueryStep
	Bitmap    *db.Bitmap
	ProfileId uint64
}

type SetQueryResult struct {
	*BaseQueryResult
}

// QueryTree for SET queries
type SetQueryTree struct {
	bitmap     *db.Bitmap
	profile_id uint64
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *SetQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("SetQueryTree.getLocation", d)
	// check here for supported frames
	if !d.IsValidFrame(qt.bitmap.FrameType) {
		return nil, NewInvalidFrame(d.Name, qt.bitmap.FrameType)
	}
	slice, err := d.GetSliceForProfile(qt.profile_id)
	if err != nil {
		return nil, NewFragmentNotFound(d.Name, qt.bitmap.FrameType, db.GetSlice(qt.profile_id))
	}
	fragment, err := d.GetFragmentForBitmap(slice, qt.bitmap)
	if err != nil {
		log.Warn("NOT FOUND:", slice, qt.bitmap)
		return nil, NewFragmentNotFound(d.Name, qt.bitmap.FrameType, db.GetSlice(qt.profile_id))
	}
	return fragment.GetLocation(), nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////

func init() {
	gob.Register(BaseQueryResult{})
	gob.Register(SetQueryResult{})
	gob.Register(ClearQueryResult{})
	gob.Register(GetQueryResult{})
	gob.Register(RangeQueryResult{})
	gob.Register(CatQueryResult{})
	gob.Register(UnionQueryResult{})
	gob.Register(IntersectQueryResult{})
	gob.Register(DifferenceQueryResult{})
	gob.Register(CountQueryResult{})
	gob.Register(TopNQueryResult{})
	gob.Register(FillResult{})
	gob.Register(StashQueryResult{})
	gob.Register(CacheItem{})
	gob.Register(Stash{})

	gob.Register(SetQueryStep{})
	gob.Register(ClearQueryStep{})
	gob.Register(GetQueryStep{})
	gob.Register(RangeQueryStep{})
	gob.Register(CatQueryStep{})
	gob.Register(UnionQueryStep{})
	gob.Register(IntersectQueryStep{})
	gob.Register(DifferenceQueryStep{})
	gob.Register(CountQueryStep{})
	gob.Register(TopNQueryStep{})
	gob.Register(StashQueryStep{})
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// This is the output of the query planner. Contains a list of steps which can be performed in parallel
//type QueryPlan []QueryStep
type QueryPlan []interface{}

type QueryPlanner struct {
	Database *db.Database
	Query    *Query
}
type QueryTree interface {
	getLocation(d *db.Database) (*db.Location, error)
}

func validateRange(Args map[string]interface{}) error {
	log.Trace("validateRange", Args)
	_, ok := Args["id"]
	if !ok {
		return errors.New("missing bitmap id")
	}
	_, ok = Args["frame"]
	if !ok {
		return errors.New("missing frame ")
	}
	_, ok = Args["start"]
	if !ok {
		return errors.New("missing start time")
	}
	_, ok = Args["end"]
	if !ok {
		return errors.New("missing end time")
	}
	return nil
}

// Builds QueryTree object from Query. Pass slice=-1 to perform operation on all slices
func (self *QueryPlanner) buildTree(query *Query, slice int) (QueryTree, error) {
	log.Trace("QueryPlanner.buildTree", query, slice)
	var tree QueryTree
	spew.Dump(query)
	// handle SET operation regardless of the slice
	if query.Operation == "set" {
		println("SET")
		log.Warn("SET")

		tree = &SetQueryTree{&db.Bitmap{query.Args["id"].(uint64), query.Args["frame"].(string), query.Args["filter"].(uint64)}, query.Args["profile_id"].(uint64)}
		return tree, nil
	}

	if query.Operation == "clear" {

		println("CLEAR")
		log.Warn("CLEAR")
		tree = &ClearQueryTree{&db.Bitmap{query.Args["id"].(uint64), query.Args["frame"].(string), query.Args["filter"].(uint64)}, query.Args["profile_id"].(uint64)}
		return tree, nil
	}

	if query.Operation == "recall" {

		tree = &RecallQueryTree{query.Args["stash"].(Stash)}
		return tree, nil
	}

	// handle the remaining operations, taking slice into consideration
	//I'm kinda thinking for stash it needs a differnt handler..i guess for now I'll see if i can use the cathandler
	if slice == -1 {
		var n int
		n_, ok := query.Args["n"]
		if ok {
			n = n_.(int)
		}
		var p Appendable

		if query.Operation == "stash" {
			//			log.Warn("STASH:", n)
			tree = &StashQueryTree{N: n}
		} else {
			tree = &CatQueryTree{N: n}
		}
		p = tree.(Appendable)
		slice_ids, err := self.Database.SliceIds()
		if err != nil {
			return nil, err
		}
		for i := range slice_ids {
			subtree, err := self.buildTree(query, slice_ids[i])
			if err != nil {
				return nil, err
			}
			p.Append(subtree)
		}
	} else {
		if query.Operation == "get" {
			tree = &GetQueryTree{&db.Bitmap{query.Args["id"].(uint64), query.Args["frame"].(string), 0}, slice}
			return tree, nil
		} else if query.Operation == "range" {
			err := validateRange(query.Args)
			if err != nil {
				return nil, err
			}
			tree = &RangeQueryTree{
				&db.Bitmap{
					query.Args["id"].(uint64),
					query.Args["frame"].(string), 0},
				slice,
				query.Args["start"].(time.Time),
				query.Args["end"].(time.Time)}
			return tree, nil
		} else if query.Operation == "count" {
			subquery, err := self.buildTree(&query.Subqueries[0], slice)
			if err != nil {
				return nil, err
			}
			tree = &CountQueryTree{subquery: subquery}
		} else if query.Operation == "all" {
			tree = &AllQueryTree{}
		} else if query.Operation == "top-n" {
			var frame string
			var n int
			var filters []uint64
			frame_, ok := query.Args["frame"]
			if ok {
				frame = frame_.(string)
			}
			n_, ok := query.Args["n"]
			if ok {
				n = n_.(int)
			}
			filters_, ok := query.Args["ids"]
			if ok {
				filters = filters_.([]uint64)
			}

			subquery, err := self.buildTree(&query.Subqueries[0], slice)
			if err != nil {
				return nil, err
			}
			tree = &TopNQueryTree{subquery: subquery, Filters: filters, N: n, Frame: frame, Slice: slice}
		} else if query.Operation == "union" {
			subqueries := make([]QueryTree, len(query.Subqueries))
			var err error
			for i, query := range query.Subqueries {
				subqueries[i], err = self.buildTree(&query, slice)
				if err != nil {
					return nil, err
				}
			}
			tree = &UnionQueryTree{subqueries: subqueries}
		} else if query.Operation == "intersect" {
			subqueries := make([]QueryTree, len(query.Subqueries))
			var err error
			for i, query := range query.Subqueries {
				subqueries[i], err = self.buildTree(&query, slice)
				if err != nil {
					return nil, err
				}
			}
			tree = &IntersectQueryTree{subqueries: subqueries}
		} else if query.Operation == "difference" {
			subqueries := make([]QueryTree, len(query.Subqueries))
			var err error
			for i, query := range query.Subqueries {
				subqueries[i], err = self.buildTree(&query, slice)
				if err != nil {
					return nil, err
				}
			}
			tree = &DifferenceQueryTree{subqueries: subqueries}
		} else if query.Operation == "stash" {

			subqueries := make([]QueryTree, len(query.Subqueries))
			var err error
			for i, query := range query.Subqueries {
				subqueries[i], err = self.buildTree(&query, slice)
				if err != nil {
					return nil, err
				}
			}
			tree = &StashQueryTree{subqueries: subqueries}
		} else {
			//TODO return error gracefully
			log.Warn(spew.Sdump(query))
			return nil, errors.New("BuildTree Issues")
		}
	}
	return tree, nil
}

// Produces flattened QueryPlan from QueryTree input
func (self *QueryPlanner) flatten(qt QueryTree, id *util.GUID, location *db.Location) (*QueryPlan, error) {
	log.Trace("QueryPlanner.flatten", self, qt, id, location)
	plan := QueryPlan{}
	if cat, ok := qt.(*CatQueryTree); ok {
		inputs := make([]*util.GUID, len(cat.subqueries))
		loc, err := cat.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := CatQueryStep{&BaseQueryStep{id, "cat", loc, location}, inputs, cat.N}
		for index, subq := range cat.subqueries {
			sub_id := util.RandomUUID()
			step.Inputs[index] = &sub_id
			subq_steps, err := self.flatten(subq, &sub_id, loc)
			if err != nil {
				return nil, err
			}
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
		/*} else if recall, ok := qt.(*RecallQueryTree); ok {
		//need to go through the stash and fetch
		//from self.Database i can fetch the locations
		step := RecallQueryStep{&BaseQueryStep{id, "recall", loc, location}, recall.Stash}

		plan := QueryPlan{step}
		return &plan, nil
		*/

	} else if stash, ok := qt.(*StashQueryTree); ok {
		inputs := make([]*util.GUID, len(stash.subqueries))
		loc, err := stash.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := StashQueryStep{&BaseQueryStep{id, "stash", loc, location}, inputs, stash.N}
		for index, subq := range stash.subqueries {
			sub_id := util.RandomUUID()
			step.Inputs[index] = &sub_id
			subq_steps, err := self.flatten(subq, &sub_id, loc)
			if err != nil {
				return nil, err
			}
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	} else if union, ok := qt.(*UnionQueryTree); ok {
		inputs := make([]*util.GUID, len(union.subqueries))
		loc, err := union.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := UnionQueryStep{&BaseQueryStep{id, "union", loc, location}, inputs}
		for index, subq := range union.subqueries {
			sub_id := util.RandomUUID()
			step.Inputs[index] = &sub_id
			subq_steps, err := self.flatten(subq, &sub_id, loc)
			if err != nil {
				return nil, err
			}
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	} else if intersect, ok := qt.(*IntersectQueryTree); ok {
		inputs := make([]*util.GUID, len(intersect.subqueries))
		loc, err := intersect.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := IntersectQueryStep{&BaseQueryStep{id, "intersect", loc, location}, inputs}
		for index, subq := range intersect.subqueries {
			sub_id := util.RandomUUID()
			step.Inputs[index] = &sub_id
			subq_steps, err := self.flatten(subq, &sub_id, loc)
			if err != nil {
				return nil, err
			}
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	} else if difference, ok := qt.(*DifferenceQueryTree); ok {
		inputs := make([]*util.GUID, len(difference.subqueries))
		loc, err := difference.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := DifferenceQueryStep{&BaseQueryStep{id, "difference", loc, location}, inputs}
		for index, subq := range difference.subqueries {
			sub_id := util.RandomUUID()
			step.Inputs[index] = &sub_id
			subq_steps, err := self.flatten(subq, &sub_id, loc)
			if err != nil {
				return nil, err
			}
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	} else if get, ok := qt.(*GetQueryTree); ok {
		loc, err := get.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := GetQueryStep{&BaseQueryStep{id, "get", loc, location}, get.bitmap, get.slice}
		plan := QueryPlan{step}
		return &plan, nil
		/*
			} else if mask, ok := qt.(*MaskQueryTree); ok {
				loc, err := mask.getLocation(self.Database)
				if err != nil {
					return nil, err
				}
				step := MaskQueryStep{&BaseQueryStep{id, "mask", loc, location}, mask.start, mask.end}
				plan := QueryPlan{step}
				return &plan, nil
		*/
	} else if rang, ok := qt.(*RangeQueryTree); ok {
		loc, err := rang.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := RangeQueryStep{&BaseQueryStep{id, "range", loc, location}, rang.bitmap, rang.start, rang.end}
		plan := QueryPlan{step}
		return &plan, nil
	} else if set, ok := qt.(*SetQueryTree); ok {
		loc, err := set.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := SetQueryStep{&BaseQueryStep{id, "set", loc, location}, set.bitmap, set.profile_id}
		plan := QueryPlan{step}
		return &plan, nil
	} else if clear, ok := qt.(*ClearQueryTree); ok {
		loc, err := clear.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := ClearQueryStep{&BaseQueryStep{id, "clear", loc, location}, clear.bitmap, clear.profile_id}
		plan := QueryPlan{step}
		return &plan, nil
	} else if cnt, ok := qt.(*CountQueryTree); ok {
		sub_id := util.RandomUUID()
		loc, err := cnt.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := &CountQueryStep{&BaseQueryStep{id, "count", loc, location}, &sub_id}
		subq_steps, err := self.flatten(cnt.subquery, &sub_id, loc)
		if err != nil {
			return nil, err
		}
		plan = append(plan, *subq_steps...)
		plan = append(plan, step)
	} else if topn, ok := qt.(*TopNQueryTree); ok {
		sub_id := util.RandomUUID()
		loc, err := topn.getLocation(self.Database)
		if err != nil {
			return nil, err
		}
		step := &TopNQueryStep{&BaseQueryStep{id, "top-n", loc, location}, nil, topn.Filters, topn.N, topn.Frame}
		switch topn.subquery.(type) {
		case *AllQueryTree:
			// do nothing
		default:
			step.Input = &sub_id
			subq_steps, err := self.flatten(topn.subquery, &sub_id, loc)
			if err != nil {
				return nil, err
			}
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	}
	return &plan, nil
}

// Transforms Query into QueryTree and flattens to QueryPlan object
func (self *QueryPlanner) Plan(query *Query, id *util.GUID, destination *db.Location) (*QueryPlan, error) {
	log.Trace("QueryPlanner.Plan", self, query, id, destination)
	queryTree, err := self.buildTree(query, -1)
	if err != nil {
		return nil, err
	}
	return self.flatten(queryTree, query.Id, destination)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//MASK
///////////////////////////////////////////////////////////////////////////////////////////////////
type MaskQueryStep struct {
	*BaseQueryStep
	start, end uint64
}

type MaskQueryResult struct {
	*BaseQueryResult
}

// QueryTree for Mask queries
type MaskQueryTree struct {
	start, end uint64
	bitmap     *db.Bitmap
}

// Uses consistent hashing function to select node containing data for GET operation
/*
func (qt *MaskQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	slice := d.GetOrCreateSlice(qt.slice) // TODO: this should probably be just GetSlice (no create)
	fragment, err := d.GetFragmentForBitmap(slice, qt.bitmap)
	if err != nil {
		log.Warn("GetFragmenForBitmapFailed GetQueryTree", slice)
		return nil, err
	}
	return fragment.GetLocation(), nil
}
*/

///////////////////////////////////////////////////////////////////////////////////////////////////
//Range
///////////////////////////////////////////////////////////////////////////////////////////////////
type RangeQueryStep struct {
	*BaseQueryStep
	Bitmap     *db.Bitmap
	Start, End time.Time
}

type RangeQueryResult struct {
	*BaseQueryResult
}

// QueryTree for Mask queries
type RangeQueryTree struct {
	bitmap     *db.Bitmap
	slice      int
	start, end time.Time
}

func (qt *RangeQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("RangeQueryTree", qt, d)
	slice := d.GetOrCreateSlice(qt.slice) // TODO: this should probably be just GetSlice (no create)
	fragment, err := d.GetFragmentForBitmap(slice, qt.bitmap)
	if err != nil {
		log.Warn("GetFragmenForBitmapFailed GetQueryTree", slice)
		return nil, err
	}
	return fragment.GetLocation(), nil
}

type FillResult struct {
	*BaseQueryResult
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//Stash
///////////////////////////////////////////////////////////////////////////////////////////////////

type CacheItem struct {
	FragmentId util.SUUID
	Handle     index.BitmapHandle
}

type Stash struct {
	Stash      []CacheItem //index.BitmapHandle //probably need to make the a struct with fragment_id and handle
	incomplete bool
}

func NewStash() Stash {
	return Stash{make([]CacheItem, 0), false}
}

func (st *Stash) Add(i util.SUUID) {
	item := CacheItem{i, 0}
	st.Stash = append(st.Stash, item)
	st.incomplete = true
}

func (st *Stash) Assign(i index.BitmapHandle) {
	st.Stash[len(st.Stash)-1].Handle = i //big assumption that item already exists
	st.incomplete = false
}

type StashQueryStep struct {
	*BaseQueryStep
	Inputs []*util.GUID
	N      int
}

type StashQueryResult struct {
	*BaseQueryResult
}

// QueryTree for UNION queries
type StashQueryTree struct {
	subqueries []QueryTree
	location   *db.Location
	N          int
}

func (qt *StashQueryTree) Append(subtree QueryTree) {
	qt.subqueries = append(qt.subqueries, subtree)
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *StashQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	var err error
	if qt.location == nil {
		subqueryLength := len(qt.subqueries)
		if subqueryLength > 0 {
			locationIndex := rand.Intn(subqueryLength)
			subquery := qt.subqueries[locationIndex]
			qt.location, err = subquery.getLocation(d)
		}
	}
	return qt.location, err
}

type RecallQueryStep struct {
	*BaseQueryStep
	Stash Stash
}
type RecallQueryTree struct {
	Stash Stash
}

func (rqt *RecallQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	return nil, nil
}

type RecallQueryResult struct {
	*BaseQueryResult
}

type ClearQueryStep struct {
	*BaseQueryStep
	Bitmap    *db.Bitmap
	ProfileId uint64
}

type ClearQueryResult struct {
	*BaseQueryResult
}

type ClearQueryTree struct {
	bitmap     *db.Bitmap
	profile_id uint64
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *ClearQueryTree) getLocation(d *db.Database) (*db.Location, error) {
	log.Trace("ClearQueryTree.getLocation", d)
	// check here for supported frames
	if !d.IsValidFrame(qt.bitmap.FrameType) {
		return nil, NewInvalidFrame(d.Name, qt.bitmap.FrameType)
	}
	slice, err := d.GetSliceForProfile(qt.profile_id)
	if err != nil {
		return nil, NewFragmentNotFound(d.Name, qt.bitmap.FrameType, db.GetSlice(qt.profile_id))
	}
	fragment, err := d.GetFragmentForBitmap(slice, qt.bitmap)
	if err != nil {
		log.Warn("NOT FOUND:", slice, qt.bitmap)
		return nil, NewFragmentNotFound(d.Name, qt.bitmap.FrameType, db.GetSlice(qt.profile_id))
	}
	return fragment.GetLocation(), nil
}
