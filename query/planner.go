package query

import (
	"encoding/gob"
	"math/rand"
	"pilosa/db"

	"tux21b.org/v1/gocql/uuid"
)

type PortableQueryStep interface {
	GetId() *uuid.UUID
	GetLocation() *db.Location
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// BASE
///////////////////////////////////////////////////////////////////////////////////////////////////

type BaseQueryStep struct {
	Id          *uuid.UUID
	Operation   string
	Location    *db.Location
	Destination *db.Location
}

func (self *BaseQueryStep) GetId() *uuid.UUID {
	return self.Id
}
func (self *BaseQueryStep) GetLocation() *db.Location {
	return self.Location
}

func (self *BaseQueryStep) LocIsDest() bool {
	if self.Location.ProcessId == self.Destination.ProcessId && self.Location.FragmentId == self.Destination.FragmentId {
		return true
	}
	return false
}

type BaseQueryResult struct {
	Id   *uuid.UUID
	Data interface{}
}

func (self *BaseQueryResult) ResultId() *uuid.UUID {
	return self.Id
}

func (self *BaseQueryResult) ResultData() interface{} {
	return self.Data
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// COUNT
///////////////////////////////////////////////////////////////////////////////////////////////////
type CountQueryStep struct {
	*BaseQueryStep
	Input *uuid.UUID
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
func (qt *CountQueryTree) getLocation(d *db.Database) *db.Location {
	return qt.subquery.getLocation(d)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// TOP-N
///////////////////////////////////////////////////////////////////////////////////////////////////
type TopNQueryStep struct {
	*BaseQueryStep
	Input *uuid.UUID
	N     int
}

type TopNQueryResult struct {
	*BaseQueryResult
}

// QueryTree for TOP-N queries
type TopNQueryTree struct {
	subquery QueryTree
	location *db.Location
	N        int
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *TopNQueryTree) getLocation(d *db.Database) *db.Location {
	return qt.subquery.getLocation(d)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// UNION
///////////////////////////////////////////////////////////////////////////////////////////////////
type UnionQueryStep struct {
	*BaseQueryStep
	Inputs []*uuid.UUID
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
func (qt *UnionQueryTree) getLocation(d *db.Database) *db.Location {
	if qt.location == nil {
		subqueryLength := len(qt.subqueries)
		if subqueryLength > 0 {
			locationIndex := rand.Intn(subqueryLength)
			subquery := qt.subqueries[locationIndex]
			qt.location = subquery.getLocation(d)
		}
	}
	return qt.location
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// INTERSECT
///////////////////////////////////////////////////////////////////////////////////////////////////
type IntersectQueryStep struct {
	*BaseQueryStep
	Inputs []*uuid.UUID
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
func (qt *IntersectQueryTree) getLocation(d *db.Database) *db.Location {
	if qt.location == nil {
		subqueryLength := len(qt.subqueries)
		if subqueryLength > 0 {
			locationIndex := rand.Intn(subqueryLength)
			subquery := qt.subqueries[locationIndex]
			qt.location = subquery.getLocation(d)
		}
	}
	return qt.location
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// CAT
///////////////////////////////////////////////////////////////////////////////////////////////////
type CatQueryStep struct {
	*BaseQueryStep
	Inputs []*uuid.UUID
	N      int
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

// Uses consistent hashing function to select node containing data for GET operation
func (qt *CatQueryTree) getLocation(d *db.Database) *db.Location {
	if qt.location == nil {
		subqueryLength := len(qt.subqueries)
		if subqueryLength > 0 {
			locationIndex := rand.Intn(subqueryLength)
			subquery := qt.subqueries[locationIndex]
			qt.location = subquery.getLocation(d)
		}
	}
	return qt.location
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
func (qt *GetQueryTree) getLocation(d *db.Database) *db.Location {
	slice := d.GetOrCreateSlice(qt.slice) // TODO: this should probably be just GetSlice (no create)
	fragment, err := d.GetFragmentForBitmap(slice, qt.bitmap)
	if err != nil {
		panic(err)
	}
	return fragment.GetLocation()
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
func (qt *SetQueryTree) getLocation(d *db.Database) *db.Location {
	slice, err := d.GetSliceForProfile(qt.profile_id)
	fragment, err := d.GetFragmentForBitmap(slice, qt.bitmap)
	if err != nil {
		panic(err)
	}
	return fragment.GetLocation()
}

///////////////////////////////////////////////////////////////////////////////////////////////////

func init() {
	gob.Register(SetQueryResult{})
	gob.Register(GetQueryResult{})
	gob.Register(CatQueryResult{})
	gob.Register(UnionQueryResult{})
	gob.Register(IntersectQueryResult{})
	gob.Register(CountQueryResult{})
	gob.Register(TopNQueryResult{})

	gob.Register(SetQueryStep{})
	gob.Register(GetQueryStep{})
	gob.Register(CatQueryStep{})
	gob.Register(UnionQueryStep{})
	gob.Register(IntersectQueryStep{})
	gob.Register(CountQueryStep{})
	gob.Register(TopNQueryStep{})
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
	getLocation(d *db.Database) *db.Location
}

// Builds QueryTree object from Query. Pass slice=-1 to perform operation on all slices
func (qp *QueryPlanner) buildTree(query *Query, slice int) QueryTree {
	var tree QueryTree

	// handle SET operation regardless of the slice
	if query.Operation == "set" {

		tree = &SetQueryTree{&db.Bitmap{query.Args["id"].(uint64), query.Args["frame"].(string), query.Args["filter"].(int)}, query.Args["profile_id"].(uint64)}
		return tree
	}

	// handle the remaining operations, taking slice into consideration
	if slice == -1 {
		var n int
		n_, ok := query.Args["n"]
		if ok {
			n = n_.(int)
		}
		tree = &CatQueryTree{N: n}
		numSlices, err := qp.Database.NumSlices()
		if err != nil {
			panic(err)
		}
		for slice := 0; slice < numSlices; slice++ {
			//for slice := 0; slice < 3; slice++ {
			subtree := qp.buildTree(query, slice)
			composite := tree.(*CatQueryTree)
			composite.subqueries = append(composite.subqueries, subtree)
		}
	} else {
		if query.Operation == "get" {
			tree = &GetQueryTree{&db.Bitmap{query.Args["id"].(uint64), query.Args["frame"].(string), 0}, slice}
			return tree
		} else if query.Operation == "count" {
			subquery := qp.buildTree(&query.Subqueries[0], slice)
			tree = &CountQueryTree{subquery: subquery}
		} else if query.Operation == "top-n" {
			var n int
			n_, ok := query.Args["n"]
			if ok {
				n = n_.(int)
			}
			subquery := qp.buildTree(&query.Subqueries[0], slice)
			tree = &TopNQueryTree{subquery: subquery, N: n}
		} else if query.Operation == "union" {
			subqueries := make([]QueryTree, len(query.Subqueries))
			for i, query := range query.Subqueries {
				subqueries[i] = qp.buildTree(&query, slice)
			}
			tree = &UnionQueryTree{subqueries: subqueries}
		} else if query.Operation == "intersect" {
			subqueries := make([]QueryTree, len(query.Subqueries))
			for i, query := range query.Subqueries {
				subqueries[i] = qp.buildTree(&query, slice)
			}
			tree = &IntersectQueryTree{subqueries: subqueries}
		} else {
			panic("invalid operation")
		}
	}
	return tree
}

// Produces flattened QueryPlan from QueryTree input
func (qp *QueryPlanner) flatten(qt QueryTree, id *uuid.UUID, location *db.Location) *QueryPlan {
	plan := QueryPlan{}
	if cat, ok := qt.(*CatQueryTree); ok {
		inputs := make([]*uuid.UUID, len(cat.subqueries))
		step := CatQueryStep{&BaseQueryStep{id, "cat", cat.getLocation(qp.Database), location}, inputs, cat.N}
		for index, subq := range cat.subqueries {
			sub_id := uuid.RandomUUID()
			step.Inputs[index] = &sub_id
			subq_steps := qp.flatten(subq, &sub_id, cat.getLocation(qp.Database))
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	} else if union, ok := qt.(*UnionQueryTree); ok {
		inputs := make([]*uuid.UUID, len(union.subqueries))
		step := UnionQueryStep{&BaseQueryStep{id, "union", union.getLocation(qp.Database), location}, inputs}
		for index, subq := range union.subqueries {
			sub_id := uuid.RandomUUID()
			step.Inputs[index] = &sub_id
			subq_steps := qp.flatten(subq, &sub_id, union.getLocation(qp.Database))
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	} else if intersect, ok := qt.(*IntersectQueryTree); ok {
		inputs := make([]*uuid.UUID, len(intersect.subqueries))
		step := IntersectQueryStep{&BaseQueryStep{id, "intersect", intersect.getLocation(qp.Database), location}, inputs}
		for index, subq := range intersect.subqueries {
			sub_id := uuid.RandomUUID()
			step.Inputs[index] = &sub_id
			subq_steps := qp.flatten(subq, &sub_id, intersect.getLocation(qp.Database))
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	} else if get, ok := qt.(*GetQueryTree); ok {
		step := GetQueryStep{&BaseQueryStep{id, "get", get.getLocation(qp.Database), location}, get.bitmap, get.slice}
		plan := QueryPlan{step}
		return &plan
	} else if set, ok := qt.(*SetQueryTree); ok {
		step := SetQueryStep{&BaseQueryStep{id, "set", set.getLocation(qp.Database), location}, set.bitmap, set.profile_id}
		plan := QueryPlan{step}
		return &plan
	} else if cnt, ok := qt.(*CountQueryTree); ok {
		sub_id := uuid.RandomUUID()
		step := &CountQueryStep{&BaseQueryStep{id, "count", cnt.getLocation(qp.Database), location}, &sub_id}
		subq_steps := qp.flatten(cnt.subquery, &sub_id, cnt.getLocation(qp.Database))
		plan = append(plan, *subq_steps...)
		plan = append(plan, step)
	} else if topn, ok := qt.(*TopNQueryTree); ok {
		sub_id := uuid.RandomUUID()
		step := &TopNQueryStep{&BaseQueryStep{id, "top-n", topn.getLocation(qp.Database), location}, &sub_id, topn.N}
		subq_steps := qp.flatten(topn.subquery, &sub_id, topn.getLocation(qp.Database))
		plan = append(plan, *subq_steps...)
		plan = append(plan, step)
	}
	return &plan
}

// Transforms Query into QueryTree and flattens to QueryPlan object
func (qp *QueryPlanner) Plan(query *Query, id *uuid.UUID, destination *db.Location) *QueryPlan {
	queryTree := qp.buildTree(query, -1)
	return qp.flatten(queryTree, query.Id, destination)
}
