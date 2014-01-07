package query

import (
	"fmt"
	"math/rand"
	"pilosa/db"

	"tux21b.org/v1/gocql/uuid"
)

// A single step in the query plan.
type QueryStep struct {
	id          *uuid.UUID
	operation   string
	inputs      []QueryInput
	location    *db.Location
	destination *db.Location
}

type CatQueryStep struct {
	Id          *uuid.UUID
	Operation   string
	Inputs      []*uuid.UUID
	Location    *db.Location
	Destination *db.Location
}

type GetQueryStep struct {
	Id          *uuid.UUID
	Operation   string
	Bitmap      *db.Bitmap
	Slice       int
	Location    *db.Location
	Destination *db.Location
}

type SetQueryStep struct {
	Id          *uuid.UUID
	Operation   string
	Bitmap      *db.Bitmap
	ProfileId   uint64
	Location    *db.Location
	Destination *db.Location
}

func (q QueryStep) StringHOLD() string {
	return fmt.Sprintf("%s %s %s, LOC: %s, DEST: %s", q.operation, q.id.String(), q.inputs, q.location, q.destination)
}

// This is the output of the query planner. Contains a list of steps which can be performed in parallel
//type QueryPlan []QueryStep
type QueryPlan []interface{}

type QueryPlanner struct {
	Database *db.Database
}

type QueryTree interface {
	getLocation(d *db.Database) *db.Location
}

// QueryTree for UNION, INTER, and CAT queries
type CompositeQueryTree struct {
	operation  string
	subqueries []QueryTree
	location   *db.Location
}

// Randomly select location from subqueries (so subqueries roll up into composite queries while minimizing inter-node data traffic)
func (qt *CompositeQueryTree) getLocation(d *db.Database) *db.Location {
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

// QueryTree for CAT queries
type CatQueryTree struct {
	subqueries []QueryTree
	location   *db.Location
}

// QueryTree for GET queries
type GetQueryTree struct {
	bitmap *db.Bitmap
	slice  int
}

// QueryTree for SET queries
type SetQueryTree struct {
	bitmap     *db.Bitmap
	profile_id uint64
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *CatQueryTree) getLocation(d *db.Database) *db.Location {
	//loc := new(db.Location)
	//return loc
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

// Uses consistent hashing function to select node containing data for GET operation
func (qt *GetQueryTree) getLocation(d *db.Database) *db.Location {
	slice := d.GetOrCreateSlice(qt.slice) // TODO: this should probably be just GetSlice (no create)
	fragment, err := d.GetFragmentForBitmap(slice, qt.bitmap)
	if err != nil {
		panic(err)
	}
	return fragment.GetLocation()
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

// Builds QueryTree object from Query. Pass slice=-1 to perform operation on all slices
func (qp *QueryPlanner) buildTree(query *Query, slice int) QueryTree {
	var tree QueryTree

	// handle SET operation regardless of the slice
	if query.Operation == "set" {
		tree = &SetQueryTree{query.Inputs[0].(*db.Bitmap), query.ProfileId}
		return tree
	}

	// handle the remaining operations, taking slice into consideration
	if slice == -1 {
		tree = &CatQueryTree{}
		numSlices, err := qp.Database.NumSlices()
		if err != nil {
			panic(err)
		}
		for slice := 0; slice < numSlices; slice++ {
			subtree := qp.buildTree(query, slice)
			composite := tree.(*CatQueryTree)
			composite.subqueries = append(composite.subqueries, subtree)
		}
	} else {
		if query.Operation == "get" {
			tree = &GetQueryTree{query.Inputs[0].(*db.Bitmap), slice}
			return tree
		} else {
			subqueries := make([]QueryTree, len(query.Inputs))
			for i, input := range query.Inputs {
				subqueries[i] = qp.buildTree(input.(*Query), slice)
			}
			tree = &CompositeQueryTree{operation: query.Operation, subqueries: subqueries}
		}
	}
	return tree
}

// Produces flattened QueryPlan from QueryTree input
func (qp *QueryPlanner) flatten(qt QueryTree, id *uuid.UUID, location *db.Location) *QueryPlan {
	plan := QueryPlan{}
	if composite, ok := qt.(*CompositeQueryTree); ok {
		inputs := make([]QueryInput, len(composite.subqueries))
		step := QueryStep{id, composite.operation, inputs, composite.getLocation(qp.Database), location}
		for index, subq := range composite.subqueries {
			sub_id := uuid.RandomUUID()
			step.inputs[index] = &sub_id
			subq_steps := qp.flatten(subq, &sub_id, composite.getLocation(qp.Database))
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	} else if cat, ok := qt.(*CatQueryTree); ok {
		inputs := make([]*uuid.UUID, len(cat.subqueries))
		step := CatQueryStep{id, "cat", inputs, cat.getLocation(qp.Database), location}
		for index, subq := range cat.subqueries {
			sub_id := uuid.RandomUUID()
			step.Inputs[index] = &sub_id
			subq_steps := qp.flatten(subq, &sub_id, cat.getLocation(qp.Database))
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	} else if get, ok := qt.(*GetQueryTree); ok {
		step := GetQueryStep{id, "get", get.bitmap, get.slice, get.getLocation(qp.Database), location}
		plan := QueryPlan{step}
		return &plan
	} else if set, ok := qt.(*SetQueryTree); ok {
		step := SetQueryStep{id, "set", set.bitmap, set.profile_id, set.getLocation(qp.Database), location}
		plan := QueryPlan{step}
		return &plan
	}
	return &plan
}

// Transforms Query into QueryTree and flattens to QueryPlan object
func (qp *QueryPlanner) Plan(query *Query, id *uuid.UUID, destination *db.Location) *QueryPlan {
	queryTree := qp.buildTree(query, -1)
	return qp.flatten(queryTree, id, destination)
}
