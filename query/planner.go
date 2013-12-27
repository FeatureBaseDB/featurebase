package query

import (
	"fmt"
	"math/rand"
	"pilosa/db"

	"github.com/nu7hatch/gouuid"
)

// A single step in the query plan.
type QueryStep struct {
	id             uuid.UUID
	operation      string
	inputs         []QueryInput
	location       *db.Process
	return_process *db.Process
}

func (q QueryStep) String() string {
	return fmt.Sprintf("%s %s %s, LOC: %s, DEST: %s", q.operation, q.id.String(), q.inputs, q.location, q.return_process)
}

// This is the output of the query planner. Contains a list of steps which can be performed in parallel
type QueryPlan []QueryStep

type QueryPlanner struct {
	Database *db.Database
}

type QueryTree interface {
	getLocation(d *db.Database) *db.Process
}

// QueryTree for UNION, INTER, and CAT queries
type CompositeQueryTree struct {
	operation  string
	subqueries []QueryTree
	location   *db.Process
}

// Randomly select location from subqueries (so subqueries roll up into composite queries while minimizing inter-node data traffic)
func (qt *CompositeQueryTree) getLocation(d *db.Database) *db.Process {
	if qt.location == nil {
		subqueryLength := len(qt.subqueries)
		if subqueryLength > 1 {
			locationIndex := rand.Intn(subqueryLength)
			subquery := qt.subqueries[locationIndex]
			qt.location = subquery.getLocation(d)
		}
	}
	return qt.location
}

// QueryTree for GET queries
type GetQueryTree struct {
	bitmap *db.Bitmap
	slice  int
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *GetQueryTree) getLocation(d *db.Database) *db.Process {
	slice := d.GetOrCreateSlice(qt.slice) // TODO: this should probably be just GetSlice (no create)
	fragment, err := d.GetFragmentForBitmap(slice, qt.bitmap)
	if err != nil {
		panic(err)
	}
	return fragment.GetProcess()
}

// Builds QueryTree object from Query. Pass slice=-1 to perform operation on all slices
func (qp *QueryPlanner) buildTree(query *Query, slice int) QueryTree {
	var tree QueryTree
	if slice == -1 {
		tree = &CompositeQueryTree{operation: "cat"}
		numSlices, err := qp.Database.NumSlices()
		if err != nil {
			panic(err)
		}
		for slice := 0; slice < numSlices; slice++ {
			subtree := qp.buildTree(query, slice)
			composite := tree.(*CompositeQueryTree)
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
func (qp *QueryPlanner) flatten(qt QueryTree, id *uuid.UUID, location *db.Process) *QueryPlan {
	plan := QueryPlan{}
	if composite, ok := qt.(*CompositeQueryTree); ok {
		inputs := make([]QueryInput, len(composite.subqueries))
		step := QueryStep{*id, composite.operation, inputs, composite.getLocation(qp.Database), location}
		for index, subq := range composite.subqueries {
			sub_id, _ := uuid.NewV4()
			// this is the "wait" step
			step.inputs[index] = sub_id
			subq_steps := qp.flatten(subq, sub_id, composite.getLocation(qp.Database))
			plan = append(plan, *subq_steps...)
		}
		plan = append(plan, step)
	} else if get, ok := qt.(*GetQueryTree); ok {
		step := QueryStep{*id, "get", []QueryInput{get.bitmap, get.slice}, get.getLocation(qp.Database), location}
		plan := QueryPlan{step}
		return &plan
	}
	return &plan
}

// Transforms Query into QueryTree and flattens to QueryPlan object
func (qp *QueryPlanner) Plan(query *Query, id *uuid.UUID, destination *db.Process) *QueryPlan {
	queryTree := qp.buildTree(query, -1)
	return qp.flatten(queryTree, id, destination)
}
