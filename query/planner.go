package query

import (
	"github.com/nu7hatch/gouuid"
	"strconv"
	"fmt"
	"math/rand"
	"pilosa/db"
)

// A single step in the query plan. 
type QueryStep struct {
	id uuid.UUID
	operation string
	inputs []QueryInput
	location string
	destination string
}

func (q QueryStep) String() string {
	return fmt.Sprintf("%s %s %s, LOC: %s, DEST: %s", q.operation, q.id.String(), q.inputs, q.location, q.destination)
}

type QueryInput interface {}

// Represents a parsed query. Inputs can be Query or Bitmap objects
type Query struct{
	Operation string
	Inputs []QueryInput // Maybe Bitmap and Query objects should have different fields to avoid using interface{}
}

// This is the output of the query planner. Contains a list of steps which can be performed in parallel
type QueryPlan []QueryStep

type QueryPlanner struct {
	Cluster *db.Cluster
	Database *db.Database
}

type QueryTree interface {
	getLocation(d *db.Database) string
}

// QueryTree for UNION, INTER, and CAT queries
type CompositeQueryTree struct {
	operation string
	subqueries []QueryTree
	location string
}

// Randomly select location from subqueries (so subqueries roll up into composite queries while minimizing inter-node data traffic)
func (qt *CompositeQueryTree) getLocation(d *db.Database) string {
	if qt.location == "" {
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
	bitmap db.Bitmap
	slice int
}

// Uses consistent hashing function to select node containing data for GET operation
func (qt *GetQueryTree) getLocation(d *db.Database) string {
	frame, err := d.GetFrame(qt.bitmap.FrameType)
	if err != nil {
		panic(err)
	}
	slice := frame.Slices[qt.slice]
	hashString, err := slice.Hashring.Get(strconv.Itoa(qt.bitmap.Id))

	var sliceIndex int
	var fragIndex int
	fmt.Sscan(hashString, &fragIndex, &sliceIndex)
	fragment := slice.Fragments[fragIndex]

	return fmt.Sprintf(fragment.Node)
}

// Builds QueryTree object from Query. Pass slice=-1 to perform operation on all slices
func (qp *QueryPlanner) buildTree(query *Query, slice int) QueryTree {
	var tree QueryTree
	if slice == -1 {
		tree = &CompositeQueryTree{operation:"cat"}
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
			tree = &GetQueryTree{query.Inputs[0].(db.Bitmap), slice}
			return tree
		} else {
			subqueries := make([]QueryTree, len(query.Inputs))
			for i, input := range query.Inputs {
				subqueries[i] = qp.buildTree(input.(*Query), slice)
			}
			tree = &CompositeQueryTree{operation:query.Operation, subqueries:subqueries}
		}
	}
	return tree
}

// Produces flattened QueryPlan from QueryTree input
func (qp *QueryPlanner) flatten(qt QueryTree, id *uuid.UUID, location string) *QueryPlan {
	plan := QueryPlan{}
	if composite, ok := qt.(*CompositeQueryTree); ok {
		inputs := make([]QueryInput, len(composite.subqueries))
		step := QueryStep{*id, composite.operation, inputs, composite.getLocation(qp.Database), location}
		for index, subq := range composite.subqueries {
			sub_id, _ := uuid.NewV4()
			step.inputs[index] = []QueryInput{"wait", sub_id}
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
func (qp *QueryPlanner) Plan(query *Query, id *uuid.UUID, destination string, slice int) *QueryPlan {
	queryTree := qp.buildTree(query, -1)
	return qp.flatten(queryTree, id, destination)
}
