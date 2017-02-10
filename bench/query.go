package bench

import (
	"math/rand"

	"github.com/pilosa/pilosa/pql"
)

// NewQueryGenerator initializes a new QueryGenerator
func NewQueryGenerator(seed int64) *QueryGenerator {
	return &QueryGenerator{
		IDToFrameFn: func(id uint64) string { return "frame.n" },
		R:           rand.New(rand.NewSource(seed)),
		Frames:      []string{"frame.n"},
	}
}

// QueryGenerator holds the configuration and state for randomly generating
// queries.
type QueryGenerator struct {
	IDToFrameFn func(id uint64) string
	R           *rand.Rand
	Frames      []string
}

// Random returns a randomly generated query.
func (q *QueryGenerator) Random(maxN, depth, maxargs int, idmin, idmax uint64) *pql.Call {
	// TODO: handle depth==1 or 0
	val := q.R.Intn(5)
	switch val {
	case 0:
		return q.RandomTopN(maxN, depth, maxargs, idmin, idmax)
	default:
		return q.RandomBitmapCall(depth, maxargs, idmin, idmax)
	}
}

// RandomTopN returns a randomly generated TopN query.
func (q *QueryGenerator) RandomTopN(maxN, depth, maxargs int, idmin, idmax uint64) *pql.Call {
	frameIdx := q.R.Intn(len(q.Frames))
	return &pql.Call{
		Args: map[string]interface{}{
			"frame": q.Frames[frameIdx],
			"n":     uint64(q.R.Intn(maxN-1) + 1),
		},
		Children: []*pql.Call{q.RandomBitmapCall(depth, maxargs, idmin, idmax)},
	}
}

// RandomBitmapCall returns a randomly generate query which returns a bitmap.
func (q *QueryGenerator) RandomBitmapCall(depth, maxargs int, idmin, idmax uint64) *pql.Call {
	if depth <= 1 {
		bitmapID := q.R.Int63n(int64(idmax)-int64(idmin)) + int64(idmin)
		return Bitmap(uint64(bitmapID), q.IDToFrameFn(uint64(bitmapID)))
	}
	call := q.R.Intn(4)
	if call == 0 {
		return q.RandomBitmapCall(1, 0, idmin, idmax)
	}

	var numargs int
	if maxargs <= 2 {
		numargs = 2
	} else {
		numargs = q.R.Intn(maxargs-2) + 2
	}
	calls := make([]*pql.Call, numargs)
	for i := 0; i < numargs; i++ {
		calls[i] = q.RandomBitmapCall(depth-1, maxargs, idmin, idmax)
	}

	switch call {
	case 1:
		return Difference(calls...)
	case 2:
		return Intersect(calls...)
	case 3:
		return Union(calls...)
	}
	return nil
}

///////////////////////////////////////////////////
// Helpers TODO: move elsewhere
///////////////////////////////////////////////////

func ClearBit(id uint64, frame string, profileID uint64) *pql.Call {
	return &pql.Call{
		Name: "ClearBit",
		Args: map[string]interface{}{
			"id":        id,
			"frame":     frame,
			"profileID": profileID,
		},
	}
}

func Count(child *pql.Call) *pql.Call {
	return &pql.Call{
		Name:     "Count",
		Children: []*pql.Call{child},
	}
}

func Profile(id uint64) *pql.Call {
	return &pql.Call{
		Name: "Profile",
		Args: map[string]interface{}{"id": id},
	}
}

func SetBit(id uint64, frame string, profileID uint64) *pql.Call {
	return &pql.Call{
		Name: "SetBit",
		Args: map[string]interface{}{
			"id":        id,
			"frame":     frame,
			"profileID": profileID,
		},
	}
}

func SetBitmapAttrs(id uint64, frame string, attrs map[string]interface{}) *pql.Call {
	args := copyArgs(attrs)
	args["id"] = id
	args["profileID"] = frame

	return &pql.Call{
		Name: "SetBitmapAttrs",
		Args: args,
	}
}

func SetProfileAttrs(id uint64, attrs map[string]interface{}) *pql.Call {
	args := copyArgs(attrs)
	args["id"] = id

	return &pql.Call{
		Name: "SetProfileAttrs",
		Args: args,
	}
}

func TopN(frame string, n int, src *pql.Call, bmids []uint64, field string, filters []interface{}) *pql.Call {
	return &pql.Call{
		Name:     "TopN",
		Children: []*pql.Call{src},
		Args: map[string]interface{}{
			"frame":   frame,
			"n":       n,
			"ids":     bmids,
			"field":   field,
			"filters": filters,
		},
	}
}

func Difference(bms ...*pql.Call) *pql.Call {
	// TODO does this need to be limited to two inputs?
	return &pql.Call{
		Name:     "Difference",
		Children: bms,
	}
}

func Intersect(bms ...*pql.Call) *pql.Call {
	return &pql.Call{
		Name:     "Intersect",
		Children: bms,
	}
}

func Union(bms ...*pql.Call) *pql.Call {
	return &pql.Call{
		Name:     "Union",
		Children: bms,
	}
}

func Bitmap(id uint64, frame string) *pql.Call {
	return &pql.Call{
		Name: "Bitmap",
		Args: map[string]interface{}{
			"id":    id,
			"frame": frame,
		},
	}
}

// copyArgs returns a shallow copy of m.
func copyArgs(m map[string]interface{}) map[string]interface{} {
	other := make(map[string]interface{}, len(m))
	for k, v := range m {
		other[k] = v
	}
	return other
}
