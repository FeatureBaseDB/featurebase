package bench

import (
	"github.com/pilosa/pilosa/pql"
	"math/rand"
)

func NewQueryGenerator(seed int64) *QueryGenerator {
	return &QueryGenerator{
		IDToFrameFn: func(id uint64) string { return "frame.n" },
		R:           rand.New(rand.NewSource(seed)),
		Frames:      []string{"frame.n"},
	}
}

type QueryGenerator struct {
	IDToFrameFn func(id uint64) string
	R           *rand.Rand
	Frames      []string
}

func (q *QueryGenerator) Random(maxN, depth, maxargs int, idmin, idmax uint64) pql.Call {
	// TODO: handle depth==1 or 0
	val := q.R.Intn(5)
	switch val {
	case 0:
		return q.RandomTopN(maxN, depth, maxargs, idmin, idmax)
	default:
		return q.RandomBitmapCall(depth, maxargs, idmin, idmax)
	}
}

func (q *QueryGenerator) RandomTopN(maxN, depth, maxargs int, idmin, idmax uint64) *pql.TopN {
	frameIdx := q.R.Intn(len(q.Frames))
	return &pql.TopN{
		Frame: q.Frames[frameIdx],
		N:     q.R.Intn(maxN-1) + 1,
		Src:   q.RandomBitmapCall(depth, maxargs, idmin, idmax),
	}
}

func (q *QueryGenerator) RandomBitmapCall(depth, maxargs int, idmin, idmax uint64) pql.BitmapCall {
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
	calls := make([]pql.BitmapCall, numargs)
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

func ClearBit(id uint64, frame string, profileID uint64) *pql.ClearBit {
	return &pql.ClearBit{
		ID:        id,
		Frame:     frame,
		ProfileID: profileID,
	}
}

func Count(bm pql.BitmapCall) *pql.Count {
	return &pql.Count{
		Input: bm,
	}
}

func Profile(id uint64) *pql.Profile {
	return &pql.Profile{
		ID: id,
	}
}

func SetBit(id uint64, frame string, profileID uint64) *pql.SetBit {
	return &pql.SetBit{
		ID:        id,
		Frame:     frame,
		ProfileID: profileID,
	}
}

func SetBitmapAttrs(id uint64, frame string, attrs map[string]interface{}) *pql.SetBitmapAttrs {
	return &pql.SetBitmapAttrs{
		ID:    id,
		Frame: frame,
		Attrs: attrs,
	}
}

func SetProfileAttrs(id uint64, attrs map[string]interface{}) *pql.SetProfileAttrs {
	return &pql.SetProfileAttrs{
		ID:    id,
		Attrs: attrs,
	}
}

func TopN(frame string, n int, src pql.BitmapCall, bmids []uint64, field string, filters []interface{}) *pql.TopN {
	return &pql.TopN{
		Frame:     frame,
		N:         n,
		Src:       src,
		BitmapIDs: bmids,
		Field:     field,
		Filters:   filters,
	}
}

func Difference(bms ...pql.BitmapCall) *pql.Difference {
	// TODO does this need to be limited to two inputs?
	return &pql.Difference{
		Inputs: bms,
	}
}

func Intersect(bms ...pql.BitmapCall) *pql.Intersect {
	return &pql.Intersect{
		Inputs: bms,
	}
}

func Union(bms ...pql.BitmapCall) *pql.Union {
	return &pql.Union{
		Inputs: bms,
	}
}

func Bitmap(id uint64, frame string) *pql.Bitmap {
	return &pql.Bitmap{
		ID:    id,
		Frame: frame,
	}
}
