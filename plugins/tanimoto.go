package plugins

import (
	"context"
	"errors"

	"github.com/pilosa/pilosa"
	"fmt"
	"math"
	"container/heap"
)

func init() {
	pilosa.RegisterPlugin("Tanimoto", NewTanimotoPlugin)
}

// Tanimoto represents a plugin that will find the common bits of the top-n list.
type TanimotoPlugin struct {
	holder *pilosa.Holder
}

// NewDiffTopPlugin returns a new instance of DiffTopPlugin.
func NewTanimotoPlugin(h *pilosa.Holder) pilosa.Plugin {
	return &TanimotoPlugin{h}
}

// Map executes the plugin against a single slice.
func (p *TanimotoPlugin) Map(ctx context.Context, index string, children []interface{}, args map[string]interface{}, slice uint64) (interface{}, error) {

	var frame string
	var rowID uint64

	if fr, found := args["frame"]; found {
		frame = fr.(string)
	} else {
		return nil, errors.New("frame required")
	}

	if id, found := args["id"]; found {
		rowID = uint64(id.(int64))
	} else {
		return nil, errors.New("id required")
	}


	frag := p.holder.Fragment(index, frame, pilosa.ViewStandard, slice)
	src := frag.Row(rowID)
	opt := pilosa.TopOptions{TanimotoThreshold: 70, N: 200000, Src: src}


	//toplist, err := f.Top(pilosa.TopOptions{N: n})
	//if err != nil {
	//	return nil, err
	//}

	pairs := frag.Cache().Top()
	fmt.Println(len(pairs))
	 //Use `tanimotoThreshold > 0` to indicate whether or not we are considering Tanimoto.
	var tanimotoThreshold uint64
	var minTanimoto, maxTanimoto float64
	var srcCount uint64
	if opt.TanimotoThreshold > 0 && opt.Src != nil {
		tanimotoThreshold = opt.TanimotoThreshold
		srcCount = opt.Src.Count()
		minTanimoto = float64(srcCount*tanimotoThreshold) / 100
		maxTanimoto = float64(srcCount*100) / float64(tanimotoThreshold)
	}
	fmt.Println(minTanimoto, maxTanimoto)
	var r []pilosa.Pairs
	results := &pilosa.PairHeap{}
	for _, pair := range pairs {
		rowID, cnt := pair.ID, pair.Count
		if tanimotoThreshold > 0 {
			if float64(cnt) <= minTanimoto || float64(cnt) >= maxTanimoto {
				continue
			}
			count := opt.Src.IntersectionCount(frag.Row(rowID))
			if count == 0 {
				continue
			}
			tanimoto := math.Ceil(float64(count*100) / float64(cnt+srcCount-count))
			if tanimoto <= float64(tanimotoThreshold) {
				continue
			}
			heap.Push(results, pilosa.Pair{ID: rowID, Count: cnt})
		}
	}
	fmt.Println(results)

	r := make(pilosa.Pairs, results.Len(), results.Len())
	x := results.Len()
	i := 1
	for results.Len() > 0 {
		r[x-i] = heap.Pop(results).(pilosa.Pair)
		i++
	}
	//
	//var bm *pilosa.Bitmap
	//for _, pair := range toplist {
	//	x := f.Row(pair.ID)
	//	if bm == nil {
	//		bm = x
	//	} else {
	//		bm = bm.Intersect(x)
	//	}
	//
	//}
	return r, nil
}

// Reduce combines previous map results into a single value.
func (p *TanimotoPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {

	fmt.Println(prev)
	switch x := v.(type) {
	case *pilosa.Bitmap:
		if prev != nil {
			bm := prev.(*pilosa.Bitmap)
			return bm.Union(x)
		}
		return x
	case int:
		return x
	}

	return v
}
