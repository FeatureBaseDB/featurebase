package plugins

import (
	"context"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

func init() {
	pilosa.RegisterPlugin("MergeTop", NewMergeTopPlugin)
}

// MergeTopPlugin represents a plugin that give a set of all the bits in the top-n
type MergeTopPlugin struct {
	holder *pilosa.Holder
}

// NewMergeTopPlugin returns a new instance of MergeTopPlugin.
func NewMergeTopPlugin(e *pilosa.Executor) pilosa.Plugin {
	return &MergeTopPlugin{e.Holder}
}

// Map executes the plugin against a single slice.
func (p *MergeTopPlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {
	n := 0
	var frame string

	args := call.Args

	if x, found := args["n"]; found {
		n = int(x.(int64))
	}

	if fr, found := args["frame"]; found {
		frame = fr.(string)
	} else {
		//error if no frame label is given
	}

	view := p.holder.View(index, frame, pilosa.ViewStandard)
	f := view.Fragment(slice)

	toplist, err := f.Top(pilosa.TopOptions{N: n})
	if err != nil {
		return nil, err
	}

	bm := pilosa.NewBitmap()
	for _, pair := range toplist {
		x := f.Row(pair.ID)
		bm = bm.Union(x)

	}
	return bm, nil
}

// Reduce combines previous map results into a single value.
func (p *MergeTopPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
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
