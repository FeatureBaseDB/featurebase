package plugins

import (
	"context"
	"errors"

	"github.com/pilosa/pilosa"
)

func init() {
	pilosa.RegisterPlugin("DiffTop", NewMergeTopPlugin)
}

// DiffTopPlugin represents a plugin that will find the common bits of the top-n list.
type DiffTopPlugin struct {
	holder *pilosa.Holder
}

// NewDiffTopPlugin returns a new instance of DiffTopPlugin.
func NewDiffTopPlugin(h *pilosa.Holder) pilosa.Plugin {
	return &DiffTopPlugin{h}
}

// Map executes the plugin against a single slice.
func (p *DiffTopPlugin) Map(ctx context.Context, index string, children []interface{}, args map[string]interface{}, slice uint64) (interface{}, error) {
	n := 0
	var frame string

	if x, found := args["n"]; found {
		n = x.(int)
	} else {
		return nil, errors.New("n required")
	}

	if fr, found := args["frame"]; found {
		frame = fr.(string)
	} else {
		return nil, errors.New("frame required")
	}

	view := p.holder.View(index, frame, pilosa.ViewStandard)
	f := view.Fragment(slice)

	toplist, err := f.Top(pilosa.TopOptions{N: n})
	if err != nil {
		return nil, err
	}

	var bm *pilosa.Bitmap
	for _, pair := range toplist {
		x := f.Row(pair.ID)
		if bm == nil {
			bm = x
		} else {
			bm = bm.Intersect(x)
		}

	}
	return bm, nil
}

// Reduce combines previous map results into a single value.
func (p *DiffTopPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
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
