package plugins

import (
	"context"

	"github.com/pilosa/pilosa"
)

func init() {
	pilosa.RegisterPlugin("Mask", NewMaskPlugin)
}

// MaskPlugin represents a plugin that will print args to stderr.
type MaskPlugin struct {
}

// NewDebugPlugin returns a new instance of DebugPlugin.
func NewMaskPlugin(h *pilosa.Holder) pilosa.Plugin {
	return &MaskPlugin{}
}

// Map executes the plugin against a single slice.
func (p *MaskPlugin) Map(ctx context.Context, db string, children []interface{}, args map[string]interface{}, slice uint64) (interface{}, error) {
	start := uint64(0)
	stop := uint64(0)
	step := uint64(1)

	if x, found := args["start"]; found {
		start = uint64(x.(int64))
	}
	if x, found := args["stop"]; found {
		stop = uint64(x.(int64))
	}

	if x, found := args["step"]; found {
		step = uint64(x.(int64))
	}

	bm := pilosa.NewBitmap()
	for i := start; i <= stop; i += step {
		if i >= slice*pilosa.SliceWidth && i <= (slice+1)*(pilosa.SliceWidth) {
			bm.SetBit(i)
		}

	}

	return bm, nil
}

// Reduce combines previous map results into a single value.
func (p *MaskPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
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
