package plugins

import (
	"context"

	"github.com/pilosa/pilosa"
)

func init() {
	pilosa.RegisterPlugin("MaxIndex", NewMaxIndexPlugin)
	pilosa.RegisterPlugin("MinIndex", NewMinIndexPlugin)
}

// MaxIndexPlugin represents a plugin that give a set of all the bits in the top-n
type MaxIndexPlugin struct {
	holder *pilosa.Holder
}

// NewMaxIndexPlugin returns a new instance of MaxIndexPlugin.
func NewMaxIndexPlugin(h *pilosa.Holder) pilosa.Plugin {
	return &MaxIndexPlugin{h}
}

type pair struct {
	Id    int
	Val   uint64
	Slice uint64
	first bool
}

func (p *pair) setIfmax(i int, m uint64) {
	if p.first {
		p.Id = i
		p.Val = m
		p.first = false
	} else if m > p.Val {
		p.Id = i
		p.Val = m
	}
}

// Map executes the plugin against a single slice.
func (p *MaxIndexPlugin) Map(ctx context.Context, index string, children []interface{}, args map[string]interface{}, slice uint64) (interface{}, error) {
	max_pair := pair{}
	max_pair.first = true
	max_pair.Slice = slice

	for i, child := range children {
		switch v := child.(type) {
		case *pilosa.Bitmap: //handle bitmaps
			max_pair.setIfmax(i, v.Count())
		case int64:
			max_pair.setIfmax(i, uint64(v))
		case uint64:
			max_pair.setIfmax(i, v)
		}

	}
	return max_pair, nil
}

// Reduce combines previous map results into a single value.
func (p *MaxIndexPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
	if prev == nil {
		return v
	}
	switch x := v.(type) {
	case pair:
		if x.Val < prev.(pair).Val {
			return x
		}
		return prev
	}
	return v
}

type MinIndexPlugin struct {
	holder *pilosa.Holder
}

func NewMinIndexPlugin(h *pilosa.Holder) pilosa.Plugin {
	return &MinIndexPlugin{h}
}

func (p *pair) setIfmin(i int, m uint64) {
	if p.first {
		p.Id = i
		p.Val = m
		p.first = false
	} else if m < p.Val {
		p.Id = i
		p.Val = m
	}
}

// Map executes the plugin against a single slice.
func (p *MinIndexPlugin) Map(ctx context.Context, index string, children []interface{}, args map[string]interface{}, slice uint64) (interface{}, error) {
	min_pair := pair{}
	min_pair.Slice = slice
	min_pair.first = true

	for i, child := range children {
		switch v := child.(type) {
		case *pilosa.Bitmap: //handle bitmaps
			min_pair.setIfmin(i, v.Count())
		case int64:
			min_pair.setIfmin(i, uint64(v))
		case uint64:
			min_pair.setIfmin(i, v)
		}

	}
	return min_pair, nil
}

// Reduce combines previous map results into a single value.
func (p *MinIndexPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
	if prev == nil {
		return v
	}
	switch x := v.(type) {
	case pair:

		if x.Val < prev.(pair).Val {
			return x
		}
		return prev
	}

	return v
}
