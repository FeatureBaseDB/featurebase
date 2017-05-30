package plugins

import (
	"context"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

func init() {
	pilosa.RegisterPlugin("MaxIndex", NewMaxIndexPlugin)
	pilosa.RegisterPlugin("MinIndex", NewMinIndexPlugin)
}

// MaxIndexPlugin represents a plugin that give a set of all the bits in the top-n
type MaxIndexPlugin struct {
	executor *pilosa.Executor
}

// NewMaxIndexPlugin returns a new instance of MaxIndexPlugin.
func NewMaxIndexPlugin(e *pilosa.Executor) pilosa.Plugin {
	return &MaxIndexPlugin{e}
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
func (p *MaxIndexPlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {
	maxPair := pair{}
	maxPair.first = true
	maxPair.Slice = slice

	for i, rawChild := range call.Children {
		child, err := p.executor.ExecuteCallSlice(ctx, index, rawChild, slice, p)
		if err != nil {
			return nil, err
		}
		switch v := child.(type) {
		case *pilosa.Bitmap: //handle bitmaps
			maxPair.setIfmax(i, v.Count())
		case int64:
			maxPair.setIfmax(i, uint64(v))
		case uint64:
			maxPair.setIfmax(i, v)
		}

	}
	return maxPair, nil
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
	executor *pilosa.Executor
}

func NewMinIndexPlugin(e *pilosa.Executor) pilosa.Plugin {
	return &MinIndexPlugin{e}
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
func (p *MinIndexPlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {
	minPair := pair{}
	minPair.Slice = slice
	minPair.first = true

	for i, rawChild := range call.Children {
		child, err := p.executor.ExecuteCallSlice(ctx, index, rawChild, slice, p)
		if err != nil {
			return nil, err
		}
		switch v := child.(type) {
		case *pilosa.Bitmap: //handle bitmaps
			minPair.setIfmin(i, v.Count())
		case int64:
			minPair.setIfmin(i, uint64(v))
		case uint64:
			minPair.setIfmin(i, v)
		}

	}
	return minPair, nil
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
