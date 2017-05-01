package pilosa_test

import (
	"context"

	"github.com/pilosa/pilosa"
)

// MockPlugin represents a plugin that is implemented as mockable functions.
type MockPlugin struct {
	MapFn    func(ctx context.Context, db string, children []interface{}, args map[string]interface{}, slice uint64) (interface{}, error)
	ReduceFn func(ctx context.Context, prev, v interface{}) interface{}
}

type MockPluginConstructorWrapper struct {
	mock *MockPlugin
}

func (m *MockPluginConstructorWrapper) NewMockPluginConstruct(h *pilosa.Holder) pilosa.Plugin {
	return m.mock
}

func (p *MockPlugin) Map(ctx context.Context, db string, children []interface{}, args map[string]interface{}, slice uint64) (interface{}, error) {
	return p.MapFn(ctx, db, children, args, slice)
}

func (p *MockPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
	return p.ReduceFn(ctx, prev, v)
}
