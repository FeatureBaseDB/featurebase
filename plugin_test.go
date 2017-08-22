package pilosa_test

import (
	"context"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

// MockPlugin represents a plugin that is implemented as mockable functions.
type MockPlugin struct {
	MapFn    func(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error)
	ReduceFn func(ctx context.Context, prev, v interface{}) interface{}
}

type MockPluginConstructorWrapper struct {
	mock *MockPlugin
}

func (m *MockPluginConstructorWrapper) NewMockPluginConstruct(e *pilosa.Executor) pilosa.Plugin {
	return m.mock
}

func (p *MockPlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {
	return p.MapFn(ctx, index, call, slice)
}

func (p *MockPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
	return p.ReduceFn(ctx, prev, v)
}
