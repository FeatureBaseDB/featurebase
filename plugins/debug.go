package plugins

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/pilosa/pilosa"
)

func init() {
	pilosa.RegisterPlugin("Debug", NewDebugPlugin)
}

// DebugPlugin represents a plugin that will print args to stderr.
type DebugPlugin struct {
	holder *pilosa.Holder
}

// NewDebugPlugin returns a new instance of DebugPlugin.
func NewDebugPlugin(h *pilosa.Holder) pilosa.Plugin {
	return &DebugPlugin{holder: h}
}

// Map executes the plugin against a single slice.
func (p *DebugPlugin) Map(ctx context.Context, db string, children []interface{}, args map[string]interface{}, slice uint64) (interface{}, error) {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "Debug.Map(")
	fmt.Fprintf(&buf, "db=%#v, ", db)
	fmt.Fprintf(&buf, "children=%#v, ", children)
	fmt.Fprintf(&buf, "args=%#v, ", args)
	fmt.Fprintf(&buf, "slice=%d", slice)
	fmt.Fprintln(&buf, ")")
	buf.WriteTo(os.Stderr)

	return nil, nil
}

// Reduce combines previous map results into a single value.
func (p *DebugPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Debug.Reduce(prev=%#v, v=%#v)\n", prev, v)
	buf.WriteTo(os.Stderr)
	return nil
}
