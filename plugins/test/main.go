package main

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

// Init is the entry point into the shared object to register any plugins.
func Init(r *pilosa.PluginRegistry) error {
	if err := r.Register("Debug", NewDebugPlugin); err != nil {
		return err
	}
	return nil
}

// DebugPlugin represents a plugin that will print args to stderr.
type DebugPlugin struct{}

// NewDebugPlugin returns a new instance of DebugPlugin.
func NewDebugPlugin() pilosa.Plugin {
	return &DebugPlugin{}
}

// Map executes the plugin against a single slice.
func (p *DebugPlugin) Map(ctx context.Context, db string, args []pql.Arg, slice uint64) (interface{}, error) {
	var buf bytes.Buffer
	fmt.Fprint(&buf, "Debug.Map(")
	fmt.Fprintf(&buf, "db=%#v, ", db)

	fmt.Fprintf(&buf, "args=[")
	for i, arg := range args {
		if i != 0 {
			fmt.Fprint(&buf, " ")
		}
		fmt.Fprintf(&buf, "%#v:%#v", arg.Key, arg.Value)
	}
	fmt.Fprintf(&buf, "], ")

	fmt.Fprintf(&buf, "slice=%d", slice)

	fmt.Fprintln(&buf, ")")
	buf.WriteTo(os.Stderr)

	return nil, nil
}

// Reduce combines previous map results into a single value.
func (p *DebugPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Debug.Reduce(prev=%#v, v=%#v)", prev, v)
	buf.WriteTo(os.Stderr)
	return nil
}
