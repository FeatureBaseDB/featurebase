// Package dax defines DAX domain level types.
package dax

// ServicePrefixes are used as the service prefix value in http handlers.
const (
	ServicePrefixComputer    = "computer"
	ServicePrefixController  = "controller"
	ServicePrefixQueryer     = "queryer"
	ServicePrefixSnapshotter = "snapshotter"
	ServicePrefixWritelogger = "writelogger"
)
