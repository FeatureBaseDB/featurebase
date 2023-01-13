// Package dax defines DAX domain level types.
package dax

// ServicePrefixes are used as the service prefix value in http handlers.
const (
	ServicePrefixComputer    = "computer"
	ServicePrefixMDS         = "mds"
	ServicePrefixQueryer     = "queryer"
	ServicePrefixSnapshotter = "snapshotter"
	ServicePrefixWritelogger = "writelogger"
)
