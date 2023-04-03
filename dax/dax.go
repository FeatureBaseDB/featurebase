// Package dax defines DAX domain level types.
package dax

// ServicePrefixes are used as the service prefix value in http handlers.
const (
	ServicePrefixComputer    = "computer"
	ServicePrefixController  = "controller"
	ServicePrefixQueryer     = "queryer"
	ServicePrefixWSP         = "worker_service_provider"
	ServicePrefixSnapshotter = "snapshotter" // TODO remove?
	ServicePrefixWritelogger = "writelogger" // TODO remove?
)
