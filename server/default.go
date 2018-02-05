// +build !release
//
// This file sets defaults to be overridden by release.go

package server

import "time"

// DefaultDiagnosticsInterval is the default sync frequency diagnostic metrics. A value of 0 disables diagnostics.
const DefaultDiagnosticsInterval = time.Duration(0)
