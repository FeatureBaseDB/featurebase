// +build release
//
// This file sets release-specific variables.

package server

import "time"

// DefaultDiagnosticsInterval is the default sync frequency diagnostic metrics.
const DefaultDiagnosticsInterval = 1 * time.Hour
