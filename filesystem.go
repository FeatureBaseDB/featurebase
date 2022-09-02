// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"fmt"
	"net/http"
)

// Ensure nopFileSystem implements interface.
var _ FileSystem = &nopFileSystem{}

// FileSystem represents an interface for file system for serving the Lattice UI.
type FileSystem interface {
	New() (http.FileSystem, error)
}

func init() {
	NopFileSystem = &nopFileSystem{}
}

// NopFileSystem represents a FileSystem that returns an error if called.
var NopFileSystem FileSystem

type nopFileSystem struct{}

// New is a no-op implementation of FileSystem New method.
func (n *nopFileSystem) New() (http.FileSystem, error) {
	return nil, fmt.Errorf("file system not implemented")
}
