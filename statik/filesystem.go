// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
//
//go:generate statik -src=../lattice/build -dest=../
//
// Package statik contains static assets for the Lattice UI. `go generate` or
// `make generate-statik` will produce statik.go, which is ignored by git.
package statik

import (
	"net/http"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/rakyll/statik/fs"
)

// Ensure nopFileSystem implements interface.
var _ pilosa.FileSystem = &FileSystem{}

// FileSystem represents a static FileSystem.
type FileSystem struct{}

// New is a statik implementation of FileSystem New method.
func (s *FileSystem) New() (http.FileSystem, error) {
	return fs.New()
}
