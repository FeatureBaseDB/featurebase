// Copyright (c) 2018 Pilosa Corp. All rights reserved.
//
// This file is part of Pilosa Enterprise Edition.
//
// Pilosa Enterprise Edition is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Pilosa Enterprise Edition is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with Pilosa Enterprise Edition.  If not, see <http://www.gnu.org/licenses/>.

// Package enterprise injects enterprise implementations of various Pilosa
// features when Pilosa is built with "ENTERPRISE=1 make install". These
// features are dual-licensed separately from Pilosa community edition under the
// AGPL and Pilosa's commercial license.
package enterprise

import (
	"github.com/pilosa/pilosa/enterprise/b"
	"github.com/pilosa/pilosa/roaring"
)

func init() { // nolint: gochecknoinits
	// Replace Bitmap constructor with B+Tree implementation
	roaring.NewFileBitmap = b.NewBTreeBitmap
}
