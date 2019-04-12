// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// Package enterprise is now deprecated, and the functionality under
// enterprise/b has been copied into roaring/. It existed to inject enterprise
// implementations of various Pilosa features when Pilosa was built with
// "ENTERPRISE=1 make install". These features were dual-licensed separately
// from Pilosa community edition under the AGPL and Pilosa's commercial license.
package enterprise

import (
	"github.com/pilosa/pilosa/enterprise/b"
	"github.com/pilosa/pilosa/roaring"
)

func init() { // nolint: gochecknoinits
	// Replace Bitmap constructor with B+Tree implementation
	roaring.NewFileBitmap = b.NewBTreeBitmap
}
