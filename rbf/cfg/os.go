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

//go:build !386
// +build !386

package cfg

// DefaultMaxSize is the default mmap size and therefore the maximum allowed
// size of the database. The size can be increased by updating the DB.MaxSize
// and reopening the database. This setting mainly affects virtual space usage.
const DefaultMaxSize = 4 * (1 << 30)

// DefaultMaxWALSize is the default mmap size and therefore the maximum allowed
// size of the WAL. The size can be increased by updating the DB.MaxWALSize
// and reopening the database. This setting mainly affects virtual space usage.
const DefaultMaxWALSize = 4 * (1 << 30)
