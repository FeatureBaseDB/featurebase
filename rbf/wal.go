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

package rbf

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/pilosa/pilosa/v2/syswrap"
)

// WALSegment represents a single file in the WAL.
type WALSegment struct {
	db       *DB
	Path     string // path to file
	MinWALID int64  // base WALID; calculated from path
	PageN    int    // number of written pages

	data []byte // read-only mmap data
}

// NewWALSegment returns a new instance of WALSegment for a given path.
func (db *DB) NewWALSegment(path string) WALSegment {
	return WALSegment{
		db:   db,
		Path: path,
	}
}

// MaxWALID returns the maximum WAL ID of the segment. Only available after Open().
func (s WALSegment) MaxWALID() int64 {
	return s.MinWALID + int64(s.PageN) - 1
}

// Size returns the current size of the segment, in bytes.
func (s WALSegment) Size() int64 {
	return int64(s.PageN) * PageSize
}

func (s *WALSegment) Open() (err error) {
	// Extract base WAL ID and validate path.
	if s.MinWALID, err = ParseWALSegmentPath(s.Path); err != nil {
		return err
	}

	// Determine file size & create if necessary.
	var sz int64
	if fi, err := os.Stat(s.Path); os.IsNotExist(err) {
		if f, err := os.OpenFile(s.Path, os.O_RDWR|os.O_CREATE, 0666); err != nil {
			return fmt.Errorf("touch wal segment file: %w", err)
		} else if err := f.Close(); err != nil {
			return fmt.Errorf("close touched wal segment file: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("stat wal segment file: %w", err)
	} else {
		sz = fi.Size()
	}

	// Determine page count & truncate if a partial page is written.
	s.PageN = int(sz / PageSize)
	if sz%PageSize != 0 {
		sz = int64(s.PageN * PageSize)
		if err := s.db.truncate(s.Path, sz); err != nil {
			return fmt.Errorf("truncate wal file: %w", err)
		}
	}

	// Default the mmap size to the max size plus a page of padding for bitmap pages.
	// If the actual size is larger, then increase to that size.
	mmapSize := int64(MaxWALSegmentFileSize + PageSize)
	if sz > mmapSize {
		mmapSize = sz
	}

	// Open file as a read-only memory map.
	if f, err := os.OpenFile(s.Path, os.O_RDONLY, 0666); err != nil {
		return fmt.Errorf("open wal segment file: %w", err)
	} else if s.data, err = syswrap.Mmap(int(f.Fd()), 0, int(mmapSize), syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
		f.Close()
		return fmt.Errorf("mmap wal segment: %w", err)
	} else if err := f.Close(); err != nil {
		return fmt.Errorf("close wal segment mmap file: %w", err)
	}

	return nil
}

// Close closes the write handle and the read-only mmap.
func (s *WALSegment) Close() error {
	if s.data != nil {
		if err := syswrap.Munmap(s.data); err != nil {
			return err
		}
		s.data = nil
	}
	return nil
}

// ReadWALPage reads a single page at the given WAL ID.
func (s *WALSegment) ReadWALPage(walID int64) ([]byte, error) {
	// Ensure requested ID is contained in this file.
	if walID < s.MinWALID || walID > s.MinWALID+int64(s.PageN) {
		return nil, fmt.Errorf("wal segment page read out of range: id=%d base=%d pageN=%d", walID, s.MinWALID, s.PageN)
	}

	offset := (walID - s.MinWALID) * PageSize
	return s.data[offset : offset+PageSize], nil
}

func walSegmentByPath(segments []WALSegment, path string) *WALSegment {
	for i := range segments {
		if segments[i].Path == path {
			return &segments[i]
		}
	}
	return nil
}

func activeWALSegment(segments []WALSegment) WALSegment {
	if len(segments) == 0 {
		return WALSegment{}
	}
	return segments[len(segments)-1]
}

func minWALID(segments []WALSegment) int64 {
	if len(segments) == 0 {
		return 0
	}
	return segments[0].MinWALID
}

func maxWALID(segments []WALSegment) int64 {
	if len(segments) == 0 {
		return 0
	}
	s := segments[len(segments)-1]
	return s.MaxWALID()
}

func walSize(segments []WALSegment) int64 {
	var sz int64
	for _, s := range segments {
		sz += s.Size()
	}
	return sz
}

// readWALPage reads a single page at the given WAL ID.
func readWALPage(segments []WALSegment, walID int64) ([]byte, error) {
	// TODO(BBJ): Binary search for segment.
	for _, s := range segments {
		if walID >= s.MinWALID && walID <= s.MaxWALID() {
			return s.ReadWALPage(walID)
		}
	}
	return nil, fmt.Errorf("cannot find segment containing WAL page: %d", walID)
}

func findNextWALMetaPage(segments []WALSegment, walID int64) (metaWALID int64, err error) {
	maxWALID := maxWALID(segments)

	for ; walID <= maxWALID; walID++ {
		// Read page data from WAL and return if it is a meta page.
		page, err := readWALPage(segments, walID)
		if err != nil {
			return walID, err
		} else if IsMetaPage(page) {
			return walID, nil
		}

		// Skip over next page if this is a bitmap header.
		if IsBitmapHeader(page) {
			walID++
		}
	}

	return -1, io.EOF
}

func findLastWALMetaPage(segments []WALSegment) (walID int64, err error) {
	if len(segments) == 0 {
		return 0, nil
	}

	var maxMetaWALID int64
	maxWALID := maxWALID(segments)
	for walID := minWALID(segments); walID <= maxWALID; walID++ {
		if page, err := readWALPage(segments, walID); err != nil {
			return walID, err
		} else if IsBitmapHeader(page) {
			walID++ // skip next page for bitmap headers
		} else if IsMetaPage(page) {
			maxMetaWALID = walID // save max meta WAL ID
		}
	}
	return maxMetaWALID, nil
}

// truncateWALAfter removes all pages in the WAL after walID.
func (db *DB) truncateWALAfter(segments []WALSegment, walID int64) ([]WALSegment, error) {
	var newSegments []WALSegment

	for i := range segments {
		segment := &segments[i]

		// Append entire segment if WAL range entirely before target WAL ID.
		if walID > segment.MaxWALID() {
			newSegments = append(newSegments, *segment)
			continue
		}

		// If we only remove some of the WAL pages then truncate and append.
		if segment.MinWALID < walID {
			newSegment := *segment
			newSegment.PageN = int((walID - newSegment.MinWALID) + 1)

			if err := db.truncate(newSegment.Path, int64(newSegment.PageN)*PageSize); err != nil {
				return segments, err
			}
			newSegments = append(newSegments, newSegment)
			continue
		}

		// Drop entire segment if all pages are after WAL ID.
		if err := segment.Close(); err != nil {
			return segments, err
		} else if err := os.Remove(segment.Path); err != nil {
			return segments, err
		}
	}

	return newSegments, nil
}

func DumpWALSegments(segments []WALSegment) {
	fmt.Printf("WAL (%d segments)\n", len(segments))
	for i, s := range segments {
		fmt.Printf("[%d] WALIDs=(%d-%d) PageN=%d\n", i, s.MinWALID, s.MaxWALID(), s.PageN)
	}
}

// FormatWALSegmentPath returns a path for a WAL segment using a WAL ID.
func FormatWALSegmentPath(walID int64) string {
	return fmt.Sprintf("%016x.wal", walID)
}

// ParseWALSegmentPath returns the WAL ID for a given WAL segment path.
func ParseWALSegmentPath(s string) (walID int64, err error) {
	if _, err = fmt.Sscanf(filepath.Base(s), "%016x.wal", &walID); err != nil {
		return 0, fmt.Errorf("invalid WAL path: %s", s)
	}
	return walID, nil
}

// uint32Hasher implements Hasher for uint32 keys.
type uint32Hasher struct{}

// Hash returns a hash for key.
func (h *uint32Hasher) Hash(key interface{}) uint32 {
	return hashUint64(uint64(key.(uint32)))
}

// Equal returns true if a is equal to b. Otherwise returns false.
// Panics if a and b are not ints.
func (h *uint32Hasher) Equal(a, b interface{}) bool {
	return a.(uint32) == b.(uint32)
}

// hashUint64 returns a 32-bit hash for a 64-bit value.
func hashUint64(value uint64) uint32 {
	hash := value
	for value > 0xffffffff {
		value /= 0xffffffff
		hash ^= value
	}
	return uint32(hash)
}
