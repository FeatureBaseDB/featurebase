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
	"os"
	"path/filepath"
	"syscall"

	"github.com/pilosa/pilosa/v2/syswrap"
)

// WALSegment represents a single file in the WAL.
type WALSegment struct {
	minWALID int64    // base WALID; calculated from path
	path     string   // path to file
	w        *os.File // write handle
	data     []byte   // read-only mmap data
	pageN    int      // number of written pages
}

// NewWALSegment returns a new instance of WALSegment for a given path.
func NewWALSegment(path string) *WALSegment {
	return &WALSegment{
		path: path,
	}
}

// Path returns the path the segment was initialized with.
func (s *WALSegment) Path() string { return s.path }

// MinWALID returns the initial WAL ID of the segment. Only available after Open().
func (s *WALSegment) MinWALID() int64 { return s.minWALID }

// MaxWALID returns the maximum WAL ID of the segment. Only available after Open().
func (s *WALSegment) MaxWALID() int64 {
	return s.minWALID + int64(s.pageN) - 1
}

// PageN returns the number of pages in the segment.
func (s *WALSegment) PageN() int { return s.pageN }

// Size returns the current size of the segment, in bytes.
func (s *WALSegment) Size() int64 { return int64(s.pageN) * PageSize }

func (s *WALSegment) Open() (err error) {
	// Extract base WAL ID and validate path.
	if s.minWALID, err = ParseWALSegmentPath(s.path); err != nil {
		return err
	}

	// Determine file size & create if necessary.
	var sz int64
	if fi, err := os.Stat(s.path); os.IsNotExist(err) {
		if f, err := os.OpenFile(s.path, os.O_RDWR|os.O_CREATE, 0666); err != nil {
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
	s.pageN = int(sz / PageSize)
	if sz%PageSize != 0 {
		sz = int64(s.pageN * PageSize)
		if err := os.Truncate(s.path, sz); err != nil {
			return fmt.Errorf("truncate wal segment file: %w", err)
		}
	}

	// Default the mmap size to the max size plus a page of padding for bitmap pages.
	// If the actual size is larger, then increase to that size.
	mmapSize := int64(MaxWALSegmentFileSize + PageSize)
	if sz > mmapSize {
		mmapSize = sz
	}

	// Open file as a read-only memory map.
	if f, err := os.OpenFile(s.path, os.O_RDONLY, 0666); err != nil {
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
	if err := s.CloseForWrite(); err != nil {
		return err
	}
	if s.data != nil {
		if err := syswrap.Munmap(s.data); err != nil {
			return err
		}
		s.data = nil
	}
	return nil
}

// CloseForWrite closes the write handle, if initialized.
func (s *WALSegment) CloseForWrite() error {
	if s.w != nil {
		if err := s.w.Close(); err != nil {
			return err
		}
		s.w = nil
	}
	return nil
}

// ReadWALPage reads a single page at the given WAL ID.
func (s *WALSegment) ReadWALPage(walID int64) ([]byte, error) {
	// Ensure requested ID is contained in this file.
	if walID < s.minWALID || walID > s.minWALID+int64(s.pageN) {
		return nil, fmt.Errorf("wal segment page read out of range: id=%d base=%d pageN=%d", walID, s.minWALID, s.pageN)
	}

	offset := (walID - s.minWALID) * PageSize
	return s.data[offset : offset+PageSize], nil
}

// WriteWALPage writes a single page to the WAL segment and returns its WAL identifier.
func (s *WALSegment) WriteWALPage(page []byte, isMeta bool) (walID int64, err error) {
	assert(len(page) == PageSize)

	// Initialize write file handle if not yet initialized.
	if s.w == nil {
		if s.w, err = os.OpenFile(s.path, os.O_WRONLY, 0666); err != nil {
			return 0, fmt.Errorf("open wal segment write handle: %w", err)
		}
	}

	// Determine current WAL position.
	walID = s.minWALID + int64(s.pageN)

	// Write WAL ID if this is a meta page.
	if isMeta {
		writeMetaWALID(page, walID)
		// TODO: Write meta page checksum
	}

	// Write page at position & increment page count.
	if _, err := s.w.WriteAt(page, int64(s.pageN*PageSize)); err != nil {
		return 0, fmt.Errorf("wal segment write: %w", err)
	}
	s.pageN++

	return walID, nil
}

// Sync flushes all changes to disk.
func (s *WALSegment) Sync() error {
	if s.w == nil {
		return nil
	}
	return s.w.Sync()
}

// trimBitmapHeaderTrailer removes the last page if the last page is a bitmap header.
// This should only be called on the last segment during recovery. A bitmap
// header write is a 2-page write so a partial write would corrupt the WAL.
func (s *WALSegment) trimBitmapHeaderTrailer() error {
	// Skip if there are no pages in this segment.
	if s.PageN() == 0 {
		return nil
	}

	// Skip if this is not a bitmap header page.
	if page, err := s.ReadWALPage(s.MaxWALID()); err != nil {
		return err
	} else if !IsBitmapHeader(page) {
		return nil
	}

	// Truncate last page and reduce page count.
	if err := os.Truncate(s.Path(), s.Size()-PageSize); err != nil {
		return err
	}
	s.pageN--

	return nil
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
