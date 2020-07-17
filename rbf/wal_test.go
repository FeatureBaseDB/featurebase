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

package rbf_test

import (
	"bytes"
        "encoding/hex"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/pilosa/pilosa/v2/rbf"
)


func TestWALSegment_Open(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := MustOpenWALSegment(t, 10)
		defer MustCloseWALSegment(t, s)
		if got, want := s.MinWALID(), int64(10); got != want {
			t.Fatalf("Base()=%d, want %d", got, want)
		} else if got, want := s.PageN(), 0; got != want {
			t.Fatalf("PageN()=%d, want %d", got, want)
		}
	})

	// TODO(BBJ): Test open w/ partially written pages.
}

func TestWALSegment_WritePage(t *testing.T) {
	rand := rand.New(rand.NewSource(0))
	s := MustOpenWALSegment(t, 10)
	defer MustCloseWALSegment(t, s)

	pages := [][]byte{
		make([]byte, rbf.PageSize),
		make([]byte, rbf.PageSize),
	}
	rand.Read(pages[0])
	rand.Read(pages[1])

	// Write first page.
	if walID, err := s.WriteWALPage(pages[0], false); err != nil {
		t.Fatal(err)
	} else if got, want := walID, int64(10); got != want {
		t.Fatalf("WALID=%d, want %d", got, want)
	} else if got, want := s.PageN(), 1; got != want {
		t.Fatalf("PageN()=%d, want %d", got, want)
	}

	// Write second page.
	if walID, err := s.WriteWALPage(pages[1], false); err != nil {
		t.Fatal(err)
	} else if got, want := walID, int64(11); got != want {
		t.Fatalf("WALID=%d, want %d", got, want)
	} else if got, want := s.PageN(), 2; got != want {
		t.Fatalf("PageN()=%d, want %d", got, want)
	}

	// Read & verify first page.
	if buf, err := s.ReadWALPage(10); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(pages[0], buf) {
		t.Fatalf("unexpected first page:\n%s", hex.Dump(buf))
	}

	// Read & verify second page.
	if buf, err := s.ReadWALPage(11); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(pages[1], buf) {
		t.Fatal("unexpected second page")
	}
}

func TestFormatWALSegmentPath(t *testing.T) {
	if got, want := rbf.FormatWALSegmentPath(1234), "00000000000004d2.wal"; got != want {
		t.Fatalf("FormatWALSegmentPath()=%q, want %q", got, want)
	}
}

func TestParseWALSegmentPath(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if walID, err := rbf.ParseWALSegmentPath("/tmp/00000000000004d2.wal"); err != nil {
			t.Fatal(err)
		} else if got, want := walID, int64(1234); got != want {
			t.Fatalf("ParseWALSegmentPath()=%q, want %q", got, want)
		}
	})

	t.Run("ErrInvalidWALPath", func(t *testing.T) {
		if _, err := rbf.ParseWALSegmentPath("/tmp/xyz"); err == nil || err.Error() != "invalid WAL path: /tmp/xyz" {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

// MustOpenWALSegment opens a WAL segment in a temporary path. Fails on error.
func MustOpenWALSegment(tb testing.TB, walID int64) *rbf.WALSegment {
	tb.Helper()

	dir, err := ioutil.TempDir("", "")
	if err != nil {
		tb.Fatal(err)
	}
	path := filepath.Join(dir, rbf.FormatWALSegmentPath(walID))
	if err := ioutil.WriteFile(path, nil, 0666); err != nil {
		tb.Fatal(err)
	}

	s := rbf.NewWALSegment(path)
	if err := s.Open(); err != nil {
		tb.Fatal(err)
	}
	return s
}

// MustCloseWALSegment closes s. Fails on error.
func MustCloseWALSegment(tb testing.TB, s *rbf.WALSegment) {
	tb.Helper()
	if err := s.Close(); err != nil {
		tb.Fatal(err)
	} else if err := os.Remove(s.Path()); err != nil {
		tb.Fatal(err)
	}
}
