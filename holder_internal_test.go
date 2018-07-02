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

package pilosa

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type tHolder struct {
	*Holder
}

// Close closes the holder and removes all underlying data.
func (h *tHolder) Close() error {
	defer os.RemoveAll(h.Path)
	return h.Holder.Close()
}

// Reopen instantiates and opens a new holder.
// Note that the holder must be Closed first.
func (h *tHolder) Reopen() error {
	path, logger := h.Path, h.Holder.Logger
	h.Holder = NewHolder()
	h.Holder.Path = path
	h.Holder.Logger = logger
	if err := h.Holder.Open(); err != nil {
		return err
	}

	return nil
}

func newHolder() *tHolder {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	h := &tHolder{Holder: NewHolder()}
	h.Path = path
	return h
}

func TestHolder_Optn(t *testing.T) {
	t.Run("ErrViewPermission", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := newHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if field, err := idx.CreateField("bar", FieldOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := field.createViewIfNotExists(ViewStandard); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard"), 0000); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard"), 0777)

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrViewFragmentsMkdir", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := newHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if field, err := idx.CreateField("bar", FieldOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := field.createViewIfNotExists(ViewStandard); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments"), 0000); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments"), 0777)

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrFragmentCachePermission", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := newHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if field, err := idx.CreateField("bar", FieldOptions{}); err != nil {
			t.Fatal(err)
		} else if view, err := field.createViewIfNotExists(ViewStandard); err != nil {
			t.Fatal(err)
		} else if _, err := field.SetBit(0, 0, nil); err != nil {
			t.Fatal(err)
		} else if err := view.Fragment(0).FlushCache(); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments", "0.cache"), 0000); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments", "0.cache"), 0666)

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("unexpected error: %s", err)
		}
	})

}
