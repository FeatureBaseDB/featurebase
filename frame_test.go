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

package pilosa_test

import (
	"io/ioutil"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

// Ensure frame can open and retrieve a view.
func TestFrame_CreateViewIfNotExists(t *testing.T) {
	f := test.MustOpenFrame()
	defer f.Close()

	// Create view.
	view, err := f.CreateViewIfNotExists("v")
	if err != nil {
		t.Fatal(err)
	} else if view == nil {
		t.Fatal("expected view")
	}

	// Retrieve existing view.
	view2, err := f.CreateViewIfNotExists("v")
	if err != nil {
		t.Fatal(err)
	} else if view != view2 {
		t.Fatal("view mismatch")
	}

	if view != f.View("v") {
		t.Fatal("view mismatch")
	}
}

// Ensure frame can set its time quantum.
func TestFrame_SetTimeQuantum(t *testing.T) {
	f := test.MustOpenFrame()
	defer f.Close()

	// Set & retrieve time quantum.
	if err := f.SetTimeQuantum(pilosa.TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum: %s", q)
	}

	// Reload frame and verify that it is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum (reopen): %s", q)
	}
}

func TestFrame_NameRestriction(t *testing.T) {
	path, err := ioutil.TempDir("", "pilosa-frame-")
	if err != nil {
		panic(err)
	}
	frame, err := pilosa.NewFrame(path, "i", ".meta")
	if frame != nil {
		t.Fatalf("unexpected frame name %s", err)
	}
}

// Ensure that frame name validation is consistent.
func TestFrame_NameValidation(t *testing.T) {
	validFrameNames := []string{
		"foo",
		"hyphen-ated",
		"under_score",
		"abc123",
		"trailing_",
	}
	invalidFrameNames := []string{
		"",
		"123abc",
		"x.y",
		"_foo",
		"-bar",
		"abc def",
		"camelCase",
		"UPPERCASE",
		"a12345678901234567890123456789012345678901234567890123456789012345",
	}

	path, err := ioutil.TempDir("", "pilosa-frame-")
	if err != nil {
		panic(err)
	}
	for _, name := range validFrameNames {
		_, err := pilosa.NewFrame(path, "i", name)
		if err != nil {
			t.Fatalf("unexpected frame name: %s %s", name, err)
		}
	}
	for _, name := range invalidFrameNames {
		_, err := pilosa.NewFrame(path, "i", name)
		if err == nil {
			t.Fatalf("expected error on frame name: %s", name)
		}
	}
}

// Ensure that frame RowLable validation is consistent.
func TestFrame_RowLabelValidation(t *testing.T) {
	validRowLabels := []string{
		"",
		"foo",
		"hyphen-ated",
		"under_score",
		"abc123",
		"trailing_",
		"camelCase",
		"UPPERCASE",
	}
	invalidRowLabels := []string{
		"123abc",
		"x.y",
		"_foo",
		"-bar",
		"abc def",
		"a12345678901234567890123456789012345678901234567890123456789012345",
	}

	path, err := ioutil.TempDir("", "pilosa-frame-")
	if err != nil {
		panic(err)
	}
	f, err := pilosa.NewFrame(path, "i", "f")
	if err != nil {
		t.Fatalf("unexpected frame error: %s", err)
	}

	for _, label := range validRowLabels {
		if err := f.SetRowLabel(label); err != nil {
			t.Fatalf("unexpected row label: %s %s", label, err)
		}
	}
	for _, label := range invalidRowLabels {
		if err := f.SetRowLabel(label); err == nil {
			t.Fatalf("expected error on row label: %s", label)
		}
	}

}
