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
	"math"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pilosa/pilosa/test"
)

// Ensure a field can set & read a bsiGroup float.
func TestField_SetFloat(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeFloat())
		if err != nil {
			t.Fatal(err)
		}

		tests := []struct {
			flt float64
			err string
		}{
			{flt: 7.0e+0},
			{flt: -1.120e+308},
			{flt: 1.120e-292},
			{flt: math.NaN(), err: "float provided is NaN"},
			{flt: math.Inf(0), err: "float provided is infinity"},

			// The following came from ftoa.go tests: https://gist.github.com/rsc/1057873
			{flt: math.Ldexp(8511030020275656, -342)},
			{flt: math.Ldexp(5201988407066741, -824)},
			{flt: math.Ldexp(6406892948269899, +237)},
			{flt: math.Ldexp(8431154198732492, +72)},
			{flt: math.Ldexp(6475049196144587, +99)},
			{flt: math.Ldexp(8274307542972842, +726)},
			{flt: math.Ldexp(5381065484265332, -456)},
			{flt: math.Ldexp(6761728585499734, -1057)},
			{flt: math.Ldexp(7976538478610756, +376)},
			{flt: math.Ldexp(5982403858958067, +377)},
			{flt: math.Ldexp(5536995190630837, +93)},
			{flt: math.Ldexp(7225450889282194, +710)},
			{flt: math.Ldexp(7225450889282194, +709)},
			{flt: math.Ldexp(8703372741147379, +117)},
			{flt: math.Ldexp(8944262675275217, -1001)},
			{flt: math.Ldexp(7459803696087692, -707)},
			{flt: math.Ldexp(6080469016670379, -381)},
			{flt: math.Ldexp(8385515147034757, +721)},
			{flt: math.Ldexp(7514216811389786, -828)},
			{flt: math.Ldexp(8397297803260511, -345)},
			{flt: math.Ldexp(6733459239310543, +202)},
			{flt: math.Ldexp(8091450587292794, -473)},
			{flt: math.Ldexp(6567258882077402, +952)},
			{flt: math.Ldexp(6712731423444934, +535)},
			{flt: math.Ldexp(6712731423444934, +534)},
			{flt: math.Ldexp(5298405411573037, -957)},
			{flt: math.Ldexp(5137311167659507, -144)},
			{flt: math.Ldexp(6722280709661868, +363)},
			{flt: math.Ldexp(5344436398034927, -169)},
			{flt: math.Ldexp(8369123604277281, -853)},
			{flt: math.Ldexp(8995822108487663, -780)},
			{flt: math.Ldexp(8942832835564782, -383)},
			{flt: math.Ldexp(8942832835564782, -384)},
			{flt: math.Ldexp(8942832835564782, -385)},
			{flt: math.Ldexp(6965949469487146, -249)},
			{flt: math.Ldexp(6965949469487146, -250)},
			{flt: math.Ldexp(6965949469487146, -251)},
			{flt: math.Ldexp(7487252720986826, +548)},
			{flt: math.Ldexp(5592117679628511, +164)},
			{flt: math.Ldexp(8887055249355788, +665)},
			{flt: math.Ldexp(6994187472632449, +690)},
			{flt: math.Ldexp(8797576579012143, +588)},
			{flt: math.Ldexp(7363326733505337, +272)},
			{flt: math.Ldexp(8549497411294502, -448)},
			{flt: 12345000},
		}
		for i, test := range tests {
			// Set float value on field.
			changed, err := f.SetFloat(100, test.flt)
			if test.err != "" {
				if !strings.Contains(err.Error(), test.err) {
					t.Errorf("test %d expected error: %s, but got: %s", i, test.err, err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			} else if !changed {
				t.Errorf("test %d expected change", i)
			}

			// Read value.
			if value, exists, err := f.Float(100); err != nil {
				t.Fatal(err)
			} else if value != test.flt {
				t.Errorf("test %d expected value: %v, but got: %v", i, test.flt, value)
			} else if !exists {
				t.Errorf("test %d expected value to exist", i)
			}

			// Setting value should return no change.
			if changed, err := f.SetFloat(100, test.flt); err != nil {
				t.Fatal(err)
			} else if changed {
				t.Errorf("test %d expected no change", i)
			}
		}
	})
}

// Ensure a field can set & read a bsiGroup value.
func TestField_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(0, 30))
		if err != nil {
			t.Fatal(err)
		}

		// Set value on field.
		if changed, err := f.SetValue(100, 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.Value(100); err != nil {
			t.Fatal(err)
		} else if value != 21 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}

		// Setting value should return no change.
		if changed, err := f.SetValue(100, 21); err != nil {
			t.Fatal(err)
		} else if changed {
			t.Fatal("expected no change")
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(0, 30))
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if changed, err := f.SetValue(100, 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Set different value.
		if changed, err := f.SetValue(100, 23); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.Value(100); err != nil {
			t.Fatal(err)
		} else if value != 23 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}
	})

	t.Run("ErrBSIGroupNotFound", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if _, err := f.SetValue(100, 21); err != pilosa.ErrBSIGroupNotFound {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBSIGroupValueTooLow", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(20, 30))
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if _, err := f.SetValue(100, 15); err != pilosa.ErrBSIGroupValueTooLow {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBSIGroupValueTooHigh", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(20, 30))
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if _, err := f.SetValue(100, 31); err != pilosa.ErrBSIGroupValueTooHigh {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestField_NameRestriction(t *testing.T) {
	path, err := ioutil.TempDir("", "pilosa-field-")
	if err != nil {
		panic(err)
	}
	field, err := pilosa.NewField(path, "i", ".meta", pilosa.OptFieldTypeDefault())
	if field != nil {
		t.Fatalf("unexpected field name %s", err)
	}
}

// Ensure that field name validation is consistent.
func TestField_NameValidation(t *testing.T) {
	validFieldNames := []string{
		"foo",
		"hyphen-ated",
		"under_score",
		"abc123",
		"trailing_",
	}
	invalidFieldNames := []string{
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

	path, err := ioutil.TempDir("", "pilosa-field-")
	if err != nil {
		panic(err)
	}
	for _, name := range validFieldNames {
		_, err := pilosa.NewField(path, "i", name, pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatalf("unexpected field name: %s %s", name, err)
		}
	}
	for _, name := range invalidFieldNames {
		_, err := pilosa.NewField(path, "i", name, pilosa.OptFieldTypeDefault())
		if err == nil {
			t.Fatalf("expected error on field name: %s", name)
		}
	}
}

// Ensure can update and delete available shards.
func TestField_AvailableShards(t *testing.T) {
	idx := test.MustOpenIndex()
	defer idx.Close()

	f, err := idx.CreateField("f", pilosa.OptFieldTypeDefault())
	if err != nil {
		t.Fatal(err)
	}

	// Set values on shards 0 & 2, and verify.
	if _, err := f.SetBit(0, 100, nil); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(0, ShardWidth*2, nil); err != nil {
		t.Fatal(err)
	} else if diff := cmp.Diff(f.AvailableShards().Slice(), []uint64{0, 2}); diff != "" {
		t.Fatal(diff)
	}

	// Set remote shards and verify.
	f.AddRemoteAvailableShards(roaring.NewBitmap(1, 2, 4))
	if diff := cmp.Diff(f.AvailableShards().Slice(), []uint64{0, 1, 2, 4}); diff != "" {
		t.Fatal(diff)
	}

	// Delete shards; only local shards should remain.
	f.RemoveAvailableShard(0)
	f.RemoveAvailableShard(1)
	f.RemoveAvailableShard(2)
	f.RemoveAvailableShard(3)
	f.RemoveAvailableShard(4)
	if diff := cmp.Diff(f.AvailableShards().Slice(), []uint64{0, 2}); diff != "" {
		t.Fatal(diff)
	}
}
