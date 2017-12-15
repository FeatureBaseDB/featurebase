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
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/test"
)

// Ensure a bitmap query can be executed.
func TestExecutor_Execute_Bitmap(t *testing.T) {
	t.Run("Row", func(t *testing.T) {
		hldr := test.MustOpenHolder()
		defer hldr.Close()
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		f, err := index.CreateFrame("f", pilosa.FrameOptions{InverseEnabled: true})
		if err != nil {
			t.Fatal(err)
		}

		e := test.NewExecutor(hldr.Holder, test.NewCluster(1))

		// Set bits.
		if _, err := e.Execute(context.Background(), "i", test.MustParse(``+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 10, 3)+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 10, SliceWidth+1)+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 20, SliceWidth+1),
		), nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := f.RowAttrStore().SetAttrs(10, map[string]interface{}{"foo": "bar", "baz": uint64(123)}); err != nil {
			t.Fatal(err)
		}

		if res, err := e.Execute(context.Background(), "i", test.MustParse(`Bitmap(rowID=10, frame=f)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{3, SliceWidth + 1}) {
			t.Fatalf("unexpected bits: %+v", bits)
		} else if attrs := res[0].(*pilosa.Bitmap).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123)}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}

		// Inhibit bits.
		if res, err := e.Execute(context.Background(), "i", test.MustParse(`Bitmap(rowID=10, frame=f)`), nil, &pilosa.ExecOptions{ExcludeBits: true}); err != nil {
			t.Fatal(err)
		} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected bits: %+v", bits)
		} else if attrs := res[0].(*pilosa.Bitmap).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123)}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}

		// Inhibit attributes.
		if res, err := e.Execute(context.Background(), "i", test.MustParse(`Bitmap(rowID=10, frame=f)`), nil, &pilosa.ExecOptions{ExcludeAttrs: true}); err != nil {
			t.Fatal(err)
		} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{3, SliceWidth + 1}) {
			t.Fatalf("unexpected bits: %+v", bits)
		} else if attrs := res[0].(*pilosa.Bitmap).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})

	t.Run("Column", func(t *testing.T) {
		hldr := test.MustOpenHolder()
		defer hldr.Close()
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFrame("f", pilosa.FrameOptions{InverseEnabled: true}); err != nil {
			t.Fatal(err)
		}

		e := test.NewExecutor(hldr.Holder, test.NewCluster(1))

		// Set bits.
		if _, err := e.Execute(context.Background(), "i", test.MustParse(``+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 10, 3)+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 10, SliceWidth+1)+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 20, SliceWidth+1),
		), nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := index.ColumnAttrStore().SetAttrs(SliceWidth+1, map[string]interface{}{"foo": "bar", "baz": uint64(123)}); err != nil {
			t.Fatal(err)
		}

		if res, err := e.Execute(context.Background(), "i", test.MustParse(fmt.Sprintf(`Bitmap(columnID=%d, frame=f)`, SliceWidth+1)), nil, nil); err != nil {
			t.Fatal(err)
		} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{10, 20}) {
			t.Fatalf("unexpected bits: %+v", bits)
		} else if attrs := res[0].(*pilosa.Bitmap).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123)}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})
}

// Ensure a difference query can be executed.
func TestExecutor_Execute_Difference(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 2)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 3)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 2)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 4)

	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Difference(Bitmap(rowID=10), Bitmap(rowID=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, 3}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure an empty difference query behaves properly.
func TestExecutor_Execute_Empty_Difference(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 1)

	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Difference()`), nil, nil); err == nil {
		t.Fatalf("Empty Difference query should give error, but got %v", res)
	}
}

// Ensure an intersect query can be executed.
func TestExecutor_Execute_Intersect(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+2)

	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 2)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(11, SliceWidth+2)

	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Intersect(Bitmap(rowID=10), Bitmap(rowID=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, SliceWidth + 2}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure an empty intersect query behaves properly.
func TestExecutor_Execute_Empty_Intersect(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Intersect()`), nil, nil); err == nil {
		t.Fatalf("Empty Intersect query should give error, but got %v", res)
	}
}

// Ensure a union query can be executed.
func TestExecutor_Execute_Union(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 0)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+2)

	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 2)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(11, SliceWidth+2)

	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Union(Bitmap(rowID=10), Bitmap(rowID=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{0, 2, SliceWidth + 1, SliceWidth + 2}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure an empty union query behaves properly.
func TestExecutor_Execute_Empty_Union(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 0)

	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Union()`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure a xor query can be executed.
func TestExecutor_Execute_Xor(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 0)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+2)

	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 2)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(11, SliceWidth+2)

	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Xor(Bitmap(rowID=10), Bitmap(rowID=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{0, 2, SliceWidth + 1}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure a count query can be executed.
func TestExecutor_Execute_Count(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).MustSetBits(10, 3)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+1)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+2)

	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Count(Bitmap(rowID=10, frame=f))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if res[0] != uint64(3) {
		t.Fatalf("unexpected n: %d", res[0])
	}
}

// Ensure a set query can be executed.
func TestExecutor_Execute_SetBit(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	f := hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0)
	if n := f.Row(11).Count(); n != 0 {
		t.Fatalf("unexpected bitmap count: %d", n)
	}

	if res, err := e.Execute(context.Background(), "i", test.MustParse(`SetBit(rowID=11, frame=f, columnID=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else {
		if !res[0].(bool) {
			t.Fatalf("expected bit changed")
		}
	}

	if n := f.Row(11).Count(); n != 1 {
		t.Fatalf("unexpected bitmap count: %d", n)
	}
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`SetBit(rowID=11, frame=f, columnID=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else {
		if res[0].(bool) {
			t.Fatalf("expected bit unchanged")
		}
	}
}

// Ensure a SetFieldValue() query can be executed.
func TestExecutor_Execute_SetFieldValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		hldr := test.MustOpenHolder()
		defer hldr.Close()

		// Create frames.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{
			RangeEnabled: true,
			Fields: []*pilosa.Field{
				{Name: "field0", Type: pilosa.FieldTypeInt, Min: 0, Max: 50},
				{Name: "field1", Type: pilosa.FieldTypeInt, Min: 1, Max: 2},
			},
		}); err != nil {
			t.Fatal(err)
		} else if _, err := index.CreateFrameIfNotExists("xxx", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		}

		// Set field values.
		e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
		if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetFieldValue(columnID=10, frame=f, field0=25, field1=2)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetFieldValue(columnID=100, frame=f, field0=10)`), nil, nil); err != nil {
			t.Fatal(err)
		}

		f := hldr.Frame("i", "f")
		if value, exists, err := f.FieldValue(10, "field0"); err != nil {
			t.Fatal(err)
		} else if !exists {
			t.Fatal("expected value to exist")
		} else if value != 25 {
			t.Fatalf("unexpected value: %v", value)
		}

		if value, exists, err := f.FieldValue(10, "field1"); err != nil {
			t.Fatal(err)
		} else if !exists {
			t.Fatal("expected value to exist")
		} else if value != 2 {
			t.Fatalf("unexpected value: %v", value)
		}

		if value, exists, err := f.FieldValue(100, "field0"); err != nil {
			t.Fatal(err)
		} else if !exists {
			t.Fatal("expected value to exist")
		} else if value != 10 {
			t.Fatalf("unexpected value: %v", value)
		}
	})

	t.Run("", func(t *testing.T) {
		hldr := test.MustOpenHolder()
		defer hldr.Close()
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{
			RangeEnabled: true,
			Fields: []*pilosa.Field{
				{Name: "field0", Type: pilosa.FieldTypeInt, Min: 0, Max: 100},
			},
		}); err != nil {
			t.Fatal(err)
		}

		t.Run("ErrFrameRequired", func(t *testing.T) {
			e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
			if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetFieldValue(columnID=10, field0=100)`), nil, nil); err == nil || err.Error() != `SetFieldValue() frame required` {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("ErrColumnFieldRequired", func(t *testing.T) {
			e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
			if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetFieldValue(invalid_column_name=10, frame=f, field0=100)`), nil, nil); err == nil || err.Error() != `SetFieldValue() column field 'columnID' required` {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("ErrColumnFieldValue", func(t *testing.T) {
			e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
			if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetFieldValue(invalid_column_name="bad_column", frame=f, field0=100)`), nil, nil); err == nil || err.Error() != `SetFieldValue() column field 'columnID' required` {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("ErrInvalidFieldValueType", func(t *testing.T) {
			e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
			if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetFieldValue(columnID=10, frame=f, field0="hello")`), nil, nil); err == nil || err.Error() != `invalid field value type` {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	})
}

// Ensure a SetRowAttrs() query can be executed.
func TestExecutor_Execute_SetRowAttrs(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	// Create frames.
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if _, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := index.CreateFrameIfNotExists("xxx", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Set two fields on f/10.
	// Also set fields on other bitmaps and frames to test isolation.
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetRowAttrs(rowID=10, frame=f, foo="bar")`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetRowAttrs(rowID=200, frame=f, YYY=1)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetRowAttrs(rowID=10, frame=xxx, YYY=1)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetRowAttrs(rowID=10, frame=f, baz=123, bat=true)`), nil, nil); err != nil {
		t.Fatal(err)
	}

	f := hldr.Frame("i", "f")
	if m, err := f.RowAttrStore().Attrs(10); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"foo": "bar", "baz": int64(123), "bat": true}) {
		t.Fatalf("unexpected bitmap attr: %#v", m)
	}
}

// Ensure a TopN() query can be executed.
func TestExecutor_Execute_TopN(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))

	// Set bits for rows 0, 10, & 20 across two slices.
	if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := idx.CreateFrame("f", pilosa.FrameOptions{InverseEnabled: true}); err != nil {
		t.Fatal(err)
	} else if _, err := idx.CreateFrame("other", pilosa.FrameOptions{InverseEnabled: true}); err != nil {
		t.Fatal(err)
	} else if _, err := e.Execute(context.Background(), "i", test.MustParse(`
		SetBit(frame=f, rowID=0, columnID=0)
		SetBit(frame=f, rowID=0, columnID=1)
		SetBit(frame=f, rowID=0, columnID=`+strconv.Itoa(SliceWidth)+`)
		SetBit(frame=f, rowID=0, columnID=`+strconv.Itoa(SliceWidth+2)+`)
		SetBit(frame=f, rowID=0, columnID=`+strconv.Itoa((5*SliceWidth)+100)+`)
		SetBit(frame=f, rowID=10, columnID=0)
		SetBit(frame=f, rowID=10, columnID=`+strconv.Itoa(SliceWidth)+`)
		SetBit(frame=f, rowID=20, columnID=`+strconv.Itoa(SliceWidth)+`)
		SetBit(frame=other, rowID=0, columnID=0)
	`), nil, nil); err != nil {
		t.Fatal(err)
	}

	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).RecalculateCache()
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewInverse, 0).RecalculateCache()
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).RecalculateCache()
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 5).RecalculateCache()

	t.Run("Standard", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(frame=f, n=2)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result[0], []pilosa.Pair{
			{ID: 0, Count: 5},
			{ID: 10, Count: 2},
		}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("Inverse", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(frame=f, inverse=true, n=2)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result[0], []pilosa.Pair{
			{ID: SliceWidth, Count: 3},
			{ID: 0, Count: 2},
		}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})
}

func TestExecutor_Execute_TopN_fill(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	// Set bits for rows 0, 10, & 20 across two slices.
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 0)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 2)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(0, SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(1, SliceWidth+2)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(1, SliceWidth)

	// Execute query.
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(frame=f, n=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{ID: 0, Count: 4},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure
func TestExecutor_Execute_TopN_fill_small(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 0)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(0, SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 2).SetBit(0, 2*SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 3).SetBit(0, 3*SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 4).SetBit(0, 4*SliceWidth)

	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(1, 0)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(1, 1)

	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(2, SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(2, SliceWidth+1)

	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 2).SetBit(3, 2*SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 2).SetBit(3, 2*SliceWidth+1)

	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 3).SetBit(4, 3*SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 3).SetBit(4, 3*SliceWidth+1)

	// Execute query.
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(frame=f, n=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{ID: 0, Count: 5},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure a TopN() query with a source bitmap can be executed.
func TestExecutor_Execute_TopN_Src(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	// Set bits for rows 0, 10, & 20 across two slices.
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 0)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(0, SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(10, SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(10, SliceWidth+1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(20, SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(20, SliceWidth+1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(20, SliceWidth+2)

	// Create an intersecting row.
	hldr.MustCreateRankedFragmentIfNotExists("i", "other", pilosa.ViewStandard, 1).SetBit(100, SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "other", pilosa.ViewStandard, 1).SetBit(100, SliceWidth+1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "other", pilosa.ViewStandard, 1).SetBit(100, SliceWidth+2)

	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).RecalculateCache()
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).RecalculateCache()
	hldr.MustCreateRankedFragmentIfNotExists("i", "other", pilosa.ViewStandard, 1).RecalculateCache()

	// Execute query.
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(Bitmap(rowID=100, frame=other), frame=f, n=3)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{ID: 20, Count: 3},
		{ID: 10, Count: 2},
		{ID: 0, Count: 1},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

//Ensure TopN handles Attribute filters
func TestExecutor_Execute_TopN_Attr(t *testing.T) {
	//
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 0)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(10, SliceWidth)

	if err := hldr.Frame("i", "f").RowAttrStore().SetAttrs(10, map[string]interface{}{"category": int64(123)}); err != nil {
		t.Fatal(err)
	}
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(frame="f", n=1, field="category", filters=[123])`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{ID: 10, Count: 1},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}

}

//Ensure TopN handles Attribute filters with source bitmap
func TestExecutor_Execute_TopN_Attr_Src(t *testing.T) {
	//
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 0)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(10, SliceWidth)

	if err := hldr.Frame("i", "f").RowAttrStore().SetAttrs(10, map[string]interface{}{"category": uint64(123)}); err != nil {
		t.Fatal(err)
	}
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(Bitmap(rowID=10,frame=f),frame="f", n=1, field="category", filters=[123])`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{ID: 10, Count: 1},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure a Sum() query can be executed.
func TestExecutor_Execute_Sum(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateFrame("f", pilosa.FrameOptions{
		RangeEnabled: true,
		Fields: []*pilosa.Field{
			{Name: "foo", Type: pilosa.FieldTypeInt, Min: 10, Max: 100},
			{Name: "bar", Type: pilosa.FieldTypeInt, Min: 0, Max: 100000},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateFrame("other", pilosa.FrameOptions{
		RangeEnabled: true,
		Fields: []*pilosa.Field{
			{Name: "foo", Type: pilosa.FieldTypeInt, Min: 0, Max: 1000},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := e.Execute(context.Background(), "i", test.MustParse(`
		SetBit(frame=f, rowID=0, columnID=0)
		SetBit(frame=f, rowID=0, columnID=`+strconv.Itoa(SliceWidth+1)+`)

		SetFieldValue(frame=f, foo=20, bar=2000, columnID=0)
		SetFieldValue(frame=f, foo=30, columnID=`+strconv.Itoa(SliceWidth)+`)
		SetFieldValue(frame=f, foo=40, columnID=`+strconv.Itoa(SliceWidth+2)+`)
		SetFieldValue(frame=f, foo=50, columnID=`+strconv.Itoa((5*SliceWidth)+100)+`)
		SetFieldValue(frame=f, foo=60, columnID=`+strconv.Itoa(SliceWidth+1)+`)
		SetFieldValue(frame=other, foo=1000, columnID=0)
	`), nil, nil); err != nil {
		t.Fatal(err)
	}

	t.Run("NoFilter", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Sum(frame=f, field=foo)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result[0], pilosa.SumCount{Sum: 200, Count: 5}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("WithFilter", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Sum(Bitmap(frame=f, rowID=0), frame=f, field=foo)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result[0], pilosa.SumCount{Sum: 80, Count: 2}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})
}

// Ensure a range query can be executed.
func TestExecutor_Execute_Range(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))

	// Create index.
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})

	// Create frame.
	if _, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{
		InverseEnabled: true,
		TimeQuantum:    pilosa.TimeQuantum("YMDH"),
	}); err != nil {
		t.Fatal(err)
	}

	// Set bits.
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`
        SetBit(frame=f, rowID=1, columnID=2, timestamp="1999-12-31T00:00")
        SetBit(frame=f, rowID=1, columnID=3, timestamp="2000-01-01T00:00")
        SetBit(frame=f, rowID=1, columnID=4, timestamp="2000-01-02T00:00")
        SetBit(frame=f, rowID=1, columnID=5, timestamp="2000-02-01T00:00")
        SetBit(frame=f, rowID=1, columnID=6, timestamp="2001-01-01T00:00")
        SetBit(frame=f, rowID=1, columnID=7, timestamp="2002-01-01T02:00")

        SetBit(frame=f, rowID=1, columnID=2, timestamp="1999-12-30T00:00")
        SetBit(frame=f, rowID=1, columnID=2, timestamp="2002-02-01T00:00")
        SetBit(frame=f, rowID=10, columnID=2, timestamp="2001-01-01T00:00")
	`), nil, nil); err != nil {
		t.Fatal(err)
	}

	t.Run("Standard", func(t *testing.T) {
		if res, err := e.Execute(context.Background(), "i", test.MustParse(`Range(rowID=1, frame=f, start="1999-12-31T00:00", end="2002-01-01T03:00")`), nil, nil); err != nil {
			t.Fatal(err)
		} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{2, 3, 4, 5, 6, 7}) {
			t.Fatalf("unexpected bits: %+v", bits)
		}
	})

	t.Run("Inverse", func(t *testing.T) {
		e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
		if res, err := e.Execute(context.Background(), "i", test.MustParse(`Range(columnID=2, frame=f, start="1999-01-01T00:00", end="2003-01-01T00:00")`), nil, nil); err != nil {
			t.Fatal(err)
		} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, 10}) {
			t.Fatalf("unexpected bits: %+v", bits)
		}
	})
}

// Ensure a Range(field) query can be executed.
func TestExecutor_Execute_FieldRange(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateFrame("f", pilosa.FrameOptions{
		RangeEnabled: true,
		Fields: []*pilosa.Field{
			{Name: "foo", Type: pilosa.FieldTypeInt, Min: 10, Max: 100},
			{Name: "bar", Type: pilosa.FieldTypeInt, Min: 0, Max: 100000},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateFrame("other", pilosa.FrameOptions{
		RangeEnabled: true,
		Fields: []*pilosa.Field{
			{Name: "foo", Type: pilosa.FieldTypeInt, Min: 0, Max: 1000},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateFrame("edge", pilosa.FrameOptions{
		RangeEnabled: true,
		Fields: []*pilosa.Field{
			{Name: "foo", Type: pilosa.FieldTypeInt, Min: -100, Max: 100},
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := e.Execute(context.Background(), "i", test.MustParse(`
		SetBit(frame=f, rowID=0, columnID=0)
		SetBit(frame=f, rowID=0, columnID=`+strconv.Itoa(SliceWidth+1)+`)

		SetFieldValue(frame=f, foo=20, bar=2000, columnID=50)
		SetFieldValue(frame=f, foo=30, columnID=`+strconv.Itoa(SliceWidth)+`)
		SetFieldValue(frame=f, foo=10, columnID=`+strconv.Itoa(SliceWidth+2)+`)
		SetFieldValue(frame=f, foo=20, columnID=`+strconv.Itoa((5*SliceWidth)+100)+`)
		SetFieldValue(frame=f, foo=60, columnID=`+strconv.Itoa(SliceWidth+1)+`)
		SetFieldValue(frame=other, foo=1000, columnID=0)
		SetFieldValue(frame=edge, foo=100, columnID=0)
		SetFieldValue(frame=edge, foo=-100, columnID=1)
	`), nil, nil); err != nil {
		t.Fatal(err)
	}

	t.Run("EQ", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=f, foo == 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, (5 * SliceWidth) + 100}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("NEQ", func(t *testing.T) {
		// NEQ null
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=other, foo != null)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
		// NEQ <int>
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=f, foo != 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{SliceWidth, SliceWidth + 1, SliceWidth + 2}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
		// NEQ -<int>
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=other, foo != -20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result[0].(*pilosa.Bitmap).Bits()) {
			//t.Fatalf("unexpected result: %s", spew.Sdump(result))
			t.Fatalf("unexpected result: %v", result[0].(*pilosa.Bitmap).Bits())
		}
	})

	t.Run("LT", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=f, foo < 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{SliceWidth + 2}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTE", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=f, foo <= 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, SliceWidth + 2, (5 * SliceWidth) + 100}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("GT", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=f, foo > 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{SliceWidth, SliceWidth + 1}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("GTE", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=f, foo >= 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, SliceWidth, SliceWidth + 1, (5 * SliceWidth) + 100}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BETWEEN", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=other, foo >< [1, 1000])`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	// Ensure that the FieldNotNull code path gets run.
	t.Run("FieldNotNull", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=other, foo >< [0, 1000])`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BelowMin", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=f, foo == 0)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("AboveMax", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=f, foo == 200)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTAboveMax", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=edge, foo < 200)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result[0].(*pilosa.Bitmap).Bits()))
		}
	})

	t.Run("GTBelowMin", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=edge, foo > -200)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result[0].(*pilosa.Bitmap).Bits()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result[0].(*pilosa.Bitmap).Bits()))
		}
	})

	t.Run("ErrFrameNotFound", func(t *testing.T) {
		if _, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=bad_frame, foo >= 20)`), nil, nil); err != pilosa.ErrFrameNotFound {
			t.Fatal(err)
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		if _, err := e.Execute(context.Background(), "i", test.MustParse(`Range(frame=f, bad_field >= 20)`), nil, nil); err != pilosa.ErrFieldNotFound {
			t.Fatal(err)
		}
	})
}

// Ensure a remote query can return a bitmap.
func TestExecutor_Execute_Remote_Bitmap(t *testing.T) {
	c := test.NewCluster(2)

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()
	c.Nodes[1].Scheme = "http"
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to verify arguments and return a bitmap.
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if index != "i" {
			t.Fatalf("unexpected index: %s", index)
		} else if query.String() != `Bitmap(frame="f", rowID=10)` {
			t.Fatalf("unexpected query: %s", query.String())
		} else if !reflect.DeepEqual(slices, []uint64{1}) {
			t.Fatalf("unexpected slices: %+v", slices)
		}

		// Set bits in slice 0 & 2.
		bm := pilosa.NewBitmap(
			(0*SliceWidth)+1,
			(0*SliceWidth)+2,
			(2*SliceWidth)+4,
		)
		return []interface{}{bm}, nil
	}

	// Create local executor data.
	// The local node owns slice 1.
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	s.Handler.Holder = hldr.Holder
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).MustSetBits(10, (1*SliceWidth)+1)

	e := test.NewExecutor(hldr.Holder, c)
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Bitmap(rowID=10, frame=f)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, 2, 2*SliceWidth + 4}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure a remote query can return a count.
func TestExecutor_Execute_Remote_Count(t *testing.T) {
	c := test.NewCluster(2)

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to return a count.
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{uint64(10)}, nil
	}

	// Create local executor data. The local node owns slice 1.
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	s.Handler.Holder = hldr.Holder
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 2).MustSetBits(10, (2*SliceWidth)+1)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 2).MustSetBits(10, (2*SliceWidth)+2)

	e := test.NewExecutor(hldr.Holder, c)
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Count(Bitmap(rowID=10, frame=f))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if res[0] != uint64(12) {
		t.Fatalf("unexpected n: %d", res[0])
	}
}

// Ensure a remote query can set bits on multiple nodes.
func TestExecutor_Execute_Remote_SetBit(t *testing.T) {
	c := test.NewCluster(2)
	c.ReplicaN = 2

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to verify arguments.
	var remoteCalled bool
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if index != `i` {
			t.Fatalf("unexpected index: %s", index)
		} else if query.String() != `SetBit(columnID=2, frame="f", rowID=10)` {
			t.Fatalf("unexpected query: %s", query.String())
		}
		remoteCalled = true
		return []interface{}{nil}, nil
	}

	// Create local executor data.
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	s.Handler.Holder = hldr.Holder

	// Create frame.
	if _, err := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{}).CreateFrame("f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	e := test.NewExecutor(hldr.Holder, c)
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetBit(rowID=10, frame=f, columnID=2)`), nil, nil); err != nil {
		t.Fatal(err)
	}

	// Verify that one bit is set on both node's holder.
	if n := hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).Row(10).Count(); n != 1 {
		t.Fatalf("unexpected local count: %d", n)
	}
	if !remoteCalled {
		t.Fatalf("expected remote execution")
	}
}

// Ensure a remote query can set bits on multiple nodes.
func TestExecutor_Execute_Remote_SetBit_With_Timestamp(t *testing.T) {
	c := test.NewCluster(2)
	c.ReplicaN = 2

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to verify arguments.
	var remoteCalled bool
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if index != `i` {
			t.Fatalf("unexpected index: %s", index)
		} else if query.String() != `SetBit(columnID=2, frame="f", rowID=10, timestamp="2016-12-11T10:09")` {
			t.Fatalf("unexpected query: %s", query.String())
		}
		remoteCalled = true
		return []interface{}{nil}, nil
	}

	// Create local executor data.
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	s.Handler.Holder = hldr.Holder

	// Create frame.
	if f, err := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{}).CreateFrame("f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if err := f.SetTimeQuantum("Y"); err != nil {
		t.Fatal(err)
	}

	e := test.NewExecutor(hldr.Holder, c)
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetBit(rowID=10, frame=f, columnID=2, timestamp="2016-12-11T10:09")`), nil, nil); err != nil {
		t.Fatal(err)
	}

	// Verify that one bit is set on both node's holder.
	if n := hldr.MustCreateFragmentIfNotExists("i", "f", "standard_2016", 0).Row(10).Count(); n != 1 {
		t.Fatalf("unexpected local count: %d", n)
	}
	if !remoteCalled {
		t.Fatalf("expected remote execution")
	}
}

// Ensure a remote query can return a top-n query.
func TestExecutor_Execute_Remote_TopN(t *testing.T) {
	c := test.NewCluster(2)

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to verify arguments and return a bitmap.
	var remoteExecN int
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if index != "i" {
			t.Fatalf("unexpected index: %s", index)
		} else if !reflect.DeepEqual(slices, []uint64{1, 3}) {
			t.Fatalf("unexpected slices: %+v", slices)
		}

		// Query should be executed twice. Once to get the top bitmaps for the
		// slices and a second time to get the counts for a set of bitmaps.
		switch remoteExecN {
		case 0:
			if query.String() != `TopN(frame="f", n=3)` {
				t.Fatalf("unexpected query(0): %s", query.String())
			}
		case 1:
			if query.String() != `TopN(frame="f", ids=[0,10,30], n=3)` {
				t.Fatalf("unexpected query(1): %s", query.String())
			}
		default:
			t.Fatalf("too many remote exec calls")
		}
		remoteExecN++

		// Return pair counts.
		return []interface{}{[]pilosa.Pair{
			{ID: 0, Count: 5},
			{ID: 10, Count: 2},
			{ID: 30, Count: 2},
		}}, nil
	}

	// Create local executor data on slice 2 & 4.
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	s.Handler.Holder = hldr.Holder
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 2).MustSetBits(30, (2*SliceWidth)+1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 4).MustSetBits(30, (4*SliceWidth)+2)

	e := test.NewExecutor(hldr.Holder, c)
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(frame=f, n=3)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(res, []interface{}{[]pilosa.Pair{
		{ID: 0, Count: 5},
		{ID: 30, Count: 4},
		{ID: 10, Count: 2},
	}}) {
		t.Fatalf("unexpected results: %s", spew.Sdump(res))
	}
}

// Ensure executor returns an error if too many writes are in a single request.
func TestExecutor_Execute_ErrMaxWritesPerRequest(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))
	e.MaxWritesPerRequest = 3
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetBit() ClearBit() SetBit() SetBit()`), nil, nil); err != pilosa.ErrTooManyWrites {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure SetColumnAttrs doesn't save `frame` as an attribute
func TestExectutor_SetColumnAttrs_ExcludeFrame(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	index.CreateFrame("f", pilosa.FrameOptions{InverseEnabled: true})
	targetAttrs := map[string]interface{}{
		"foo": "bar",
	}
	e := test.NewExecutor(hldr.Holder, test.NewCluster(1))

	// SetColumnAttrs call should exclude the frame attribute
	_, err := e.Execute(context.Background(), "i", test.MustParse("SetBit(frame='f', rowID=1, columnID=10)"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Execute(context.Background(), "i", test.MustParse("SetColumnAttrs(frame='f', columnID=10, foo='bar')"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	attrs, err := index.ColumnAttrStore().Attrs(10)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(attrs, targetAttrs) {
		t.Fatalf("%#v != %#v", targetAttrs, attrs)
	}

	// SetColumnAttrs call should not break if frame is not specified
	_, err = e.Execute(context.Background(), "i", test.MustParse("SetBit(frame='f', rowID=1, columnID=20)"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Execute(context.Background(), "i", test.MustParse("SetColumnAttrs(columnID=20, foo='bar')"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	attrs, err = index.ColumnAttrStore().Attrs(20)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(attrs, targetAttrs) {
		t.Fatalf("%#v != %#v", targetAttrs, attrs)
	}

}
