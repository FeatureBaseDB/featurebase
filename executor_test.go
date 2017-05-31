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
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

// Ensure a bitmap query can be executed.
func TestExecutor_Execute_Bitmap(t *testing.T) {
	t.Run("Row", func(t *testing.T) {
		hldr := MustOpenHolder()
		defer hldr.Close()
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		f, err := index.CreateFrame("f", pilosa.FrameOptions{InverseEnabled: true})
		if err != nil {
			t.Fatal(err)
		}

		e := NewExecutor(hldr.Holder, NewCluster(1))

		// Set bits.
		if _, err := e.Execute(context.Background(), "i", MustParse(``+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 10, 3)+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 10, SliceWidth+1)+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 20, SliceWidth+1),
		), nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := f.RowAttrStore().SetAttrs(10, map[string]interface{}{"foo": "bar", "baz": uint64(123)}); err != nil {
			t.Fatal(err)
		}

		if res, err := e.Execute(context.Background(), "i", MustParse(`Bitmap(rowID=10, frame=f)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{3, SliceWidth + 1}) {
			t.Fatalf("unexpected bits: %+v", bits)
		} else if attrs := res[0].(*pilosa.Bitmap).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123)}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})

	t.Run("Column", func(t *testing.T) {
		hldr := MustOpenHolder()
		defer hldr.Close()
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFrame("f", pilosa.FrameOptions{InverseEnabled: true}); err != nil {
			t.Fatal(err)
		}

		e := NewExecutor(hldr.Holder, NewCluster(1))

		// Set bits.
		if _, err := e.Execute(context.Background(), "i", MustParse(``+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 10, 3)+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 10, SliceWidth+1)+
			fmt.Sprintf("SetBit(frame=f, rowID=%d, columnID=%d)\n", 20, SliceWidth+1),
		), nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := index.ColumnAttrStore().SetAttrs(SliceWidth+1, map[string]interface{}{"foo": "bar", "baz": uint64(123)}); err != nil {
			t.Fatal(err)
		}

		if res, err := e.Execute(context.Background(), "i", MustParse(fmt.Sprintf(`Bitmap(columnID=%d, frame=f)`, SliceWidth+1)), nil, nil); err != nil {
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
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 2)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 3)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 2)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 4)

	e := NewExecutor(hldr.Holder, NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", MustParse(`Difference(Bitmap(rowID=10), Bitmap(rowID=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, 3}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure an empty difference query behaves properly.
func TestExecutor_Execute_Empty_Difference(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 1)

	e := NewExecutor(hldr.Holder, NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", MustParse(`Difference()`), nil, nil); err == nil {
		t.Fatalf("Empty Difference query should give error, but got %v", res)
	}
}

// Ensure an intersect query can be executed.
func TestExecutor_Execute_Intersect(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+2)

	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 2)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(11, SliceWidth+2)

	e := NewExecutor(hldr.Holder, NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", MustParse(`Intersect(Bitmap(rowID=10), Bitmap(rowID=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, SliceWidth + 2}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure an empty intersect query behaves properly.
func TestExecutor_Execute_Empty_Intersect(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	e := NewExecutor(hldr.Holder, NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", MustParse(`Intersect()`), nil, nil); err == nil {
		t.Fatalf("Empty Intersect query should give error, but got %v", res)
	}
}

// Ensure a union query can be executed.
func TestExecutor_Execute_Union(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 0)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+1)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+2)

	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(11, 2)
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 1).MustSetBits(11, SliceWidth+2)

	e := NewExecutor(hldr.Holder, NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", MustParse(`Union(Bitmap(rowID=10), Bitmap(rowID=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{0, 2, SliceWidth + 1, SliceWidth + 2}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure an empty union query behaves properly.
func TestExecutor_Execute_Empty_Union(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "general", pilosa.ViewStandard, 0).MustSetBits(10, 0)

	e := NewExecutor(hldr.Holder, NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", MustParse(`Union()`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure a count query can be executed.
func TestExecutor_Execute_Count(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).MustSetBits(10, 3)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+1)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).MustSetBits(10, SliceWidth+2)

	e := NewExecutor(hldr.Holder, NewCluster(1))
	if res, err := e.Execute(context.Background(), "i", MustParse(`Count(Bitmap(rowID=10, frame=f))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if res[0] != uint64(3) {
		t.Fatalf("unexpected n: %d", res[0])
	}
}

// Ensure a set query can be executed.
func TestExecutor_Execute_SetBit(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	e := NewExecutor(hldr.Holder, NewCluster(1))
	f := hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0)
	if n := f.Row(11).Count(); n != 0 {
		t.Fatalf("unexpected bitmap count: %d", n)
	}

	if res, err := e.Execute(context.Background(), "i", MustParse(`SetBit(rowID=11, frame=f, columnID=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else {
		if !res[0].(bool) {
			t.Fatalf("expected bit changed")
		}
	}

	if n := f.Row(11).Count(); n != 1 {
		t.Fatalf("unexpected bitmap count: %d", n)
	}
	if res, err := e.Execute(context.Background(), "i", MustParse(`SetBit(rowID=11, frame=f, columnID=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else {
		if res[0].(bool) {
			t.Fatalf("expected bit unchanged")
		}
	}
}

// Ensure a SetRowAttrs() query can be executed.
func TestExecutor_Execute_SetRowAttrs(t *testing.T) {
	hldr := MustOpenHolder()
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
	e := NewExecutor(hldr.Holder, NewCluster(1))
	if _, err := e.Execute(context.Background(), "i", MustParse(`SetRowAttrs(rowID=10, frame=f, foo="bar")`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "i", MustParse(`SetRowAttrs(rowID=200, frame=f, YYY=1)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "i", MustParse(`SetRowAttrs(rowID=10, frame=xxx, YYY=1)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "i", MustParse(`SetRowAttrs(rowID=10, frame=f, baz=123, bat=true)`), nil, nil); err != nil {
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
	hldr := MustOpenHolder()
	defer hldr.Close()
	e := NewExecutor(hldr.Holder, NewCluster(1))

	// Set bits for rows 0, 10, & 20 across two slices.
	if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := idx.CreateFrame("f", pilosa.FrameOptions{InverseEnabled: true}); err != nil {
		t.Fatal(err)
	} else if _, err := idx.CreateFrame("other", pilosa.FrameOptions{InverseEnabled: true}); err != nil {
		t.Fatal(err)
	} else if _, err := e.Execute(context.Background(), "i", MustParse(`
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
		if result, err := e.Execute(context.Background(), "i", MustParse(`TopN(frame=f, n=2)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result[0], []pilosa.Pair{
			{ID: 0, Count: 5},
			{ID: 10, Count: 2},
		}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("Inverse", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", MustParse(`TopN(frame=f, inverse=true, n=2)`), nil, nil); err != nil {
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
	hldr := MustOpenHolder()
	defer hldr.Close()

	// Set bits for rows 0, 10, & 20 across two slices.
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 0)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 2)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(0, SliceWidth)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(1, SliceWidth+2)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(1, SliceWidth)

	// Execute query.
	e := NewExecutor(hldr.Holder, NewCluster(1))
	if result, err := e.Execute(context.Background(), "i", MustParse(`TopN(frame=f, n=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{ID: 0, Count: 4},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure
func TestExecutor_Execute_TopN_fill_small(t *testing.T) {
	hldr := MustOpenHolder()
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
	e := NewExecutor(hldr.Holder, NewCluster(1))
	if result, err := e.Execute(context.Background(), "i", MustParse(`TopN(frame=f, n=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{ID: 0, Count: 5},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure a TopN() query with a source bitmap can be executed.
func TestExecutor_Execute_TopN_Src(t *testing.T) {
	hldr := MustOpenHolder()
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
	e := NewExecutor(hldr.Holder, NewCluster(1))
	if result, err := e.Execute(context.Background(), "i", MustParse(`TopN(Bitmap(rowID=100, frame=other), frame=f, n=3)`), nil, nil); err != nil {
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
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 0)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(10, SliceWidth)

	if err := hldr.Frame("i", "f").RowAttrStore().SetAttrs(10, map[string]interface{}{"category": int64(123)}); err != nil {
		t.Fatal(err)
	}
	e := NewExecutor(hldr.Holder, NewCluster(1))
	if result, err := e.Execute(context.Background(), "i", MustParse(`TopN(frame="f", n=1, field="category", filters=[123])`), nil, nil); err != nil {
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
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 0)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(10, SliceWidth)

	if err := hldr.Frame("i", "f").RowAttrStore().SetAttrs(10, map[string]interface{}{"category": uint64(123)}); err != nil {
		t.Fatal(err)
	}
	e := NewExecutor(hldr.Holder, NewCluster(1))
	if result, err := e.Execute(context.Background(), "i", MustParse(`TopN(Bitmap(rowID=10,frame=f),frame="f", n=1, field="category", filters=[123])`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{ID: 10, Count: 1},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}

}

// Ensure a range query can be executed.
func TestExecutor_Execute_Range(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()
	e := NewExecutor(hldr.Holder, NewCluster(1))

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
	if _, err := e.Execute(context.Background(), "i", MustParse(`
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
		if res, err := e.Execute(context.Background(), "i", MustParse(`Range(rowID=1, frame=f, start="1999-12-31T00:00", end="2002-01-01T03:00")`), nil, nil); err != nil {
			t.Fatal(err)
		} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{2, 3, 4, 5, 6, 7}) {
			t.Fatalf("unexpected bits: %+v", bits)
		}
	})

	t.Run("Inverse", func(t *testing.T) {
		e := NewExecutor(hldr.Holder, NewCluster(1))
		if res, err := e.Execute(context.Background(), "i", MustParse(`Range(columnID=2, frame=f, start="1999-01-01T00:00", end="2003-01-01T00:00")`), nil, nil); err != nil {
			t.Fatal(err)
		} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, 10}) {
			t.Fatalf("unexpected bits: %+v", bits)
		}
	})
}

// Ensure an external plugin call can be executed.
func TestExecutor_Execute_ExternalCall(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).SetBit(10, 3)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).SetBit(10, SliceWidth+1)

	// Initialize executor with two plugins.
	e := NewExecutor(hldr.Holder, NewCluster(1))
	p := MockPluginConstructorWrapper{
		mock: &MockPlugin{
			MapFn: func(ctx context.Context, db string, children []interface{}, args map[string]interface{}, slice uint64) (interface{}, error) {
				bm := children[0].(*pilosa.Bitmap)
				return uint64(bm.Count() + 10), nil
			},
			ReduceFn: func(ctx context.Context, prev, v interface{}) interface{} {
				u64, _ := prev.(uint64)
				return u64 + v.(uint64)
			},
		},
	}

	//	type NewPluginConstructor func(*Holder) Plugin

	pilosa.RegisterPlugin("test1", pilosa.NewPluginConstructor(p.NewMockPluginConstruct))

	// Execute function with plugin call.
	// The result should include the total bit count plus 10 for each slice
	// executed during the map phase: 1 + 10 + 1 + 10 = 22
	if res, err := e.Execute(context.Background(), "i", MustParse(`test1(Bitmap(rowID=10, frame=f))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if res[0] != uint64(22) {
		t.Fatalf("unexpected result: %v", res)
	}
}

// Ensure a remote query can return a bitmap.
func TestExecutor_Execute_Remote_Bitmap(t *testing.T) {
	c := NewCluster(2)

	// Create secondary server and update second cluster node.
	s := NewServer()
	defer s.Close()
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
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).MustSetBits(10, (1*SliceWidth)+1)

	e := NewExecutor(hldr.Holder, c)
	if res, err := e.Execute(context.Background(), "i", MustParse(`Bitmap(rowID=10, frame=f)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, 2, 2*SliceWidth + 4}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure a remote query can return a count.
func TestExecutor_Execute_Remote_Count(t *testing.T) {
	c := NewCluster(2)

	// Create secondary server and update second cluster node.
	s := NewServer()
	defer s.Close()
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to return a count.
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{uint64(10)}, nil
	}

	// Create local executor data. The local node owns slice 1.
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 2).MustSetBits(10, (2*SliceWidth)+1)
	hldr.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 2).MustSetBits(10, (2*SliceWidth)+2)

	e := NewExecutor(hldr.Holder, c)
	if res, err := e.Execute(context.Background(), "i", MustParse(`Count(Bitmap(rowID=10, frame=f))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if res[0] != uint64(12) {
		t.Fatalf("unexpected n: %d", res[0])
	}
}

// Ensure a remote query can set bits on multiple nodes.
func TestExecutor_Execute_Remote_SetBit(t *testing.T) {
	c := NewCluster(2)
	c.ReplicaN = 2

	// Create secondary server and update second cluster node.
	s := NewServer()
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
	hldr := MustOpenHolder()
	defer hldr.Close()

	// Create frame.
	if _, err := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{}).CreateFrame("f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	e := NewExecutor(hldr.Holder, c)
	if _, err := e.Execute(context.Background(), "i", MustParse(`SetBit(rowID=10, frame=f, columnID=2)`), nil, nil); err != nil {
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
	c := NewCluster(2)
	c.ReplicaN = 2

	// Create secondary server and update second cluster node.
	s := NewServer()
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
	hldr := MustOpenHolder()
	defer hldr.Close()

	// Create frame.
	if f, err := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{}).CreateFrame("f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	} else if err := f.SetTimeQuantum("Y"); err != nil {
		t.Fatal(err)
	}

	e := NewExecutor(hldr.Holder, c)
	if _, err := e.Execute(context.Background(), "i", MustParse(`SetBit(rowID=10, frame=f, columnID=2, timestamp="2016-12-11T10:09")`), nil, nil); err != nil {
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
	c := NewCluster(2)

	// Create secondary server and update second cluster node.
	s := NewServer()
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
	hldr := MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 2).MustSetBits(30, (2*SliceWidth)+1)
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 4).MustSetBits(30, (4*SliceWidth)+2)

	e := NewExecutor(hldr.Holder, c)
	if res, err := e.Execute(context.Background(), "i", MustParse(`TopN(frame=f, n=3)`), nil, nil); err != nil {
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
	hldr := MustOpenHolder()
	defer hldr.Close()
	e := NewExecutor(hldr.Holder, NewCluster(1))
	e.MaxWritesPerRequest = 3
	if _, err := e.Execute(context.Background(), "i", MustParse(`SetBit() ClearBit() SetBit() SetBit()`), nil, nil); err != pilosa.ErrTooManyWrites {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Executor represents a test wrapper for pilosa.Executor.
type Executor struct {
	*pilosa.Executor
}

// NewExecutor returns a new instance of Executor.
// The executor always matches the hostname of the first cluster node.
func NewExecutor(holder *pilosa.Holder, cluster *pilosa.Cluster) *Executor {
	e := &Executor{Executor: pilosa.NewExecutor()}
	e.Holder = holder
	e.Cluster = cluster
	e.Host = cluster.Nodes[0].Host
	return e
}

// MustParse parses s into a PQL query. Panic on error.
func MustParse(s string) *pql.Query {
	q, err := pql.NewParser(strings.NewReader(s)).Parse()
	if err != nil {
		panic(err)
	}
	return q
}
