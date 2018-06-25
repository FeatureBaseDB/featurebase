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
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
		f, err := index.CreateField("f", pilosa.FieldOptions{})
		if err != nil {
			t.Fatal(err)
		}

		e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))

		// Set bits.
		if _, err := e.Execute(context.Background(), "i", test.MustParse(``+
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10)+
			fmt.Sprintf("Set(%d, f=%d)\n", SliceWidth+1, 10)+
			fmt.Sprintf("Set(%d, f=%d)\n", SliceWidth+1, 20),
		), nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := f.RowAttrStore().SetAttrs(10, map[string]interface{}{"foo": "bar", "baz": uint64(123)}); err != nil {
			t.Fatal(err)
		}

		if res, err := e.Execute(context.Background(), "i", test.MustParse(`Row(f=10)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if bits := res[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, SliceWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := res[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123)}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}

		// Inhibit column attributes.
		if res, err := e.Execute(context.Background(), "i", test.MustParse(`Row(f=10)`), nil, &pilosa.ExecOptions{ExcludeColumns: true}); err != nil {
			t.Fatal(err)
		} else if columns := res[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", columns)
		} else if attrs := res[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123)}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}

		// Inhibit row attributes.
		if res, err := e.Execute(context.Background(), "i", test.MustParse(`Row(f=10)`), nil, &pilosa.ExecOptions{ExcludeRowAttrs: true}); err != nil {
			t.Fatal(err)
		} else if columns := res[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{3, SliceWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", columns)
		} else if attrs := res[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})

	t.Run("Column", func(t *testing.T) {
		hldr := test.MustOpenHolder()
		defer hldr.Close()
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateField("f", pilosa.FieldOptions{}); err != nil {
			t.Fatal(err)
		}

		e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))

		// Set bits.
		if _, err := e.Execute(context.Background(), "i", test.MustParse(``+
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10)+
			fmt.Sprintf("Set(%d, f=%d)\n", SliceWidth+1, 10)+
			fmt.Sprintf("Set(%d, f=%d)\n", SliceWidth+1, 20),
		), nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := index.ColumnAttrStore().SetAttrs(SliceWidth+1, map[string]interface{}{"foo": "bar", "baz": uint64(123)}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Keys", func(t *testing.T) {
		hldr := test.MustOpenHolder()
		defer hldr.Close()
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{Keys: true})
		if _, err := index.CreateField("f", pilosa.FieldOptions{Keys: true}); err != nil {
			t.Fatal(err)
		}

		e := test.NewExecutor(hldr.Holder, test.NewCluster(1))

		// Set bits.
		if _, err := e.Execute(context.Background(), "i", test.MustParse(``+
			`Set("foo", f="bar")`+"\n"+
			`Set("foo", f="baz")`+"\n"+
			`Set("bat", f="bar")`+"\n"+
			`Set("aaa", f="bbb")`+"\n",
		), nil, nil); err != nil {
			t.Fatal(err)
		}

		if results, err := e.Execute(context.Background(), "i", test.MustParse(`Row(f="bar")`), nil, nil); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(results, []interface{}{
			&pilosa.Row{Keys: []string{"foo", "bat"}, Attrs: map[string]interface{}{}},
		}, cmpopts.IgnoreUnexported(pilosa.Row{})); diff != "" {
			t.Fatal(diff)
		}
	})
}

// Ensure a difference query can be executed.
func TestExecutor_Execute_Difference(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.SetBit("i", "general", 10, 1)
	hldr.SetBit("i", "general", 10, 2)
	hldr.SetBit("i", "general", 10, 3)
	hldr.SetBit("i", "general", 11, 2)
	hldr.SetBit("i", "general", 11, 4)

	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Difference(Row(general=10), Row(general=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if columns := res[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, 3}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure an empty difference query behaves properly.
func TestExecutor_Execute_Empty_Difference(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.SetBit("i", "general", 10, 1)

	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Difference()`), nil, nil); err == nil {
		t.Fatalf("Empty Difference query should give error, but got %v", res)
	}
}

// Ensure an intersect query can be executed.
func TestExecutor_Execute_Intersect(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.SetBit("i", "general", 10, 1)
	hldr.SetBit("i", "general", 10, SliceWidth+1)
	hldr.SetBit("i", "general", 10, SliceWidth+2)

	hldr.SetBit("i", "general", 11, 1)
	hldr.SetBit("i", "general", 11, 2)
	hldr.SetBit("i", "general", 11, SliceWidth+2)

	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Intersect(Row(general=10), Row(general=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if columns := res[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, SliceWidth + 2}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure an empty intersect query behaves properly.
func TestExecutor_Execute_Empty_Intersect(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Intersect()`), nil, nil); err == nil {
		t.Fatalf("Empty Intersect query should give error, but got %v", res)
	}
}

// Ensure a union query can be executed.
func TestExecutor_Execute_Union(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.SetBit("i", "general", 10, 0)
	hldr.SetBit("i", "general", 10, SliceWidth+1)
	hldr.SetBit("i", "general", 10, SliceWidth+2)

	hldr.SetBit("i", "general", 11, 2)
	hldr.SetBit("i", "general", 11, SliceWidth+2)

	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Union(Row(general=10), Row(general=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if columns := res[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{0, 2, SliceWidth + 1, SliceWidth + 2}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure an empty union query behaves properly.
func TestExecutor_Execute_Empty_Union(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.SetBit("i", "general", 10, 0)

	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Union()`), nil, nil); err != nil {
		t.Fatal(err)
	} else if columns := res[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure a xor query can be executed.
func TestExecutor_Execute_Xor(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.SetBit("i", "general", 10, 0)
	hldr.SetBit("i", "general", 10, SliceWidth+1)
	hldr.SetBit("i", "general", 10, SliceWidth+2)

	hldr.SetBit("i", "general", 11, 2)
	hldr.SetBit("i", "general", 11, SliceWidth+2)

	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Xor(Row(general=10), Row(general=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if columns := res[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{0, 2, SliceWidth + 1}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure a count query can be executed.
func TestExecutor_Execute_Count(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.SetBit("i", "f", 10, 3)
	hldr.SetBit("i", "f", 10, SliceWidth+1)
	hldr.SetBit("i", "f", 10, SliceWidth+2)

	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Count(Row(f=10))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if res[0] != uint64(3) {
		t.Fatalf("unexpected n: %d", res[0])
	}
}

// Ensure a set query can be executed.
func TestExecutor_Execute_Set(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	// set a bit so the view gets created.
	hldr.SetBit("i", "f", 1, 0)

	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if n := hldr.Row("i", "f", 11).Count(); n != 0 {
		t.Fatalf("unexpected bitmap count: %d", n)
	}

	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Set(1, f=11)`), nil, nil); err != nil {
		t.Fatal(err)
	} else {
		if !res[0].(bool) {
			t.Fatalf("expected column changed")
		}
	}

	if n := hldr.Row("i", "f", 11).Count(); n != 1 {
		t.Fatalf("unexpected bitmap count: %d", n)
	}
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Set(1, f=11)`), nil, nil); err != nil {
		t.Fatal(err)
	} else {
		if res[0].(bool) {
			t.Fatalf("expected column unchanged")
		}
	}
}

// Ensure old PQL syntax doesn't break anything too badly.
func TestExecutor_Execute_OldPQL(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	// set a bit so the view gets created.
	hldr.SetBit("i", "f", 1, 0)

	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))

	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetBit(frame=f, row=11, col=1)`), nil, nil); err == nil || err.Error() != "unknown call: SetBit" {
		t.Fatal("Expected error: 'unknown call: SetBit'")
	}
}

// Ensure a SetValue() query can be executed.
func TestExecutor_Execute_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		hldr := test.MustOpenHolder()
		defer hldr.Close()

		// Create felds.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.FieldOptions{
			Type: pilosa.FieldTypeInt,
			Min:  0,
			Max:  50,
		}); err != nil {
			t.Fatal(err)
		} else if _, err := index.CreateFieldIfNotExists("xxx", pilosa.FieldOptions{}); err != nil {
			t.Fatal(err)
		}

		// Set bsiGroup values.
		e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
		if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetValue(col=10, f=25)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetValue(col=100, f=10)`), nil, nil); err != nil {
			t.Fatal(err)
		}

		f := hldr.Field("i", "f")
		if value, exists, err := f.Value(10); err != nil {
			t.Fatal(err)
		} else if !exists {
			t.Fatal("expected value to exist")
		} else if value != 25 {
			t.Fatalf("unexpected value: %v", value)
		}

		if value, exists, err := f.Value(100); err != nil {
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
		if _, err := index.CreateFieldIfNotExists("f", pilosa.FieldOptions{
			Type: pilosa.FieldTypeInt,
			Min:  0,
			Max:  100,
		}); err != nil {
			t.Fatal(err)
		}

		t.Run("ErrColumnBSIGroupRequired", func(t *testing.T) {
			e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
			if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetValue(invalid_column_name=10, f=100)`), nil, nil); err == nil || err.Error() != `SetValue() column field 'col' required` {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("ErrColumnBSIGroupValue", func(t *testing.T) {
			e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
			if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetValue(invalid_column_name="bad_column", f=100)`), nil, nil); err == nil || err.Error() != `SetValue() column field 'col' required` {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("ErrInvalidBSIGroupValueType", func(t *testing.T) {
			e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
			if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetValue(col=10, f="hello")`), nil, nil); err == nil || err != pilosa.ErrInvalidBSIGroupValueType {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	})
}

// Ensure a SetRowAttrs() query can be executed.
func TestExecutor_Execute_SetRowAttrs(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	// Create fields.
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if _, err := index.CreateFieldIfNotExists("f", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	} else if _, err := index.CreateFieldIfNotExists("xxx", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}

	// Set two attrs on f/10.
	// Also set attrs on other bitmaps and fields to test isolation.
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetRowAttrs(f, 10, foo="bar")`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetRowAttrs(f, 200, YYY=1)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetRowAttrs(xxx, 10, YYY=1)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetRowAttrs(f, 10, baz=123, bat=true)`), nil, nil); err != nil {
		t.Fatal(err)
	}

	f := hldr.Field("i", "f")
	if m, err := f.RowAttrStore().Attrs(10); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"foo": "bar", "baz": int64(123), "bat": true}) {
		t.Fatalf("unexpected bitmap attr: %#v", m)
	}
}

// Ensure a TopN() query can be executed.
func TestExecutor_Execute_TopN(t *testing.T) {
	t.Run("ID", func(t *testing.T) {
		hldr := test.MustOpenHolder()
		defer hldr.Close()
		e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))

		// Set columns for rows 0, 10, & 20 across two slices.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.FieldOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("other", pilosa.FieldOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := e.Execute(context.Background(), "i", test.MustParse(`
			Set(0, f=0)
			Set(1, f=0)
			Set(`+strconv.Itoa(SliceWidth)+`, f=0)
			Set(`+strconv.Itoa(SliceWidth+2)+`, f=0)
			Set(`+strconv.Itoa((5*SliceWidth)+100)+`, f=0)
			Set(0, f=10)
			Set(`+strconv.Itoa(SliceWidth)+`, f=10)
			Set(`+strconv.Itoa(SliceWidth)+`, f=20)
			Set(0, other=0)
		`), nil, nil); err != nil {
			t.Fatal(err)
		}

		hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).RecalculateCache()
		hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).RecalculateCache()
		hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 5).RecalculateCache()

		if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(f, n=2)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result[0], []pilosa.Pair{
			{ID: 0, Count: 5},
			{ID: 10, Count: 2},
		}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("Keys", func(t *testing.T) {
		hldr := test.MustOpenHolder()
		defer hldr.Close()
		e := test.NewExecutor(hldr.Holder, test.NewCluster(1))

		// Set columns for rows 0, 10, & 20 across two slices.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.FieldOptions{Keys: true}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("other", pilosa.FieldOptions{Keys: true}); err != nil {
			t.Fatal(err)
		} else if _, err := e.Execute(context.Background(), "i", test.MustParse(`
			Set("a", f="foo")
			Set("b", f="foo")
			Set("c", f="foo")
			Set("d", f="foo")
			Set("e", f="foo")
			Set("a", f="bar")
			Set("b", f="bar")
			Set("b", f="baz")
			Set("a", other="foo")
		`), nil, nil); err != nil {
			t.Fatal(err)
		}

		hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).RecalculateCache()

		if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(f, n=2)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(result, []interface{}{
			[]pilosa.Pair{
				{Key: "foo", Count: 5},
				{Key: "bar", Count: 2},
			},
		}); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestExecutor_Execute_TopN_fill(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	// Set columns for rows 0, 10, & 20 across two slices.
	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", 0, 2)
	hldr.SetBit("i", "f", 0, SliceWidth)
	hldr.SetBit("i", "f", 1, SliceWidth+2)
	hldr.SetBit("i", "f", 1, SliceWidth)

	// Execute query.
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(f, n=1)`), nil, nil); err != nil {
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

	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, SliceWidth)
	hldr.SetBit("i", "f", 0, 2*SliceWidth)
	hldr.SetBit("i", "f", 0, 3*SliceWidth)
	hldr.SetBit("i", "f", 0, 4*SliceWidth)

	hldr.SetBit("i", "f", 1, 0)
	hldr.SetBit("i", "f", 1, 1)

	hldr.SetBit("i", "f", 2, SliceWidth)
	hldr.SetBit("i", "f", 2, SliceWidth+1)

	hldr.SetBit("i", "f", 3, 2*SliceWidth)
	hldr.SetBit("i", "f", 3, 2*SliceWidth+1)

	hldr.SetBit("i", "f", 4, 3*SliceWidth)
	hldr.SetBit("i", "f", 4, 3*SliceWidth+1)

	// Execute query.
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(f, n=1)`), nil, nil); err != nil {
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

	// Set columns for rows 0, 10, & 20 across two slices.
	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", 0, SliceWidth)
	hldr.SetBit("i", "f", 10, SliceWidth)
	hldr.SetBit("i", "f", 10, SliceWidth+1)
	hldr.SetBit("i", "f", 20, SliceWidth)
	hldr.SetBit("i", "f", 20, SliceWidth+1)
	hldr.SetBit("i", "f", 20, SliceWidth+2)

	// Create an intersecting row.
	hldr.SetBit("i", "other", 100, SliceWidth)
	hldr.SetBit("i", "other", 100, SliceWidth+1)
	hldr.SetBit("i", "other", 100, SliceWidth+2)

	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0).RecalculateCache()
	hldr.MustCreateRankedFragmentIfNotExists("i", "f", pilosa.ViewStandard, 1).RecalculateCache()
	hldr.MustCreateRankedFragmentIfNotExists("i", "other", pilosa.ViewStandard, 1).RecalculateCache()

	// Execute query.
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(f, Row(other=100), n=3)`), nil, nil); err != nil {
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
	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", 10, SliceWidth)

	if err := hldr.Field("i", "f").RowAttrStore().SetAttrs(10, map[string]interface{}{"category": int64(123)}); err != nil {
		t.Fatal(err)
	}
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(f, n=1, attrName="category", attrValues=[123])`), nil, nil); err != nil {
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
	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", 10, SliceWidth)

	if err := hldr.Field("i", "f").RowAttrStore().SetAttrs(10, map[string]interface{}{"category": uint64(123)}); err != nil {
		t.Fatal(err)
	}
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	if result, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(f, Row(f=10), n=1, attrName="category", attrValues=[123])`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{ID: 10, Count: 1},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure Min()  and Max() queries can be executed.
func TestExecutor_Execute_MinMax(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("x", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("f", pilosa.FieldOptions{
		Type: pilosa.FieldTypeInt,
		Min:  -10,
		Max:  100,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := e.Execute(context.Background(), "i", test.MustParse(`
		Set(0, x=0)
		Set(3, x=0)
		Set(`+strconv.Itoa(SliceWidth+1)+`, x=0)
		Set(1, x=1)
		Set(`+strconv.Itoa(SliceWidth+2)+`, x=2)

		SetValue(col=0, f=20)
		SetValue(col=1, f=-5)
		SetValue(col=2, f=-5)
		SetValue(col=3, f=10)
		SetValue(col=`+strconv.Itoa(SliceWidth)+`, f=30)
		SetValue(col=`+strconv.Itoa(SliceWidth+2)+`, f=40)
		SetValue(col=`+strconv.Itoa((5*SliceWidth)+100)+`, f=50)
		SetValue(col=`+strconv.Itoa(SliceWidth+1)+`, f=60)
	`), nil, nil); err != nil {
		t.Fatal(err)
	}

	t.Run("Min", func(t *testing.T) {
		tests := []struct {
			filter string
			exp    int64
			cnt    int64
		}{
			{filter: ``, exp: -5, cnt: 2},
			{filter: `Row(x=0)`, exp: 10, cnt: 1},
			{filter: `Row(x=1)`, exp: -5, cnt: 1},
			{filter: `Row(x=2)`, exp: 40, cnt: 1},
		}
		for i, tt := range tests {
			var pql string
			if tt.filter == "" {
				pql = `Min(field=f)`
			} else {
				pql = fmt.Sprintf(`Min(%s, field=f)`, tt.filter)
			}
			if result, err := e.Execute(context.Background(), "i", test.MustParse(pql), nil, nil); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(result[0], pilosa.ValCount{Val: tt.exp, Count: tt.cnt}) {
				t.Fatalf("unexpected result, test %d: %s", i, spew.Sdump(result))
			}
		}
	})

	t.Run("Max", func(t *testing.T) {
		tests := []struct {
			filter string
			exp    int64
			cnt    int64
		}{
			{filter: ``, exp: 60, cnt: 1},
			{filter: `Row(x=0)`, exp: 60, cnt: 1},
			{filter: `Row(x=1)`, exp: -5, cnt: 1},
			{filter: `Row(x=2)`, exp: 40, cnt: 1},
		}
		for i, tt := range tests {
			var pql string
			if tt.filter == "" {
				pql = `Max(field=f)`
			} else {
				pql = fmt.Sprintf(`Max(%s, field=f)`, tt.filter)
			}
			if result, err := e.Execute(context.Background(), "i", test.MustParse(pql), nil, nil); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(result[0], pilosa.ValCount{Val: tt.exp, Count: tt.cnt}) {
				t.Fatalf("unexpected result, test %d: %s", i, spew.Sdump(result))
			}
		}
	})
}

// Ensure a Sum() query can be executed.
func TestExecutor_Execute_Sum(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("x", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("foo", pilosa.FieldOptions{
		Type: pilosa.FieldTypeInt,
		Min:  10,
		Max:  100,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("bar", pilosa.FieldOptions{
		Type: pilosa.FieldTypeInt,
		Min:  0,
		Max:  100000,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("other", pilosa.FieldOptions{
		Type: pilosa.FieldTypeInt,
		Min:  0,
		Max:  1000,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := e.Execute(context.Background(), "i", test.MustParse(`
		Set(0, x=0)
		Set(`+strconv.Itoa(SliceWidth+1)+`, x=0)

		SetValue(col=0, foo=20)
		SetValue(col=0, bar=2000)
		SetValue(col=`+strconv.Itoa(SliceWidth)+`, foo=30)
		SetValue(col=`+strconv.Itoa(SliceWidth+2)+`, foo=40)
		SetValue(col=`+strconv.Itoa((5*SliceWidth)+100)+`, foo=50)
		SetValue(col=`+strconv.Itoa(SliceWidth+1)+`, foo=60)
		SetValue(col=0, other=1000)
	`), nil, nil); err != nil {
		t.Fatal(err)
	}

	t.Run("NoFilter", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Sum(field=foo)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result[0], pilosa.ValCount{Val: 200, Count: 5}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("WithFilter", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Sum(Row(x=0), field=foo)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result[0], pilosa.ValCount{Val: 80, Count: 2}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})
}

// Ensure a range query can be executed.
func TestExecutor_Execute_Range(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))

	// Create index.
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})

	// Create field.
	if _, err := index.CreateFieldIfNotExists("f", pilosa.FieldOptions{
		Type:        pilosa.FieldTypeTime,
		TimeQuantum: pilosa.TimeQuantum("YMDH"),
	}); err != nil {
		t.Fatal(err)
	}

	// Set columns.
	cc := test.MustParse(`
        Set(2, f=1, 1999-12-31T00:00)
        Set(3, f=1, 2000-01-01T00:00)
        Set(4, f=1, 2000-01-02T00:00)
        Set(5, f=1, 2000-02-01T00:00)
        Set(6, f=1, 2001-01-01T00:00)
        Set(7, f=1, 2002-01-01T02:00)

        Set(2, f=1, 1999-12-30T00:00)
        Set(2, f=1, 2002-02-01T00:00)
        Set(2, f=10, 2001-01-01T00:00)
	`)
	if _, err := e.Execute(context.Background(), "i", cc, nil, nil); err != nil {
		t.Fatal(err)
	}

	t.Run("Standard", func(t *testing.T) {
		if res, err := e.Execute(context.Background(), "i", test.MustParse(`Range(f=1, 1999-12-31T00:00, 2002-01-01T03:00)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if columns := res[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})
}

// Ensure a Range(bsiGroup) query can be executed.
func TestExecutor_Execute_BSIGroupRange(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("f", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("foo", pilosa.FieldOptions{
		Type: pilosa.FieldTypeInt,
		Min:  10,
		Max:  100,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("bar", pilosa.FieldOptions{
		Type: pilosa.FieldTypeInt,
		Min:  0,
		Max:  100000,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("other", pilosa.FieldOptions{
		Type: pilosa.FieldTypeInt,
		Min:  0,
		Max:  1000,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("edge", pilosa.FieldOptions{
		Type: pilosa.FieldTypeInt,
		Min:  -100,
		Max:  100,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := e.Execute(context.Background(), "i", test.MustParse(`
		Set(0, f=0)
		Set(`+strconv.Itoa(SliceWidth+1)+`, f=0)

		SetValue(col=50, foo=20)
		SetValue(col=50, bar=2000)
		SetValue(col=`+strconv.Itoa(SliceWidth)+`, foo=30)
		SetValue(col=`+strconv.Itoa(SliceWidth+2)+`, foo=10)
		SetValue(col=`+strconv.Itoa((5*SliceWidth)+100)+`, foo=20)
		SetValue(col=`+strconv.Itoa(SliceWidth+1)+`, foo=60)
		SetValue(col=0, other=1000)
		SetValue(col=0, edge=100)
		SetValue(col=1, edge=-100)
	`), nil, nil); err != nil {
		t.Fatal(err)
	}

	t.Run("EQ", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(foo == 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, (5 * SliceWidth) + 100}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("NEQ", func(t *testing.T) {
		// NEQ null
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(other != null)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
		// NEQ <int>
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(foo != 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{SliceWidth, SliceWidth + 1, SliceWidth + 2}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
		// NEQ -<int>
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(other != -20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result[0].(*pilosa.Row).Columns()) {
			//t.Fatalf("unexpected result: %s", spew.Sdump(result))
			t.Fatalf("unexpected result: %v", result[0].(*pilosa.Row).Columns())
		}
	})

	t.Run("LT", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(foo < 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{SliceWidth + 2}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTE", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(foo <= 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, SliceWidth + 2, (5 * SliceWidth) + 100}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("GT", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(foo > 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{SliceWidth, SliceWidth + 1}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("GTE", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(foo >= 20)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, SliceWidth, SliceWidth + 1, (5 * SliceWidth) + 100}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BETWEEN", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(0 < other < 1000)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	// Ensure that the NotNull code path gets run.
	t.Run("NotNull", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(-1 < other < 1000)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BelowMin", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(foo == 0)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("AboveMax", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(foo == 200)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTAboveMax", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(edge < 200)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result[0].(*pilosa.Row).Columns()))
		}
	})

	t.Run("GTBelowMin", func(t *testing.T) {
		if result, err := e.Execute(context.Background(), "i", test.MustParse(`Range(edge > -200)`), nil, nil); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result[0].(*pilosa.Row).Columns()))
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		if _, err := e.Execute(context.Background(), "i", test.MustParse(`Range(bad_field >= 20)`), nil, nil); err != pilosa.ErrFieldNotFound {
			t.Fatal(err)
		}
	})
}

// Ensure a remote query can return a row.
func TestExecutor_Execute_Remote_Row(t *testing.T) {
	t.Skip() // Until test.NewServer() works

	c := pilosa.NewTestCluster(2)

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()

	uri, err := pilosa.NewURIFromAddress(s.Host())
	if err != nil {
		t.Fatal(err)
	}
	c.Nodes[1].URI = *uri

	// Mock secondary server's executor to verify arguments and return a bitmap.
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if index != "i" {
			t.Fatalf("unexpected index: %s", index)
		} else if query.String() != `Row(f=10)` {
			t.Fatalf("unexpected query: %s", query.String())
		} else if !reflect.DeepEqual(slices, []uint64{1}) {
			t.Fatalf("unexpected slices: %+v", slices)
		}

		// Set columns in slice 0 & 2.
		r := pilosa.NewRow(
			(0*SliceWidth)+1,
			(0*SliceWidth)+2,
			(2*SliceWidth)+4,
		)
		return []interface{}{r}, nil
	}

	// Create local executor data.
	// The local node owns slice 1.
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	s.Handler.API.Holder = hldr.Holder
	hldr.SetBit("i", "f", 10, SliceWidth+1)

	e := test.NewExecutor(hldr.Holder, c)
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Row(f=10)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if columns := res[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, 2, 2*SliceWidth + 4}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure a remote query can return a count.
func TestExecutor_Execute_Remote_Count(t *testing.T) {
	t.Skip() // Until test.NewServer() works

	c := pilosa.NewTestCluster(2)

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()

	uri, err := pilosa.NewURIFromAddress(s.Host())
	if err != nil {
		t.Fatal(err)
	}

	c.Nodes[1].URI = *uri

	// Mock secondary server's executor to return a count.
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{uint64(10)}, nil
	}

	// Create local executor data. The local node owns slice 1.
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	s.Handler.API.Holder = hldr.Holder
	hldr.SetBit("i", "f", 10, (2*SliceWidth)+1)
	hldr.SetBit("i", "f", 10, (2*SliceWidth)+2)

	e := test.NewExecutor(hldr.Holder, c)
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`Count(Row(f=10))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if res[0] != uint64(12) {
		t.Fatalf("unexpected n: %d", res[0])
	}
}

// Ensure a remote query can set columns on multiple nodes.
func TestExecutor_Execute_Remote_SetBit(t *testing.T) {
	t.Skip() // Until test.NewServer() works

	c := pilosa.NewTestCluster(2)
	c.ReplicaN = 2

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()

	uri, err := pilosa.NewURIFromAddress(s.Host())
	if err != nil {
		t.Fatal(err)
	}

	c.Nodes[1].URI = *uri

	// Mock secondary server's executor to verify arguments.
	var remoteCalled bool
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if index != `i` {
			t.Fatalf("unexpected index: %s", index)
		} else if query.String() != `Set(_col=2, f=10)` {
			t.Fatalf("unexpected query: %s", query.String())
		}
		remoteCalled = true
		return []interface{}{nil}, nil
	}

	// Create local executor data.
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	s.Handler.API.Holder = hldr.Holder

	// Create field.
	if _, err := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{}).CreateField("f", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}

	e := test.NewExecutor(hldr.Holder, c)
	cc := test.MustParse("Set(2, f=10)")
	if _, err := e.Execute(context.Background(), "i", cc, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Verify that one column is set on both node's holder.
	if n := hldr.Row("i", "f", 10).Count(); n != 1 {
		t.Fatalf("unexpected local count: %d", n)
	}
	if !remoteCalled {
		t.Fatalf("expected remote execution")
	}
}

// Ensure a remote query can set columns on multiple nodes.
func TestExecutor_Execute_Remote_SetBit_With_Timestamp(t *testing.T) {
	t.Skip() // Until test.NewServer() works

	c := pilosa.NewTestCluster(2)
	c.ReplicaN = 2

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()

	uri, err := pilosa.NewURIFromAddress(s.Host())
	if err != nil {
		t.Fatal(err)
	}

	c.Nodes[1].URI = *uri

	// Mock secondary server's executor to verify arguments.
	var remoteCalled bool
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if index != `i` {
			t.Fatalf("unexpected index: %s", index)
		} else if query.String() != `Set(_col=2, _timestamp="2016-12-11T10:09", f=10)` {
			t.Fatalf("unexpected query: %s", query.String())
		}
		remoteCalled = true
		return []interface{}{nil}, nil
	}

	// Create local executor data.
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	s.Handler.API.Holder = hldr.Holder

	// Create field.
	if f, err := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{}).CreateField("f", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	} else if err := f.SetTimeQuantum("Y"); err != nil {
		t.Fatal(err)
	}

	e := test.NewExecutor(hldr.Holder, c)
	cc := test.MustParse(`Set(2, f=10, 2016-12-11T10:09)`)
	if _, err := e.Execute(context.Background(), "i", cc, nil, nil); err != nil {
		t.Fatal(err)
	}

	// Verify that one column is set on both node's holder.
	if n := hldr.ViewRow("i", "f", "standard_2016", 10).Count(); n != 1 {
		t.Fatalf("unexpected local count: %d", n)
	}
	if !remoteCalled {
		t.Fatalf("expected remote execution")
	}
}

// Ensure a remote query can return a top-n query.
func TestExecutor_Execute_Remote_TopN(t *testing.T) {
	t.Skip() // Until test.NewServer() works

	c := pilosa.NewTestCluster(2)

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()

	uri, err := pilosa.NewURIFromAddress(s.Host())
	if err != nil {
		t.Fatal(err)
	}

	c.Nodes[1].URI = *uri

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
			if query.String() != `TopN(_field="f", n=3)` {
				t.Fatalf("unexpected query(0): %s", query.String())
			}
		case 1:
			if query.String() != `TopN(_field="f", ids=[0,10,30], n=3)` {
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
	s.Handler.API.Holder = hldr.Holder
	hldr.SetBit("i", "f", 30, (2*SliceWidth)+1)
	hldr.SetBit("i", "f", 30, (4*SliceWidth)+2)

	e := test.NewExecutor(hldr.Holder, c)
	if res, err := e.Execute(context.Background(), "i", test.MustParse(`TopN(f, n=3)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(res, []interface{}{[]pilosa.Pair{
		{ID: 0, Count: 5},
		{ID: 30, Count: 4},
		{ID: 10, Count: 2},
	}}) {
		t.Fatalf("unexpected results: %s", spew.Sdump(res))
	}
}

// Ensure a remote query can set RowAttrs
func TestExecutor_Execute_Remote_SetRowAttrs(t *testing.T) {
	t.Skip("test.NewServer broken")
	c := pilosa.NewTestCluster(2)

	// Create secondary server and update second cluster node.
	s := test.NewServer()
	defer s.Close()

	uri, err := pilosa.NewURIFromAddress(s.Host())
	if err != nil {
		t.Fatal(err)
	}
	c.Nodes[1].URI = *uri

	// Mock secondary server's executor to verify arguments and return a bitmap.
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if index != "i" {
			t.Fatalf("unexpected index: %s", index)
		} else if query.String() != `SetRowAttrs(_field="f", _row=10, bat=true, baz=123)` {
			t.Fatalf("unexpected query: %s", query.String())
		}

		return []interface{}{}, nil
	}

	// Create local executor data.
	// The local node owns slice 1.
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if _, err := index.CreateFieldIfNotExists("f", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}
	f := hldr.Field("i", "f")
	s.Handler.API.Holder = hldr.Holder
	hldr.SetBit("i", "f", 10, SliceWidth+1)

	e := test.NewExecutor(hldr.Holder, c)
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`SetRowAttrs(f, 10, baz=123, bat=true)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if m, err := f.RowAttrStore().Attrs(10); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"bat": true, "baz": int64(123)}) {
		t.Fatalf("unexpected bitmap attr: %#v", m)

	}
}

// Ensure executor returns an error if too many writes are in a single request.
func TestExecutor_Execute_ErrMaxWritesPerRequest(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	e.MaxWritesPerRequest = 3
	if _, err := e.Execute(context.Background(), "i", test.MustParse(`Set() Clear() Set() Set()`), nil, nil); err != pilosa.ErrTooManyWrites {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure SetColumnAttrs doesn't save `field` as an attribute
func TestExecutor_SetColumnAttrs_ExcludeField(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	index.CreateField("f", pilosa.FieldOptions{})
	targetAttrs := map[string]interface{}{
		"foo": "bar",
	}
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))

	// SetColumnAttrs call should exclude the field attribute
	_, err := e.Execute(context.Background(), "i", test.MustParse("Set(10, f=1)"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Execute(context.Background(), "i", test.MustParse("SetColumnAttrs(10, foo='bar')"), nil, nil)
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

	// SetColumnAttrs call should not break if field is not specified
	_, err = e.Execute(context.Background(), "i", test.MustParse("Set(20, f=10)"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Execute(context.Background(), "i", test.MustParse("SetColumnAttrs(20, foo='bar')"), nil, nil)
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
