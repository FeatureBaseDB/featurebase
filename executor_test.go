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
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/server"
	"github.com/pilosa/pilosa/test"
	"github.com/pkg/errors"
)

// Ensure a row query can be executed.
func TestExecutor_Execute_Row(t *testing.T) {
	t.Run("Row", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		f, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}

		// Set bits.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `` +
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 20),
		}); err != nil {
			t.Fatal(err)
		}
		if err := f.RowAttrStore().SetAttrs(10, map[string]interface{}{"foo": "bar", "baz": uint64(123)}); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := res.Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123)}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}

		// Inhibit column attributes.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`, ExcludeColumns: true}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", columns)
		} else if attrs := res.Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123)}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}

		// Inhibit row attributes.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`, ExcludeRowAttrs: true}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{3, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", columns)
		} else if attrs := res.Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})

	t.Run("Column", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		// Set bits.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `` +
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 20),
		}); err != nil {
			t.Fatal(err)
		}
		if err := index.ColumnAttrStore().SetAttrs(ShardWidth+1, map[string]interface{}{"foo": "bar", "baz": uint64(123)}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Keys", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{Keys: true})
		if _, err := index.CreateField("f", pilosa.OptFieldTypeDefault(), pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		}

		_, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "i",
			Query: `` +
				`Set("foo", f="bar")` + "\n" +
				`Set("foo", f="baz")` + "\n" +
				`Set("bat", f="bar")` + "\n" +
				`Set("aaa", f="bbb")` + "\n",
		})
		if err != nil {
			t.Fatalf("querying: %v", err)
		}

		if results, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "i",
			Query: `Row(f="bar")`,
		}); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(results.Results, []interface{}{
			&pilosa.Row{Keys: []string{"foo", "bat"}, Attrs: map[string]interface{}{}},
		}, cmpopts.IgnoreUnexported(pilosa.Row{})); diff != "" {
			t.Fatal(diff)
		}
	})
}

// Ensure a difference query can be executed.
func TestExecutor_Execute_Difference(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}
	hldr.SetBit("i", "general", 10, 1)
	hldr.SetBit("i", "general", 10, 2)
	hldr.SetBit("i", "general", 10, 3)
	hldr.SetBit("i", "general", 11, 2)
	hldr.SetBit("i", "general", 11, 4)

	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Difference(Row(general=10), Row(general=11))`}); err != nil {
		t.Fatal(err)
	} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, 3}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure an empty difference query behaves properly.
func TestExecutor_Execute_Empty_Difference(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}
	hldr.SetBit("i", "general", 10, 1)

	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Difference()`}); err == nil {
		t.Fatalf("Empty Difference query should give error, but got %v", res)
	}
}

// Ensure an intersect query can be executed.
func TestExecutor_Execute_Intersect(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}
	hldr.SetBit("i", "general", 10, 1)
	hldr.SetBit("i", "general", 10, ShardWidth+1)
	hldr.SetBit("i", "general", 10, ShardWidth+2)

	hldr.SetBit("i", "general", 11, 1)
	hldr.SetBit("i", "general", 11, 2)
	hldr.SetBit("i", "general", 11, ShardWidth+2)

	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Intersect(Row(general=10), Row(general=11))`}); err != nil {
		t.Fatal(err)
	} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, ShardWidth + 2}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure an empty intersect query behaves properly.
func TestExecutor_Execute_Empty_Intersect(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Intersect()`}); err == nil {
		t.Fatalf("Empty Intersect query should give error, but got %v", res)
	}
}

// Ensure a union query can be executed.
func TestExecutor_Execute_Union(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}
	hldr.SetBit("i", "general", 10, 0)
	hldr.SetBit("i", "general", 10, ShardWidth+1)
	hldr.SetBit("i", "general", 10, ShardWidth+2)

	hldr.SetBit("i", "general", 11, 2)
	hldr.SetBit("i", "general", 11, ShardWidth+2)

	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Union(Row(general=10), Row(general=11))`}); err != nil {
		t.Fatal(err)
	} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{0, 2, ShardWidth + 1, ShardWidth + 2}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure an empty union query behaves properly.
func TestExecutor_Execute_Empty_Union(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}
	hldr.SetBit("i", "general", 10, 0)

	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Union()`}); err != nil {
		t.Fatal(err)
	} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure a xor query can be executed.
func TestExecutor_Execute_Xor(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	hldr.SetBit("i", "general", 10, 0)
	hldr.SetBit("i", "general", 10, ShardWidth+1)
	hldr.SetBit("i", "general", 10, ShardWidth+2)

	hldr.SetBit("i", "general", 11, 2)
	hldr.SetBit("i", "general", 11, ShardWidth+2)

	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Xor(Row(general=10), Row(general=11))`}); err != nil {
		t.Fatal(err)
	} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{0, 2, ShardWidth + 1}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}
}

// Ensure a count query can be executed.
func TestExecutor_Execute_Count(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	hldr.SetBit("i", "f", 10, 3)
	hldr.SetBit("i", "f", 10, ShardWidth+1)
	hldr.SetBit("i", "f", 10, ShardWidth+2)

	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Count(Row(f=10))`}); err != nil {
		t.Fatal(err)
	} else if res.Results[0] != uint64(3) {
		t.Fatalf("unexpected n: %d", res.Results[0])
	}
}

// Ensure a set query can be executed.
func TestExecutor_Execute_SetBit(t *testing.T) {
	t.Run("ID", func(t *testing.T) {
		cmd := test.MustRunCluster(t, 1)[0]
		holder := cmd.Server.Holder()
		hldr := test.Holder{Holder: holder}
		hldr.SetBit("i", "f", 1, 0)

		t.Run("OK", func(t *testing.T) {
			hldr.ClearBit("i", "f", 11, 1)
			if n := hldr.Row("i", "f", 11).Count(); n != 0 {
				t.Fatalf("unexpected row count: %d", n)
			}

			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1, f=11)`}); err != nil {
				t.Fatal(err)
			} else {
				if !res.Results[0].(bool) {
					t.Fatalf("expected column changed")
				}
			}

			if n := hldr.Row("i", "f", 11).Count(); n != 1 {
				t.Fatalf("unexpected row count: %d", n)
			}
			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1, f=11)`}); err != nil {
				t.Fatal(err)
			} else {
				if res.Results[0].(bool) {
					t.Fatalf("expected column unchanged")
				}
			}
		})

		t.Run("ErrInvalidColValueType", func(t *testing.T) {
			if _, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set("foo", f=1)`}); err == nil || errors.Cause(err).Error() != `string 'col' value not allowed unless index 'keys' option enabled` {
				t.Fatalf("The error is: '%v'", err)
			}
		})

		t.Run("ErrInvalidRowValueType", func(t *testing.T) {
			if _, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(2, f="bar")`}); err == nil || errors.Cause(err).Error() != `string 'row' value not allowed unless field 'keys' option enabled` {
				t.Fatal(err)
			}
		})
	})

	t.Run("Keys", func(t *testing.T) {
		cmd := test.MustRunCluster(t, 1)[0]
		holder := cmd.Server.Holder()
		hldr := test.Holder{Holder: holder}
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{Keys: true})

		t.Run("OK", func(t *testing.T) {
			hldr.SetBit("i", "f", 1, 0)
			if n := hldr.Row("i", "f", 11).Count(); n != 0 {
				t.Fatalf("unexpected row count: %d", n)
			}

			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set("foo", f=11)`}); err != nil {
				t.Fatal(err)
			} else {
				if !res.Results[0].(bool) {
					t.Fatalf("expected column changed")
				}
			}

			if n := hldr.Row("i", "f", 11).Count(); n != 1 {
				t.Fatalf("unexpected row count: %d", n)
			}
			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set("foo", f=11)`}); err != nil {
				t.Fatal(err)
			} else {
				if res.Results[0].(bool) {
					t.Fatalf("expected column unchanged")
				}
			}
		})

		t.Run("ErrInvalidColValueType", func(t *testing.T) {
			if err := index.DeleteField("f"); err != nil {
				t.Fatal(err)
			}
			if _, err := index.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
				t.Fatal(err)
			}

			if _, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(2, f=1)`}); err == nil || errors.Cause(err).Error() != `column value must be a string when index 'keys' option enabled` {
				t.Fatal(err)
			}
		})

		t.Run("ErrInvalidRowValueType", func(t *testing.T) {
			index := hldr.MustCreateIndexIfNotExists("inokey", pilosa.IndexOptions{})
			if _, err := index.CreateField("f", pilosa.OptFieldTypeDefault(), pilosa.OptFieldKeys()); err != nil {
				t.Fatal(err)
			}
			if _, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "inokey", Query: `Set(2, f=1)`}); err == nil || errors.Cause(err).Error() != `row value must be a string when field 'keys' option enabled` {
				t.Fatal(err)
			}
		})
	})
}

// Ensure a set query can be executed on a bool field.
func TestExecutor_Execute_SetBool(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Create fields.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeBool()); err != nil {
			t.Fatal(err)
		}

		// Set a true bit.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f=true)`}); err != nil {
			t.Fatal(err)
		} else if !res.Results[0].(bool) {
			t.Fatalf("expected column changed")
		}

		// Set the same bit to true again verify nothing changed.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f=true)`}); err != nil {
			t.Fatal(err)
		} else if res.Results[0].(bool) {
			t.Fatalf("expected column to be unchanged")
		}

		// Set the same bit to false.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f=false)`}); err != nil {
			t.Fatal(err)
		} else if !res.Results[0].(bool) {
			t.Fatalf("expected column changed")
		}

		// Ensure that the false row is set.
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=false)`}); err != nil {
			t.Fatal(err)
		} else if columns := result.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{100}) {
			t.Fatalf("unexpected colums: %+v", columns)
		}

		// Ensure that the true row is empty.
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=true)`}); err != nil {
			t.Fatal(err)
		} else if columns := result.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{}) {
			t.Fatalf("unexpected colums: %+v", columns)
		}
	})
	t.Run("Error", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Create fields.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeBool()); err != nil {
			t.Fatal(err)
		}

		// Set bool using a string value.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f="true")`}); err == nil {
			t.Fatalf("expected invalid bool type error")
		}

		// Set bool using an integer.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f=1)`}); err == nil {
			t.Fatalf("expected invalid bool type error")
		}

	})
}

// Ensure old PQL syntax doesn't break anything too badly.
func TestExecutor_Execute_OldPQL(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	// set a bit so the view gets created.
	hldr.SetBit("i", "f", 1, 0)

	if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `SetBit(frame=f, row=11, col=1)`}); err == nil || errors.Cause(err).Error() != "unknown call: SetBit" {
		t.Fatalf("Expected error: 'unknown call: SetBit', got: %v. Full: %v", errors.Cause(err), err)
	}
}

// Ensure a SetValue() query can be executed.
func TestExecutor_Execute_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Create fields.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeInt(0, 50)); err != nil {
			t.Fatal(err)
		} else if _, err := index.CreateFieldIfNotExists("xxx", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		// Set bsiGroup values.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(10, f=25)`}); err != nil {
			t.Fatal(err)
		} else if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f=10)`}); err != nil {
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
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeInt(0, 100)); err != nil {
			t.Fatal(err)
		}

		t.Run("ErrColumnBSIGroupRequired", func(t *testing.T) {
			if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(invalid_column_name=10, f=100)`}); err == nil || errors.Cause(err).Error() != `Set() column argument 'col' required` {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("ErrColumnBSIGroupValue", func(t *testing.T) {
			if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set("bad_column", f=100)`}); err == nil || errors.Cause(err).Error() != `string 'col' value not allowed unless index 'keys' option enabled` {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("ErrInvalidBSIGroupValueType", func(t *testing.T) {
			if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(10, f="hello")`}); err == nil || errors.Cause(err).Error() != `string 'row' value not allowed unless field 'keys' option enabled` {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	})
}

// Ensure a SetRowAttrs() query can be executed.
func TestExecutor_Execute_SetRowAttrs(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	// Create fields.
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	} else if _, err := index.CreateFieldIfNotExists("xxx", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	} else if _, err := index.CreateFieldIfNotExists("kf", pilosa.OptFieldTypeDefault(), pilosa.OptFieldKeys()); err != nil {
		t.Fatal(err)
	}
	t.Run("rowID", func(t *testing.T) {
		// Set two attrs on f/10.
		// Also set attrs on other rows and fields to test isolation.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `SetRowAttrs(f, 10, foo="bar")`}); err != nil {
			t.Fatal(err)
		}
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `SetRowAttrs(f, 200, YYY=1)`}); err != nil {
			t.Fatal(err)
		}
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `SetRowAttrs(xxx, 10, YYY=1)`}); err != nil {
			t.Fatal(err)
		}
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `SetRowAttrs(f, 10, baz=123, bat=true)`}); err != nil {
			t.Fatal(err)
		}

		f := hldr.Field("i", "f")
		if m, err := f.RowAttrStore().Attrs(10); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(m, map[string]interface{}{"foo": "bar", "baz": int64(123), "bat": true}) {
			t.Fatalf("unexpected row attr: %#v", m)
		}
	})

	t.Run("rowKey", func(t *testing.T) {
		// Set two attrs on f/10.
		// Also set attrs on other rows and fields to test isolation.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `SetRowAttrs(kf, "row10", foo="bar")`}); err != nil {
			t.Fatal(err)
		}
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `SetRowAttrs(kf, "row200", YYY=1)`}); err != nil {
			t.Fatal(err)
		}
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `SetRowAttrs(kf, "row10", baz=123, bat=true)`}); err != nil {
			t.Fatal(err)
		}

		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(kf="row10")`}); err != nil {
			t.Fatal(err)
		} else if attrs := result.Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123), "bat": true}) {
			t.Fatalf("unexpected attrs: %+v", attrs)
		}
	})
}

// Ensure a TopN() query can be executed.
func TestExecutor_Execute_TopN(t *testing.T) {
	t.Run("ID", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("other", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set(0, f=0)
			Set(1, f=0)
			Set(` + strconv.Itoa(ShardWidth) + `, f=0)
			Set(` + strconv.Itoa(ShardWidth+2) + `, f=0)
			Set(` + strconv.Itoa((5*ShardWidth)+100) + `, f=0)
			Set(0, f=10)
			Set(` + strconv.Itoa(ShardWidth) + `, f=10)
			Set(` + strconv.Itoa(ShardWidth) + `, f=20)
			Set(0, other=0)
		`}); err != nil {
			t.Fatal(err)
		}

		err := c[0].RecalculateCaches()
		if err != nil {
			t.Fatalf("recalculating caches: %v", err)
		}

		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=2)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result.Results[0], []pilosa.Pair{
			{ID: 0, Count: 5},
			{ID: 10, Count: 2},
		}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("Keys", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault(), pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("other", pilosa.OptFieldTypeDefault(), pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		} else if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set("a", f="foo")
			Set("b", f="foo")
			Set("c", f="foo")
			Set("d", f="foo")
			Set("e", f="foo")
			Set("a", f="bar")
			Set("b", f="bar")
			Set("b", f="baz")
			Set("a", other="foo")
		`}); err != nil {
			t.Fatal(err)
		}

		err := c[0].RecalculateCaches()
		if err != nil {
			t.Fatalf("recalculating caches: %v", err)
		}

		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=2)`}); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(result.Results, []interface{}{
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
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	// Set columns for rows 0, 10, & 20 across two shards.
	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", 0, 2)
	hldr.SetBit("i", "f", 0, ShardWidth)
	hldr.SetBit("i", "f", 1, ShardWidth+2)
	hldr.SetBit("i", "f", 1, ShardWidth)

	// Execute query.
	if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=1)`}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result.Results, []interface{}{[]pilosa.Pair{
		{ID: 0, Count: 4},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure
func TestExecutor_Execute_TopN_fill_small(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, ShardWidth)
	hldr.SetBit("i", "f", 0, 2*ShardWidth)
	hldr.SetBit("i", "f", 0, 3*ShardWidth)
	hldr.SetBit("i", "f", 0, 4*ShardWidth)

	hldr.SetBit("i", "f", 1, 0)
	hldr.SetBit("i", "f", 1, 1)

	hldr.SetBit("i", "f", 2, ShardWidth)
	hldr.SetBit("i", "f", 2, ShardWidth+1)

	hldr.SetBit("i", "f", 3, 2*ShardWidth)
	hldr.SetBit("i", "f", 3, 2*ShardWidth+1)

	hldr.SetBit("i", "f", 4, 3*ShardWidth)
	hldr.SetBit("i", "f", 4, 3*ShardWidth+1)

	// Execute query.
	if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=1)`}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result.Results, []interface{}{[]pilosa.Pair{
		{ID: 0, Count: 5},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure a TopN() query with a source row can be executed.
func TestExecutor_Execute_TopN_Src(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	// Set columns for rows 0, 10, & 20 across two shards.
	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", 0, ShardWidth)
	hldr.SetBit("i", "f", 10, ShardWidth)
	hldr.SetBit("i", "f", 10, ShardWidth+1)
	hldr.SetBit("i", "f", 20, ShardWidth)
	hldr.SetBit("i", "f", 20, ShardWidth+1)
	hldr.SetBit("i", "f", 20, ShardWidth+2)

	// Create an intersecting row.
	hldr.SetBit("i", "other", 100, ShardWidth)
	hldr.SetBit("i", "other", 100, ShardWidth+1)
	hldr.SetBit("i", "other", 100, ShardWidth+2)

	err := c[0].RecalculateCaches()
	if err != nil {
		t.Fatalf("recalculating caches: %v", err)
	}

	// Execute query.
	if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, Row(other=100), n=3)`}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result.Results, []interface{}{[]pilosa.Pair{
		{ID: 20, Count: 3},
		{ID: 10, Count: 2},
		{ID: 0, Count: 1},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

//Ensure TopN handles Attribute filters
func TestExecutor_Execute_TopN_Attr(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}
	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", 10, ShardWidth)

	if err := hldr.Field("i", "f").RowAttrStore().SetAttrs(10, map[string]interface{}{"category": int64(123)}); err != nil {
		t.Fatal(err)
	}
	if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=1, attrName="category", attrValues=[123])`}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result.Results, []interface{}{[]pilosa.Pair{
		{ID: 10, Count: 1},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}

}

//Ensure TopN handles Attribute filters with source row
func TestExecutor_Execute_TopN_Attr_Src(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", 10, ShardWidth)

	if err := hldr.Field("i", "f").RowAttrStore().SetAttrs(10, map[string]interface{}{"category": uint64(123)}); err != nil {
		t.Fatal(err)
	}
	if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, Row(f=10), n=1, attrName="category", attrValues=[123])`}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result.Results, []interface{}{[]pilosa.Pair{
		{ID: 10, Count: 1},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure Min()  and Max() queries can be executed.
func TestExecutor_Execute_MinMax(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("x", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("f", pilosa.OptFieldTypeInt(-10, 100)); err != nil {
		t.Fatal(err)
	}

	if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
		Set(0, x=0)
		Set(3, x=0)
		Set(` + strconv.Itoa(ShardWidth+1) + `, x=0)
		Set(1, x=1)
		Set(` + strconv.Itoa(ShardWidth+2) + `, x=2)

		Set(0, f=20)
		Set(1, f=-5)
		Set(2, f=-5)
		Set(3, f=10)
		Set(` + strconv.Itoa(ShardWidth) + `, f=30)
		Set(` + strconv.Itoa(ShardWidth+2) + `, f=40)
		Set(` + strconv.Itoa((5*ShardWidth)+100) + `, f=50)
		Set(` + strconv.Itoa(ShardWidth+1) + `, f=60)
	`}); err != nil {
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
			if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: tt.exp, Count: tt.cnt}) {
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
			if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: tt.exp, Count: tt.cnt}) {
				t.Fatalf("unexpected result, test %d: %s", i, spew.Sdump(result))
			}
		}
	})
}

// Ensure a Sum() query can be executed.
func TestExecutor_Execute_Sum(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("x", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("foo", pilosa.OptFieldTypeInt(10, 100)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("bar", pilosa.OptFieldTypeInt(0, 100000)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("other", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
		Set(0, x=0)
		Set(` + strconv.Itoa(ShardWidth+1) + `, x=0)

		Set(0, foo=20)
		Set(0, bar=2000)
		Set(` + strconv.Itoa(ShardWidth) + `, foo=30)
		Set(` + strconv.Itoa(ShardWidth+2) + `, foo=40)
		Set(` + strconv.Itoa((5*ShardWidth)+100) + `, foo=50)
		Set(` + strconv.Itoa(ShardWidth+1) + `, foo=60)
		Set(0, other=1000)
	`}); err != nil {
		t.Fatal(err)
	}

	t.Run("NoFilter", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(field=foo)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: 200, Count: 5}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("WithFilter", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(Row(x=0), field=foo)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: 80, Count: 2}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})
}

// Ensure a range query can be executed.
func TestExecutor_Execute_Range(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	// Create index.
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})

	// Create field.
	if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH"))); err != nil {
		t.Fatal(err)
	}

	// Set columns.
	cc := `
        Set(2, f=1, 1999-12-31T00:00)
        Set(3, f=1, 2000-01-01T00:00)
        Set(4, f=1, 2000-01-02T00:00)
        Set(5, f=1, 2000-02-01T00:00)
        Set(6, f=1, 2001-01-01T00:00)
        Set(7, f=1, 2002-01-01T02:00)

        Set(2, f=1, 1999-12-30T00:00)
        Set(2, f=1, 2002-02-01T00:00)
        Set(2, f=10, 2001-01-01T00:00)
	`
	if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: cc}); err != nil {
		t.Fatal(err)
	}

	t.Run("Standard", func(t *testing.T) {
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(f=1, 1999-12-31T00:00, 2002-01-01T03:00)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("Clear", func(t *testing.T) {
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Clear( 2, f=1)`}); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(f=1, 1999-12-31T00:00, 2002-01-01T03:00)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{3, 4, 5, 6, 7}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})
}

// Ensure a Range(bsiGroup) query can be executed.
func TestExecutor_Execute_BSIGroupRange(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("foo", pilosa.OptFieldTypeInt(10, 100)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("bar", pilosa.OptFieldTypeInt(0, 100000)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("other", pilosa.OptFieldTypeInt(0, 1000)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("edge", pilosa.OptFieldTypeInt(-100, 100)); err != nil {
		t.Fatal(err)
	}

	if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
		Set(0, f=0)
		Set(` + strconv.Itoa(ShardWidth+1) + `, f=0)

		Set(50, foo=20)
		Set(50, bar=2000)
		Set(` + strconv.Itoa(ShardWidth) + `, foo=30)
		Set(` + strconv.Itoa(ShardWidth+2) + `, foo=10)
		Set(` + strconv.Itoa((5*ShardWidth)+100) + `, foo=20)
		Set(` + strconv.Itoa(ShardWidth+1) + `, foo=60)
		Set(0, other=1000)
		Set(0, edge=100)
		Set(1, edge=-100)
	`}); err != nil {
		t.Fatal(err)
	}

	t.Run("EQ", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo == 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, (5 * ShardWidth) + 100}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("NEQ", func(t *testing.T) {
		// NEQ null
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(other != null)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
		// NEQ <int>
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo != 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth, ShardWidth + 1, ShardWidth + 2}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
		// NEQ -<int>
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(other != -20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			//t.Fatalf("unexpected result: %s", spew.Sdump(result))
			t.Fatalf("unexpected result: %v", result.Results[0].(*pilosa.Row).Columns())
		}
	})

	t.Run("LT", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo < 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth + 2}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTE", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo <= 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, ShardWidth + 2, (5 * ShardWidth) + 100}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("GT", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo > 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth, ShardWidth + 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("GTE", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo >= 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, ShardWidth, ShardWidth + 1, (5 * ShardWidth) + 100}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BETWEEN", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(0 < other < 1000)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	// Ensure that the NotNull code path gets run.
	t.Run("NotNull", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(-1 < other < 1000)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BelowMin", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo == 0)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("AboveMax", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo == 200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTAboveMax", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(edge < 200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result.Results[0].(*pilosa.Row).Columns()))
		}
	})

	t.Run("GTBelowMin", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(edge > -200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result.Results[0].(*pilosa.Row).Columns()))
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(bad_field >= 20)`}); errors.Cause(err) != pilosa.ErrFieldNotFound {
			t.Fatal(err)
		}
	})
}

// Ensure a remote query can return a row.
func TestExecutor_Execute_Remote_Row(t *testing.T) {
	c := test.MustRunCluster(t, 2,
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node0"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node1"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
	)
	defer c.Close()
	hldr0 := test.Holder{Holder: c[0].Server.Holder()}
	hldr1 := test.Holder{Holder: c[1].Server.Holder()}

	_, err := c[0].API.CreateIndex(context.Background(), "i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = c[0].API.CreateField(context.Background(), "i", "f", pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, pilosa.DefaultCacheSize))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}

	hldr1.MustSetBits("i", "f", 10, ShardWidth+1, ShardWidth+2, (3*ShardWidth)+4)
	hldr0.SetBit("i", "f", 10, 1)

	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
		t.Fatal(err)
	} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, ShardWidth + 1, ShardWidth + 2, (3 * ShardWidth) + 4}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}

	t.Run("Count", func(t *testing.T) {
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Count(Row(f=10))`}); err != nil {
			t.Fatal(err)
		} else if res.Results[0] != uint64(4) {
			t.Fatalf("unexpected n: %d", res.Results[0])
		}
	})

	t.Run("Remote SetBit", func(t *testing.T) {
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1500000, f=7)`}); err != nil {
			t.Fatalf("quuerying remote: %v", err)
		}

		if !reflect.DeepEqual(hldr1.Row("i", "f", 7).Columns(), []uint64{1500000}) {
			t.Fatalf("unexpected cols from row 7: %v", hldr1.Row("i", "f", 7).Columns())
		}
	})

	t.Run("remote with timestamp", func(t *testing.T) {
		_, err = c[0].API.CreateField(context.Background(), "i", "z", pilosa.OptFieldTypeTime("Y"))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1500000, z=5, 2010-07-08T00:00)`}); err != nil {
			t.Fatalf("quuerying remote: %v", err)
		}

		if !reflect.DeepEqual(hldr1.RowTime("i", "z", 5, time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC), "Y").Columns(), []uint64{1500000}) {
			t.Fatalf("unexpected cols from row 7: %v", hldr1.RowTime("i", "z", 5, time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC), "Y").Columns())
		}
	})

	t.Run("remote topn", func(t *testing.T) {
		_, err = c[0].API.CreateField(context.Background(), "i", "fn", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 100))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
Set(500001, fn=5)
Set(1500001, fn=5)
Set(2500001, fn=5)
Set(3500001, fn=5)
Set(1500001, fn=3)
Set(1500002, fn=3)
Set(3500003, fn=3)
Set(500001, fn=4)
Set(4500001, fn=4)
`}); err != nil {
			t.Fatalf("quuerying remote: %v", err)
		}
		err := c[0].API.RecalculateCaches(context.Background())
		if err != nil {
			t.Fatalf("recalcing caches: %v", err)
		}

		if res, err := c[1].API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "i",
			Query: `TopN(fn, n=3)`,
		}); err != nil {
			t.Fatalf("topn querying: %v", err)
		} else if !reflect.DeepEqual(res.Results, []interface{}{[]pilosa.Pair{
			{ID: 5, Count: 4},
			{ID: 3, Count: 3},
			{ID: 4, Count: 2},
		}}) {
			t.Fatalf("topn wrong results: %v", res.Results)
		}
	})

	t.Run("remote setrowattrs", func(t *testing.T) {
		if _, err := c[1].API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "i",
			Query: `SetRowAttrs(_field="f", _row=10, bat=true, baz=123)`,
		}); err != nil {
			t.Fatalf("setrowattrs querying: %v", err)
		} else if attrst, err := hldr0.RowAttrStore("i", "f").Attrs(10); err != nil || !attrst["bat"].(bool) || attrst["baz"].(int64) != 123 {
			t.Fatalf("wrong attrs: %v", attrst)
		}
	})
}

// Ensure executor returns an error if too many writes are in a single request.
func TestExecutor_Execute_ErrMaxWritesPerRequest(t *testing.T) {
	c := test.MustNewCluster(t, 1)
	c[0].Config.MaxWritesPerRequest = 3
	err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	hldr := test.Holder{Holder: c[0].Server.Holder()}
	hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set() Clear() Set() Set()`}); errors.Cause(err) != pilosa.ErrTooManyWrites {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure SetColumnAttrs doesn't save `field` as an attribute
func TestExecutor_SetColumnAttrs_ExcludeField(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	_, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	targetAttrs := map[string]interface{}{
		"foo": "bar",
	}

	// SetColumnAttrs call should exclude the field attribute
	_, err = c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: "Set(10, f=1)"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: "SetColumnAttrs(10, foo='bar')"})
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
	_, err = c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: "Set(20, f=10)"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: "SetColumnAttrs(20, foo='bar')"})
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

func TestExecutor_Time_Clear_Quantums(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	var rangeTests = []struct {
		quantum  pilosa.TimeQuantum
		expected []uint64
	}{
		{quantum: "Y", expected: []uint64{3, 4, 5, 6}},
		{quantum: "M", expected: []uint64{3, 4, 5, 6}},
		{quantum: "D", expected: []uint64{3, 4, 5, 6}},
		{quantum: "H", expected: []uint64{3, 4, 5, 6, 7}},
		{quantum: "YM", expected: []uint64{3, 4, 5, 6}},
		{quantum: "YMD", expected: []uint64{3, 4, 5, 6}},
		{quantum: "YMDH", expected: []uint64{3, 4, 5, 6, 7}},
		{quantum: "MD", expected: []uint64{3, 4, 5, 6}},
		{quantum: "MDH", expected: []uint64{3, 4, 5, 6, 7}},
		{quantum: "DH", expected: []uint64{3, 4, 5, 6, 7}},
	}
	populateBatch := `
				  Set(2, f=1, 1999-12-31T00:00)
				  Set(3, f=1, 2000-01-01T00:00)
				  Set(4, f=1, 2000-01-02T00:00)
				  Set(5, f=1, 2000-02-01T00:00)
				  Set(6, f=1, 2001-01-01T00:00)
				  Set(7, f=1, 2002-01-01T02:00)
				  Set(2, f=1, 1999-12-30T00:00)
				  Set(2, f=1, 2002-02-01T00:00)
				  Set(2, f=10, 2001-01-01T00:00)
			`
	clearColumn := `Clear( 2, f=1)`
	rangeCheckQuery := `Range(f=1, 1999-12-31T00:00, 2002-01-01T03:00)`

	for i, tt := range rangeTests {
		t.Run(fmt.Sprintf("#%d Quantum %s", i+1, tt.quantum), func(t *testing.T) {
			// Create index.
			indexName := strings.ToLower(string(tt.quantum))
			index := hldr.MustCreateIndexIfNotExists(indexName, pilosa.IndexOptions{})
			// Create field.
			if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeTime(tt.quantum)); err != nil {
				t.Fatal(err)
			}
			// Populate
			if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: indexName, Query: populateBatch}); err != nil {
				t.Fatal(err)
			}
			if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: indexName, Query: clearColumn}); err != nil {
				t.Fatal(err)
			}
			if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: indexName, Query: rangeCheckQuery}); err != nil {
				t.Fatal(err)
			} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, tt.expected) {
				t.Fatalf("unexpected columns: %+v", columns)
			}

		})
	}

}

func TestExecutor_QueryCall(t *testing.T) {
	t.Run("excludeRowAttrs", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
				Set(100, f=10)
				SetRowAttrs(f, 10, foo="bar")
			`}); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Options(Row(f=10), excludeRowAttrs=true)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{100}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := res.Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})

	t.Run("excludeColumns", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
				Set(100, f=10)
				SetRowAttrs(f, 10, foo="bar")
			`}); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Options(Row(f=10), excludeColumns=true)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := res.Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar"}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})

	t.Run("columnAttrs", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set(100, f=10)
			SetColumnAttrs(100, foo="bar")
		`}); err != nil {
			t.Fatal(err)
		}

		targetColAttrSets := []*pilosa.ColumnAttrSet{
			{ID: 100, Attrs: map[string]interface{}{"foo": "bar"}},
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Options(Row(f=10), columnAttrs=true)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{100}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := res.ColumnAttrSets; !reflect.DeepEqual(attrs, targetColAttrSets) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})

	t.Run("shards", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf(`
				Set(100, f=10)
				Set(%d, f=10)
				Set(%d, f=10)
			`, ShardWidth, ShardWidth*2)}); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Options(Row(f=10), shards=[0, 2])`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{100, ShardWidth * 2}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})

	t.Run("multipleOpt", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
				Set(100, f=10)
				SetRowAttrs(f, 10, foo="bar")
			`}); err != nil {
			t.Fatal(err)
		}

		req := &pilosa.QueryRequest{
			Index: "i",
			Query: `Options(Row(f=10), excludeColumns=true)Options(Row(f=10), excludeRowAttrs=true)`,
		}
		if res, err := c[0].API.Query(context.Background(), req); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := res.Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar"}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		} else if bits := res.Results[1].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{100}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := res.Results[1].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})
}

// Ensure an existence field is maintained.
func TestExecutor_Execute_Existence(t *testing.T) {
	t.Run("Row", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		_, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}

		// Set bits.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `` +
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+2, 20),
		}); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(exists=0)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1, ShardWidth + 2}) {
			t.Fatalf("unexpected existence columns: %+v", bits)
		}

		// Reopen cluster to ensure existence field is reloaded.
		if err := c[0].Reopen(); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(exists=0)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1, ShardWidth + 2}) {
			t.Fatalf("unexpected existence columns after reopen: %+v", bits)
		}
	})
}

// Ensure a not query can be executed.
func TestExecutor_Execute_Not(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}
	index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
	_, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
	if err != nil {
		t.Fatal(err)
	}

	// Set bits.
	if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `` +
		fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
		fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
		fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+2, 20),
	}); err != nil {
		t.Fatal(err)
	}

	// Populated row.
	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Not(Row(f=20))`}); err != nil {
		t.Fatal(err)
	} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1}) {
		t.Fatalf("unexpected columns: %+v", bits)
	}

	// Populated row.
	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Not(Row(f=0))`}); err != nil {
		t.Fatal(err)
	} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1, ShardWidth + 2}) {
		t.Fatalf("unexpected columns: %+v", bits)
	}

	// All existing.
	if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Not(Union(Row(f=10), Row(f=20)))`}); err != nil {
		t.Fatal(err)
	} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
		t.Fatalf("unexpected columns: %+v", bits)
	}
}

func benchmarkExistence(nn bool, b *testing.B) {
	c := test.MustRunCluster(b, 1)
	defer c.Close()
	hldr := test.Holder{Holder: c[0].Server.Holder()}

	indexName := "i"
	fieldName := "f"

	index := hldr.MustCreateIndexIfNotExists(indexName, pilosa.IndexOptions{TrackExistence: nn})
	// Create field.
	if _, err := index.CreateFieldIfNotExists(fieldName); err != nil {
		b.Fatal(err)
	}

	bitCount := 10000
	req := &pilosa.ImportRequest{
		Index:     indexName,
		Field:     fieldName,
		Shard:     0,
		RowIDs:    make([]uint64, bitCount),
		ColumnIDs: make([]uint64, bitCount),
	}
	for i := 0; i < bitCount; i++ {
		req.RowIDs[i] = uint64(rand.Intn(100000))
		req.ColumnIDs[i] = uint64(rand.Intn(1 << 20))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c[0].API.Import(context.Background(), req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecutor_Existence_True(b *testing.B)  { benchmarkExistence(true, b) }
func BenchmarkExecutor_Existence_False(b *testing.B) { benchmarkExistence(false, b) }
