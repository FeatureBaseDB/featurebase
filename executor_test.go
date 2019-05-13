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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
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

var (
	TempDir = getTempDirString()
)

func getTempDirString() (td *string) {
	tdflag := flag.Lookup("temp-dir")

	if tdflag == nil {
		td = flag.String("temp-dir", "", "Directory in which to place temporary data (e.g. for benchmarking). Useful if you are trying to benchmark different storage configurations.")
	} else {
		s := tdflag.Value.String()
		td = &s
	}
	return td
}

// Ensure a row query can be executed.
func TestExecutor_Execute_Row(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		writeQuery := `` +
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 20) +
			`SetRowAttrs(f, 10, foo="bar", baz=123)` +
			`Set(1000, f=100)` +
			`SetColumnAttrs(1000, foo="bar", baz=123)`
		readQueries := []string{
			`Row(f=10)`,
			`Options(Row(f=10), excludeColumns=true)`,
			`Options(Row(f=10), excludeRowAttrs=true)`,
		}
		responses := runCallTest(t, writeQuery, readQueries, nil)
		if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := responses[0].Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123)}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}

		// Inhibit column attributes.
		if columns := responses[1].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", columns)
		} else if attrs := responses[1].Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": int64(123)}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}

		// Inhibit row attributes.
		if columns := responses[2].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{3, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", columns)
		} else if attrs := responses[2].Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("one-hundred", f=1)
			Set("two-hundred", f=1)`
		readQueries := []string{`Row(f=1)`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true})
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one-hundred", "two-hundred"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		writeQuery := `
			Set(100, f="one")
			Set(200, f="one")`
		readQueries := []string{`Row(f="one")`}
		responses := runCallTest(t, writeQuery, readQueries,
			nil, pilosa.OptFieldKeys())
		if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{100, 200}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		writeQuery := `` +
			`Set("foo", f="bar")` + "\n" +
			`Set("foo", f="baz")` + "\n" +
			`Set("bat", f="bar")` + "\n" +
			`Set("aaa", f="bbb")` + "\n"
		readQueries := []string{`Row(f="bar")`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldKeys())
		if diff := cmp.Diff(responses[0].Results, []interface{}{
			&pilosa.Row{Keys: []string{"foo", "bat"}, Attrs: map[string]interface{}{}},
		}, cmpopts.IgnoreUnexported(pilosa.Row{})); diff != "" {
			t.Fatal(diff)
		}
	})
}

// Ensure a difference query can be executed.
func TestExecutor_Execute_Difference(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
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
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("one", f=10)
			Set("two", f=10)
			Set("three", f=10)
			Set("two", f=11)
			Set("four", f=11)`
		readQueries := []string{`Difference(Row(f=10), Row(f=11))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true})
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one", "three"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		writeQuery := `
			Set(1, f="ten")
			Set(2, f="ten")
			Set(3, f="ten")
			Set(2, f="eleven")
			Set(4, f="eleven")`
		readQueries := []string{`Difference(Row(f="ten"), Row(f="eleven"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			nil, pilosa.OptFieldKeys())
		if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, 3}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("one", f="ten")
			Set("two", f="ten")
			Set("three", f="ten")
			Set("two", f="eleven")
			Set("four", f="eleven")`
		readQueries := []string{`Difference(Row(f="ten"), Row(f="eleven"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldKeys())
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one", "three"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})
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
	t.Run("RowIDColumnID", func(t *testing.T) {
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
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("one", f=10)
			Set("one-hundred", f=10)
			Set("two-hundred", f=10)
			Set("one", f=11)
			Set("two", f=11)
			Set("two-hundred", f=11)`
		readQueries := []string{`Intersect(Row(f=10), Row(f=11))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true})
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one", "two-hundred"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		writeQuery := `
			Set(1, f="ten")
			Set(100, f="ten")
			Set(200, f="ten")
			Set(1, f="eleven")
			Set(2, f="eleven")
			Set(200, f="eleven")`
		readQueries := []string{`Intersect(Row(f="ten"), Row(f="eleven"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			nil, pilosa.OptFieldKeys())
		if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, 200}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("one", f="ten")
			Set("one-hundred", f="ten")
			Set("two-hundred", f="ten")
			Set("one", f="eleven")
			Set("two", f="eleven")
			Set("two-hundred", f="eleven")`
		readQueries := []string{`Intersect(Row(f="ten"), Row(f="eleven"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldKeys())
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one", "two-hundred"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})
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
	t.Run("RowIDColumnID", func(t *testing.T) {
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
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("one", f=10)
			Set("one-hundred", f=10)
			Set("two-hundred", f=10)
			Set("one", f=11)
			Set("two", f=11)
			Set("two-hundred", f=11)`
		readQueries := []string{`Union(Row(f=10), Row(f=11))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true})
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one", "one-hundred", "two-hundred", "two"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		writeQuery := `
			Set(1, f="ten")
			Set(100, f="ten")
			Set(200, f="ten")
			Set(1, f="eleven")
			Set(2, f="eleven")
			Set(200, f="eleven")`
		readQueries := []string{`Union(Row(f="ten"), Row(f="eleven"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			nil, pilosa.OptFieldKeys())
		if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, 2, 100, 200}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("one", f="ten")
			Set("one-hundred", f="ten")
			Set("two-hundred", f="ten")
			Set("one", f="eleven")
			Set("two", f="eleven")
			Set("two-hundred", f="eleven")`
		readQueries := []string{`Union(Row(f="ten"), Row(f="eleven"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldKeys())
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one", "one-hundred", "two-hundred", "two"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})
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
	t.Run("RowIDColumnID", func(t *testing.T) {
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
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("one", f=10)
			Set("one-hundred", f=10)
			Set("two-hundred", f=10)
			Set("one", f=11)
			Set("two", f=11)
			Set("two-hundred", f=11)`
		readQueries := []string{`Xor(Row(f=10), Row(f=11))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true})
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one-hundred", "two"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		writeQuery := `
			Set(1, f="ten")
			Set(100, f="ten")
			Set(200, f="ten")
			Set(1, f="eleven")
			Set(2, f="eleven")
			Set(200, f="eleven")`
		readQueries := []string{`Xor(Row(f="ten"), Row(f="eleven"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			nil, pilosa.OptFieldKeys())
		if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 100}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("one", f="ten")
			Set("one-hundred", f="ten")
			Set("two-hundred", f="ten")
			Set("one", f="eleven")
			Set("two", f="eleven")
			Set("two-hundred", f="eleven")`
		readQueries := []string{`Xor(Row(f="ten"), Row(f="eleven"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldKeys())
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one-hundred", "two"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})
}

// Ensure a count query can be executed.
func TestExecutor_Execute_Count(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
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
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("three", f=10)
			Set("one-hundred", f=10)
			Set("two-hundred", f=11)`
		readQueries := []string{`Count(Row(f=10))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true})
		if responses[0].Results[0] != uint64(2) {
			t.Fatalf("unexpected n: %d", responses[0].Results[0])
		}
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		writeQuery := `
			Set(1, f="ten")
			Set(100, f="ten")
			Set(200, f="eleven")`
		readQueries := []string{`Count(Row(f="ten"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			nil, pilosa.OptFieldKeys())
		if responses[0].Results[0] != uint64(2) {
			t.Fatalf("unexpected n: %d", responses[0].Results[0])
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("one", f="ten")
			Set("one-hundred", f="ten")
			Set("two-hundred", f="eleven")`
		readQueries := []string{`Count(Row(f="ten"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldKeys())
		if responses[0].Results[0] != uint64(2) {
			t.Fatalf("unexpected n: %d", responses[0].Results[0])
		}
	})

}

// Ensure a set query can be executed.
func TestExecutor_Execute_Set(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster[0]
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

	t.Run("RowIDColumnKey", func(t *testing.T) {
		readQueries := []string{`Set("three", f=10)`}
		responses := runCallTest(t, "", readQueries,
			&pilosa.IndexOptions{Keys: true})
		if !responses[0].Results[0].(bool) {
			t.Fatalf("expected column changed")
		}
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		readQueries := []string{`Set(1, f="ten")`}
		responses := runCallTest(t, "", readQueries,
			nil, pilosa.OptFieldKeys())
		if !responses[0].Results[0].(bool) {
			t.Fatalf("expected column changed")
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster[0]
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

// Ensure a set query can be executed.
func TestExecutor_Execute_Clear(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		writeQuery := `Set(3, f=10)`
		readQueries := []string{`Clear(3, f=10)`}
		responses := runCallTest(t, writeQuery, readQueries, nil)
		if !responses[0].Results[0].(bool) {
			t.Fatalf("expected column changed")
		}
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		writeQuery := `Set("three", f=10)`
		readQueries := []string{`Clear("three", f=10)`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true})
		if !responses[0].Results[0].(bool) {
			t.Fatalf("expected column changed")
		}
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		writeQuery := `Set(1, f="ten")`
		readQueries := []string{`Clear(1, f="ten")`}
		responses := runCallTest(t, writeQuery, readQueries,
			nil, pilosa.OptFieldKeys())
		if !responses[0].Results[0].(bool) {
			t.Fatalf("expected column changed")
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		writeQuery := `Set("one", f="ten")`
		readQueries := []string{`Clear("one", f="ten")`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldKeys())
		if !responses[0].Results[0].(bool) {
			t.Fatalf("expected column changed")
		}
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
	t.Run("RowIDColumnID", func(t *testing.T) {
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

	t.Run("RowIDColumnKey", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f"); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("other"); err != nil {
			t.Fatal(err)
		} else if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set("zero", f=0)
			Set("one", f=0)
			Set("sw", f=0)
			Set("sw2", f=0)
			Set("sw3", f=0)
			Set("zero", f=10)
			Set("sw", f=10)
			Set("sw", f=20)
			Set("zero", other=0)
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

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("other", pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		} else if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set("zero", f="zero")
			Set("one", f="zero")
			Set("sw", f="zero")
			Set("sw2", f="zero")
			Set("sw3", f="zero")
			Set("zero", f="ten")
			Set("sw", f="ten")
			Set("sw", f="twenty")
			Set("zero", other="zero")
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
			{Key: "zero", Count: 5},
			{Key: "ten", Count: 2},
		}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
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
	t.Run("ColumnID", func(t *testing.T) {
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
	})

	t.Run("ColumnKey", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true})
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
			Set("zero", x=0)
			Set("three", x=0)
			Set("sw1", x=0)
			Set("one", x=1)
			Set("sw2", x=2)

			Set("zero", f=20)
			Set("one", f=-5)
			Set("two", f=-5)
			Set("three", f=10)
			Set("sw", f=30)
			Set("sw2", f=40)
			Set("sw3", f=50)
			Set("sw1", f=60)
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
	})
}

// Ensure a Sum() query can be executed.
func TestExecutor_Execute_Sum(t *testing.T) {
	t.Run("ColumnID", func(t *testing.T) {
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
	})

	t.Run("ColumnKey", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true})
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
			Set("zero", x=0)
			Set("sw1", x=0)

			Set("zero", foo=20)
			Set("zero", bar=2000)
			Set("sw", foo=30)
			Set("sw2", foo=40)
			Set("sw3", foo=50)
			Set("sw1", foo=60)
			Set("zero", other=1000)
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
	})
}

// Ensure a range query can be executed.
func TestExecutor_Execute_Row_Range(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		// Create a timestamp just out of the current date + 1 day timestamp (default end timestamp).
		nextDayExclusive := time.Now().AddDate(0, 0, 2)

		writeQuery := fmt.Sprintf(`
		Set(2, f=1, 1999-12-31T00:00)
		Set(3, f=1, 2000-01-01T00:00)
		Set(4, f=1, 2000-01-02T00:00)
		Set(5, f=1, 2000-02-01T00:00)
		Set(6, f=1, 2001-01-01T00:00)
		Set(7, f=1, 2002-01-01T02:00)
		Set(8, f=1, %s)

		Set(2, f=1, 1999-12-30T00:00)
		Set(2, f=1, 2002-02-01T00:00)
		Set(2, f=10, 2001-01-01T00:00)`, nextDayExclusive.Format("2006-01-02T15:04"))
		readQueries := []string{
			`Row(f=1, from=1999-12-31T00:00, to=2002-01-01T03:00)`,
			`Row(f=1, from=1999-12-31T00:00)`,
			`Row(f=1, to=2002-01-01T02:00)`,
			`Clear( 2, f=1)`,
			`Row(f=1, from=1999-12-31T00:00, to=2002-01-01T03:00)`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			nil, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))

		t.Run("Standard", func(t *testing.T) {
			if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})

		t.Run("From", func(t *testing.T) {
			if columns := responses[1].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})

		t.Run("To", func(t *testing.T) {
			if columns := responses[2].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})

		t.Run("Clear", func(t *testing.T) {
			if columns := responses[4].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		writeQuery := `
		Set("two", f=1, 1999-12-31T00:00)
		Set("three", f=1, 2000-01-01T00:00)
		Set("four", f=1, 2000-01-02T00:00)
		Set("five", f=1, 2000-02-01T00:00)
		Set("six", f=1, 2001-01-01T00:00)
		Set("seven", f=1, 2002-01-01T02:00)

		Set("two", f=1, 1999-12-30T00:00)
		Set("two", f=1, 2002-02-01T00:00)
		Set("two", f=10, 2001-01-01T00:00)`
		readQueries := []string{
			`Row(f=1, from=1999-12-31T00:00, to=2002-01-01T03:00)`,
			`Clear("two", f=1)`,
			`Row(f=1, from=1999-12-31T00:00, to=2002-01-01T03:00)`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))

		t.Run("Standard", func(t *testing.T) {
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"two", "three", "four", "five", "six", "seven"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})

		t.Run("Clear", func(t *testing.T) {
			if keys := responses[2].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "four", "five", "six", "seven"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		writeQuery := `
		Set(2, f="foo", 1999-12-31T00:00)
		Set(3, f="foo", 2000-01-01T00:00)
		Set(4, f="foo", 2000-01-02T00:00)
		Set(5, f="foo", 2000-02-01T00:00)
		Set(6, f="foo", 2001-01-01T00:00)
		Set(7, f="foo", 2002-01-01T02:00)

		Set(2, f="foo", 1999-12-30T00:00)
		Set(2, f="foo", 2002-02-01T00:00)
		Set(2, f="bar", 2001-01-01T00:00)`
		readQueries := []string{
			`Row(f="foo", from=1999-12-31T00:00, to=2002-01-01T03:00)`,
			`Clear( 2, f="foo")`,
			`Row(f="foo", from=1999-12-31T00:00, to=2002-01-01T03:00)`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			nil,
			pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")),
			pilosa.OptFieldKeys())

		t.Run("Standard", func(t *testing.T) {
			if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})

		t.Run("Clear", func(t *testing.T) {
			if columns := responses[2].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		writeQuery := `
		Set("two", f="foo", 1999-12-31T00:00)
		Set("three", f="foo", 2000-01-01T00:00)
		Set("four", f="foo", 2000-01-02T00:00)
		Set("five", f="foo", 2000-02-01T00:00)
		Set("six", f="foo", 2001-01-01T00:00)
		Set("seven", f="foo", 2002-01-01T02:00)

		Set("two", f="foo", 1999-12-30T00:00)
		Set("two", f="foo", 2002-02-01T00:00)
		Set("two", f="bar", 2001-01-01T00:00)`
		readQueries := []string{
			`Row(f="foo", from=1999-12-31T00:00, to=2002-01-01T03:00)`,
			`Clear("two", f="foo")`,
			`Row(f="foo", from=1999-12-31T00:00, to=2002-01-01T03:00)`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")),
			pilosa.OptFieldKeys())

		t.Run("Standard", func(t *testing.T) {
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"two", "three", "four", "five", "six", "seven"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})

		t.Run("Clear", func(t *testing.T) {
			if keys := responses[2].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "four", "five", "six", "seven"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})
	})

	t.Run("UnixTimestamp", func(t *testing.T) {
		writeQuery := `
		Set(2, f=1, 1999-12-31T00:00)
		Set(3, f=1, 2000-01-01T00:00)
		Set(4, f=1, 2000-01-02T00:00)
		Set(5, f=1, 2000-02-01T00:00)
		Set(6, f=1, 2001-01-01T00:00)
		Set(7, f=1, 2002-01-01T02:00)

		Set(2, f=1, 1999-12-30T00:00)
		Set(2, f=1, 2002-02-01T00:00)
		Set(2, f=10, 2001-01-01T00:00)`
		readQueries := []string{
			`Row(f=1, from=946598400, to=1009854000)`,
			`Clear( 2, f=1)`,
			`Row(f=1, from=946598400, to=1009854000)`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			nil, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))

		t.Run("Standard", func(t *testing.T) {
			if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})

		t.Run("Clear", func(t *testing.T) {
			if columns := responses[2].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})
	})
}

// Ensure a range query can be executed.
func TestExecutor_Execute_Range_Deprecated(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		writeQuery := `
		Set(2, f=1, 1999-12-31T00:00)
		Set(3, f=1, 2000-01-01T00:00)
		Set(4, f=1, 2000-01-02T00:00)
		Set(5, f=1, 2000-02-01T00:00)
		Set(6, f=1, 2001-01-01T00:00)
		Set(7, f=1, 2002-01-01T02:00)

		Set(2, f=1, 1999-12-30T00:00)
		Set(2, f=1, 2002-02-01T00:00)
		Set(2, f=10, 2001-01-01T00:00)`
		readQueries := []string{
			`Range(f=1, from=1999-12-31T00:00, to=2002-01-01T03:00)`,
			`Clear( 2, f=1)`,
			`Range(f=1, from=1999-12-31T00:00, to=2002-01-01T03:00)`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			nil, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))

		t.Run("Standard", func(t *testing.T) {
			if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})

		t.Run("Clear", func(t *testing.T) {
			if columns := responses[2].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})

		rq2 := []string{
			`Range(f=1, 1999-12-31T00:00, 2002-01-01T03:00)`,
		}
		responses = runCallTest(t, writeQuery, rq2,
			nil, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))
		t.Run("OldRange", func(t *testing.T) {
			if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		writeQuery := `
		Set("two", f=1, 1999-12-31T00:00)
		Set("three", f=1, 2000-01-01T00:00)
		Set("four", f=1, 2000-01-02T00:00)
		Set("five", f=1, 2000-02-01T00:00)
		Set("six", f=1, 2001-01-01T00:00)
		Set("seven", f=1, 2002-01-01T02:00)

		Set("two", f=1, 1999-12-30T00:00)
		Set("two", f=1, 2002-02-01T00:00)
		Set("two", f=10, 2001-01-01T00:00)`
		readQueries := []string{
			`Range(f=1, from=1999-12-31T00:00, to=2002-01-01T03:00)`,
			`Clear("two", f=1)`,
			`Range(f=1, from=1999-12-31T00:00, to=2002-01-01T03:00)`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))

		t.Run("Standard", func(t *testing.T) {
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"two", "three", "four", "five", "six", "seven"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})

		t.Run("Clear", func(t *testing.T) {
			if keys := responses[2].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "four", "five", "six", "seven"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		writeQuery := `
		Set(2, f="foo", 1999-12-31T00:00)
		Set(3, f="foo", 2000-01-01T00:00)
		Set(4, f="foo", 2000-01-02T00:00)
		Set(5, f="foo", 2000-02-01T00:00)
		Set(6, f="foo", 2001-01-01T00:00)
		Set(7, f="foo", 2002-01-01T02:00)

		Set(2, f="foo", 1999-12-30T00:00)
		Set(2, f="foo", 2002-02-01T00:00)
		Set(2, f="bar", 2001-01-01T00:00)`
		readQueries := []string{
			`Range(f="foo", from=1999-12-31T00:00, to=2002-01-01T03:00)`,
			`Clear( 2, f="foo")`,
			`Range(f="foo", from=1999-12-31T00:00, to=2002-01-01T03:00)`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			nil,
			pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")),
			pilosa.OptFieldKeys())

		t.Run("Standard", func(t *testing.T) {
			if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})

		t.Run("Clear", func(t *testing.T) {
			if columns := responses[2].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		writeQuery := `
		Set("two", f="foo", 1999-12-31T00:00)
		Set("three", f="foo", 2000-01-01T00:00)
		Set("four", f="foo", 2000-01-02T00:00)
		Set("five", f="foo", 2000-02-01T00:00)
		Set("six", f="foo", 2001-01-01T00:00)
		Set("seven", f="foo", 2002-01-01T02:00)

		Set("two", f="foo", 1999-12-30T00:00)
		Set("two", f="foo", 2002-02-01T00:00)
		Set("two", f="bar", 2001-01-01T00:00)`
		readQueries := []string{
			`Range(f="foo", from=1999-12-31T00:00, to=2002-01-01T03:00)`,
			`Clear("two", f="foo")`,
			`Range(f="foo", from=1999-12-31T00:00, to=2002-01-01T03:00)`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")),
			pilosa.OptFieldKeys())

		t.Run("Standard", func(t *testing.T) {
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"two", "three", "four", "five", "six", "seven"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})

		t.Run("Clear", func(t *testing.T) {
			if keys := responses[2].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "four", "five", "six", "seven"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})
	})
}

// Ensure a Row(bsiGroup) query can be executed.
func TestExecutor_Execute_Row_BSIGroup(t *testing.T) {
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
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo == 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, (5 * ShardWidth) + 100}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("NEQ", func(t *testing.T) {
		// NEQ null
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(other != null)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
		// NEQ <int>
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo != 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth, ShardWidth + 1, ShardWidth + 2}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
		// NEQ -<int>
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(other != -20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			//t.Fatalf("unexpected result: %s", spew.Sdump(result))
			t.Fatalf("unexpected result: %v", result.Results[0].(*pilosa.Row).Columns())
		}
	})

	t.Run("LT", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo < 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth + 2}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTE", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo <= 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, ShardWidth + 2, (5 * ShardWidth) + 100}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("GT", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo > 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth, ShardWidth + 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("GTE", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo >= 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, ShardWidth, ShardWidth + 1, (5 * ShardWidth) + 100}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BETWEEN", func(t *testing.T) {
		tests := []struct {
			q   string
			exp bool
		}{
			{q: `Row(0 < other < 1000)`, exp: false},
			{q: `Row(0 <= other < 1000)`, exp: false},
			{q: `Row(0 <= other <= 1000)`, exp: true},
			{q: `Row(0 < other <= 1000)`, exp: true},

			{q: `Row(1000 < other < 1000)`, exp: false},
			{q: `Row(1000 <= other < 1000)`, exp: false},
			{q: `Row(1000 <= other <= 1000)`, exp: true},
			{q: `Row(1000 < other <= 1000)`, exp: false},

			{q: `Row(1000 < other < 2000)`, exp: false},
			{q: `Row(1000 <= other < 2000)`, exp: true},
			{q: `Row(1000 <= other <= 2000)`, exp: true},
			{q: `Row(1000 < other <= 2000)`, exp: false},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("#%d_%s", i, test.q), func(t *testing.T) {
				var expected = []uint64{}
				if test.exp {
					expected = []uint64{0}
				}
				if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: test.q}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(expected, result.Results[0].(*pilosa.Row).Columns()) {
					t.Fatalf("unexpected result for query: %s", test.q)
				}
			})
		}

	})

	// Ensure that the NotNull code path gets run.
	t.Run("NotNull", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(0 <= other <= 1000)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BelowMin", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo == 0)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("AboveMax", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo == 200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTAboveMax", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(edge < 200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result.Results[0].(*pilosa.Row).Columns()))
		}
	})

	t.Run("GTBelowMin", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(edge > -200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result.Results[0].(*pilosa.Row).Columns()))
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(bad_field >= 20)`}); errors.Cause(err) != pilosa.ErrFieldNotFound {
			t.Fatal(err)
		}
	})
}

// Ensure a Range(bsiGroup) query can be executed. (Deprecated)
func TestExecutor_Execute_Range_BSIGroup_Deprecated(t *testing.T) {
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
		} else if !reflect.DeepEqual([]uint64{}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	// Ensure that the NotNull code path gets run.
	t.Run("NotNull", func(t *testing.T) {
		if result, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(0 <= other <= 1000)`}); err != nil {
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
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf(`Set(%d, f=7)`, pilosa.ShardWidth+1)}); err != nil {
			t.Fatalf("querying remote: %v", err)
		}

		if !reflect.DeepEqual(hldr1.Row("i", "f", 7).Columns(), []uint64{pilosa.ShardWidth + 1}) {
			t.Fatalf("unexpected cols from row 7: %v", hldr1.Row("i", "f", 7).Columns())
		}
	})

	t.Run("remote with timestamp", func(t *testing.T) {
		_, err = c[0].API.CreateField(context.Background(), "i", "z", pilosa.OptFieldTypeTime("Y"))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf(`Set(%d, z=5, 2010-07-08T00:00)`, pilosa.ShardWidth+1)}); err != nil {
			t.Fatalf("quuerying remote: %v", err)
		}

		if !reflect.DeepEqual(hldr1.RowTime("i", "z", 5, time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC), "Y").Columns(), []uint64{pilosa.ShardWidth + 1}) {
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

	t.Run("remote groupBy", func(t *testing.T) {
		if res, err := c[1].API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "i",
			Query: `GroupBy(Rows(f))`,
		}); err != nil {
			t.Fatalf("GroupBy querying: %v", err)
		} else {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "f", RowID: 7}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "f", RowID: 10}}, Count: 4},
			}
			results := res.Results[0].([]pilosa.GroupCount)
			test.CheckGroupBy(t, expected, results)
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
	rangeCheckQuery := `Row(f=1, from=1999-12-31T00:00, to=2002-01-01T03:00)`

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

func TestExecutor_ExecuteOptions(t *testing.T) {
	t.Run("excludeRowAttrs", func(t *testing.T) {
		writeQuery := `
			Set(100, f=10)
			SetRowAttrs(f, 10, foo="bar")`
		readQueries := []string{`Options(Row(f=10), excludeRowAttrs=true)`}
		responses := runCallTest(t, writeQuery, readQueries, nil)
		if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{100}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := responses[0].Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})

	t.Run("excludeColumns", func(t *testing.T) {
		writeQuery := `
			Set(100, f=10)
			SetRowAttrs(f, 10, foo="bar")`
		readQueries := []string{`Options(Row(f=10), excludeColumns=true)`}
		responses := runCallTest(t, writeQuery, readQueries, nil)
		if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := responses[0].Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar"}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		}
	})

	t.Run("columnAttrs", func(t *testing.T) {
		writeQuery := `
			Set(0, f=10)
			SetColumnAttrs(0, foo="baz")
			Set(100, f=10)
			SetColumnAttrs(100, foo="bar")`
		readQueries := []string{`Options(Row(f=10), columnAttrs=true)`}
		responses := runCallTest(t, writeQuery, readQueries, nil)
		targetColAttrSets := []*pilosa.ColumnAttrSet{
			{ID: 0, Attrs: map[string]interface{}{"foo": "baz"}},
			{ID: 100, Attrs: map[string]interface{}{"foo": "bar"}},
		}

		targetJSON := `[{"id":0,"attrs":{"foo":"baz"}},{"id":100,"attrs":{"foo":"bar"}}]`

		if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{0, 100}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := responses[0].ColumnAttrSets; !reflect.DeepEqual(attrs, targetColAttrSets) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		} else {
			// Ensure the JSON is marshaled correctly.
			jres, err := json.Marshal(attrs)
			if err != nil {
				t.Fatal(err)
			} else if string(jres) != targetJSON {
				t.Fatalf("json marshal expected: %s, but got: %s", targetJSON, jres)
			}
		}
	})

	t.Run("columnAttrsWithKeys", func(t *testing.T) {
		writeQuery := `
			Set("one-hundred", f="ten")
			SetColumnAttrs("one-hundred", foo="bar")`
		readQueries := []string{`Options(Row(f="ten"), columnAttrs=true)`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{Keys: true},
			pilosa.OptFieldKeys())

		targetColAttrSets := []*pilosa.ColumnAttrSet{
			{Key: "one-hundred", Attrs: map[string]interface{}{"foo": "bar"}},
		}

		targetJSON := `[{"key":"one-hundred","attrs":{"foo":"bar"}}]`

		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one-hundred"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		} else if attrs := responses[0].ColumnAttrSets; !reflect.DeepEqual(attrs, targetColAttrSets) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		} else {
			// Ensure the JSON is marshaled correctly.
			jres, err := json.Marshal(attrs)
			if err != nil {
				t.Fatal(err)
			} else if string(jres) != targetJSON {
				t.Fatalf("json marshal expected: %s, but got: %s", targetJSON, jres)
			}
		}
	})

	t.Run("shards", func(t *testing.T) {
		writeQuery := fmt.Sprintf(`
			Set(100, f=10)
			Set(%d, f=10)
			Set(%d, f=10)`, ShardWidth, ShardWidth*2)
		readQueries := []string{`Options(Row(f=10), shards=[0, 2])`}
		responses := runCallTest(t, writeQuery, readQueries, nil)
		if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{100, ShardWidth * 2}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})

	t.Run("multipleOpt", func(t *testing.T) {
		writeQuery := `
			Set(100, f=10)
			SetRowAttrs(f, 10, foo="bar")`
		readQueries := []string{
			`Options(Row(f=10), excludeColumns=true)
			 Options(Row(f=10), excludeRowAttrs=true)`,
		}
		responses := runCallTest(t, writeQuery, readQueries, nil)
		if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := responses[0].Results[0].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar"}) {
			t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
		} else if bits := responses[0].Results[1].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{100}) {
			t.Fatalf("unexpected columns: %+v", bits)
		} else if attrs := responses[0].Results[1].(*pilosa.Row).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{}) {
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

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Not(Row(f=10))`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{ShardWidth + 2}) {
			t.Fatalf("unexpected columns after Not: %+v", bits)
		}

		// Reopen cluster to ensure existence field is reloaded.
		if err := c[0].Reopen(); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Not(Row(f=10))`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{ShardWidth + 2}) {
			t.Fatalf("unexpected columns after reopen: %+v", bits)
		}
	})
}

// Ensure a not query can be executed.
func TestExecutor_Execute_Not(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		writeQuery := `` +
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+2, 20)
		readQueries := []string{
			`Not(Row(f=20))`,
			`Not(Row(f=0))`,
			`Not(Union(Row(f=10), Row(f=20)))`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{TrackExistence: true})

		if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		if bits := responses[1].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1, ShardWidth + 2}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		if bits := responses[2].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("three", f=10)
			Set("sw1", f=10)
			Set("sw2", f=20)`
		readQueries := []string{`Not(Row(f=20))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{
				TrackExistence: true,
				Keys:           true,
			})
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "sw1"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})

	t.Run("RowKeyColumnID", func(t *testing.T) {
		writeQuery := fmt.Sprintf(`
			Set(3, f="ten")
			Set(%d, f="ten")
			Set(%d, f="twenty")`, ShardWidth+1, ShardWidth+2)
		readQueries := []string{`Not(Row(f="twenty"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{TrackExistence: true},
			pilosa.OptFieldKeys(),
		)
		if cols := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(cols, []uint64{3, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", cols)
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		writeQuery := `
			Set("three", f="ten")
			Set("sw1", f="ten")
			Set("sw2", f="twenty")`
		readQueries := []string{`Not(Row(f="twenty"))`}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{
				TrackExistence: true,
				Keys:           true,
			}, pilosa.OptFieldKeys())
		if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "sw1"}) {
			t.Fatalf("unexpected keys: %+v", keys)
		}
	})
}

// Ensure a row can be cleared.
func TestExecutor_Execute_ClearRow(t *testing.T) {
	// Set and Mutex tests use the same data and queries
	writeQuery := `` +
		fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
		fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth-1, 10) +
		fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
		fmt.Sprintf("Set(%d, f=%d)\n", 1, 20) +
		fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 20)

	readQueries := []string{
		`Row(f=10)`,
		`ClearRow(f=10)`,
		`ClearRow(f=10)`,
		`Row(f=10)`,
		`Row(f=20)`,
	}

	t.Run("Set", func(t *testing.T) {
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{TrackExistence: true})
		if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Clear the row and ensure we get a `true` response.
		if res := responses[1].Results[0].(bool); !res {
			t.Fatalf("unexpected clear row result: %+v", res)
		}

		// Clear the row again and ensure we get a `false` response.
		if res := responses[2].Results[0].(bool); res {
			t.Fatalf("unexpected clear row result: %+v", res)
		}

		// Ensure the row is empty.
		if bits := responses[3].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Ensure other rows were not affected.
		if bits := responses[4].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})

	t.Run("Mutex", func(t *testing.T) {
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{TrackExistence: true},
			pilosa.OptFieldTypeMutex("none", 0))
		if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Clear the row and ensure we get a `true` response.
		if res := responses[1].Results[0].(bool); !res {
			t.Fatalf("unexpected clear row result: %+v", res)
		}

		// Clear the row again and ensure we get a `false` response.
		if res := responses[2].Results[0].(bool); res {
			t.Fatalf("unexpected clear row result: %+v", res)
		}

		// Ensure the row is empty.
		if bits := responses[3].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Ensure other rows were not affected.
		if bits := responses[4].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})

	t.Run("Time", func(t *testing.T) {
		writeQuery := `
			Set(2, f=1, 1999-12-31T00:00)
			Set(3, f=1, 2000-01-01T00:00)
			Set(4, f=1, 2000-01-02T00:00)
			Set(5, f=1, 2000-02-01T00:00)
			Set(6, f=1, 2001-01-01T00:00)
			Set(7, f=1, 2002-01-01T02:00)

			Set(2, f=1, 1999-12-30T00:00)
			Set(2, f=1, 2002-02-01T00:00)
			Set(2, f=10, 2001-01-01T00:00)`
		readQueries := []string{
			`Row(f=1, from=1999-12-31T00:00, to=2003-01-01T03:00)`,
			`ClearRow(f=1)`,
			`Row(f=1, from=1999-12-31T00:00, to=2003-01-01T03:00)`,
			`Row(f=10, from=1999-12-31T00:00, to=2003-01-01T03:00)`,
		}
		responses := runCallTest(t, writeQuery, readQueries,
			&pilosa.IndexOptions{TrackExistence: true},
			pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD")))
		if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}

		// Clear the row and ensure we get a `true` response.
		if res := responses[1].Results[0].(bool); !res {
			t.Fatalf("unexpected clear row result: %+v", res)
		}

		// Ensure the row is empty.
		if columns := responses[2].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}

		// Ensure other rows were not affected.
		if columns := responses[3].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("Int", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		_, err := index.CreateField("f", pilosa.OptFieldTypeInt(0, 100))
		if err != nil {
			t.Fatal(err)
		}

		// Ensure that clearing a row raises an error.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `ClearRow(f=1)`}); err == nil {
			t.Fatal("expected clear row to return an error")
		}
	})

	t.Run("TopN", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		_, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}

		cc := `
			Set(2, f=1)
			Set(3, f=1)
			Set(4, f=1)
			Set(5, f=1)
			Set(6, f=1)
			Set(7, f=1)
			Set(8, f=1)

			Set(2, f=2)
			Set(3, f=2)
			Set(4, f=2)
			Set(5, f=2)
			Set(6, f=2)
			Set(7, f=2)

			Set(2, f=3)
			Set(3, f=3)
			Set(4, f=3)
			Set(5, f=3)
			Set(6, f=3)
		`

		// Set bits.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: cc}); err != nil {
			t.Fatal(err)
		}

		if err := c[0].RecalculateCaches(); err != nil {
			t.Fatalf("recalculating caches: %v", err)
		}

		// Check the TopN results.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=5)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(res.Results, []interface{}{[]pilosa.Pair{
			{ID: 1, Count: 7},
			{ID: 2, Count: 6},
			{ID: 3, Count: 5},
		}}) {
			t.Fatalf("topn wrong results: %v", res.Results)
		}

		// Clear the row and ensure we get a `true` response.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `ClearRow(f=2)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected clear row result: %+v", res)
		}

		// Ensure that the cleared row doesn't show up in TopN (i.e. it was removed from the cache).
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=5)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(res.Results, []interface{}{[]pilosa.Pair{
			{ID: 1, Count: 7},
			{ID: 3, Count: 5},
		}}) {
			t.Fatalf("topn wrong results: %v", res.Results)
		}
	})

	// Ensure that ClearRow returns false when the row to clear needs translation.
	t.Run("WithKeys", func(t *testing.T) {
		wq := ""
		rq := []string{
			`ClearRow(f="bar")`,
		}

		responses := runCallTest(t, wq, rq, &pilosa.IndexOptions{}, pilosa.OptFieldKeys())
		if res := responses[0].Results[0].(bool); res {
			t.Fatalf("unexpected result: %+v", res)
		}
	})
}

// Ensure a row can be set.
func TestExecutor_Execute_SetRow(t *testing.T) {
	t.Run("Set_NewRow", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		if _, err := index.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}
		if _, err := index.CreateField("tmp", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		// Set bits.
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `` +
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth-1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10),
		}); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row 10 into a different row.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f=10), tmp=20)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(tmp=20)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})
	t.Run("Set_NoSource", func(t *testing.T) {
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
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth-1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10),
		}); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row 9 (which doesn't exist) into a different row.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f=9), f=20)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=20)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row 9 (which doesn't exist) into a row that does exist.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f=9), f=10)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})
	t.Run("Set_ExistingDestination", func(t *testing.T) {
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
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth-1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", 1, 20) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 20),
		}); err != nil {
			t.Fatal(err)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=20)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row 10 into an existing row.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f=10), f=20)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=20)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})
}

func benchmarkExistence(nn bool, b *testing.B) {
	c := test.MustNewCluster(b, 1)
	var err error
	c[0].Config.DataDir, err = ioutil.TempDir(*TempDir, "benchmarkExistence")
	if err != nil {
		b.Fatalf("getting temp dir: %v", err)
	}
	err = c.Start()
	if err != nil {
		b.Fatalf("starting cluster: %v", err)
	}
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

func TestExecutor_Execute_Rows(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()
	c.CreateField(t, "i", pilosa.IndexOptions{}, "general")
	c.ImportBits(t, "i", "general", [][2]uint64{
		{10, 0},
		{10, ShardWidth + 1},
		{11, 2},
		{11, ShardWidth + 2},
		{12, 2},
		{12, ShardWidth + 2},
		{13, 3},
	})

	rows := c.Query(t, "i", `Rows(general)`).Results[0].(pilosa.RowIdentifiers)
	if !reflect.DeepEqual(rows, pilosa.RowIdentifiers{Rows: []uint64{10, 11, 12, 13}}) {
		t.Fatalf("unexpected rows: %+v", rows)
	}

	// backwards compatibility
	// TODO: remove at Pilosa 2.0
	rows = c.Query(t, "i", `Rows(field=general)`).Results[0].(pilosa.RowIdentifiers)
	if !reflect.DeepEqual(rows, pilosa.RowIdentifiers{Rows: []uint64{10, 11, 12, 13}}) {
		t.Fatalf("unexpected rows: %+v", rows)
	}

	rows = c.Query(t, "i", `Rows(general, limit=2)`).Results[0].(pilosa.RowIdentifiers)
	if !reflect.DeepEqual(rows, pilosa.RowIdentifiers{Rows: []uint64{10, 11}}) {
		t.Fatalf("unexpected rows: %+v", rows)
	}

	rows = c.Query(t, "i", `Rows(general, previous=10,limit=2)`).Results[0].(pilosa.RowIdentifiers)
	if !reflect.DeepEqual(rows, pilosa.RowIdentifiers{Rows: []uint64{11, 12}}) {
		t.Fatalf("unexpected rows: %+v", rows)
	}

	rows = c.Query(t, "i", `Rows(general, column=2)`).Results[0].(pilosa.RowIdentifiers)
	if !reflect.DeepEqual(rows, pilosa.RowIdentifiers{Rows: []uint64{11, 12}}) {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestExecutor_Execute_RowsTime(t *testing.T) {
	writeQuery := fmt.Sprintf(`
		Set(9, f=1, 2001-01-01T00:00)
		Set(9, f=2, 2002-01-01T00:00)
		Set(9, f=3, 2003-01-01T00:00)
		Set(9, f=4, 2004-01-01T00:00)

		Set(%d, f=13, 2003-02-02T00:00)
		`, pilosa.ShardWidth+9)
	readQueries := []string{
		`Rows(f, from=1999-12-31T00:00, to=2002-01-01T03:00)`,
		`Rows(f, from=2002-01-01T00:00, to=2004-01-01T00:00)`,
		`Rows(f, from=1990-01-01T00:00, to=1999-01-01T00:00)`,
		`Rows(f)`,
		`Rows(f, from=2002-01-01T00:00)`,
		`Rows(f, to=2003-02-03T00:00)`,
	}
	expResults := [][]uint64{
		{1},
		{2, 3, 13},
		{},
		{1, 2, 3, 4, 13},
		{2, 3, 4, 13},
		{1, 2, 3, 13},
	}

	responses := runCallTest(t, writeQuery, readQueries,
		nil, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD"), true))

	for i := range responses {
		t.Run(fmt.Sprintf("response-%d", i), func(t *testing.T) {
			if rows := responses[i].Results[0].(pilosa.RowIdentifiers).Rows; !reflect.DeepEqual(rows, expResults[i]) {
				t.Fatalf("unexpected rows: %+v", rows)
			}
		})
	}
}

// Ensure that an empty time field returns empty Rows().
func TestExecutor_Execute_RowsTimeEmpty(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	c.CreateField(t, "i", pilosa.IndexOptions{}, "x", pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD"), true))
	rows := c.Query(t, "i", `Rows(x, from=1999-12-31T00:00, to=2002-01-01T03:00)`).Results[0].(pilosa.RowIdentifiers).Rows
	if !reflect.DeepEqual(rows, []uint64{}) {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestExecutor_Execute_Query_Error(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	c.CreateField(t, "i", pilosa.IndexOptions{}, "general")

	tests := []struct {
		query string
		error string
	}{
		{
			query: "GroupBy(Rows())",
			error: "Rows call must have field",
		},
		{
			query: "GroupBy(Rows(\"true\"))",
			error: "parsing: parsing:",
		},
		{
			query: "GroupBy(Rows(1))",
			error: "parsing: parsing:",
		},
		{
			query: "GroupBy(Rows(general, limit=-1))",
			error: "must be positive, but got",
		},
		{
			query: "GroupBy(Rows(general), limit=-1)",
			error: "must be positive, but got",
		},
		{
			query: "GroupBy(Rows(general), filter=Rows(general))",
			error: "parsing: parsing:",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{
				Index: "i",
				Query: test.query,
			})
			if err == nil {
				t.Fatalf("should have gotten an error on invalid rows query, but got %#v", r)
			}
			if !strings.Contains(err.Error(), test.error) {
				t.Fatalf("unexpected error message:\n%s != %s", test.error, err.Error())
			}
		})
	}
}

func TestExecutor_GroupByStrings(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	c.CreateField(t, "istring", pilosa.IndexOptions{Keys: true}, "generals", pilosa.OptFieldKeys())

	req := &pilosa.ImportRequest{
		Index:      "istring",
		Field:      "generals",
		Shard:      0,
		RowKeys:    []string{"r1", "r2", "r1", "r2", "r1", "r2", "r1", "r2", "r1", "r2"},
		ColumnKeys: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
	}
	if err := c[0].API.Import(context.Background(), req); err != nil {
		t.Fatalf("importing: %v", err)
	}

	tests := []struct {
		query    string
		expected []pilosa.GroupCount
	}{
		{
			query: "GroupBy(Rows(generals))",
			expected: []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 1, RowKey: "r1"}}, Count: 5},
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 2, RowKey: "r2"}}, Count: 5},
			},
		},
		{
			query: "GroupBy(Rows(generals), filter=Row(generals=r2))",
			expected: []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 2, RowKey: "r2"}}, Count: 5},
			},
		},
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{
				Index: "istring",
				Query: tst.query,
			})
			if err != nil {
				t.Fatalf("got an error %v", err)
			}
			results := r.Results[0].([]pilosa.GroupCount)
			test.CheckGroupBy(t, tst.expected, results)
		})
	}
}

func TestExecutor_Execute_Rows_Keys(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	_, err := c[0].API.CreateIndex(context.Background(), "i", pilosa.IndexOptions{Keys: true})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}

	_, err = c[0].API.CreateField(context.Background(), "i", "f", pilosa.OptFieldKeys())
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}

	// setup some data. 10 bits in each of shards 0 through 9. starting at
	// row/col shardNum and progressing to row/col shardNum+10. Also set the
	// previous 2 for each bit if row >0.
	query := strings.Builder{}
	for shard := 0; shard < 10; shard++ {
		for i := shard; i < shard+10; i++ {
			for row := i; row >= 0 && row > i-3; row-- {
				query.WriteString(fmt.Sprintf("Set(\"%d\", f=\"%d\")", shard*pilosa.ShardWidth+i, row))

			}

		}
	}
	_, err = c[0].API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "i",
		Query: query.String(),
	})
	if err != nil {
		t.Fatalf("querying: %v", err)
	}

	tests := []struct {
		q   string
		exp []string
	}{
		{
			q:   `Rows(f)`,
			exp: []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18"},
		},
		// backwards compatibility
		// TODO: remove at Pilosa 2.0
		{
			q:   `Rows(field=f)`,
			exp: []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18"},
		},
		{
			q:   `Rows(f, limit=2)`,
			exp: []string{"0", "1"},
		},
		// backwards compatibility
		// TODO: remove at Pilosa 2.0
		{
			q:   `Rows(field=f, limit=2)`,
			exp: []string{"0", "1"},
		},
		{
			q:   `Rows(f, previous="15")`,
			exp: []string{"16", "17", "18"},
		},
		{
			q:   `Rows(f, previous="11", limit=2)`,
			exp: []string{"12", "13"},
		},
		{
			q:   `Rows(f, previous="17", limit=5)`,
			exp: []string{"18"},
		},
		{
			q:   `Rows(f, previous="18")`,
			exp: []string{},
		},
		{
			q:   `Rows(f, previous="1", limit=0)`,
			exp: []string{},
		},
		{
			q:   `Rows(f, column="1")`,
			exp: []string{"0", "1"},
		},
		{
			q:   `Rows(f, column="2")`,
			exp: []string{"0", "1", "2"},
		},
		{
			q:   `Rows(f, column="3")`,
			exp: []string{"1", "2", "3"},
		},
		{
			q:   `Rows(f, limit=2, column="3")`,
			exp: []string{"1", "2"},
		},
		{
			q:   fmt.Sprintf(`Rows(f, previous="15", column="%d")`, ShardWidth*9+17),
			exp: []string{"16", "17"},
		},
		{
			q:   fmt.Sprintf(`Rows(f, previous="11", limit=2, column="%d")`, ShardWidth*5+14),
			exp: []string{"12", "13"},
		},
		{
			q:   fmt.Sprintf(`Rows(f, previous="17", limit=5, column="%d")`, ShardWidth*9+18),
			exp: []string{"18"},
		},
		{
			q:   `Rows(f, previous="18", column="19")`,
			exp: []string{},
		},
		{
			q:   `Rows(f, previous="1", limit=0, column="0")`,
			exp: []string{},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("#%d_%s", i, test.q), func(t *testing.T) {
			if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: test.q}); err != nil {
				t.Fatal(err)
			} else if rows := res.Results[0].(pilosa.RowIdentifiers); !reflect.DeepEqual(
				rows, pilosa.RowIdentifiers{Keys: test.exp}) {
				t.Fatalf("\ngot: %+v\nexp: %+v", rows, pilosa.RowIdentifiers{Keys: test.exp})
			}
		})
	}

}

func TestExecutor_Execute_GroupBy(t *testing.T) {
	groupByTest := func(t *testing.T, clusterSize int) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		c.CreateField(t, "i", pilosa.IndexOptions{}, "general")
		c.CreateField(t, "i", pilosa.IndexOptions{}, "sub")
		c.ImportBits(t, "i", "general", [][2]uint64{
			{10, 0},
			{10, 1},
			{10, ShardWidth + 1},
			{11, 2},
			{11, ShardWidth + 2},
			{12, 2},
			{12, ShardWidth + 2},
		})
		c.ImportBits(t, "i", "sub", [][2]uint64{
			{100, 0},
			{100, 1},
			{100, 3},
			{100, ShardWidth + 1},

			{110, 2},
			{110, 0},
		})

		t.Run("No Field List Arguments", func(t *testing.T) {
			if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `GroupBy()`}); err != nil {
				if !strings.Contains(err.Error(), "need at least one child call") {
					t.Fatalf("unexpected error: \"%v\"", err)
				}
			}
		})

		t.Run("Unknown Field ", func(t *testing.T) {
			if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `GroupBy(Rows(missing))`}); err != nil {
				if errors.Cause(err) != pilosa.ErrFieldNotFound {
					t.Fatalf("unexpected error\n\"%s\" not returned instead \n\"%s\"", pilosa.ErrFieldNotFound, err)
				}
			}
		})

		// backwards compatibility
		// TODO: remove at Pilosa 2.0
		t.Run("BasicLegacy", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 100}}, Count: 3},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 110}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 11}, {Field: "sub", RowID: 110}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 12}, {Field: "sub", RowID: 110}}, Count: 1},
			}

			results := c.Query(t, "i", `GroupBy(Rows(field=general), Rows(sub))`).Results[0].([]pilosa.GroupCount)
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("Basic", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 100}}, Count: 3},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 110}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 11}, {Field: "sub", RowID: 110}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 12}, {Field: "sub", RowID: 110}}, Count: 1},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general), Rows(sub))`).Results[0].([]pilosa.GroupCount)
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("Filter", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 100}}, Count: 3},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 110}}, Count: 1},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general), Rows(sub), filter=Row(general=10))`).Results[0].([]pilosa.GroupCount)
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("check field offset no limit", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 11}}, Count: 2},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 12}}, Count: 2},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general, previous=10))`).Results[0].([]pilosa.GroupCount)
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("check field offset limit", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 11}}, Count: 2},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general, previous=10), limit=1)`).Results[0].([]pilosa.GroupCount)
			test.CheckGroupBy(t, expected, results)

		})

		c.CreateField(t, "i", pilosa.IndexOptions{}, "a")
		c.CreateField(t, "i", pilosa.IndexOptions{}, "b")
		c.ImportBits(t, "i", "a", [][2]uint64{
			{0, 1},
			{1, ShardWidth + 1},
		})
		c.ImportBits(t, "i", "b", [][2]uint64{
			{0, ShardWidth + 1},
			{1, 1},
		})

		t.Run("tricky data", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "a", RowID: 0}, {Field: "b", RowID: 1}}, Count: 1},
			}

			results := c.Query(t, "i", `GroupBy(Rows(a), Rows(b), limit=1)`).Results[0].([]pilosa.GroupCount)
			test.CheckGroupBy(t, expected, results)
		})

		// set the same bits in a single shard in three fields
		c.CreateField(t, "i", pilosa.IndexOptions{}, "wa")
		c.CreateField(t, "i", pilosa.IndexOptions{}, "wb")
		c.CreateField(t, "i", pilosa.IndexOptions{}, "wc")
		c.ImportBits(t, "i", "wa", [][2]uint64{
			{0, 0}, {0, 1}, {0, 2}, // all
			{1, 1},         // odds
			{2, 0}, {2, 2}, // evens
			{3, 3}, // no overlap
		})
		c.ImportBits(t, "i", "wb", [][2]uint64{
			{0, 0}, {0, 1}, {0, 2},
			{1, 1},
			{2, 0}, {2, 2},
			{3, 3},
		})
		c.ImportBits(t, "i", "wc", [][2]uint64{
			{0, 0}, {0, 1}, {0, 2},
			{1, 1},
			{2, 0}, {2, 2},
			{3, 3},
		})

		t.Run("test wrapping with previous", func(t *testing.T) {
			results := c.Query(t, "i", `GroupBy(Rows(wa), Rows(wb), Rows(wc, previous=1), limit=3)`).Results[0].([]pilosa.GroupCount)
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "wa", RowID: 0}, {Field: "wb", RowID: 0}, {Field: "wc", RowID: 2}}, Count: 2},
				{Group: []pilosa.FieldRow{{Field: "wa", RowID: 0}, {Field: "wb", RowID: 1}, {Field: "wc", RowID: 0}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "wa", RowID: 0}, {Field: "wb", RowID: 1}, {Field: "wc", RowID: 1}}, Count: 1},
			}
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("test previous is last result", func(t *testing.T) {
			results := c.Query(t, "i", `GroupBy(Rows(wa, previous=3), Rows(wb, previous=3), Rows(wc, previous=3), limit=3)`).Results[0].([]pilosa.GroupCount)
			if len(results) > 0 {
				t.Fatalf("expected no results because previous specified last result")
			}
		})

		t.Run("test wrapping multiple", func(t *testing.T) {
			results := c.Query(t, "i", `GroupBy(Rows(wa), Rows(wb, previous=2), Rows(wc, previous=2), limit=1)`).Results[0].([]pilosa.GroupCount)
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "wa", RowID: 1}, {Field: "wb", RowID: 0}, {Field: "wc", RowID: 0}}, Count: 1},
			}
			test.CheckGroupBy(t, expected, results)
		})

		// test multiple shards with distinct results (different rows) and same
		// rows to ensure ordering, limit behavior and correctness
		c.CreateField(t, "i", pilosa.IndexOptions{}, "ma")
		c.CreateField(t, "i", pilosa.IndexOptions{}, "mb")
		c.ImportBits(t, "i", "ma", [][2]uint64{
			{0, 0},
			{1, ShardWidth},
			{2, 0},
			{3, ShardWidth},
		})
		c.ImportBits(t, "i", "mb", [][2]uint64{
			{0, 0},
			{1, ShardWidth},
			{2, 0},
			{3, ShardWidth},
		})
		t.Run("distinct rows in different shards", func(t *testing.T) {
			results := c.Query(t, "i", `GroupBy(Rows(ma), Rows(mb), limit=5)`).Results[0].([]pilosa.GroupCount)
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 0}, {Field: "mb", RowID: 0}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 0}, {Field: "mb", RowID: 2}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 1}, {Field: "mb", RowID: 1}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 1}, {Field: "mb", RowID: 3}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 2}, {Field: "mb", RowID: 0}}, Count: 1},
			}
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("distinct rows in different shards with row limit", func(t *testing.T) {
			results := c.Query(t, "i", `GroupBy(Rows(ma), Rows(mb, limit=2), limit=5)`).Results[0].([]pilosa.GroupCount)
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 0}, {Field: "mb", RowID: 0}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 1}, {Field: "mb", RowID: 1}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 2}, {Field: "mb", RowID: 0}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 3}, {Field: "mb", RowID: 1}}, Count: 1},
			}
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("distinct rows in different shards with column arg", func(t *testing.T) {
			results := c.Query(t, "i", fmt.Sprintf(`GroupBy(Rows(ma), Rows(mb, column=%d), limit=5)`, ShardWidth)).Results[0].([]pilosa.GroupCount)
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 1}, {Field: "mb", RowID: 1}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 1}, {Field: "mb", RowID: 3}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 3}, {Field: "mb", RowID: 1}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 3}, {Field: "mb", RowID: 3}}, Count: 1},
			}
			test.CheckGroupBy(t, expected, results)
		})

		c.CreateField(t, "i", pilosa.IndexOptions{}, "na")
		c.CreateField(t, "i", pilosa.IndexOptions{}, "nb")
		c.ImportBits(t, "i", "na", [][2]uint64{
			{0, 0},
			{0, ShardWidth},
			{1, 0},
			{1, ShardWidth},
		})
		c.ImportBits(t, "i", "nb", [][2]uint64{
			{0, 0},
			{0, ShardWidth},
			{1, 0},
			{1, ShardWidth},
		})
		t.Run("same rows in different shards", func(t *testing.T) {
			results := c.Query(t, "i", `GroupBy(Rows(na), Rows(nb))`).Results[0].([]pilosa.GroupCount)
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "na", RowID: 0}, {Field: "nb", RowID: 0}}, Count: 2},
				{Group: []pilosa.FieldRow{{Field: "na", RowID: 0}, {Field: "nb", RowID: 1}}, Count: 2},
				{Group: []pilosa.FieldRow{{Field: "na", RowID: 1}, {Field: "nb", RowID: 0}}, Count: 2},
				{Group: []pilosa.FieldRow{{Field: "na", RowID: 1}, {Field: "nb", RowID: 1}}, Count: 2},
			}
			test.CheckGroupBy(t, expected, results)

		})

		// test paging over results using previous. set the same bits in three
		// fields
		c.CreateField(t, "i", pilosa.IndexOptions{}, "ppa")
		c.CreateField(t, "i", pilosa.IndexOptions{}, "ppb")
		c.CreateField(t, "i", pilosa.IndexOptions{}, "ppc")
		c.ImportBits(t, "i", "ppa", [][2]uint64{
			{0, 0},
			{1, 0},
			{2, 0},
			{3, 0}, {3, 91000}, {3, ShardWidth}, {3, ShardWidth * 2}, {3, ShardWidth * 3},
		})
		c.ImportBits(t, "i", "ppb", [][2]uint64{
			{0, 0},
			{1, 0},
			{2, 0},
			{3, 0}, {3, 91000}, {3, ShardWidth}, {3, ShardWidth * 2}, {3, ShardWidth * 3},
		})
		c.ImportBits(t, "i", "ppc", [][2]uint64{
			{0, 0},
			{1, 0},
			{2, 0},
			{3, 0}, {3, 91000}, {3, ShardWidth}, {3, ShardWidth * 2}, {3, ShardWidth * 3},
		})

		t.Run("test wrapping with previous", func(t *testing.T) {
			totalResults := make([]pilosa.GroupCount, 0)
			results := c.Query(t, "i", `GroupBy(Rows(ppa), Rows(ppb), Rows(ppc), limit=3)`).Results[0].([]pilosa.GroupCount)
			totalResults = append(totalResults, results...)
			for len(totalResults) < 64 {
				lastGroup := results[len(results)-1].Group
				query := fmt.Sprintf("GroupBy(Rows(ppa, previous=%d), Rows(ppb, previous=%d), Rows(ppc, previous=%d), limit=3)", lastGroup[0].RowID, lastGroup[1].RowID, lastGroup[2].RowID)
				results = c.Query(t, "i", query).Results[0].([]pilosa.GroupCount)
				totalResults = append(totalResults, results...)
			}

			expected := make([]pilosa.GroupCount, 64)
			for i := 0; i < 64; i++ {
				expected[i] = pilosa.GroupCount{Group: []pilosa.FieldRow{{Field: "ppa", RowID: uint64(i / 16)}, {Field: "ppb", RowID: uint64((i % 16) / 4)}, {Field: "ppc", RowID: uint64(i % 4)}}, Count: 1}
			}
			expected[63].Count = 5

			test.CheckGroupBy(t, expected, totalResults)
		})

		// test row keys
		c.CreateField(t, "i", pilosa.IndexOptions{}, "generalk", pilosa.OptFieldKeys())
		c.CreateField(t, "i", pilosa.IndexOptions{}, "subk", pilosa.OptFieldKeys())
		c.Query(t, "i", `
			Set(0, generalk="ten")
			Set(1, generalk="ten")
			Set(1001, generalk="ten")
			Set(2, generalk="eleven")
			Set(1002, generalk="eleven")
			Set(2, generalk="twelve")
			Set(1002, generalk="twelve")

			Set(0, subk="one-hundred")
			Set(1, subk="one-hundred")
			Set(3, subk="one-hundred")
			Set(1001, subk="one-hundred")
			Set(2, subk="one-hundred-ten")
			Set(0, subk="one-hundred-ten")
		`)

		t.Run("test row keys", func(t *testing.T) {
			// the execututor returns row IDs when the field has keys, so they should be included in the target.
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "generalk", RowID: 1, RowKey: "ten"}, {Field: "subk", RowID: 1, RowKey: "one-hundred"}}, Count: 3},
				{Group: []pilosa.FieldRow{{Field: "generalk", RowID: 1, RowKey: "ten"}, {Field: "subk", RowID: 2, RowKey: "one-hundred-ten"}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "generalk", RowID: 2, RowKey: "eleven"}, {Field: "subk", RowID: 2, RowKey: "one-hundred-ten"}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "generalk", RowID: 3, RowKey: "twelve"}, {Field: "subk", RowID: 2, RowKey: "one-hundred-ten"}}, Count: 1},
			}

			results := c.Query(t, "i", `GroupBy(Rows(generalk), Rows(subk))`).Results[0].([]pilosa.GroupCount)
			test.CheckGroupBy(t, expected, results)

		})

	}
	for size := range []int{1, 3} {
		t.Run(fmt.Sprintf("%d_nodes", size), func(t *testing.T) {
			groupByTest(t, size)
		})
	}
}

func BenchmarkGroupBy(b *testing.B) {
	c := test.MustNewCluster(b, 1)
	var err error
	c[0].Config.DataDir, err = ioutil.TempDir(*TempDir, "benchmarkGroupBy")
	if err != nil {
		b.Fatalf("getting temp dir: %v", err)
	}
	err = c.Start()
	if err != nil {
		b.Fatalf("starting cluster: %v", err)
	}
	defer c.Close()
	c.CreateField(b, "i", pilosa.IndexOptions{}, "a")
	c.CreateField(b, "i", pilosa.IndexOptions{}, "b")
	c.CreateField(b, "i", pilosa.IndexOptions{}, "c")
	// Set up identical representative data in 3 fields. In each row, we'll set
	// a certain bit pattern for 100 bits, then skip 1000 up to ShardWidth.
	bits := make([][2]uint64, 0)
	for i := uint64(0); i < ShardWidth; i++ {
		// row 0 has 100 bit runs
		bits = append(bits, [2]uint64{0, i})
		if i%2 == 1 {
			// row 1 has odd bits set
			bits = append(bits, [2]uint64{1, i})
		}
		if i%2 == 0 {
			// row 2 has even bits set
			bits = append(bits, [2]uint64{2, i})
		}
		if i%27 == 0 {
			// row 3 has every 27th bit set
			bits = append(bits, [2]uint64{3, i})
		}
		if i%100 == 99 {
			i += 1000
		}
	}
	c.ImportBits(b, "i", "a", bits)
	c.ImportBits(b, "i", "b", bits)
	c.ImportBits(b, "i", "c", bits)

	b.Run("single shard group by", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c.Query(b, "i", `GroupBy(Rows(a), Rows(b), Rows(c))`)
		}
	})

	b.Run("single shard with limit", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c.Query(b, "i", `GroupBy(Rows(a), Rows(b), Rows(c), limit=4)`)
		}
	})

	// TODO benchmark over multiple shards

	// TODO benchmark paging over large numbers of rows

}

func runCallTest(t *testing.T, writeQuery string, readQueries []string, indexOptions *pilosa.IndexOptions, fieldOption ...pilosa.FieldOption) []pilosa.QueryResponse {
	t.Helper()

	if indexOptions == nil {
		indexOptions = &pilosa.IndexOptions{}
	}

	c := test.MustRunCluster(t, 1)
	defer c.Close()

	hldr := test.Holder{Holder: c[0].Server.Holder()}
	index := hldr.MustCreateIndexIfNotExists("i", *indexOptions)
	_, err := index.CreateField("f", fieldOption...)
	if err != nil {
		t.Fatal(err)
	}
	if writeQuery != "" {
		if _, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "i",
			Query: writeQuery,
		}); err != nil {
			t.Fatal(err)
		}
	}

	responses := []pilosa.QueryResponse{}
	for _, query := range readQueries {
		res, err := c[0].API.Query(context.Background(),
			&pilosa.QueryRequest{
				Index: "i",
				Query: query,
			})
		if err != nil {
			t.Fatal(err)
		}
		responses = append(responses, res)
	}

	return responses
}

func TestExecutor_Execute_Shift(t *testing.T) {
	t.Run("Shift Bit 0", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}
		hldr.SetBit("i", "general", 10, 0)

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Row(general=10), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Shift(Row(general=10), n=1), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("Shift container boundary", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}
		hldr.SetBit("i", "general", 10, 65535)

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Row(general=10), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{65536}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("Shift shard boundary", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}

		orig := []uint64{1, ShardWidth - 1, ShardWidth + 1}
		shift1 := []uint64{2, ShardWidth, ShardWidth + 2}
		shift2 := []uint64{3, ShardWidth + 1, ShardWidth + 3}

		for _, bit := range orig {
			hldr.SetBit("i", "general", 10, bit)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Row(general=10), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, shift1) {
			t.Fatalf("unexpected shift by 1: expected: %+v, but got: %+v", shift1, columns)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Row(general=10), n=2)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, shift2) {
			t.Fatalf("unexpected shift by 2: expected: %+v, but got: %+v", shift2, columns)
		}

		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Shift(Row(general=10)))`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, orig) {
			t.Fatalf("unexpected shift by 0: expected: %+v, but got: %+v", orig, columns)
		}
	})

	t.Run("Shift shard boundary no create", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := test.Holder{Holder: c[0].Server.Holder()}
		hldr.SetBit("i", "general", 10, ShardWidth-2) //shardwidth -1
		hldr.SetBit("i", "general", 10, ShardWidth-1) //shardwidth
		hldr.SetBit("i", "general", 10, ShardWidth)   //shardwidth +1
		hldr.SetBit("i", "general", 10, ShardWidth+2) //shardwidth +3

		exp := []uint64{ShardWidth - 1, ShardWidth, ShardWidth + 1, ShardWidth + 3}
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Row(general=10), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, exp) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
		if res, err := c[0].API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Shift(Row(general=10), n=1), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{ShardWidth, ShardWidth + 1, ShardWidth + 2, ShardWidth + 4}) {
			t.Fatalf("unexpected columns: \n%+v\n%+v", columns, exp)
		}
	})
}
