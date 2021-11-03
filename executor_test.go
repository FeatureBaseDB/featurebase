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
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	_ "net/http/pprof"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	pilosa "github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/boltdb"
	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/http"
	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/proto"
	"github.com/molecula/featurebase/v2/server"
	"github.com/molecula/featurebase/v2/storage"
	"github.com/molecula/featurebase/v2/test"
	"github.com/molecula/featurebase/v2/testhook"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
	"github.com/pkg/errors"
)

// writable initializes Tx that update, use !writable for read-only.
const writable = true

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

func TestExecutor(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	// Ensure a row query can be executed.
	t.Run("ExecuteRow", func(t *testing.T) {
		t.Run("RowIDColumnID", func(t *testing.T) {
			writeQuery := `` +
				fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
				fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
				fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 20) +
				`Set(1000, f=100)`
			readQueries := []string{`Row(f=10)`}
			responses := runCallTest(c, t, writeQuery, readQueries, nil)
			if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1}) {
				t.Fatalf("unexpected columns: %+v", bits)
			}
		})

		t.Run("RowIDColumnKey", func(t *testing.T) {
			writeQuery := `
				Set("one-hundred", f=1)
				Set("two-hundred", f=1)`
			readQueries := []string{`Row(f=1)`}
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldKeys())
			if diff := cmp.Diff(responses[0].Results, []interface{}{
				&pilosa.Row{Keys: []string{"bat", "foo"}},
			}, cmpopts.IgnoreUnexported(pilosa.Row{})); diff != "" {
				t.Fatal(diff)
			}
		})
	})

	t.Run("Execute_Difference", func(t *testing.T) {
		t.Run("RowIDColumnKey", func(t *testing.T) {
			writeQuery := `
				Set("one", f=10)
				Set("two", f=10)
				Set("three", f=10)
				Set("two", f=11)
				Set("four", f=11)`
			readQueries := []string{`Difference(Row(f=10), Row(f=11))`}
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true})
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "one"}) {
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldKeys())
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one", "three"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})
	})

	t.Run("Intersect", func(t *testing.T) {
		t.Run("RowIDColumnKey", func(t *testing.T) {
			writeQuery := `
				Set("one", f=10)
				Set("one-hundred", f=10)
				Set("two-hundred", f=10)
				Set("one", f=11)
				Set("two", f=11)
				Set("two-hundred", f=11)`
			readQueries := []string{`Intersect(Row(f=10), Row(f=11))`}
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldKeys())
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one", "two-hundred"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})
	})

	t.Run("Union", func(t *testing.T) {
		t.Run("RowIDColumnKey", func(t *testing.T) {
			writeQuery := `
				Set("one", f=10)
				Set("one-hundred", f=10)
				Set("two-hundred", f=10)
				Set("one", f=11)
				Set("two", f=11)
				Set("two-hundred", f=11)`
			readQueries := []string{`Union(Row(f=10), Row(f=11))`}
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true})
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"one", "two-hundred", "one-hundred", "two"}) {
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldKeys())
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"two-hundred", "two", "one-hundred", "one"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})
	})

	t.Run("Xor", func(t *testing.T) {
		t.Run("RowIDColumnKey", func(t *testing.T) {
			writeQuery := `
				Set("one", f=10)
				Set("one-hundred", f=10)
				Set("two-hundred", f=10)
				Set("one", f=11)
				Set("two", f=11)
				Set("two-hundred", f=11)`
			readQueries := []string{`Xor(Row(f=10), Row(f=11))`}
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true})
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"two", "one-hundred"}) {
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldKeys())
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"two", "one-hundred"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})
	})

	t.Run("Count", func(t *testing.T) {
		t.Run("RowIDColumnKey", func(t *testing.T) {
			writeQuery := `
				Set("three", f=10)
				Set("one-hundred", f=10)
				Set("two-hundred", f=11)`
			readQueries := []string{`Count(Row(f=10))`}
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldKeys())
			if responses[0].Results[0] != uint64(2) {
				t.Fatalf("unexpected n: %d", responses[0].Results[0])
			}
		})
	})

	t.Run("Set", func(t *testing.T) {
		t.Run("RowIDColumnKey", func(t *testing.T) {
			readQueries := []string{`Set("three", f=10)`}
			responses := runCallTest(c, t, "", readQueries,
				&pilosa.IndexOptions{Keys: true})
			if !responses[0].Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
		})

		t.Run("RowKeyColumnID", func(t *testing.T) {
			readQueries := []string{`Set(1, f="ten")`}
			responses := runCallTest(c, t, "", readQueries,
				nil, pilosa.OptFieldKeys())
			if !responses[0].Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
		})
	})

	t.Run("Clear", func(t *testing.T) {
		t.Run("RowIDColumnID", func(t *testing.T) {
			writeQuery := `Set(3, f=10)`
			readQueries := []string{`Clear(3, f=10)`}
			responses := runCallTest(c, t, writeQuery, readQueries, nil)
			if !responses[0].Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
		})

		t.Run("RowIDColumnKey", func(t *testing.T) {
			writeQuery := `Set("three", f=10)`
			readQueries := []string{`Clear("three", f=10)`}
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true})
			if !responses[0].Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
		})

		t.Run("RowKeyColumnID", func(t *testing.T) {
			writeQuery := `Set(1, f="ten")`
			readQueries := []string{`Clear(1, f="ten")`}
			responses := runCallTest(c, t, writeQuery, readQueries,
				nil, pilosa.OptFieldKeys())
			if !responses[0].Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
		})

		t.Run("RowKeyColumnKey", func(t *testing.T) {
			writeQuery := `Set("one", f="ten")`
			readQueries := []string{`Clear("one", f="ten")`}
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldKeys())
			if !responses[0].Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
		})

		t.Run("RowKeyColumnKey_NotClearNot", func(t *testing.T) {
			writeQuery := `Set("056009039|q2db_3385|11", f="all_users")`
			readQueries := []string{
				`Not(Row(f="has_deleted_date"))`,
				`Clear("056009039|q2db_3385|11", f="has_deleted_date")`,
				`Not(Row(f="has_deleted_date")) `,
			}
			results := []interface{}{
				"056009039|q2db_3385|11",
				false,
				"056009039|q2db_3385|11",
			}

			responses := runCallTest(c, t, writeQuery, readQueries, &pilosa.IndexOptions{
				Keys:           true,
				TrackExistence: true,
			}, pilosa.OptFieldKeys())
			for i, resp := range responses {
				if len(resp.Results) != 1 {
					t.Fatalf("response %d: len(results) expected: 1, got: %d", i, len(resp.Results))
				}

				switch r := resp.Results[0].(type) {
				case bool:
					if results[i] != r {
						t.Fatalf("response %d: expected: %v, got: %v", i, results[i], r)
					}

				case *pilosa.Row:
					if len(r.Keys) != 1 {
						t.Fatalf("response %d: len(keys) expected: 1, got: %d", i, len(r.Keys))
					}
					if results[i] != r.Keys[0] {
						t.Fatalf("response %d: expected: %v, got: %v", i, results[i], r.Keys[0])
					}

				default:
					t.Fatalf("response %d: expected: %T, got: %T", i, results[i], r)
				}
			}
		})
	})

	t.Run("Range", func(t *testing.T) {
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))

			t.Run("Standard", func(t *testing.T) {
				if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"six", "four", "five", "seven", "two", "three"}) {
					t.Fatalf("unexpected keys: %+v", keys)
				}
			})

			t.Run("Clear", func(t *testing.T) {
				if keys := responses[2].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"six", "four", "five", "seven", "three"}) {
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")),
				pilosa.OptFieldKeys())

			t.Run("Standard", func(t *testing.T) {
				if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "two", "five", "seven", "six", "four"}) {
					t.Fatalf("unexpected keys: %+v", keys)
				}
			})

			t.Run("Clear", func(t *testing.T) {
				if keys := responses[2].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "five", "seven", "six", "four"}) {
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
	})

	t.Run("Range_Deprecated", func(t *testing.T) {
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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

			t.Run("OtherRange", func(t *testing.T) {
				rq2 := []string{
					`Range(f=1, 1999-12-31T00:00, 2002-01-01T03:00)`,
				}
				responses = runCallTest(c, t, writeQuery, rq2,
					nil, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))
				t.Run("OldRange", func(t *testing.T) {
					if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
						t.Fatalf("unexpected columns: %+v", columns)
					}
				})
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))

			t.Run("Standard", func(t *testing.T) {
				if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"two", "three", "seven", "four", "five", "six"}) {
					t.Fatalf("unexpected keys: %+v", keys)
				}
			})

			t.Run("Clear", func(t *testing.T) {
				if keys := responses[2].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "seven", "four", "five", "six"}) {
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{Keys: true},
				pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")),
				pilosa.OptFieldKeys())

			t.Run("Standard", func(t *testing.T) {
				if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "five", "six", "two", "seven", "four"}) {
					t.Fatalf("unexpected keys: %+v", keys)
				}
			})

			t.Run("Clear", func(t *testing.T) {
				if keys := responses[2].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"three", "five", "six", "seven", "four"}) {
					t.Fatalf("unexpected keys: %+v", keys)
				}
			})
		})
	})

	t.Run("Options", func(t *testing.T) {
		t.Run("shards", func(t *testing.T) {
			writeQuery := fmt.Sprintf(`
				Set(100, f=10)
				Set(%d, f=10)
				Set(%d, f=10)`, ShardWidth, ShardWidth*2)
			readQueries := []string{`Options(Row(f=10), shards=[0, 2])`}
			responses := runCallTest(c, t, writeQuery, readQueries, nil)
			if bits := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{100, ShardWidth * 2}) {
				t.Fatalf("unexpected columns: %+v", bits)
			}
		})
	})

	t.Run("Not", func(t *testing.T) {
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{
					TrackExistence: true,
					Keys:           true,
				}, pilosa.OptFieldKeys())
			if keys := responses[0].Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(keys, []string{"sw1", "three"}) {
				t.Fatalf("unexpected keys: %+v", keys)
			}
		})
	})

	t.Run("ClearRow", func(t *testing.T) {
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
			responses := runCallTest(c, t, writeQuery, readQueries,
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
				`Row(f=1, from=2002-01-01T00:00, to=2002-01-02T00:00)`,
				`ClearRow(f=1)`,
				`Row(f=1, from=1999-12-31T00:00, to=2003-01-01T03:00)`,
				`Row(f=10, from=1999-12-31T00:00, to=2003-01-01T03:00)`,
			}
			responses := runCallTest(c, t, writeQuery, readQueries,
				&pilosa.IndexOptions{TrackExistence: true},
				pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD")))
			if columns := responses[0].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2, 3, 4, 5, 6, 7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}

			// Single day query (regression test)
			if columns := responses[1].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{7}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}

			// Clear the row and ensure we get a `true` response.
			if res := responses[2].Results[0].(bool); !res {
				t.Fatalf("unexpected clear row result: %+v", res)
			}

			// Ensure the row is empty.
			if columns := responses[3].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}

			// Ensure other rows were not affected.
			if columns := responses[4].Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2}) {
				t.Fatalf("unexpected columns: %+v", columns)
			}

		})
		// Ensure that ClearRow returns false when the row to clear needs translation.
		t.Run("WithKeys", func(t *testing.T) {
			wq := ""
			rq := []string{
				`ClearRow(f="bar")`,
			}

			responses := runCallTest(c, t, wq, rq, &pilosa.IndexOptions{}, pilosa.OptFieldKeys())
			if res := responses[0].Results[0].(bool); res {
				t.Fatalf("unexpected result: %+v", res)
			}
		})
	})

	t.Run("RowsTime", func(t *testing.T) {
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
			`Rows(f, from=2002-01-01T00:00, to=2002-01-02T00:00)`,
		}
		expResults := [][]uint64{
			{1},
			{2, 3, 13},
			{},
			{1, 2, 3, 4, 13},
			{2, 3, 4, 13},
			{1, 2, 3, 13},
			{2},
		}

		responses := runCallTest(c, t, writeQuery, readQueries,
			nil, pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMD"), true))

		for i := range responses {
			t.Run(fmt.Sprintf("response-%d", i), func(t *testing.T) {
				if rows := responses[i].Results[0].(pilosa.RowIdentifiers).Rows; !reflect.DeepEqual(rows, expResults[i]) {
					t.Fatalf("unexpected rows: %+v", rows)
				}
			})
		}
	})
}

func runCallTest(c *test.Cluster, t *testing.T, writeQuery string, readQueries []string, indexOptions *pilosa.IndexOptions, fieldOption ...pilosa.FieldOption) []pilosa.QueryResponse {
	t.Helper()
	indexName := fmt.Sprintf("i_%x", md5.Sum([]byte(t.Name())))

	if indexOptions == nil {
		indexOptions = &pilosa.IndexOptions{}
	}

	hldr := c.GetHolder(0)
	index, err := hldr.CreateIndex(indexName, *indexOptions)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	_, err = index.CreateField("f", fieldOption...)
	if err != nil {
		t.Fatal(err)
	}
	if writeQuery != "" {
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
			Index: indexName,
			Query: writeQuery,
		}); err != nil {
			t.Fatal(err)
		}
	}

	responses := []pilosa.QueryResponse{}
	for _, query := range readQueries {
		res, err := c.GetNode(0).API.Query(context.Background(),
			&pilosa.QueryRequest{
				Index: indexName,
				Query: query,
			})
		if err != nil {
			t.Fatal(err)
		}
		responses = append(responses, res)
	}

	return responses
}

func TestExecutor_Execute_ConstRow(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	c.CreateField(t, "i", pilosa.IndexOptions{}, "h")
	c.ImportBits(t, "i", "h", [][2]uint64{
		{1, 2},
		{3, 4},
		{5, 6},
	})

	resp := c.Query(t, "i", `ConstRow(columns=[2,6])`)
	expect := []uint64{2, 6}
	got := resp.Results[0].(*pilosa.Row).Columns()
	if !reflect.DeepEqual(expect, got) {
		t.Errorf("expected %v but got %v", expect, got)
	}
}

// Ensure a difference query can be executed.
func TestExecutor_Execute_Difference(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		hldr.SetBit("i", "general", 10, 1)
		hldr.SetBit("i", "general", 10, 2)
		hldr.SetBit("i", "general", 10, 3)
		hldr.SetBit("i", "general", 11, 2)
		hldr.SetBit("i", "general", 11, 4)

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Difference(Row(general=10), Row(general=11))`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, 3}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})
}

// Ensure an empty difference query behaves properly.
func TestExecutor_Execute_Empty_Difference(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)
	hldr.SetBit("i", "general", 10, 1)

	if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Difference()`}); err == nil {
		t.Fatalf("Empty Difference query should give error, but got %v", res)
	}
}

// Ensure an intersect query can be executed.
func TestExecutor_Execute_Intersect(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		hldr.SetBit("i", "general", 10, 1)
		hldr.SetBit("i", "general", 10, ShardWidth+1)
		hldr.SetBit("i", "general", 10, ShardWidth+2)
		hldr.SetBit("i", "general", 11, 1)
		hldr.SetBit("i", "general", 11, 2)
		hldr.SetBit("i", "general", 11, ShardWidth+2)

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Intersect(Row(general=10), Row(general=11))`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, ShardWidth + 2}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})
}

// Ensure an empty intersect query behaves properly.
func TestExecutor_Execute_Empty_Intersect(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Intersect()`}); err == nil {
		t.Fatalf("Empty Intersect query should give error, but got %v", res)
	}
}

// Ensure a union query can be executed.
func TestExecutor_Execute_Union(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		hldr.SetBit("i", "general", 10, 0)
		hldr.SetBit("i", "general", 10, ShardWidth+1)
		hldr.SetBit("i", "general", 10, ShardWidth+2)

		hldr.SetBit("i", "general", 11, 2)
		hldr.SetBit("i", "general", 11, ShardWidth+2)

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Union(Row(general=10), Row(general=11))`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{0, 2, ShardWidth + 1, ShardWidth + 2}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})
}

// Ensure an empty union query behaves properly.
func TestExecutor_Execute_Empty_Union(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)
	hldr.SetBit("i", "general", 10, 0)

	if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Union()`}); err != nil {
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
		hldr := c.GetHolder(0)

		hldr.SetBit("i", "general", 10, 0)
		hldr.SetBit("i", "general", 10, ShardWidth+1)
		hldr.SetBit("i", "general", 10, ShardWidth+2)

		hldr.SetBit("i", "general", 11, 2)
		hldr.SetBit("i", "general", 11, ShardWidth+2)

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Xor(Row(general=10), Row(general=11))`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{0, 2, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

}

// Ensure a count query can be executed.
func TestExecutor_Execute_Count(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		hldr.SetBit("i", "f", 10, 3)
		hldr.SetBit("i", "f", 10, ShardWidth+1)
		hldr.SetBit("i", "f", 10, ShardWidth+2)

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Count(Row(f=10))`}); err != nil {
			t.Fatal(err)
		} else if res.Results[0] != uint64(3) {
			t.Fatalf("unexpected n: %d", res.Results[0])
		}
	})

}

func roaringOnlyTest(t *testing.T) {
	src := pilosa.CurrentBackend()
	if src == pilosa.RoaringTxn || (storage.DefaultBackend == pilosa.RoaringTxn && src == "") {
		// okay to run, we are under roaring only
	} else {
		t.Skip("skip for everything but roaring")
	}
}

// Ensure a set query can be executed.
func TestExecutor_Execute_Set(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster.GetNode(0)
		hldr := cluster.GetHolder(0)
		hldr.SetBit("i", "f", 1, 0) // creates and commits a Tx internally.

		t.Run("OK", func(t *testing.T) {
			hldr.ClearBit("i", "f", 11, 1)
			if n := hldr.Row("i", "f", 11).Count(); n != 0 {
				t.Fatalf("unexpected row count: %d", n)
			}

			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1, f=11)`}); err != nil {
				t.Fatal(err)
			} else if !res.Results[0].(bool) {
				t.Fatalf("expected column changed")
			}

			if n := hldr.Row("i", "f", 11).Count(); n != 1 {
				t.Fatalf("unexpected row count: %d", n)
			}
			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1, f=11)`}); err != nil {
				t.Fatal(err)
			} else if res.Results[0].(bool) {
				t.Fatalf("expected column unchanged")
			}
		})

		t.Run("ErrInvalidColValueType", func(t *testing.T) {
			if _, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set("foo", f=1)`}); err == nil || !strings.Contains(err.Error(), "unkeyed index") {
				t.Fatalf("The error is: '%v'", err)
			}
		})

		t.Run("ErrInvalidRowValueType", func(t *testing.T) {
			if _, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(2, f="bar")`}); err == nil || !strings.Contains(err.Error(), "cannot create keys on unkeyed field") {
				t.Fatal(err)
			}
		})
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		cluster := test.MustRunCluster(t, 1)
		defer cluster.Close()
		cmd := cluster.GetNode(0)
		hldr := cluster.GetHolder(0)
		idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{Keys: true})

		t.Run("OK", func(t *testing.T) {
			hldr.SetBit("i", "f", 1, 0) // creates and Commits a Tx internally.
			if n := hldr.Row("i", "f", 11).Count(); n != 0 {
				t.Fatalf("unexpected row count: %d", n)
			}

			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set("foo", f=11)`}); err != nil {
				t.Fatal(err)
			} else if !res.Results[0].(bool) {
				t.Fatalf("expected column changed")
			}

			if n := hldr.Row("i", "f", 11).Count(); n != 1 {
				t.Fatalf("unexpected row count: %d", n)
			}
			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set("foo", f=11)`}); err != nil {
				t.Fatal(err)
			} else if res.Results[0].(bool) {
				t.Fatalf("expected column unchanged")
			}

			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(2, f=11)`}); err != nil {
				t.Fatal(err)
			} else if !res.Results[0].(bool) {
				t.Fatalf("expected column changed with integer column key")
			}

			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(2, f=11)`}); err != nil {
				t.Fatal(err)
			} else if res.Results[0].(bool) {
				t.Fatalf("expected column unchanged with integer column key")
			}
		})

		t.Run("ErrInvalidColValueType", func(t *testing.T) {
			hldr.SetBit("i", "f", 1, 0) // creates and Commits a Tx internally.

			if err := idx.DeleteField("f"); err != nil {
				t.Fatal(err)
			}

			if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
				t.Fatal(err)
			}

			if _, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(2.1, f=1)`}); err == nil || !strings.Contains(err.Error(), "parse error") {
				t.Fatal(err)
			}

			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(2, f=1)`}); err != nil {
				t.Fatal(err)
			} else if !res.Results[0].(bool) {
				t.Fatalf("expected column changed with integer column key")
			}
		})

		t.Run("ErrInvalidRowValueType", func(t *testing.T) {
			idx := hldr.MustCreateIndexIfNotExists("inokey", pilosa.IndexOptions{})
			if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault(), pilosa.OptFieldKeys()); err != nil {
				t.Fatal(err)
			}
			if _, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "inokey", Query: `Set(2, f=1.2)`}); err == nil || !strings.Contains(err.Error(), "invalid value") {
				t.Fatal(err)
			}

			if res, err := cmd.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(2, f=9)`}); err != nil {
				t.Fatal(err)
			} else if !res.Results[0].(bool) {
				t.Fatalf("expected column changed with integer column key")
			}

		})
	})
}

// Ensure a set query can be executed on a bool field.
func TestExecutor_Execute_SetBool(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Create fields.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeBool()); err != nil {
			t.Fatal(err)
		}

		// Set a true bit.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f=true)`}); err != nil {
			t.Fatal(err)
		} else if !res.Results[0].(bool) {
			t.Fatalf("expected column changed")
		}

		// Set the same bit to true again verify nothing changed.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f=true)`}); err != nil {
			t.Fatal(err)
		} else if res.Results[0].(bool) {
			t.Fatalf("expected column to be unchanged")
		}

		// Set the same bit to false.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f=false)`}); err != nil {
			t.Fatal(err)
		} else if !res.Results[0].(bool) {
			t.Fatalf("expected column changed")
		}

		// Ensure that the false row is set.
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=false)`}); err != nil {
			t.Fatal(err)
		} else if columns := result.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{100}) {
			t.Fatalf("unexpected colums: %+v", columns)
		}

		// Ensure that the true row is empty.
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=true)`}); err != nil {
			t.Fatal(err)
		} else if columns := result.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{}) {
			t.Fatalf("unexpected colums: %+v", columns)
		}
	})
	t.Run("Error", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Create fields.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeBool()); err != nil {
			t.Fatal(err)
		}

		// Set bool using a string value.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f="true")`}); err == nil {
			t.Fatalf("expected invalid bool type error")
		}

		// Set bool using an integer.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f=1)`}); err == nil {
			t.Fatalf("expected invalid bool type error")
		}

	})
}

// Ensure a set query can be executed on a decimal field.
func TestExecutor_Execute_SetDecimal(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Create fields.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeDecimal(2)); err != nil {
			t.Fatal(err)
		}

		// Set a value.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1000, f=1.5)`}); err != nil {
			t.Fatal(err)
		} else if !res.Results[0].(bool) {
			t.Fatalf("expected column changed")
		}

		// Set the same value again verify nothing changed.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1000, f=1.5)`}); err != nil {
			t.Fatal(err)
		} else if res.Results[0].(bool) {
			t.Fatalf("expected column to be unchanged")
		}

		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f == 1.5)`}); err != nil {
			t.Fatal(err)
		} else if columns := result.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1000}) {
			t.Fatalf("unexpected colums: %+v", columns)
		}

		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f > 1.4999)`}); err != nil {
			t.Fatal(err)
		} else if columns := result.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1000}) {
			t.Fatalf("unexpected colums: %+v", columns)
		}
	})
	t.Run("Error", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Create fields.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeDecimal(2)); err != nil {
			t.Fatal(err)
		}

		// Set decimal using a string value.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1000, f="1.5")`}); err == nil {
			t.Fatalf("expected invalid decimal type error")
		}
	})
}

// Ensure old PQL syntax doesn't break anything too badly.
func TestExecutor_Execute_OldPQL(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)

	// set a bit so the view gets created.
	hldr.SetBit("i", "f", 1, 0)

	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `SetBit(frame=f, row=11, col=1)`}); err == nil || errors.Cause(err).Error() != "unknown call: SetBit" {
		t.Fatalf("Expected error: 'unknown call: SetBit', got: %v. Full: %v", errors.Cause(err), err)
	}
}

// Ensure a SetValue() query can be executed.
func TestExecutor_Execute_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Create fields.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
			t.Fatal(err)
		} else if _, err := index.CreateFieldIfNotExists("xxx", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		// Set bsiGroup values.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(10, f=25)`}); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f=10)`}); err != nil {
			t.Fatal(err)
		}

		// Obtain transaction.
		idx := index.Index
		shard := uint64(0)
		tx := idx.Txf().NewTx(pilosa.Txo{Write: !writable, Index: idx, Shard: shard})
		defer tx.Rollback()

		f := hldr.Field("i", "f")
		if value, exists, err := f.Value(tx, 10); err != nil {
			t.Fatal(err)
		} else if !exists {
			t.Fatal("expected value to exist")
		} else if value != 25 {
			t.Fatalf("unexpected value: %v", value)
		}

		if value, exists, err := f.Value(tx, 100); err != nil {
			t.Fatal(err)
		} else if !exists {
			t.Fatal("expected value to exist")
		} else if value != 10 {
			t.Fatalf("unexpected value: %v", value)
		}
	})

	t.Run("Err", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
			t.Fatal(err)
		}

		t.Run("ColumnBSIGroupRequired", func(t *testing.T) {
			if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(f=100)`}); err == nil || errors.Cause(err).Error() != `Set() column argument 'col' required` {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("ColumnBSIGroupValue", func(t *testing.T) {
			if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set("bad_column", f=100)`}); err == nil || !strings.Contains(err.Error(), "unkeyed index") {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("InvalidBSIGroupValueType", func(t *testing.T) {
			if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(10, f="hello")`}); err == nil || !strings.Contains(err.Error(), "cannot create keys on unkeyed field") {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	})

	t.Run("Timestamp", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Create fields.
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
		if _, err := index.CreateFieldIfNotExists("f", pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds)); err != nil {
			t.Fatal(err)
		} else if _, err := index.CreateFieldIfNotExists("xxx", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		// Set bsiGroup values.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(10, f='2000-01-01T00:00:00.000000000Z')`}); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(100, f='2000-01-02T00:00:00Z')`}); err != nil {
			t.Fatal(err)
		}

		// Obtain transaction.
		idx := index.Index
		shard := uint64(0)
		tx := idx.Txf().NewTx(pilosa.Txo{Write: !writable, Index: idx, Shard: shard})
		defer tx.Rollback()

		f := hldr.Field("i", "f")
		if value, exists, err := f.Value(tx, 10); err != nil {
			t.Fatal(err)
		} else if !exists {
			t.Fatal("expected value to exist")
		} else if value != time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()/int64(time.Second) {
			t.Fatalf("unexpected value: %v", value)
		}

		if value, exists, err := f.Value(tx, 100); err != nil {
			t.Fatal(err)
		} else if !exists {
			t.Fatal("expected value to exist")
		} else if value != time.Date(2000, time.January, 2, 0, 0, 0, 0, time.UTC).UnixNano()/int64(time.Second) {
			t.Fatalf("unexpected value: %v", value)
		}
	})

}

func TestExecutor_Execute_TopK_Set(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	// Load some test data into a set field.
	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "f")
	c.ImportBits(t, "i", "f", [][2]uint64{
		{0, 0},
		{0, 1},
		{0, ShardWidth + 2},
		{10, 2},
		{10, ShardWidth},
		{10, 2 * ShardWidth},
		{10, ShardWidth + 1},
		{20, ShardWidth},
	})

	// Execute query.
	if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopK(f, k=2)`}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result.Results, []interface{}{&pilosa.PairsField{
		Pairs: []pilosa.Pair{
			{ID: 10, Count: 4},
			{ID: 0, Count: 3},
		},
		Field: "f",
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

func TestExecutor_Execute_TopK_Time(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	// Load some test data into a time field.
	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "f", pilosa.OptFieldTypeTime("YMD", true))
	c.Query(t, "i", `
		Set(0, f=0, 2016-01-02T00:00)
		Set(0, f=1, 2016-01-02T00:00)
		Set(0, f=0, 2016-01-03T00:00)
		Set(1, f=0, 2016-01-10T00:00)
		Set(100000000, f=2, 2016-02-02T00:00)
		Set(200000000, f=3, 2015-01-02T00:00)
	`)

	// Execute query.
	if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopK(f, k=3, from=2016-01-01T00:00, to=2016-01-11T00:00)`}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result.Results, []interface{}{&pilosa.PairsField{
		Pairs: []pilosa.Pair{
			{ID: 0, Count: 2},
			{ID: 1, Count: 1},
		},
		Field: "f",
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure a TopN() query can be executed.
func TestExecutor_Execute_TopN(t *testing.T) {
	t.Run("RowIDColumnID", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("other", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
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

		err := c.GetNode(0).RecalculateCaches(t)
		if err != nil {
			t.Fatalf("recalculating caches: %v", err)
		}

		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=2)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result.Results[0], &pilosa.PairsField{
			Pairs: []pilosa.Pair{
				{ID: 0, Count: 5},
				{ID: 10, Count: 2},
			},
			Field: "f",
		}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("RowIDColumnKey", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f"); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("other"); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
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

		err := c.GetNode(0).RecalculateCaches(t)
		if err != nil {
			t.Fatalf("recalculating caches: %v", err)
		}

		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=2)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(result.Results[0], &pilosa.PairsField{
			Pairs: []pilosa.Pair{
				{ID: 0, Count: 5},
				{ID: 10, Count: 2},
			},
			Field: "f",
		}) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("other", pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
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

		err := c.GetNode(0).RecalculateCaches(t)
		if err != nil {
			t.Fatalf("recalculating caches: %v", err)
		}

		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=2)`}); err != nil {
			t.Fatal(err)
		} else {
			if !reflect.DeepEqual(result.Results[0], &pilosa.PairsField{
				Pairs: []pilosa.Pair{
					{Key: "zero", Count: 5},
					{Key: "ten", Count: 2},
				},
				Field: "f",
			}) {
				t.Fatalf("unexpected result: %s", spew.Sdump(result))
			}
		}
	})

	t.Run("RowKeyColumnKey", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Set columns for rows 0, 10, & 20 across two shards.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault(), pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("other", pilosa.OptFieldTypeDefault(), pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
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

		err := c.GetNode(0).RecalculateCaches(t)
		if err != nil {
			t.Fatalf("recalculating caches: %v", err)
		}

		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=2)`}); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(result.Results, []interface{}{
			&pilosa.PairsField{
				Pairs: []pilosa.Pair{
					{Key: "foo", Count: 5},
					{Key: "bar", Count: 2},
				},
				Field: "f",
			},
		}); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Set data on the "f" field.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set(0, f=0)
			Set(0, f=1)
		`}); err != nil {
			t.Fatal(err)
		} else if err := c.GetNode(0).RecalculateCaches(t); err != nil {
			t.Fatalf("recalculating caches: %v", err)
		}

		// Attempt to query the "g" field.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(g, n=2)`}); err == nil || err.Error() != `executing: field "g" not found` {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ErrBSIField", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		// Create BSI "f" field.
		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeInt(0, 100)); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=2)`}); err == nil || !strings.Contains(err.Error(), `finding top results: mapping on primary node: cannot compute TopN() on integer, decimal, or timestamp field: "f"`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ErrCacheNone", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		if idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateField("f", pilosa.OptFieldTypeSet(pilosa.CacheTypeNone, 0)); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set(0, f=0)
			Set(0, f=1)
		`}); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=2)`}); err == nil || !strings.Contains(err.Error(), `finding top results: mapping on primary node: cannot compute TopN(), field has no cache: "f"`) {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestExecutor_Execute_TopN_fill(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)

	// Set columns for rows 0, 10, & 20 across two shards.
	hldr.SetBit("i", "f", 0, 0)
	hldr.SetBit("i", "f", 0, 1)
	hldr.SetBit("i", "f", 0, 2)
	hldr.SetBit("i", "f", 0, ShardWidth)
	hldr.SetBit("i", "f", 1, ShardWidth+2)
	hldr.SetBit("i", "f", 1, ShardWidth)

	// Execute query.
	if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=1)`}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result.Results, []interface{}{&pilosa.PairsField{
		Pairs: []pilosa.Pair{
			{ID: 0, Count: 4},
		},
		Field: "f",
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure
func TestExecutor_Execute_TopN_fill_small(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)

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
	if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=1)`}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result.Results, []interface{}{&pilosa.PairsField{
		Pairs: []pilosa.Pair{
			{ID: 0, Count: 5},
		},
		Field: "f",
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure a TopN() query with a source row can be executed.
func TestExecutor_Execute_TopN_Src(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)

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

	err := c.GetNode(0).RecalculateCaches(t)
	if err != nil {
		t.Fatalf("recalculating caches: %v", err)
	}

	// Execute query.
	if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, Row(other=100), n=3)`}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result.Results, []interface{}{&pilosa.PairsField{
		Pairs: []pilosa.Pair{
			{ID: 20, Count: 3},
			{ID: 10, Count: 2},
			{ID: 0, Count: 1},
		},
		Field: "f",
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure Min()  and Max() queries can be executed.
func TestExecutor_Execute_MinMax(t *testing.T) {
	t.Run("WithOffset", func(t *testing.T) {
		t.Run("Int", func(t *testing.T) {
			c := test.MustRunCluster(t, 1)
			defer c.Close()
			hldr := c.GetHolder(0)

			idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
			if err != nil {
				t.Fatal(err)
			}

			tests := []struct {
				min int64
				max int64
				set int64
			}{
				{10, 20, 11},
				{-10, 20, 11},
				{-10, 20, -9},
				{-20, -10, -11},
			}
			for i, test := range tests {
				fld := fmt.Sprintf("f%d", i)
				t.Run("MinMaxField_"+fld, func(t *testing.T) {
					if _, err := idx.CreateField(fld, pilosa.OptFieldTypeInt(test.min, test.max)); err != nil {
						t.Fatal(err)
					}

					if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf(`
				Set(10, %s=%d)
			`, fld, test.set)}); err != nil {
						t.Fatal(err)
					}

					var pql string

					t.Run("Min", func(t *testing.T) {
						pql = fmt.Sprintf(`Min(field=%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: test.set, Count: 1}) {
							t.Fatalf("unexpected min result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Max", func(t *testing.T) {
						pql = fmt.Sprintf(`Max(field=%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: test.set, Count: 1}) {
							t.Fatalf("unexpected max result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Min", func(t *testing.T) {
						pql = fmt.Sprintf(`Min(field="%s")`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: test.set, Count: 1}) {
							t.Fatalf("unexpected min result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Max", func(t *testing.T) {
						pql = fmt.Sprintf(`Max(field="%s")`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: test.set, Count: 1}) {
							t.Fatalf("unexpected max result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Min", func(t *testing.T) {
						pql = fmt.Sprintf(`Min(%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: test.set, Count: 1}) {
							t.Fatalf("unexpected min result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Max", func(t *testing.T) {
						pql = fmt.Sprintf(`Max(%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: test.set, Count: 1}) {
							t.Fatalf("unexpected max result, test %d: %s", i, spew.Sdump(result))
						}
					})
				})
			}
		})

		t.Run("Decimal", func(t *testing.T) {
			c := test.MustRunCluster(t, 1)
			defer c.Close()
			hldr := c.GetHolder(0)

			idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
			if err != nil {
				t.Fatal(err)
			}

			tests := []struct {
				scale int64
				min   pql.Decimal
				max   pql.Decimal
				set   pql.Decimal
				exp   pql.Decimal
			}{
				{
					2,
					pql.Decimal{Value: 1, Scale: -1},
					pql.Decimal{Value: 2, Scale: -1},
					pql.Decimal{Value: 115, Scale: 1},
					pql.Decimal{Value: 1150, Scale: 2},
				},
				{
					2,
					pql.Decimal{Value: -1, Scale: -1},
					pql.Decimal{Value: 2, Scale: -1},
					pql.Decimal{Value: 115, Scale: 1},
					pql.Decimal{Value: 1150, Scale: 2},
				},
				{
					2,
					pql.Decimal{Value: -1, Scale: -1},
					pql.Decimal{Value: 2, Scale: -1},
					pql.Decimal{Value: -95, Scale: 1},
					pql.Decimal{Value: -950, Scale: 2},
				},
				{
					2,
					pql.Decimal{Value: -2, Scale: -1},
					pql.Decimal{Value: -1, Scale: -1},
					pql.Decimal{Value: -115, Scale: 1},
					pql.Decimal{Value: -1150, Scale: 2},
				},
			}
			// This extra field exists to make there be shards which are present,
			// but have no decimal values set, to make sure they don't break
			// the results.
			if _, err := idx.CreateFieldIfNotExists("z", pilosa.OptFieldTypeDefault()); err != nil {
				t.Fatal(err)
			}
			if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1, z=0)`}); err != nil {
				t.Fatal(err)
			} else if !res.Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
			// set things in other shards, that won't have decimal values
			if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1234567, z=0)`}); err != nil {
				t.Fatal(err)
			} else if !res.Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
			if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(2345678, z=0)`}); err != nil {
				t.Fatal(err)
			} else if !res.Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
			if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(3456789, z=0)`}); err != nil {
				t.Fatal(err)
			} else if !res.Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
			if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(4567890, z=0)`}); err != nil {
				t.Fatal(err)
			} else if !res.Results[0].(bool) {
				t.Fatalf("expected column changed")
			}
			for i, test := range tests {
				fld := fmt.Sprintf("f%d", i)
				t.Run("MinMaxField_"+fld, func(t *testing.T) {
					if _, err := idx.CreateField(fld, pilosa.OptFieldTypeDecimal(test.scale, test.min, test.max)); err != nil {
						t.Fatal(err)
					}

					if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf(`
                Set(6700000, %s=%s)
            `, fld, test.set)}); err != nil {
						t.Fatal(err)
					}

					var pql string

					t.Run("Min", func(t *testing.T) {
						pql = fmt.Sprintf(`Min(field=%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{DecimalVal: &test.exp, Count: 1}) {
							t.Fatalf("unexpected min result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Max", func(t *testing.T) {
						pql = fmt.Sprintf(`Max(field=%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{DecimalVal: &test.exp, Count: 1}) {
							t.Fatalf("unexpected max result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Min", func(t *testing.T) {
						pql = fmt.Sprintf(`Min(%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{DecimalVal: &test.exp, Count: 1}) {
							t.Fatalf("unexpected min result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Max", func(t *testing.T) {
						pql = fmt.Sprintf(`Max(%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{DecimalVal: &test.exp, Count: 1}) {
							t.Fatalf("unexpected max result, test %d: %s", i, spew.Sdump(result))
						}
					})
				})
			}
		})

		t.Run("Timestamp", func(t *testing.T) {
			c := test.MustRunCluster(t, 1)
			defer c.Close()
			hldr := c.GetHolder(0)

			idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
			if err != nil {
				t.Fatal(err)
			}

			tests := []struct {
				epoch time.Time
				set   time.Time
			}{
				{
					time.Date(2000, time.January, 10, 0, 0, 0, 0, time.UTC),
					time.Date(2000, time.January, 11, 0, 0, 0, 0, time.UTC),
				},
			}
			for i, test := range tests {
				fld := fmt.Sprintf("f%d", i)
				t.Run("MinMaxField_"+fld, func(t *testing.T) {
					if _, err := idx.CreateField(fld, pilosa.OptFieldTypeTimestamp(test.epoch, pilosa.TimeUnitSeconds)); err != nil {
						t.Fatal(err)
					} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf(`Set(10, %s="%s")`, fld, test.set.Format(time.RFC3339))}); err != nil {
						t.Fatal(err)
					}

					var pql string

					t.Run("Min", func(t *testing.T) {
						pql = fmt.Sprintf(`Min(field=%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{TimestampVal: test.set, Count: 1}) {
							t.Fatalf("unexpected min result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Max", func(t *testing.T) {
						pql = fmt.Sprintf(`Max(field=%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{TimestampVal: test.set, Count: 1}) {
							t.Fatalf("unexpected max result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Min", func(t *testing.T) {
						pql = fmt.Sprintf(`Min(field="%s")`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{TimestampVal: test.set, Count: 1}) {
							t.Fatalf("unexpected min result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Max", func(t *testing.T) {
						pql = fmt.Sprintf(`Max(field="%s")`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{TimestampVal: test.set, Count: 1}) {
							t.Fatalf("unexpected max result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Min", func(t *testing.T) {
						pql = fmt.Sprintf(`Min(%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{TimestampVal: test.set, Count: 1}) {
							t.Fatalf("unexpected min result, test %d: %s", i, spew.Sdump(result))
						}
					})

					t.Run("Max", func(t *testing.T) {
						pql = fmt.Sprintf(`Max(%s)`, fld)
						if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
							t.Fatal(err)
						} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{TimestampVal: test.set, Count: 1}) {
							t.Fatalf("unexpected max result, test %d: %s", i, spew.Sdump(result))
						}
					})
				})
			}
		})
	})

	t.Run("ColumnID", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("x", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("f", pilosa.OptFieldTypeInt(-1100, 1000)); err != nil {
			t.Fatal(err)
		}

		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
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
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
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
		hldr := c.GetHolder(0)

		idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("x", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("f", pilosa.OptFieldTypeInt(-1110, 1000)); err != nil {
			t.Fatal(err)
		}

		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
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
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
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
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: tt.exp, Count: tt.cnt}) {
					t.Fatalf("unexpected result, test %d: %s", i, spew.Sdump(result))
				}
			}
		})
	})
}

// Ensure MinRow() and MaxRow() queries can be executed.
func TestExecutor_Execute_MinMaxRow(t *testing.T) {
	t.Run("RowID", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set(0, f=7000)
			Set(3, f=50)
			Set(` + strconv.Itoa(ShardWidth+1) + `, f=10000)
			Set(1000, f=1)
			Set(` + strconv.Itoa(ShardWidth+2) + `, f=5000)
		`}); err != nil {
			t.Fatal(err)
		}

		t.Run("MinRow", func(t *testing.T) {
			result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: "MinRow(field=f)"})
			if err != nil {
				t.Fatal(err)
			}
			target := pilosa.PairField{
				Pair:  pilosa.Pair{ID: 1, Count: 1},
				Field: "f",
			}
			if !reflect.DeepEqual(target, result.Results[0]) {
				t.Fatalf("unexpected result %v != %v", target, result.Results[0])
			}
		})

		t.Run("MaxRow", func(t *testing.T) {
			result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: "MaxRow(field=f)"})
			if err != nil {
				t.Fatal(err)
			}
			target := pilosa.PairField{
				Pair:  pilosa.Pair{ID: 10000, Count: 1},
				Field: "f",
			}
			if !reflect.DeepEqual(target, result.Results[0]) {
				t.Fatalf("unexpected result %v != %v", target, result.Results[0])
			}
		})
	})

	t.Run("RowKey", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("f", pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		}

		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set(0, f="seven-thousand")
			Set(3, f="fifty")
			Set(` + strconv.Itoa(ShardWidth+1) + `, f="ten-thousand")
			Set(1000, f="one")
			Set(` + strconv.Itoa(ShardWidth+2) + `, f="five-thousand")
		`}); err != nil {
			t.Fatal(err)
		}

		t.Run("MinRow", func(t *testing.T) {
			result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: "MinRow(field=f)"})
			if err != nil {
				t.Fatal(err)
			}
			target := pilosa.PairField{
				Pair:  pilosa.Pair{Key: "seven-thousand", ID: 1, Count: 1},
				Field: "f",
			}
			if !reflect.DeepEqual(target, result.Results[0]) {
				t.Fatalf("unexpected result %v != %v", target, result.Results[0])
			}
		})

		t.Run("MaxRow", func(t *testing.T) {
			result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: "MaxRow(field=f)"})
			if err != nil {
				t.Fatal(err)
			}
			target := pilosa.PairField{
				Pair:  pilosa.Pair{Key: "five-thousand", ID: 5, Count: 1},
				Field: "f",
			}
			if !reflect.DeepEqual(target, result.Results[0]) {
				t.Fatalf("unexpected result %v != %v", target, result.Results[0])
			}
		})
	})
}

// Ensure a Sum() query can be executed.
func TestExecutor_Execute_Sum(t *testing.T) {
	t.Run("ColumnID", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("x", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("foo", pilosa.OptFieldTypeInt(-990, 1000)); err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("bar", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("other", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("dec", pilosa.OptFieldTypeDecimal(3)); err != nil {
			t.Fatal(err)
		}

		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set(0, x=0)
			Set(` + strconv.Itoa(ShardWidth+1) + `, x=0)

			Set(0, foo=20)
			Set(0, bar=2000)
			Set(` + strconv.Itoa(ShardWidth) + `, foo=30)
			Set(` + strconv.Itoa(ShardWidth+2) + `, foo=40)
			Set(` + strconv.Itoa((5*ShardWidth)+100) + `, foo=50)
			Set(` + strconv.Itoa(ShardWidth+1) + `, foo=60)
			Set(0, other=1000)

			Set(0, dec=100.001)
			Set(` + strconv.Itoa(ShardWidth) + `, dec=200.002)
			Set(` + strconv.Itoa(ShardWidth+1) + `, dec=400.004)
		`}); err != nil {
			t.Fatal(err)
		}

		t.Run("Integer", func(t *testing.T) {
			t.Run("NoFilter", func(t *testing.T) {
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(field=foo)`}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: 200, Count: 5}) {
					t.Fatalf("unexpected result: %s", spew.Sdump(result))
				}
			})

			t.Run("NoFilter", func(t *testing.T) {
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(field="foo")`}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: 200, Count: 5}) {
					t.Fatalf("unexpected result: %s", spew.Sdump(result))
				}
			})

			t.Run("NoFilter", func(t *testing.T) {
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(foo)`}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: 200, Count: 5}) {
					t.Fatalf("unexpected result: %s", spew.Sdump(result))
				}
			})

			t.Run("WithFilter", func(t *testing.T) {
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(Row(x=0), field=foo)`}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: 80, Count: 2}) {
					t.Fatalf("unexpected result: %s", spew.Sdump(result))
				}
			})

			t.Run("WithFilter", func(t *testing.T) {
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(foo, Row(x=0))`}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: 80, Count: 2}) {
					t.Fatalf("unexpected result: %s", spew.Sdump(result))
				}
			})
		})

		t.Run("Decimal", func(t *testing.T) {
			t.Run("NoFilter", func(t *testing.T) {
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(field=dec)`}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{DecimalVal: &pql.Decimal{Value: 700007, Scale: 3}, Count: 3}) {
					t.Fatalf("unexpected result: %s", spew.Sdump(result))
				}
			})

			t.Run("WithFilter", func(t *testing.T) {
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(Row(x=0), field=dec)`}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{DecimalVal: &pql.Decimal{Value: 500005, Scale: 3}, Count: 2}) {
					t.Fatalf("unexpected result: %s", spew.Sdump(result))
				}
			})

			t.Run("NoFilter", func(t *testing.T) {
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(dec)`}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{DecimalVal: &pql.Decimal{Value: 700007, Scale: 3}, Count: 3}) {
					t.Fatalf("unexpected result: %s", spew.Sdump(result))
				}
			})

			t.Run("WithFilter", func(t *testing.T) {
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(dec, Row(x=0))`}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{DecimalVal: &pql.Decimal{Value: 500005, Scale: 3}, Count: 2}) {
					t.Fatalf("unexpected result: %s", spew.Sdump(result))
				}
			})

		})
	})

	t.Run("ColumnKey", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{Keys: true})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("x", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("foo", pilosa.OptFieldTypeInt(-990, 1000)); err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("bar", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
			t.Fatal(err)
		}

		if _, err := idx.CreateField("other", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
			t.Fatal(err)
		}

		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
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
			if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(field=foo)`}); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: 200, Count: 5}) {
				t.Fatalf("unexpected result: %s", spew.Sdump(result))
			}
		})

		t.Run("WithFilter", func(t *testing.T) {
			if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Sum(Row(x=0), field=foo)`}); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: 80, Count: 2}) {
				t.Fatalf("unexpected result: %s", spew.Sdump(result))
			}
		})
	})
}

// Ensure decimal args are supported for Decimal fields.
func TestExecutor_DecimalArgs(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	min, err := pql.ParseDecimal("-10.5")
	if err != nil {
		t.Fatal(err)
	}
	max, err := pql.ParseDecimal("10.5")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("f", pilosa.OptFieldTypeDecimal(2, min, max)); err != nil {
		t.Fatal(err)
	}

	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
		Set(0, f=0)
	`}); err != nil {
		t.Fatal(err)
	}
}

// Ensure a Row(bsiGroup) query can be executed.
func TestExecutor_Execute_Row_BSIGroup(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("foo", pilosa.OptFieldTypeInt(-990, 1000)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("bar", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("other", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("edge", pilosa.OptFieldTypeInt(-900, 1000)); err != nil {
		t.Fatal(err)
	}

	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
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
		// EQ null
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(other == null)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{1,
			50,
			ShardWidth,
			ShardWidth + 1,
			ShardWidth + 2,
			(5 * ShardWidth) + 100,
		}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %#v", result.Results[0].(*pilosa.Row).Columns())
		}
		// EQ <int>
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo == 20)`}); err != nil {
			t.Fatal(err)
		} else if got, exp := result.Results[0].(*pilosa.Row).Columns(), []uint64{50, (5 * ShardWidth) + 100}; !reflect.DeepEqual(exp, got) {
			t.Fatalf("Query().Row.Columns=%#v, expected %#v", got, exp)
		}

		// EQ (single = form) <int>
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo = 20)`}); err != nil {
			t.Fatal(err)
		} else if got, exp := result.Results[0].(*pilosa.Row).Columns(), []uint64{50, (5 * ShardWidth) + 100}; !reflect.DeepEqual(exp, got) {
			t.Fatalf("Query().Row.Columns=%#v, expected %#v", got, exp)
		}
	})

	t.Run("NEQ", func(t *testing.T) {
		// NEQ null
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(other != null)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %#v", result.Results[0].(*pilosa.Row).Columns())
		}
		// NEQ <int>
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo != 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth, ShardWidth + 1, ShardWidth + 2}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %#v", result.Results[0].(*pilosa.Row).Columns())
		}
		// NEQ -<int>
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(other != -20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %#v", result.Results[0].(*pilosa.Row).Columns())
		}
	})

	t.Run("LT", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo < 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth + 2}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTE", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo <= 20)`}); err != nil {
			t.Fatal(err)
		} else if got, exp := result.Results[0].(*pilosa.Row).Columns(), []uint64{50, ShardWidth + 2, (5 * ShardWidth) + 100}; !reflect.DeepEqual(got, exp) {
			t.Fatalf("unexpected result: got=%v, exp=%v", got, exp)
		}
	})

	t.Run("GT", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo > 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth, ShardWidth + 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %v", result.Results[0].(*pilosa.Row).Columns())
		}
	})

	t.Run("GTE", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo >= 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, ShardWidth, ShardWidth + 1, (5 * ShardWidth) + 100}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %v", result.Results[0].(*pilosa.Row).Columns())
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
			{q: `Row(1000 <= other < 20000)`, exp: true},
			{q: `Row(1000 <= other <= 2000)`, exp: true},
			{q: `Row(1000 < other <= 2000)`, exp: false},
		}
		for i, test := range tests {
			t.Run(fmt.Sprintf("#%d_%s", i, test.q), func(t *testing.T) {
				var expected = []uint64{}
				if test.exp {
					expected = []uint64{0}
				}
				if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: test.q}); err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(expected, result.Results[0].(*pilosa.Row).Columns()) {
					t.Fatalf("unexpected result for query: %s (%#v)", test.q, result.Results[0].(*pilosa.Row).Columns())
				}
			})
		}

	})

	// Ensure that the NotNull code path gets run.
	t.Run("NotNull", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(0 <= other <= 1000)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BelowMin", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo == 0)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("AboveMax", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(foo == 200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTAboveMax", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(edge < 200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result.Results[0].(*pilosa.Row).Columns()))
		}
	})

	t.Run("GTBelowMin", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(edge > -1000)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result.Results[0].(*pilosa.Row).Columns()))
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(bad_field >= 20)`}); errors.Cause(err) != pilosa.ErrFieldNotFound {
			t.Fatal(err)
		}
	})
}

// Ensure a Row(bsiGroup) query can be executed (edge cases).
func TestExecutor_Execute_Row_BSIGroupEdge(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("LT", func(t *testing.T) {
		if _, err := idx.CreateField("f1", pilosa.OptFieldTypeInt(-2000, 2000)); err != nil {
			t.Fatal(err)
		}

		// Set a value at the edge of bitDepth (i.e. 2^n-1; here, n=3).
		// It must also be the max value in the field; in other words,
		// set the value to bsiGroup.bitDepthMax().
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set(100, f1=7)
		`}); err != nil {
			t.Fatal(err)
		}

		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f1 < 10)`}); err != nil {
			t.Fatal(err)
		} else if got, exp := result.Results[0].(*pilosa.Row).Columns(), []uint64{100}; !reflect.DeepEqual(got, exp) {
			t.Fatalf("unexpected result: got=%v, exp=%v", got, exp)
		}
	})

	t.Run("GT", func(t *testing.T) {
		if _, err := idx.CreateField("f2", pilosa.OptFieldTypeInt(-2000, 2000)); err != nil {
			t.Fatal(err)
		}

		// Set a value at the negative edge of bitDepth (i.e. -(2^n-1); here, n=3).
		// It must also be the min value in the field; in other words,
		// set the value to bsiGroup.bitDepthMin().
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
		Set(200, f2=-7)
	`}); err != nil {
			t.Fatal(err)
		}

		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f2 > -10)`}); err != nil {
			t.Fatal(err)
		} else if got, exp := result.Results[0].(*pilosa.Row).Columns(), []uint64{200}; !reflect.DeepEqual(got, exp) {
			t.Fatalf("unexpected result: got=%v, exp=%v", got, exp)
		}
	})

	t.Run("BTWN_LT_LT", func(t *testing.T) {
		if _, err := idx.CreateField("f3", pilosa.OptFieldTypeInt(-2000, 2000)); err != nil {
			t.Fatal(err)
		}

		// Set a value anywhere in range.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
		Set(300, f3=10)
	`}); err != nil {
			t.Fatal(err)
		}

		// Query INT_MAX < x < INT_MIN. Because that's an invalid range, we should
		// get back an empty result set.
		tests := []struct {
			predA int64
			predB int64
		}{
			{math.MaxInt64, math.MinInt64},
			{math.MaxInt64, 1000},
			{-1000, math.MinInt64},
		}

		for i, test := range tests {
			pql := fmt.Sprintf("Row(%d < f3 < %d)", test.predA, test.predB)
			if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
				t.Fatal(err)
			} else if got, exp := result.Results[0].(*pilosa.Row).Columns(), []uint64{}; !reflect.DeepEqual(got, exp) {
				t.Fatalf("test %d unexpected result: got=%v, exp=%v", i, got, exp)
			}
		}
	})
}

// Ensure a Range(bsiGroup) query can be executed. (Deprecated)
func TestExecutor_Execute_Range_BSIGroup_Deprecated(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("foo", pilosa.OptFieldTypeInt(-990, 1000)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("bar", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("other", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("edge", pilosa.OptFieldTypeInt(-1100, 1000)); err != nil {
		t.Fatal(err)
	}

	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
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
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo == 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, (5 * ShardWidth) + 100}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("NEQ", func(t *testing.T) {
		// NEQ null
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(other != null)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
		// NEQ <int>
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo != 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth, ShardWidth + 1, ShardWidth + 2}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
		// NEQ -<int>
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(other != -20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %v", result.Results[0].(*pilosa.Row).Columns())
		}
	})

	t.Run("LT", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo < 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth + 2}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTE", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo <= 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, ShardWidth + 2, (5 * ShardWidth) + 100}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("GT", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo > 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{ShardWidth, ShardWidth + 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("GTE", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo >= 20)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{50, ShardWidth, ShardWidth + 1, (5 * ShardWidth) + 100}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BETWEEN", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(0 < other < 1000)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	// Ensure that the NotNull code path gets run.
	t.Run("NotNull", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(0 <= other <= 1000)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("BelowMin", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo == 0)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("AboveMax", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(foo == 200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result))
		}
	})

	t.Run("LTAboveMax", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(edge < 200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result.Results[0].(*pilosa.Row).Columns()))
		}
	})

	t.Run("GTBelowMin", func(t *testing.T) {
		if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(edge > -1200)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual([]uint64{0, 1}, result.Results[0].(*pilosa.Row).Columns()) {
			t.Fatalf("unexpected result: %s", spew.Sdump(result.Results[0].(*pilosa.Row).Columns()))
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Range(bad_field >= 20)`}); errors.Cause(err) != pilosa.ErrFieldNotFound {
			t.Fatal(err)
		}
	})
}

// Ensure a remote query can return a row.
func TestExecutor_Execute_Remote_Row(t *testing.T) {
	c := test.MustRunCluster(t, 3,
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node0"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node1"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
		[]server.CommandOption{
			server.OptCommandServerOptions(pilosa.OptServerNodeID("node2"), pilosa.OptServerClusterHasher(&test.ModHasher{}))},
	)
	defer c.Close()
	hldr0 := c.GetHolder(0)
	hldr1 := c.GetHolder(1)
	hldr2 := c.GetHolder(2)

	_, err := c.GetPrimary().API.CreateIndex(context.Background(), "i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}
	_, err = c.GetPrimary().API.CreateField(context.Background(), "i", "f", pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, pilosa.DefaultCacheSize))
	if err != nil {
		t.Fatalf("creating field: %v", err)
	}
	hldr0.MustSetBits("i", "f", 10, ShardWidth+1, ShardWidth+2, (3*ShardWidth)+4)
	hldr2.SetBit("i", "f", 10, 1)
	if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
		t.Fatal(err)
	} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1, ShardWidth + 1, ShardWidth + 2, (3 * ShardWidth) + 4}) {
		t.Fatalf("unexpected columns: %+v", columns)
	}

	t.Run("Count", func(t *testing.T) {
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Count(Row(f=10))`}); err != nil {
			t.Fatal(err)
		} else if res.Results[0] != uint64(4) {
			t.Fatalf("unexpected n: %d", res.Results[0])
		}
	})

	t.Run("Remote SetBit", func(t *testing.T) {
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf(`Set(%d, f=7)`, pilosa.ShardWidth+1)}); err != nil {
			t.Fatalf("querying remote: %v", err)
		}

		if !reflect.DeepEqual(hldr0.Row("i", "f", 7).Columns(), []uint64{pilosa.ShardWidth + 1}) {
			t.Fatalf("unexpected cols from row 7: %v", hldr1.Row("i", "f", 7).Columns())
		}
	})

	t.Run("remote with timestamp", func(t *testing.T) {
		_, err = c.GetPrimary().API.CreateField(context.Background(), "i", "z", pilosa.OptFieldTypeTime("Y"))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf(`Set(%d, z=5, 2010-07-08T00:00)`, pilosa.ShardWidth+1)}); err != nil {
			t.Fatalf("quuerying remote: %v", err)
		}

		if !reflect.DeepEqual(hldr0.RowTime("i", "z", 5, time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC), "Y").Columns(), []uint64{pilosa.ShardWidth + 1}) {
			t.Fatalf("unexpected cols from row 7: %v", hldr1.RowTime("i", "z", 5, time.Date(2010, time.January, 1, 0, 0, 0, 0, time.UTC), "Y").Columns())
		}
	})

	t.Run("remote topn", func(t *testing.T) {
		_, err = c.GetPrimary().API.CreateField(context.Background(), "i", "fn", pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 100))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
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
			t.Fatalf("querying remote: %v", err)
		}

		if res, err := c.GetNode(1).API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "i",
			Query: `TopN(fn, n=3)`,
		}); err != nil {
			t.Fatalf("topn querying: %v", err)
		} else if !reflect.DeepEqual(res.Results, []interface{}{&pilosa.PairsField{
			Pairs: []pilosa.Pair{
				{ID: 5, Count: 4},
				{ID: 3, Count: 3},
				{ID: 4, Count: 2},
			},
			Field: "fn",
		}}) {
			t.Fatalf("topn wrong results: %v", res.Results)
		}
	})

	t.Run("remote groupBy", func(t *testing.T) {
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "i",
			Query: `GroupBy(Rows(f))`,
		}); err != nil {
			t.Fatalf("GroupBy querying: %v", err)
		} else {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "f", RowID: 7}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "f", RowID: 10}}, Count: 4},
			}
			results := res.Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		}
	})

	t.Run("remote groupBy on ints", func(t *testing.T) {
		_, err = c.GetPrimary().API.CreateField(context.Background(), "i", "fint", pilosa.OptFieldTypeInt(-1000, 1000))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
		Set(0, fint=1)
		Set(1, fint=2)

		Set(2,fint=-2)
		Set(3,fint=-1)

		Set(4,fint=4)

		Set(10, fint=0)
		Set(100, fint=0)
		Set(1000, fint=0)
		Set(10000,fint=0)
		Set(100000,fint=0)
		`}); err != nil {
			t.Fatalf("querying remote: %v", err)
		}

		if res, err := c.GetNode(1).API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "i",
			Query: `GroupBy(Rows(fint), limit=4, filter=Union(Row(fint < 1), Row(fint > 2)))`,
		}); err != nil {
			t.Fatalf("GroupBy querying: %v", err)
		} else {
			var a, b, c, d int64 = -2, -1, 0, 4
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "fint", Value: &a}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "fint", Value: &b}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "fint", Value: &c}}, Count: 5},
				{Group: []pilosa.FieldRow{{Field: "fint", Value: &d}}, Count: 1},
			}

			results := res.Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		}
	})

	t.Run("groupBy on ints with offset regression", func(t *testing.T) {
		_, err = c.GetPrimary().API.CreateField(context.Background(), "i", "hint", pilosa.OptFieldTypeInt(1, 1000))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
		Set(0, hint=1)
		Set(1, hint=2)
		Set(2, hint=3)
		`}); err != nil {
			t.Fatalf("querying remote: %v", err)
		}

		if res, err := c.GetNode(1).API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "i",
			Query: `GroupBy(Rows(hint))`,
		}); err != nil {
			t.Fatalf("GroupBy querying: %v", err)
		} else {
			var a, b, c int64 = 1, 2, 3
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "hint", Value: &a}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "hint", Value: &b}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "hint", Value: &c}}, Count: 1},
			}

			results := res.Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		}
	})

	t.Run("Row on ints with ASSIGN condition", func(t *testing.T) {
		_, err := c.GetPrimary().API.CreateIndex(context.Background(), "intidx", pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}

		_, err = c.GetPrimary().API.CreateField(context.Background(), "intidx", "gint", pilosa.OptFieldTypeInt(-1000, 1000))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "intidx", Query: `
		Set(1000, gint=1)
		Set(2000, gint=2)
		Set(3000, gint=3)
		`}); err != nil {
			t.Fatalf("querying remote: %v", err)
		}

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "intidx",
			Query: `Row(gint=2)Row(gint==1)`,
		}); err != nil {
			t.Fatalf("Row querying: %v", err)
		} else {

			row0, row1 := res.Results[0].(*pilosa.Row), res.Results[1].(*pilosa.Row)
			if len(row0.Columns()) != 1 || len(row1.Columns()) != 1 {
				t.Fatalf(`Expected: []uint64{2000} []uint64{1000}, Got: %+v %+v`, row0.Columns(), row1.Columns())
			}
			if row0.Columns()[0] != 2000 || row1.Columns()[0] != 1000 {
				t.Fatalf(`Expected: []uint64{2000} []uint64{1000}, Got: %+v %+v`, row0.Columns(), row1.Columns())
			}
		}
	})

	t.Run("Row on decimals with ASSIGN condition", func(t *testing.T) {
		_, err := c.GetPrimary().API.CreateIndex(context.Background(), "decidx", pilosa.IndexOptions{})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}

		_, err = c.GetPrimary().API.CreateField(context.Background(), "decidx", "fdec", pilosa.OptFieldTypeDecimal(0))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "decidx", Query: `
		Set(11, fdec=1.1)
		Set(22, fdec=2.2)
		Set(33, fdec=3.3)
		`}); err != nil {
			t.Fatalf("querying remote: %v", err)
		}

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "decidx",
			Query: `Row(fdec=2.2)Row(fdec==1.1)`,
		}); err != nil {
			t.Fatalf("Row querying: %v", err)
		} else {
			row0, row1 := res.Results[0].(*pilosa.Row), res.Results[1].(*pilosa.Row)
			if len(row0.Columns()) != 1 || len(row1.Columns()) != 1 {
				t.Fatalf(`Expected: []uint64{22} []uint64{11}, Got: %+v %+v`, row0.Columns(), row1.Columns())
			}
			if row0.Columns()[0] != 22 || row1.Columns()[0] != 11 {
				t.Fatalf(`Expected: []uint64{22} []uint64{11}, Got: %+v %+v`, row0.Columns(), row1.Columns())
			}
		}
	})

	t.Run("Row on foreign key with ASSIGN condition", func(t *testing.T) {
		_, err := c.GetPrimary().API.CreateIndex(context.Background(), "parent", pilosa.IndexOptions{Keys: true})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = c.GetPrimary().API.CreateField(context.Background(), "parent", "general", pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, pilosa.DefaultCacheSize))
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}
		_, err = c.GetPrimary().API.CreateIndex(context.Background(), "child", pilosa.IndexOptions{Keys: false})
		if err != nil {
			t.Fatalf("creating index: %v", err)
		}
		_, err = c.GetPrimary().API.CreateField(context.Background(), "child", "parentid",
			pilosa.OptFieldForeignIndex("parent"),
			pilosa.OptFieldTypeInt(-9223372036854775808, 9223372036854775807),
		)
		if err != nil {
			t.Fatalf("creating field: %v", err)
		}

		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "child", Query: `
		Set(1, parentid="one")
		Set(2, parentid="two")
		Set(3, parentid="three")
		`}); err != nil {
			t.Fatalf("querying remote: %v", err)
		}

		if res, err := c.GetNode(1).API.Query(context.Background(), &pilosa.QueryRequest{
			Index: "child",
			Query: `Row(parentid="two")Row(parentid=="one")`,
		}); err != nil {
			t.Fatalf("Row querying: %v", err)
		} else {

			row0, row1 := res.Results[0].(*pilosa.Row), res.Results[1].(*pilosa.Row)
			if len(row0.Columns()) != 1 || len(row1.Columns()) != 1 {
				t.Fatalf(`Expected: []uint64{1} []uint64{0}, Got: %+v %+v`, row0.Columns(), row1.Columns())
			}
			if row0.Columns()[0] != 2 || row1.Columns()[0] != 1 {
				t.Fatalf(`Expected: []uint64{1} []uint64{0}, Got: %+v %+v`, row0.Columns(), row1.Columns())
			}
		}
	})
}

// Ensure executor returns an error if too many writes are in a single request.
func TestExecutor_Execute_ErrMaxWritesPerRequest(t *testing.T) {
	c := test.MustNewCluster(t, 1)
	defer c.Close()
	c.GetIdleNode(0).Config.MaxWritesPerRequest = 3
	err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	hldr := c.GetHolder(0)
	hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set() Clear() Set() Set()`}); errors.Cause(err) != pilosa.ErrTooManyWrites {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestExecutor_Time_Clear_Quantums(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)

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
			if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: indexName, Query: populateBatch}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: indexName, Query: clearColumn}); err != nil {
				t.Fatal(err)
			}
			if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: indexName, Query: rangeCheckQuery}); err != nil {
				t.Fatal(err)
			} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, tt.expected) {
				t.Fatalf("unexpected columns: %+v", columns)
			}

		})
	}

}

func TestReopenCluster(t *testing.T) {
	commandOpts := make([][]server.CommandOption, 3)
	configs := make([]*server.Config, 3)
	for i := range configs {
		conf := server.NewConfig()
		configs[i] = conf
		conf.Cluster.ReplicaN = 2
		commandOpts[i] = append(commandOpts[i], server.OptCommandConfig(conf))
	}
	c := test.MustRunCluster(t, 3, commandOpts...)
	defer c.Close()
	c.CreateField(t, "users", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "likenums")
	c.ImportIDKey(t, "users", "likenums", []test.KeyID{
		{ID: 1, Key: "userA"},
		{ID: 2, Key: "userB"},
		{ID: 3, Key: "userC"},
		{ID: 4, Key: "userD"},
		{ID: 5, Key: "userE"},
		{ID: 6, Key: "userF"},
		{ID: 7, Key: "userA"},
		{ID: 7, Key: "userB"},
		{ID: 7, Key: "userC"},
		{ID: 7, Key: "userD"},
		// we intentionally leave user E out because then there is no
		// data for userE's shard for this field, which triggered a
		// "fragment not found" problem
		{ID: 7, Key: "userF"},
	})

	node0 := c.GetNode(0)
	if err := node0.Reopen(); err != nil {
		t.Fatal(err)
	}
	if err := c.AwaitState(disco.ClusterStateNormal, 10*time.Second); err != nil {
		t.Fatalf("restarting cluster: %v", err)
	}

	// TODO this test was supposed to reproduce an issue where spooled messages got sent improperly in Server.Open in this area:
	//
	//   for i := range toSend {
	//     for {
	//		 err := s.holder.broadcaster.SendSync(toSend[i])
	//
	// It was previously sending &toSend[i] which wasn't a valid
	// pilosa.Message. Unfortunately because that's all happening in a
	// goroutine, the Reopen continues happily and things appear to
	// work whether the bug is present or not. I'd like to pass in a
	// logger and detect when "completed initial cluster state sync"
	// is logged, but it's more involved than I have time for at the
	// moment, so I'm creating a follow up ticket. (CORE-318)
}

// Ensure an existence field is maintained.
func TestExecutor_Execute_Existence(t *testing.T) {
	t.Run("Row", func(t *testing.T) {
		c := test.MustRunCluster(t, 1, []server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			),
		})
		defer c.Close()
		hldr := c.GetHolder(0)
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})

		_, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}

		node0 := c.GetNode(0)
		// Set bits.
		if _, err := node0.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `` +
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+2, 20),
		}); err != nil {
			t.Fatal(err)
		}

		if res, err := node0.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		if res, err := node0.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Not(Row(f=10))`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{ShardWidth + 2}) {
			t.Fatalf("unexpected columns after Not: %+v", bits)
		}

		// Reopen cluster to ensure existence field is reloaded.
		if err := node0.Reopen(); err != nil {
			t.Fatal(err)
		}

		if err := c.AwaitState(disco.ClusterStateNormal, 10*time.Second); err != nil {
			t.Fatalf("restarting cluster: %v", err)
		}

		hldr2 := c.GetHolder(0)
		index2 := hldr2.Index("i")
		_ = index2

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Not(Row(f=10))`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{ShardWidth + 2}) {
			t.Fatalf("unexpected columns after reopen: %+v", bits)
		}
	})
}

// Ensure a not query can be executed.
func TestExecutor_Execute_Not(t *testing.T) {

}

// Ensure an all query can be executed.
func TestExecutor_Execute_FieldValue(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	node0 := c.GetNode(0)
	node1 := c.GetNode(1)

	// Index with IDs
	c.CreateField(t, "i", pilosa.IndexOptions{Keys: false}, "f", pilosa.OptFieldTypeInt(-1100, 1000))
	c.CreateField(t, "i", pilosa.IndexOptions{Keys: false}, "dec", pilosa.OptFieldTypeDecimal(3))

	if _, err := node0.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set(1, f=3)
			Set(2, f=-4)
			Set(` + strconv.Itoa(ShardWidth+1) + `, f=3)
			Set(1, dec=12.985)
			Set(2, dec=-4.234)
		`}); err != nil {
		t.Fatal(err)
	}

	// Index with Keys
	c.CreateField(t, "ik", pilosa.IndexOptions{Keys: true}, "f", pilosa.OptFieldTypeInt(-1100, 1000))
	c.CreateField(t, "ik", pilosa.IndexOptions{Keys: true}, "dec", pilosa.OptFieldTypeDecimal(3))

	if _, err := node0.API.Query(context.Background(), &pilosa.QueryRequest{Index: "ik", Query: `
			Set("one", f=3)
			Set("two", f=-4)
			Set("one", dec=12.985)
			Set("two", dec=-4.234)
		`}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		index  string
		qry    string
		expVal interface{}
		expErr string
	}{
		// IDs
		{index: "i", qry: "FieldValue(field=f, column=1)", expVal: int64(3)},
		{index: "i", qry: "FieldValue(field=f, column=2)", expVal: int64(-4)},
		{index: "i", qry: "FieldValue(field=f, column=" + strconv.Itoa(ShardWidth+1) + ")", expVal: int64(3)},

		{index: "i", qry: "FieldValue(field=dec, column=1)", expVal: pql.NewDecimal(12985, 3)},
		{index: "i", qry: "FieldValue(field=dec, column=2)", expVal: pql.NewDecimal(-4234, 3)},

		// Keys
		{index: "ik", qry: "FieldValue(field=f, column='one')", expVal: int64(3)},
		{index: "ik", qry: "FieldValue(field=f, column='two')", expVal: int64(-4)},

		{index: "ik", qry: "FieldValue(field=dec, column='one')", expVal: pql.NewDecimal(12985, 3)},
		{index: "ik", qry: "FieldValue(field=dec, column='two')", expVal: pql.NewDecimal(-4234, 3)},

		// Errors
		{index: "i", qry: "FieldValue()", expErr: pilosa.ErrFieldRequired.Error()},
		{index: "i", qry: "FieldValue(field=dec)", expErr: pilosa.ErrColumnRequired.Error()},
		{index: "ik", qry: "FieldValue(field=f)", expErr: pilosa.ErrColumnRequired.Error()},
	}
	for n, node := range []*test.Command{node0, node1} {
		for i, test := range tests {
			if res, err := node.API.Query(context.Background(), &pilosa.QueryRequest{Index: test.index, Query: test.qry}); err != nil && test.expErr == "" {
				t.Fatal(err)
			} else if err != nil && test.expErr != "" {
				if !strings.Contains(err.Error(), test.expErr) {
					t.Fatalf("test %d on node%d expected error: %s, but got: %s", i, n, test.expErr, err)
				}
			} else if err == nil && test.expErr != "" {
				t.Fatalf("test %d on node%d expected error but got nil", i, n)
			} else if vc, ok := res.Results[0].(pilosa.ValCount); !ok {
				t.Fatalf("test %d on node%d expected pilosa.ValCount, but got: %T", i, n, res.Results[0])
			} else if vc.Count != 1 {
				t.Fatalf("test %d on node%d expected Count 1, but got: %d", i, n, vc.Count)
			} else {
				switch exp := test.expVal.(type) {
				case pql.Decimal:
					if *vc.DecimalVal != exp {
						t.Fatalf("test %d on node%d expected pql.Decimal(%s), but got: %s", i, n, exp, vc.DecimalVal)
					}
				case int64:
					if vc.Val != exp {
						t.Fatalf("test %d on node%d expected int64(%d), but got: %d", i, n, exp, vc.Val)
					}
				default:
					t.Fatalf("test %d on node%d received unhandled type: %T", i, n, test.expVal)
				}
			}
		}
	}
}

// Ensure a Limit query can be executed.
func TestExecutor_Execute_Limit(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "f")
	c.ImportBits(t, "i", "f", [][2]uint64{
		{1, 0},
		{1, 1},
		{1, ShardWidth + 1},
	})
	columns := []uint64{0, 1, ShardWidth + 1}

	// Test with only a limit specified.
	t.Run("Limit", func(t *testing.T) {
		for limit := 0; limit < 5; limit++ {
			expect := columns
			if limit < len(expect) {
				expect = expect[:limit]
			}

			resp := c.Query(t, "i", fmt.Sprintf("Limit(All(), limit=%d)", limit))
			if len(resp.Results) != 1 {
				t.Fatalf("limit=%d: expected 1 result but got %v", limit, resp.Results)
			}
			row, ok := resp.Results[0].(*pilosa.Row)
			if !ok {
				t.Fatalf("limit=%d: expected a row result but got %T", limit, resp.Results[0])
			}
			got := row.Columns()
			if !reflect.DeepEqual(expect, got) {
				t.Errorf("limit=%d: expected %v but got %v", limit, expect, got)
			}
		}
	})

	// Test with only an offset specified.
	t.Run("Offset", func(t *testing.T) {
		for offset := 0; offset < 5; offset++ {
			expect := []uint64{}
			if offset <= len(columns) {
				expect = columns[offset:]
			}

			resp := c.Query(t, "i", fmt.Sprintf("Limit(All(), offset=%d)", offset))
			if len(resp.Results) != 1 {
				t.Fatalf("offset=%d: expected 1 result but got %v", offset, resp.Results)
			}
			row, ok := resp.Results[0].(*pilosa.Row)
			if !ok {
				t.Fatalf("offset=%d: expected a row result but got %T", offset, resp.Results[0])
			}
			got := row.Columns()
			if !reflect.DeepEqual(expect, got) {
				t.Errorf("offset=%d: expected %v but got %v", offset, expect, got)
			}
		}
	})

	// Test with a limit and offset specified.
	t.Run("LimitOffset", func(t *testing.T) {
		for limit := 0; limit < 5; limit++ {
			for offset := 0; offset < 5; offset++ {
				expect := []uint64{}
				if offset <= len(columns) {
					expect = columns[offset:]
				}
				if limit < len(expect) {
					expect = expect[:limit]
				}

				resp := c.Query(t, "i", fmt.Sprintf("Limit(All(), limit=%d, offset=%d)", limit, offset))
				if len(resp.Results) != 1 {
					t.Fatalf("limit=%d,offset=%d: expected 1 result but got %v", limit, offset, resp.Results)
				}
				row, ok := resp.Results[0].(*pilosa.Row)
				if !ok {
					t.Fatalf("limit=%d,offset=%d: expected a row result but got %T", limit, offset, resp.Results[0])
				}
				got := row.Columns()
				if !reflect.DeepEqual(expect, got) {
					t.Errorf("limit=%d,offset=%d: expected %v but got %v", limit, offset, expect, got)
				}
			}
		}
	})

	t.Run("Nested", func(t *testing.T) {
		for limit := 0; limit < 5; limit++ {
			for offset := 0; offset < 5; offset++ {
				expect := []uint64{}
				if offset <= len(columns) {
					expect = columns[offset:]
				}
				if limit < len(expect) {
					expect = expect[:limit]
				}

				resp := c.Query(t, "i", fmt.Sprintf("Limit(Limit(All(), offset=%d), limit=%d)", offset, limit))
				if len(resp.Results) != 1 {
					t.Fatalf("limit=%d,offset=%d: expected 1 result but got %v", limit, offset, resp.Results)
				}
				row, ok := resp.Results[0].(*pilosa.Row)
				if !ok {
					t.Fatalf("limit=%d,offset=%d: expected a row result but got %T", limit, offset, resp.Results[0])
				}
				got := row.Columns()
				if !reflect.DeepEqual(expect, got) {
					t.Errorf("limit=%d,offset=%d: expected %v but got %v", limit, offset, expect, got)
				}
			}
		}
	})

	t.Run("Extract", func(t *testing.T) {
		resp := c.Query(t, "i", "Extract(Limit(All(), limit=1))")
		if len(resp.Results) != 1 {
			t.Fatalf("expected 1 result but got %d", len(resp.Results))
		}
		got, ok := resp.Results[0].(pilosa.ExtractedTable)
		if !ok {
			t.Fatalf("expected a table result but got %T", resp.Results[0])
		}
		expect := pilosa.ExtractedTable{
			Fields: []pilosa.ExtractedTableField{},
			Columns: []pilosa.ExtractedTableColumn{
				{
					Column: pilosa.KeyOrID{
						ID: 0,
					},
					Rows: []interface{}{},
				},
			},
		}
		if !reflect.DeepEqual(expect, got) {
			t.Errorf("expected %v but got %v", expect, got)
		}
	})
}

// Ensure an all query can be executed.
func TestExecutor_Execute_All(t *testing.T) {
	t.Run("ColumnID", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		fld, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}

		// Create an import request that sets things on either end
		// of a shard, plus a couple bits set on either side of it,
		// and a final bit set in a fourth shard.
		//
		//    shard0     shard1     shard2     shard3
		// |----------|----------|----------|----------|
		// |        **|**      **|**        | *
		//
		bitCount := 100 + 5
		req := &pilosa.ImportRequest{
			Index:     index.Name(),
			Field:     fld.Name(),
			Shard:     0,
			RowIDs:    make([]uint64, bitCount),
			ColumnIDs: make([]uint64, bitCount),
		}
		for i := 0; i < bitCount/2; i++ {
			req.RowIDs[i] = 10
			req.ColumnIDs[i] = uint64(i + ShardWidth - 2)
		}
		for i := bitCount / 2; i < bitCount-1; i++ {
			req.RowIDs[i] = 10
			req.ColumnIDs[i] = uint64(i + (ShardWidth * 2) - bitCount + 5)
		}
		req.RowIDs[bitCount-1] = 10
		req.ColumnIDs[bitCount-1] = uint64((3 * ShardWidth) + 2)

		m0 := c.GetNode(0)
		// the request gets altered by the Import operation now...
		reqs, err := req.Clone().ShardSplit()
		if err != nil {
			t.Fatalf("splitting request into shards: %v", err)
		}

		qcx := m0.API.Txf().NewQcx()
		for _, r := range reqs {
			if err := m0.API.Import(context.Background(), qcx, r); err != nil {
				t.Fatal(err)
			}
		}
		PanicOn(qcx.Finish())

		i0, err := m0.API.Index(context.Background(), "i")
		PanicOn(err)
		if i0 == nil {
			PanicOn("nil index i0?")
		}

		tests := []struct {
			qry     string
			expCols []uint64
			expCnt  uint64
		}{
			{qry: "All()", expCols: req.ColumnIDs, expCnt: uint64(bitCount)},
			{qry: "All(limit=1)", expCols: req.ColumnIDs[:1], expCnt: 1},
			{qry: "All(limit=4)", expCols: req.ColumnIDs[:4], expCnt: 4},
			{qry: "All(limit=4, offset=4)", expCols: req.ColumnIDs[4:8], expCnt: 4},
			{qry: fmt.Sprintf("All(limit=4, offset=%d)", bitCount-5), expCols: req.ColumnIDs[bitCount-5 : bitCount-1], expCnt: 4},
			{qry: fmt.Sprintf("All(limit=1, offset=%d)", bitCount-2), expCols: req.ColumnIDs[bitCount-2 : bitCount-1], expCnt: 1},
			{qry: fmt.Sprintf("All(limit=1, offset=%d)", bitCount-2), expCols: req.ColumnIDs[bitCount-2 : bitCount-1], expCnt: 1},
			{qry: fmt.Sprintf("All(limit=4, offset=%d)", bitCount-2), expCols: req.ColumnIDs[bitCount-2:], expCnt: 2},
			{qry: fmt.Sprintf("All(limit=4, offset=%d)", bitCount+1), expCols: []uint64{}, expCnt: 0},
			{qry: fmt.Sprintf("All(limit=2, offset=%d)", bitCount-3), expCols: req.ColumnIDs[bitCount-3 : bitCount-1], expCnt: 2},
			{qry: fmt.Sprintf("All(limit=2, offset=%d)", bitCount-5), expCols: req.ColumnIDs[bitCount-5 : bitCount-3], expCnt: 2},
			{qry: "All(limit=2, offset=2)", expCols: req.ColumnIDs[2:4], expCnt: 2},
			{qry: "All(limit=1, offset=1)", expCols: req.ColumnIDs[1:2], expCnt: 1},
			{qry: fmt.Sprintf("All(limit=%d, offset=2)", bitCount-3), expCols: req.ColumnIDs[2 : bitCount-1], expCnt: uint64(bitCount - 3)},
		}
		for i, test := range tests {
			if res, err := m0.API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: test.qry}); err != nil {
				t.Fatal(err)
			} else if cnt := res.Results[0].(*pilosa.Row).Count(); cnt != test.expCnt {
				t.Fatalf("test %d, unexpected count, got: %d, but expected: %d", i, cnt, test.expCnt)
			} else if cols := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(cols, test.expCols) {
				// If the error results are too large, just show the count.
				if len(cols) > 1000 || len(test.expCols) > 1000 {
					t.Fatalf("test %d, unexpected columns, got: len(%d), but expected: len(%d)", i, len(cols), len(test.expCols))
				} else {
					t.Fatalf("test %d, unexpected columns, got: %v, but expected: %v", i, cols, test.expCols)
				}
			}
		}
	})

	t.Run("ColumnKey", func(t *testing.T) {
		c := test.MustRunCluster(t, 1, []server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
			),
		})
		defer c.Close()
		hldr := c.GetHolder(0)
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true, Keys: true})
		fld, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}

		// Create an import request that sets key columns
		//
		//    shard0
		// |----------|
		// |****      |
		//
		bitCount := 4
		req := &pilosa.ImportRequest{
			Index:      index.Name(),
			Field:      fld.Name(),
			Shard:      0,
			RowIDs:     make([]uint64, bitCount),
			ColumnKeys: make([]string, bitCount),
		}
		for i := 0; i < bitCount; i++ {
			req.RowIDs[i] = 10
			req.ColumnKeys[i] = fmt.Sprintf("c%d", i)
		}

		qcx := c.GetNode(0).API.Txf().NewQcx()
		if err := c.GetNode(0).API.Import(context.Background(), qcx, req); err != nil {
			t.Fatal(err)
		}
		PanicOn(qcx.Finish())

		tests := []struct {
			qry     string
			expCols []string
			expCnt  uint64
		}{
			{qry: "All()", expCols: []string{"c1", "c0", "c3", "c2"}, expCnt: uint64(bitCount)},
			{qry: "All(limit=1)", expCols: []string{"c1"}, expCnt: 1},
			{qry: "All(limit=4)", expCols: []string{"c1", "c0", "c3", "c2"}, expCnt: 4},
			{qry: "All(limit=5)", expCols: []string{"c1", "c0", "c3", "c2"}, expCnt: 4},
			{qry: "All(limit=1, offset=1)", expCols: []string{"c0"}, expCnt: 1},
			{qry: "All(limit=4, offset=1)", expCols: []string{"c0", "c3", "c2"}, expCnt: 3},
			{qry: "All(limit=4, offset=5)", expCols: nil, expCnt: 0},
		}
		for i, test := range tests {
			if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: test.qry}); err != nil {
				t.Fatal(err)
			} else if cnt := len(res.Results[0].(*pilosa.Row).Keys); uint64(cnt) != test.expCnt {
				t.Fatalf("test %d, unexpected count, got: %d, but expected: %d", i, cnt, test.expCnt)
			} else if cols := res.Results[0].(*pilosa.Row).Keys; !reflect.DeepEqual(cols, test.expCols) {
				// If the error results are too large, just show the count.
				if len(cols) > 1000 || len(test.expCols) > 1000 {
					t.Fatalf("test %d, unexpected columns, got: len(%d), but expected: len(%d)", i, len(cols), len(test.expCols))
				} else {
					t.Fatalf("test %d, unexpected columns, got: %#v, but expected: %#v", i, cols, test.expCols)
				}
			}
		}
	})

	// Ensure that a query which uses All() at the shard level can call it.
	t.Run("AllShard", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		_, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
Set(3001, f=3)
Set(5001, f=5)
Set(5002, f=5)
`}); err != nil {
			t.Fatalf("querying remote: %v", err)
		}

		expCols := []uint64{5001, 5002}
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: "Intersect(All(), Row(f=5))"}); err != nil {
			t.Fatal(err)
		} else if cols := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(cols, expCols) {
			t.Fatalf("unexpected columns, got: %v, but expected: %v", cols, expCols)
		}
	})
}

// Ensure a row can be cleared.
func TestExecutor_Execute_ClearRow(t *testing.T) {
	t.Run("Int", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		_, err := index.CreateField("f", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		if err != nil {
			t.Fatal(err)
		}

		// Ensure that clearing a row raises an error.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `ClearRow(f=1)`}); err == nil {
			t.Fatal("expected clear row to return an error")
		}
	})

	t.Run("TopN", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
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
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: cc}); err != nil {
			t.Fatal(err)
		}

		if err := c.GetNode(0).RecalculateCaches(t); err != nil {
			t.Fatalf("recalculating caches: %v", err)
		}

		// Check the TopN results.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=5)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(res.Results, []interface{}{&pilosa.PairsField{
			Pairs: []pilosa.Pair{
				{ID: 1, Count: 7},
				{ID: 2, Count: 6},
				{ID: 3, Count: 5},
			},
			Field: "f",
		}}) {
			t.Fatalf("topn wrong results: %v", res.Results)
		}

		// Clear the row and ensure we get a `true` response.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `ClearRow(f=2)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected clear row result: %+v", res)
		}

		// Ensure that the cleared row doesn't show up in TopN (i.e. it was removed from the cache).
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `TopN(f, n=5)`}); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(res.Results, []interface{}{&pilosa.PairsField{
			Pairs: []pilosa.Pair{
				{ID: 1, Count: 7},
				{ID: 3, Count: 5},
			},
			Field: "f",
		}}) {
			t.Fatalf("topn wrong results: %v", res.Results)
		}
	})
}

// Ensure a row can be set.
func TestExecutor_Execute_SetRow(t *testing.T) {
	t.Run("Set_NewRow", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		if _, err := index.CreateField("f", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}
		if _, err := index.CreateField("tmp", pilosa.OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		}

		// Set bits.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `` +
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth-1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10),
		}); err != nil {
			t.Fatal(err)
		}

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row 10 into a different row.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f=10), tmp=20)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(tmp=20)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row 10 into a table which doesn't exist.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f=10), nonexistent=20)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(nonexistent=20)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})
	t.Run("Set_NoSource", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		idx := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		_, err := idx.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}

		// Set bits.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `` +
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth-1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10),
		}); err != nil {
			t.Fatal(err)
		}

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row 9 (which doesn't exist) into a different row.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f=9), f=20)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=20)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row 9 (which doesn't exist) into a row that does exist.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f=9), f=10)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=10)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})
	t.Run("Set_ExistingDestination", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		_, err := index.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}

		// Set bits.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `` +
			fmt.Sprintf("Set(%d, f=%d)\n", 3, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth-1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 10) +
			fmt.Sprintf("Set(%d, f=%d)\n", 1, 20) +
			fmt.Sprintf("Set(%d, f=%d)\n", ShardWidth+1, 20),
		}); err != nil {
			t.Fatal(err)
		}

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=20)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row 10 into an existing row.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f=10), f=20)`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f=20)`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{3, ShardWidth - 1, ShardWidth + 1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})
	t.Run("Set_Keyed", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{TrackExistence: true})
		if _, err := index.CreateField("f", pilosa.OptFieldTypeDefault(), pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		}

		// Set bits.
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1, f="a")`}); err != nil {
			t.Fatal(err)
		}

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f="a")`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row a into a different row.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f="a"), f="b")`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(f="b")`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}

		// Store row 10 into a table which doesn't exist.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Store(Row(f="a"), nonexistent="c")`}); err != nil {
			t.Fatal(err)
		} else if res := res.Results[0].(bool); !res {
			t.Fatalf("unexpected set row result: %+v", res)
		}

		// Ensure the row was populated.
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Row(nonexistent="c")`}); err != nil {
			t.Fatal(err)
		} else if bits := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(bits, []uint64{1}) {
			t.Fatalf("unexpected columns: %+v", bits)
		}
	})
}

func benchmarkExistence(nn bool, b *testing.B) {
	c := test.MustNewCluster(b, 1)
	var err error
	c.GetIdleNode(0).Config.DataDir, err = testhook.TempDirInDir(b, *TempDir, "benchmarkExistence")
	if err != nil {
		b.Fatalf("getting temp dir: %v", err)
	}
	err = c.Start()
	if err != nil {
		b.Fatalf("starting cluster: %v", err)
	}
	defer c.Close()
	hldr := c.GetHolder(0)

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
	nodeAPI := c.GetNode(0).API
	for i := 0; i < b.N; i++ {
		qcx := nodeAPI.Txf().NewQcx()
		if err := nodeAPI.Import(context.Background(), qcx, req); err != nil {
			b.Fatal(err)
		}
		PanicOn(qcx.Finish())
	}
}

func BenchmarkExecutor_Existence_True(b *testing.B)  { benchmarkExistence(true, b) }
func BenchmarkExecutor_Existence_False(b *testing.B) { benchmarkExistence(false, b) }

func TestExecutor_Execute_Extract(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "set")
	c.ImportBits(t, "i", "set", [][2]uint64{
		{0, 1},
		{0, 2},
		{3, 1},
		{4, 1},
		{4, 4 * ShardWidth},
		{5, ShardWidth},
	})
	c.Query(t, "i", fmt.Sprintf("Clear(%d, set=5)", ShardWidth))

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "keyset", pilosa.OptFieldKeys())
	c.Query(t, "i", `
		Set(0, keyset="h")
		Set(1, keyset="xyzzy")
		Set(0, keyset="plugh")
	`)

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "mutex", pilosa.OptFieldTypeMutex(pilosa.CacheTypeRanked, 5000))
	c.ImportBits(t, "i", "mutex", [][2]uint64{
		{0, 1},
		{0, 2},
		{4, 4 * ShardWidth},
	})

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "keymutex", pilosa.OptFieldKeys(), pilosa.OptFieldTypeMutex(pilosa.CacheTypeRanked, 5000))
	c.Query(t, "i", `
		Set(0, keymutex="h")
		Set(1, keymutex="xyzzy")
		Set(3, keymutex="plugh")
	`)

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "time", pilosa.OptFieldTypeTime("YMDH"))
	c.Query(t, "i", `
		Set(0, time=1, 2016-01-01T00:00)
		Set(1, time=2, 2017-01-01T00:00)
		Set(3, time=3, 2018-01-01T00:00)
	`)

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "keytime", pilosa.OptFieldKeys(), pilosa.OptFieldTypeTime("YMDH"))
	c.Query(t, "i", `
		Set(0, keytime="h", 2016-01-01T00:00)
		Set(1, keytime="xyzzy", 2017-01-01T00:00)
		Set(0, keytime="plugh", 2018-01-01T00:00)
	`)

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "bsint", pilosa.OptFieldTypeInt(-100, 100))
	c.Query(t, "i", `
		Set(0, bsint=1)
		Set(1, bsint=-1)
		Set(3, bsint=2)
	`)

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "bsidecimal", pilosa.OptFieldTypeDecimal(2))
	c.Query(t, "i", `
		Set(0, bsidecimal=0.01)
		Set(1, bsidecimal=1.00)
		Set(3, bsidecimal=-1.01)
	`)

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "timestamp", pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds))
	c.Query(t, "i", `
		Set(0, timestamp='2000-01-01T00:00:00Z')
		Set(1, timestamp='2000-01-01T00:00:01Z')
		Set(3, timestamp='2000-01-01T00:00:03Z')
	`)

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "bool", pilosa.OptFieldTypeBool())
	c.Query(t, "i", `
		Set(0, bool=true)
		Set(1, bool=false)
		Set(3, bool=true)
	`)

	resp := c.Query(t, "i", `Extract(All(), Rows(set), Rows(keyset), Rows(mutex), Rows(keymutex), Rows(time), Rows(keytime), Rows(bsint), Rows(bsidecimal), Rows(timestamp), Rows(bool))`)
	expect := []interface{}{
		pilosa.ExtractedTable{
			Fields: []pilosa.ExtractedTableField{
				{
					Name: "set",
					Type: "[]uint64",
				},
				{
					Name: "keyset",
					Type: "[]string",
				},
				{
					Name: "mutex",
					Type: "uint64",
				},
				{
					Name: "keymutex",
					Type: "string",
				},
				{
					Name: "time",
					Type: "[]uint64",
				},
				{
					Name: "keytime",
					Type: "[]string",
				},
				{
					Name: "bsint",
					Type: "int64",
				},
				{
					Name: "bsidecimal",
					Type: "decimal",
				},
				{
					Name: "timestamp",
					Type: "timestamp",
				},
				{
					Name: "bool",
					Type: "bool",
				},
			},
			Columns: []pilosa.ExtractedTableColumn{
				{
					Column: pilosa.KeyOrID{ID: 0},
					Rows: []interface{}{
						[]uint64{},
						[]string{
							"h",
							"plugh",
						},
						nil,
						"h",
						[]uint64{
							1,
						},
						[]string{
							"h",
							"plugh",
						},
						int64(1),
						pql.NewDecimal(1, 2),
						time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
						true,
					},
				},
				{
					Column: pilosa.KeyOrID{ID: 1},
					Rows: []interface{}{
						[]uint64{
							0,
							3,
							4,
						},
						[]string{
							"xyzzy",
						},
						uint64(0),
						"xyzzy",
						[]uint64{
							2,
						},
						[]string{
							"xyzzy",
						},
						int64(-1),
						pql.NewDecimal(100, 2),
						time.Date(2000, time.January, 1, 0, 0, 1, 0, time.UTC),
						false,
					},
				},
				{
					Column: pilosa.KeyOrID{ID: 2},
					Rows: []interface{}{
						[]uint64{
							0,
						},
						[]string{},
						uint64(0),
						nil,
						[]uint64{},
						[]string{},
						nil,
						nil,
						nil,
						nil,
					},
				},
				{
					Column: pilosa.KeyOrID{ID: 3},
					Rows: []interface{}{
						[]uint64{},
						[]string{},
						nil,
						"plugh",
						[]uint64{
							3,
						},
						[]string{},
						int64(2),
						pql.NewDecimal(-101, 2),
						time.Date(2000, time.January, 1, 0, 0, 3, 0, time.UTC),
						true,
					},
				},
				{
					Column: pilosa.KeyOrID{ID: ShardWidth},
					Rows: []interface{}{
						[]uint64{},
						[]string{},
						nil,
						nil,
						[]uint64{},
						[]string{},
						nil,
						nil,
						nil,
						nil,
					},
				},
				{
					Column: pilosa.KeyOrID{ID: 4 * ShardWidth},
					Rows: []interface{}{
						[]uint64{
							4,
						},
						[]string{},
						uint64(4),
						nil,
						[]uint64{},
						[]string{},
						nil,
						nil,
						nil,
						nil,
					},
				},
			},
		},
	}

	if !reflect.DeepEqual(expect, resp.Results) {
		t.Errorf("expected %v but got %v", expect, resp.Results)
	}
}

func TestExecutor_Execute_Extract_Keyed(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true, Keys: true}, "set")
	c.Query(t, "i", `
		Set("h", set=1)
		Set("h", set=2)
		Set("xyzzy", set=2)
		Set("plugh", set=1)
		Clear("plugh", set=1)
	`)

	resp := c.Query(t, "i", `Extract(All(), Rows(set))`)
	expect := []interface{}{
		pilosa.ExtractedTable{
			Fields: []pilosa.ExtractedTableField{
				{
					Name: "set",
					Type: "[]uint64",
				},
			},
			Columns: []pilosa.ExtractedTableColumn{
				{
					Column: pilosa.KeyOrID{Keyed: true, Key: "plugh"},
					Rows: []interface{}{
						[]uint64{},
					},
				},
				{
					Column: pilosa.KeyOrID{Keyed: true, Key: "h"},
					Rows: []interface{}{
						[]uint64{
							1,
							2,
						},
					},
				},
				{
					Column: pilosa.KeyOrID{Keyed: true, Key: "xyzzy"},
					Rows: []interface{}{
						[]uint64{
							2,
						},
					},
				},
			},
		},
	}

	if !reflect.DeepEqual(expect, resp.Results) {
		t.Errorf("expected %v but got %v", expect, resp.Results)
	}
}

func TestExecutor_Execute_MaxMemory(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "set")
	c.ImportBits(t, "i", "set", [][2]uint64{
		{0, 1},
		{0, 2},
		{3, 1},
		{4, 1},
		{4, 4 * ShardWidth},
		{5, ShardWidth},
	})
	c.Query(t, "i", fmt.Sprintf("Clear(%d, set=5)", ShardWidth))

	resp := c.GetPrimary().QueryAPI(t, &pilosa.QueryRequest{
		Index:     "i",
		Query:     `Extract(All(), Rows(set))`,
		MaxMemory: 1000,
	})
	expect := []interface{}{
		pilosa.ExtractedTable{
			Fields: []pilosa.ExtractedTableField{
				{
					Name: "set",
					Type: "[]uint64",
				},
			},
			Columns: []pilosa.ExtractedTableColumn{
				{
					Column: pilosa.KeyOrID{ID: 1},
					Rows: []interface{}{
						[]uint64{
							0,
							3,
							4,
						},
					},
				},
				{
					Column: pilosa.KeyOrID{ID: 2},
					Rows: []interface{}{
						[]uint64{
							0,
						},
					},
				},
				{
					Column: pilosa.KeyOrID{ID: ShardWidth},
					Rows: []interface{}{
						[]uint64{},
					},
				},
				{
					Column: pilosa.KeyOrID{ID: 4 * ShardWidth},
					Rows: []interface{}{
						[]uint64{
							4,
						},
					},
				},
			},
		},
	}

	if !reflect.DeepEqual(expect, resp.Results) {
		t.Errorf("expected %v but got %v", expect, resp.Results)
	}
}

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
	if !reflect.DeepEqual(rows.Rows, []uint64{10, 11, 12, 13}) {
		t.Fatalf("unexpected rows: %+v", rows.Rows)
	} else if rows.Keys != nil {
		t.Fatalf("unexpected keys: %+v", rows.Keys)
	}

	// backwards compatibility
	// TODO: remove at Pilosa 2.0
	rows = c.Query(t, "i", `Rows(field=general)`).Results[0].(pilosa.RowIdentifiers)
	if !reflect.DeepEqual(rows.Rows, []uint64{10, 11, 12, 13}) {
		t.Fatalf("unexpected rows: %+v", rows.Rows)
	} else if rows.Keys != nil {
		t.Fatalf("unexpected keys: %+v", rows.Keys)
	}

	rows = c.Query(t, "i", `Rows(general, limit=2)`).Results[0].(pilosa.RowIdentifiers)
	if !reflect.DeepEqual(rows.Rows, []uint64{10, 11}) {
		t.Fatalf("unexpected rows: %+v", rows.Rows)
	} else if rows.Keys != nil {
		t.Fatalf("unexpected keys: %+v", rows.Keys)
	}

	rows = c.Query(t, "i", `Rows(general, previous=10,limit=2)`).Results[0].(pilosa.RowIdentifiers)
	if !reflect.DeepEqual(rows.Rows, []uint64{11, 12}) {
		t.Fatalf("unexpected rows: %+v", rows.Rows)
	} else if rows.Keys != nil {
		t.Fatalf("unexpected keys: %+v", rows.Keys)
	}

	rows = c.Query(t, "i", `Rows(general, column=2)`).Results[0].(pilosa.RowIdentifiers)
	if !reflect.DeepEqual(rows.Rows, []uint64{11, 12}) {
		t.Fatalf("unexpected rows: %+v", rows.Rows)
	} else if rows.Keys != nil {
		t.Fatalf("unexpected keys: %+v", rows.Keys)
	}
}

func TestExecutor_Execute_RowsTime(t *testing.T) {

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
	c.CreateField(t, "i", pilosa.IndexOptions{}, "integer", pilosa.OptFieldTypeInt(-1000, 1000))
	c.CreateField(t, "i", pilosa.IndexOptions{}, "decimal", pilosa.OptFieldTypeDecimal(2))
	c.CreateField(t, "i", pilosa.IndexOptions{}, "bool", pilosa.OptFieldTypeBool())
	c.CreateField(t, "i", pilosa.IndexOptions{}, "keys", pilosa.OptFieldKeys())

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
		{
			query: "GroupBy(Rows(integer), prev=-1)",
			error: "unknown arg 'prev'",
		},
		{
			query: "Rows(integer)",
			error: "int fields not supported by Rows() query",
		},
		{
			query: "Rows(decimal)",
			error: "decimal fields not supported by Rows() query",
		},
		{
			query: "Rows(bool)",
			error: "bool fields not supported by Rows() query",
		},
		{
			query: `Row(keys=1)`,
			error: `found integer ID 1 on keyed field "keys"`,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
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
	c.CreateField(t, "istring", pilosa.IndexOptions{Keys: true}, "v", pilosa.OptFieldTypeInt(0, 1000))
	c.CreateField(t, "istring", pilosa.IndexOptions{Keys: true}, "vv", pilosa.OptFieldTypeInt(0, 1000))
	c.CreateField(t, "istring", pilosa.IndexOptions{Keys: true}, "nv", pilosa.OptFieldTypeInt(-1000, 1000))
	c.CreateField(t, "istring", pilosa.IndexOptions{Keys: true}, "dv", pilosa.OptFieldTypeDecimal(2))
	c.CreateField(t, "istring", pilosa.IndexOptions{Keys: true}, "ndv", pilosa.OptFieldTypeDecimal(1))

	if err := c.GetNode(0).API.Import(context.Background(), nil, &pilosa.ImportRequest{
		Index:      "istring",
		Field:      "generals",
		Shard:      0,
		RowKeys:    []string{"r1", "r2", "r1", "r2", "r1", "r2", "r1", "r2", "r1", "r2"},
		ColumnKeys: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
	}); err != nil {
		t.Fatalf("importing: %v", err)
	}
	m0 := c.GetNode(0)
	qcx := m0.API.Txf().NewQcx()
	defer qcx.Abort()

	var v1, v2, v3, v4, v5, v6, v7, v8, v9, v10 int64 = 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	var nv1, nv2, nv3, nv4 int64 = -1, -2, -3, -4
	var dv1, dv2, dv3, dv4, dv5, dv6, dv7, dv8, dv9, dv10 int64 = 111, 222, 333, 444, 555, 666, 777, 888, 999, 1000
	var ndv1, ndv2, ndv3, ndv4, ndv5, ndv6, ndv7, ndv8, ndv9, ndv10 int64 = -111, -222, -333, -444, -555, -666, -777, -888, -999, -1000
	if err := m0.API.ImportValue(context.Background(), qcx, &pilosa.ImportValueRequest{
		Index:      "istring",
		Field:      "v",
		Shard:      0,
		ColumnKeys: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		Values:     []int64{v1, v2, v3, v4, v5, v6, v7, v8, v9, v10},
	}); err != nil {
		t.Fatalf("importing: %v", err)
	}

	if err := m0.API.ImportValue(context.Background(), qcx, &pilosa.ImportValueRequest{
		Index:      "istring",
		Field:      "vv",
		Shard:      0,
		ColumnKeys: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		Values:     []int64{v1, v2, v2, v3, v3, v3, v4, v4, v4, v4},
	}); err != nil {
		t.Fatalf("importing: %v", err)
	}

	if err := m0.API.ImportValue(context.Background(), qcx, &pilosa.ImportValueRequest{
		Index:      "istring",
		Field:      "nv",
		Shard:      0,
		ColumnKeys: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		Values:     []int64{nv1, nv2, nv2, nv3, nv3, nv3, nv4, nv4, nv4, nv4},
	}); err != nil {
		t.Fatalf("importing: %v", err)
	}

	if err := m0.API.ImportValue(context.Background(), qcx, &pilosa.ImportValueRequest{
		Index:      "istring",
		Field:      "dv",
		Shard:      0,
		ColumnKeys: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		Values:     []int64{dv1, dv2, dv3, dv4, dv5, dv6, dv7, dv8, dv9, dv10},
	}); err != nil {
		t.Fatalf("importing: %v", err)
	}

	if err := m0.API.ImportValue(context.Background(), qcx, &pilosa.ImportValueRequest{
		Index:      "istring",
		Field:      "ndv",
		Shard:      0,
		ColumnKeys: []string{"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10"},
		Values:     []int64{ndv1, ndv2, ndv3, ndv4, ndv5, ndv6, ndv7, ndv8, ndv9, ndv10},
	}); err != nil {
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
		{
			query: "GroupBy(Rows(generals), aggregate=Sum(field=v))",
			expected: []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 1, RowKey: "r1"}}, Count: 5, Agg: 25},
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 2, RowKey: "r2"}}, Count: 5, Agg: 30},
			},
		},
		{
			query: "GroupBy(Rows(generals), aggregate=Sum(field=dv))",
			expected: []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 1, RowKey: "r1"}}, Count: 5, Agg: 2775, DecimalAgg: 27.75},
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 2, RowKey: "r2"}}, Count: 5, Agg: 3220, DecimalAgg: 32.20},
			},
		},
		{
			query: "GroupBy(Rows(generals), aggregate=Sum(field=ndv))",
			expected: []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 1, RowKey: "r1"}}, Count: 5, Agg: -2775, DecimalAgg: -277.5},
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 2, RowKey: "r2"}}, Count: 5, Agg: -3220, DecimalAgg: -322.0},
			},
		},
		{
			query: "GroupBy(Rows(generals), aggregate=Sum(field=v), having=Condition(sum>25))",
			expected: []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 2, RowKey: "r2"}}, Count: 5, Agg: 30},
			},
		},
		{
			query: "GroupBy(Rows(generals), aggregate=Sum(field=v), having=Condition(-5<sum<27))",
			expected: []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "generals", RowID: 1, RowKey: "r1"}}, Count: 5, Agg: 25},
			},
		},
		{
			query:    "GroupBy(Rows(generals), aggregate=Sum(field=v), having=Condition(count>5))",
			expected: []pilosa.GroupCount{},
		},
		{
			query: "GroupBy(Rows(v))",
			expected: []pilosa.GroupCount{
				{
					Group: []pilosa.FieldRow{{Field: "v", Value: &v1}},
					Count: 1,
					Agg:   0,
				},
				{
					Group: []pilosa.FieldRow{{Field: "v", Value: &v2}},
					Count: 1,
					Agg:   0,
				},
				{
					Group: []pilosa.FieldRow{{Field: "v", Value: &v3}},
					Count: 1,
					Agg:   0,
				},
				{
					Group: []pilosa.FieldRow{{Field: "v", Value: &v4}},
					Count: 1,
					Agg:   0,
				},
				{
					Group: []pilosa.FieldRow{{Field: "v", Value: &v5}},
					Count: 1,
					Agg:   0,
				},
				{
					Group: []pilosa.FieldRow{{Field: "v", Value: &v6}},
					Count: 1,
					Agg:   0,
				},
				{
					Group: []pilosa.FieldRow{{Field: "v", Value: &v7}},
					Count: 1,
					Agg:   0,
				},
				{
					Group: []pilosa.FieldRow{{Field: "v", Value: &v8}},
					Count: 1,
					Agg:   0,
				},
				{
					Group: []pilosa.FieldRow{{Field: "v", Value: &v9}},
					Count: 1,
					Agg:   0,
				},
				{
					Group: []pilosa.FieldRow{{Field: "v", Value: &v10}},
					Count: 1,
					Agg:   0,
				},
			},
		},
		{
			query: "GroupBy(Rows(vv), aggregate=Sum(field=vv), having=Condition(count > 2))",
			expected: []pilosa.GroupCount{
				{
					Group: []pilosa.FieldRow{{Field: "vv", Value: &v3}},
					Count: 3,
					Agg:   9,
				},
				{
					Group: []pilosa.FieldRow{{Field: "vv", Value: &v4}},
					Count: 4,
					Agg:   16,
				},
			},
		},
		{
			query: "GroupBy(Rows(nv), aggregate=Sum(field=nv), limit=2)",
			expected: []pilosa.GroupCount{
				{
					Group: []pilosa.FieldRow{{Field: "nv", Value: &nv4}},
					Count: 4,
					Agg:   -16,
				},
				{
					Group: []pilosa.FieldRow{{Field: "nv", Value: &nv3}},
					Count: 3,
					Agg:   -9,
				},
			},
		},
		{
			query: "GroupBy(Rows(nv), aggregate=Sum(field=nv), having=Condition(count > 2), limit=2)",
			expected: []pilosa.GroupCount{
				{
					Group: []pilosa.FieldRow{{Field: "nv", Value: &nv4}},
					Count: 4,
					Agg:   -16,
				},
				{
					Group: []pilosa.FieldRow{{Field: "nv", Value: &nv3}},
					Count: 3,
					Agg:   -9,
				},
			},
		},
		{
			query: "GroupBy(Rows(vv), Rows(nv), aggregate=Sum(field=vv), having=Condition(count > 2))",
			expected: []pilosa.GroupCount{
				{
					Group: []pilosa.FieldRow{
						{Field: "vv", Value: &v3},
						{Field: "nv", Value: &nv3},
					},
					Count: 3,
					Agg:   9,
				},
				{
					Group: []pilosa.FieldRow{
						{Field: "vv", Value: &v4},
						{Field: "nv", Value: &nv4},
					},
					Count: 4,
					Agg:   16,
				},
			},
		},
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			r, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
				Index: "istring",
				Query: tst.query,
			})
			if err != nil {
				t.Fatal(err)
			}
			results := r.Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, tst.expected, results)
		})
	}
}

func TestExecutor_Execute_Rows_Keys(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	_, err := c.GetNode(0).API.CreateIndex(context.Background(), "i", pilosa.IndexOptions{Keys: true})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}

	_, err = c.GetNode(0).API.CreateField(context.Background(), "i", "f", pilosa.OptFieldKeys())
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
	_, err = c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
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
		{
			q:   `Rows(f, like="__")`,
			exp: []string{"10", "11", "12", "13", "14", "15", "16", "17", "18"},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("#%d_%s", i, test.q), func(t *testing.T) {
			if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: test.q}); err != nil {
				t.Fatal(err)
			} else {
				rows := res.Results[0].(pilosa.RowIdentifiers)
				if !reflect.DeepEqual(rows.Keys, test.exp) {
					t.Fatalf("\ngot: %+v\nexp: %+v", rows.Keys, test.exp)
				} else if rows.Rows != nil {
					if test.exp == nil {
						if res.Results != nil {
							t.Fatalf("\ngot: %+v\nexp: nil, %[1]T, %#[1]v", res.Results)
						}
					} else {
						rows := res.Results[0].(pilosa.RowIdentifiers)
						if !reflect.DeepEqual(rows.Keys, test.exp) {
							t.Fatalf("\ngot: %+v %[1]T\nexp: %+v  %[2]T", rows.Keys, test.exp)
						} else if rows.Rows != nil {
							t.Fatalf("\ngot: %+v %[1]T\nexp: nil", rows.Rows)
						}
					}
				}
			}
		})
	}

}

func TestExecutor_ForeignIndex(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	c.CreateField(t, "parent", pilosa.IndexOptions{Keys: true}, "general")
	c.CreateField(t, "child", pilosa.IndexOptions{}, "parent_id",
		pilosa.OptFieldTypeInt(0, math.MaxInt64),
		pilosa.OptFieldForeignIndex("parent"),
	)
	c.CreateField(t, "child", pilosa.IndexOptions{}, "parent_set_id",
		pilosa.OptFieldForeignIndex("parent"),
	)
	c.CreateField(t, "child", pilosa.IndexOptions{}, "color",
		pilosa.OptFieldKeys(),
	)

	// stepchild/other field needs to have usesKeys=true
	crashSchemaJson := `{"indexes": [{"name": "stepparent","createdAt": 1611247966371721700,"options": {"keys": true,"trackExistence": true},"shardWidth": 1048576},{"name": "stepchild","createdAt": 1611247953796662800,"options": {"keys": true,"trackExistence": true},"shardWidth": 1048576,"fields": [{"name": "parent_id","createdAt": 1611247953797265700,"options": {"type": "int","base": 0,"bitDepth": 28,"min": -9223372036854776000,"max": 9223372036854776000,"keys": false,"foreignIndex": "stepparent"}},{"name": "other","createdAt": 1611247953796814000,"options": {"type": "int","base": 0,"bitDepth": 17,"min": -9223372036854776000,"max": 9223372036854776000,"keys": true,"foreignIndex": ""}}]}]}`

	crashSchema := &pilosa.Schema{}
	err := json.Unmarshal([]byte(crashSchemaJson), &crashSchema)
	if err != nil {
		t.Fatalf("json unmarshall: %v", err)
	}
	err = c.GetNode(0).API.ApplySchema(context.Background(), crashSchema, false)
	if err != nil {
		t.Fatalf("applying JSON schema: %v", err)
	}

	// Populate parent data.
	c.Query(t, "parent", fmt.Sprintf(`
			Set("one", general=1)
			Set("two", general=1)
			Set("three", general=1)

			Set("twenty-one", general=2)
			Set("twenty-two", general=2)
			Set("twenty-three", general=2)

			Set("one", general=%d)
			Set("twenty-one", general=%d)
		`, ShardWidth, ShardWidth))

	// Populate child data.
	c.Query(t, "child", fmt.Sprintf(`
			Set(1, parent_id="one")
			Set(2, parent_id="two")
			Set(%d, parent_id="one")
			Set(4, parent_id="twenty-one")
		`, ShardWidth))
	c.Query(t, "child", fmt.Sprintf(`
			Set(1, parent_set_id="one")
			Set(2, parent_set_id="two")
			Set(%d, parent_set_id="one")
			Set(4, parent_set_id="twenty-one")
		`, ShardWidth))

	// Populate color data.
	c.Query(t, "child", fmt.Sprintf(`
			Set(1, color="red")
			Set(2, color="blue")
			Set(%d, color="blue")
			Set(4, color="red")
		`, ShardWidth))

	distinct := c.Query(t, "child", `Distinct(index="child", field="parent_id")`).Results[0].(pilosa.SignedRow)
	if !sameStringSlice(distinct.Pos.Keys, []string{"one", "two", "twenty-one"}) {
		t.Fatalf("unexpected keys: %v", distinct.Pos.Keys)
	}
	row := c.Query(t, "child", `Distinct(index="child", field="parent_set_id")`).Results[0].(*pilosa.Row)
	if !sameStringSlice(row.Keys, []string{"one", "two", "twenty-one"}) {
		t.Fatalf("unexpected keys: %v", row.Keys)
	}

	crash := c.Query(t, "stepchild", `Distinct(Row(parent_id=3), field=other)`).Results[0].(pilosa.SignedRow)
	if !sameStringSlice(crash.Pos.Keys, []string{}) {
		// empty result; error condition does not require data
		t.Fatalf("unexpected columns: %v", crash.Pos.Keys)
	}

	eq := c.Query(t, "child", `Row(parent_id=="one")`).Results[0].(*pilosa.Row)
	if !reflect.DeepEqual(eq.Columns(), []uint64{1, ShardWidth}) {
		t.Fatalf("unexpected columns: %v", eq.Columns())
	}

	neq := c.Query(t, "child", `Row(parent_id!="one")`).Results[0].(*pilosa.Row)
	if !reflect.DeepEqual(neq.Columns(), []uint64{2, 4}) {
		t.Fatalf("unexpected columns: %v", neq.Columns())
	}

	join := c.Query(t, "parent", fmt.Sprintf(`Intersect(Row(general=%d), Distinct(Row(color="blue"), index="child", field="parent_id"))`, ShardWidth)).Results[0].(*pilosa.Row)
	if !reflect.DeepEqual(join.Keys, []string{"one"}) {
		t.Fatalf("unexpected keys: %v", join.Keys)
	}
	join = c.Query(t, "parent", fmt.Sprintf(`Intersect(Row(general=%d), Distinct(Row(color="blue"), index="child", field="parent_set_id"))`, ShardWidth)).Results[0].(*pilosa.Row)
	if !reflect.DeepEqual(join.Keys, []string{"one"}) {
		t.Fatalf("unexpected keys: %v", join.Keys)
	}
}

// sameStringSlice is a helper function which compares two string
// slices without enforcing order.
func sameStringSlice(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	// create a map of string -> int
	diff := make(map[string]int, len(x))
	for _, _x := range x {
		// 0 value for int is 0, so just increment a counter for the string
		diff[_x]++
	}
	for _, _y := range y {
		// If the string _y is not in diff bail out early
		if _, ok := diff[_y]; !ok {
			return false
		}
		diff[_y] -= 1
		if diff[_y] == 0 {
			delete(diff, _y)
		}
	}
	return len(diff) == 0
}

func TestExecutor_Execute_DistinctFailure(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	c.CreateField(t, "i", pilosa.IndexOptions{}, "general")
	c.CreateField(t, "i", pilosa.IndexOptions{}, "v", pilosa.OptFieldTypeInt(0, 1000))
	c.ImportBits(t, "i", "general", [][2]uint64{
		{10, 0},
		{10, 1},
		{10, ShardWidth + 1},
		{11, 2},
		{11, ShardWidth + 2},
		{12, 2},
		{12, ShardWidth + 2},
	})

	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(0, v=10)`}); err != nil {
		t.Fatal(err)
	} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1, v=100)`}); err != nil {
		t.Fatal(err)
	}

	t.Run("BasicDistinct", func(t *testing.T) {
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Distinct(field="v")`}); err != nil {
			t.Fatalf("unexpected error: \"%v\"", err)
		}
	})
}

func TestExecutor_Execute_GroupBy(t *testing.T) {
	groupByTest := func(t *testing.T, clusterSize int) {
		c := test.MustRunCluster(t, clusterSize)
		defer c.Close()
		c.CreateField(t, "i", pilosa.IndexOptions{}, "general")
		c.CreateField(t, "i", pilosa.IndexOptions{}, "sub")
		c.CreateField(t, "i", pilosa.IndexOptions{}, "v", pilosa.OptFieldTypeInt(0, 1000))
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
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(0, v=10)`}); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Set(1, v=100)`}); err != nil {
			t.Fatal(err)
		} else if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf(`Set(%d, v=100)`, ShardWidth+10)}); err != nil { // Workaround distinct bug where v must be set in every shard
			t.Fatal(err)
		}

		t.Run("No Field List Arguments", func(t *testing.T) {
			if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `GroupBy()`}); err != nil {
				if !strings.Contains(err.Error(), "need at least one child call") {
					t.Fatalf("unexpected error: \"%v\"", err)
				}
			}
		})

		t.Run("Unknown Field ", func(t *testing.T) {
			if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `GroupBy(Rows(missing))`}); err != nil {
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

			results := c.Query(t, "i", `GroupBy(Rows(field=general), Rows(sub))`).Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("Basic", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 100}}, Count: 3},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 110}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 11}, {Field: "sub", RowID: 110}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 12}, {Field: "sub", RowID: 110}}, Count: 1},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general), Rows(sub))`).Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("Filter", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 100}}, Count: 3},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 110}}, Count: 1},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general), Rows(sub), filter=Row(general=10))`).Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("Aggregate", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 100}}, Count: 2, Agg: 110},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 110}}, Count: 1, Agg: 10},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general), Rows(sub), aggregate=Sum(field=v))`).Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("AggregateCountDistinct", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 100}}, Count: 3, Agg: 2},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 110}}, Count: 1, Agg: 1},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 11}, {Field: "sub", RowID: 110}}, Count: 1, Agg: 0},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 12}, {Field: "sub", RowID: 110}}, Count: 1, Agg: 0},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general), Rows(sub), aggregate=Count(Distinct(field=v)))`).Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("AggregateCountDistinctFilter", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 100}}, Count: 1, Agg: 1},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general), Rows(sub), filter=Row(v > 10), aggregate=Count(Distinct(field=v)))`).Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("AggregateCountDistinctFilterDistinct", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 100}}, Count: 3, Agg: 1},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 10}, {Field: "sub", RowID: 110}}, Count: 1, Agg: 0},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 11}, {Field: "sub", RowID: 110}}, Count: 1, Agg: 0},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 12}, {Field: "sub", RowID: 110}}, Count: 1, Agg: 0},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general), Rows(sub), aggregate=Count(Distinct(Row(v > 10), field=v)))`).Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("check field offset no limit", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 11}}, Count: 2},
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 12}}, Count: 2},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general, previous=10))`).Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("check field offset limit", func(t *testing.T) {
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "general", RowID: 11}}, Count: 2},
			}

			results := c.Query(t, "i", `GroupBy(Rows(general, previous=10), limit=1)`).Results[0].(*pilosa.GroupCounts).Groups()
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

			results := c.Query(t, "i", `GroupBy(Rows(a), Rows(b), limit=1)`).Results[0].(*pilosa.GroupCounts).Groups()
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
			results := c.Query(t, "i", `GroupBy(Rows(wa), Rows(wb), Rows(wc, previous=1), limit=3)`).Results[0].(*pilosa.GroupCounts).Groups()
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "wa", RowID: 0}, {Field: "wb", RowID: 0}, {Field: "wc", RowID: 2}}, Count: 2},
				{Group: []pilosa.FieldRow{{Field: "wa", RowID: 0}, {Field: "wb", RowID: 1}, {Field: "wc", RowID: 0}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "wa", RowID: 0}, {Field: "wb", RowID: 1}, {Field: "wc", RowID: 1}}, Count: 1},
			}
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("test previous is last result", func(t *testing.T) {
			results := c.Query(t, "i", `GroupBy(Rows(wa, previous=3), Rows(wb, previous=3), Rows(wc, previous=3), limit=3)`).Results[0].(*pilosa.GroupCounts).Groups()
			if len(results) > 0 {
				t.Fatalf("expected no results because previous specified last result")
			}
		})

		t.Run("test wrapping multiple", func(t *testing.T) {
			results := c.Query(t, "i", `GroupBy(Rows(wa), Rows(wb, previous=2), Rows(wc, previous=2), limit=1)`).Results[0].(*pilosa.GroupCounts).Groups()
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
			results := c.Query(t, "i", `GroupBy(Rows(ma), Rows(mb), limit=5)`).Results[0].(*pilosa.GroupCounts).Groups()
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
			results := c.Query(t, "i", `GroupBy(Rows(ma), Rows(mb, limit=2), limit=5)`).Results[0].(*pilosa.GroupCounts).Groups()
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 0}, {Field: "mb", RowID: 0}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 1}, {Field: "mb", RowID: 1}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 2}, {Field: "mb", RowID: 0}}, Count: 1},
				{Group: []pilosa.FieldRow{{Field: "ma", RowID: 3}, {Field: "mb", RowID: 1}}, Count: 1},
			}
			test.CheckGroupBy(t, expected, results)
		})

		t.Run("distinct rows in different shards with column arg", func(t *testing.T) {
			results := c.Query(t, "i", fmt.Sprintf(`GroupBy(Rows(ma), Rows(mb, column=%d), limit=5)`, ShardWidth)).Results[0].(*pilosa.GroupCounts).Groups()
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
			results := c.Query(t, "i", `GroupBy(Rows(na), Rows(nb))`).Results[0].(*pilosa.GroupCounts).Groups()
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
			results := c.Query(t, "i", `GroupBy(Rows(ppa), Rows(ppb), Rows(ppc), limit=3)`).Results[0].(*pilosa.GroupCounts).Groups()
			totalResults = append(totalResults, results...)
			for len(totalResults) < 64 {
				lastGroup := results[len(results)-1].Group
				query := fmt.Sprintf("GroupBy(Rows(ppa, previous=%d), Rows(ppb, previous=%d), Rows(ppc, previous=%d), limit=3)", lastGroup[0].RowID, lastGroup[1].RowID, lastGroup[2].RowID)
				results = c.Query(t, "i", query).Results[0].(*pilosa.GroupCounts).Groups()
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

			results := c.Query(t, "i", `GroupBy(Rows(generalk), Rows(subk))`).Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupBy(t, expected, results)

		})

		// Foreign Index
		c.CreateField(t, "fip", pilosa.IndexOptions{Keys: true}, "parent")
		c.CreateField(t, "fic", pilosa.IndexOptions{}, "child",
			pilosa.OptFieldTypeInt(0, math.MaxInt64),
			pilosa.OptFieldForeignIndex("fip"),
		)
		// Set data on the parent so we have some index keys.
		c.Query(t, "fip", `
			Set("one", parent=1)
			Set("two", parent=2)
			Set("three", parent=3)
			Set("four", parent=4)
			Set("five", parent=5)
		`)
		// Set data on the child to align with the foreign index keys.
		c.Query(t, "fic", `
			Set(1, child="one")
			Set(2, child="one")
			Set(3, child="one")
			Set(4, child="three")
			Set(5, child="three")
			Set(6, child="five")
		`)

		t.Run("test foreign index with keys", func(t *testing.T) {
			// The execututor returns row IDs when the field has keys, but we
			// don't include them because they are not necessary in the result
			// comparison. Because of this, we use the CheckGroupByOnKey
			// function here to check equality only on the key field.
			expected := []pilosa.GroupCount{
				{Group: []pilosa.FieldRow{{Field: "child", RowKey: "one"}}, Count: 3},
				{Group: []pilosa.FieldRow{{Field: "child", RowKey: "three"}}, Count: 2},
				{Group: []pilosa.FieldRow{{Field: "child", RowKey: "five"}}, Count: 1},
			}

			results := c.Query(t, "fic", `GroupBy(Rows(child), sort="count desc")`).Results[0].(*pilosa.GroupCounts).Groups()
			test.CheckGroupByOnKey(t, expected, results)
		})

	}
	for _, size := range []int{1, 3} {
		t.Run(fmt.Sprintf("%d_nodes", size), func(t *testing.T) {
			groupByTest(t, size)
		})
	}
}

func BenchmarkGroupBy(b *testing.B) {
	c := test.MustNewCluster(b, 1)
	var err error
	c.GetIdleNode(0).Config.DataDir, err = testhook.TempDirInDir(b, *TempDir, "benchmarkGroupBy-")
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

// NOTE: The shift function in its current state is unsupported.
// If any of these tests fail due to improvements made to the roaring
// code, it is reasonable to remove these tests. See the `Shift()`
// method on `Row` in `row.go`.
func TestExecutor_Execute_Shift(t *testing.T) {
	t.Run("Shift Bit 0", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		hldr.SetBit("i", "general", 10, 0)

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Row(general=10), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{1}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Shift(Row(general=10), n=1), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{2}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("Shift container boundary", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		hldr.SetBit("i", "general", 10, 65535)

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Row(general=10), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{65536}) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
	})

	t.Run("Shift shard boundary", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)

		orig := []uint64{1, ShardWidth - 1, ShardWidth + 1}
		shift1 := []uint64{2, ShardWidth, ShardWidth + 2}
		shift2 := []uint64{3, ShardWidth + 1, ShardWidth + 3}

		for _, bit := range orig {
			hldr.SetBit("i", "general", 10, bit)
		}

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Row(general=10), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, shift1) {
			t.Fatalf("unexpected shift by 1: expected: %+v, but got: %+v", shift1, columns)
		}

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Row(general=10), n=2)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, shift2) {
			t.Fatalf("unexpected shift by 2: expected: %+v, but got: %+v", shift2, columns)
		}

		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Shift(Row(general=10)))`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, orig) {
			t.Fatalf("unexpected shift by 0: expected: %+v, but got: %+v", orig, columns)
		}
	})

	t.Run("Shift shard boundary no create", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		hldr.SetBit("i", "general", 10, ShardWidth-2) //shardwidth -1
		hldr.SetBit("i", "general", 10, ShardWidth-1) //shardwidth
		hldr.SetBit("i", "general", 10, ShardWidth)   //shardwidth +1
		hldr.SetBit("i", "general", 10, ShardWidth+2) //shardwidth +3

		exp := []uint64{ShardWidth - 1, ShardWidth, ShardWidth + 1, ShardWidth + 3}
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Row(general=10), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, exp) {
			t.Fatalf("unexpected columns: %+v", columns)
		}
		if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `Shift(Shift(Row(general=10), n=1), n=1)`}); err != nil {
			t.Fatal(err)
		} else if columns := res.Results[0].(*pilosa.Row).Columns(); !reflect.DeepEqual(columns, []uint64{ShardWidth, ShardWidth + 1, ShardWidth + 2, ShardWidth + 4}) {
			t.Fatalf("unexpected columns: \n%+v\n%+v", columns, exp)
		}
	})
}

func TestExecutor_Execute_IncludesColumn(t *testing.T) {
	t.Run("results-ids", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		hldr.SetBit("i", "general", 10, 1)
		hldr.SetBit("i", "general", 10, ShardWidth)
		hldr.SetBit("i", "general", 10, 2*ShardWidth)

		for i, tt := range []struct {
			col         uint64
			expIncluded bool
		}{
			{1, true},
			{2, false},
			{ShardWidth, true},
			{ShardWidth + 1, false},
			{2 * ShardWidth, true},
			{(2 * ShardWidth) + 1, false},
		} {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf("IncludesColumn(Row(general=10), column=%d)", tt.col)}); err != nil {
					t.Fatal(err)
				} else if tt.expIncluded && !res.Results[0].(bool) {
					t.Fatalf("expected to find column: %d", tt.col)
				} else if !tt.expIncluded && res.Results[0].(bool) {
					t.Fatalf("did not expect to find column: %d", tt.col)
				}
			})
		}
	})
	t.Run("results-keys", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		cmd := c.GetNode(0)
		hldr := c.GetHolder(0)
		index := hldr.MustCreateIndexIfNotExists("i", pilosa.IndexOptions{Keys: true})
		if _, err := index.CreateField("general", pilosa.OptFieldTypeDefault(), pilosa.OptFieldKeys()); err != nil {
			t.Fatal(err)
		}

		if _, err := cmd.API.Query(
			context.Background(),
			&pilosa.QueryRequest{
				Index: "i",
				Query: `Set("one", general="ten") Set("eleven", general="ten") Set("twentyone", general="ten")`,
			}); err != nil {
			t.Fatal(err)
		}

		for i, tt := range []struct {
			col         string
			expIncluded bool
		}{
			{"one", true},
			{"two", false},
			{"eleven", true},
			{"twelve", false},
			{"twentyone", true},
			{"twentytwo", false},
		} {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				if res, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: fmt.Sprintf("IncludesColumn(Row(general=ten), column=%s)", tt.col)}); err != nil {
					t.Fatal(err)
				} else if tt.expIncluded && !res.Results[0].(bool) {
					t.Fatalf("expected to find column: %s", tt.col)
				} else if !tt.expIncluded && res.Results[0].(bool) {
					t.Fatalf("did not expect to find column: %s", tt.col)
				}
			})
		}
	})
	t.Run("errors", func(t *testing.T) {
		c := test.MustRunCluster(t, 1)
		defer c.Close()
		hldr := c.GetHolder(0)
		hldr.SetBit("i", "general", 10, 1)

		t.Run("no column", func(t *testing.T) {
			expErr := "IncludesColumn call must specify a column"
			if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `IncludesColumn(Row(general=10))`}); err == nil {
				t.Fatalf("expected to get an error")
			} else if !strings.Contains(err.Error(), expErr) {
				t.Fatalf("expected error: %s, but got: %s", expErr, err.Error())
			}
		})

		t.Run("no row query", func(t *testing.T) {
			expErr := "IncludesColumn call must specify a row query"
			if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `IncludesColumn(column=1)`}); err == nil {
				t.Fatalf("expected to get an error")
			} else if !strings.Contains(err.Error(), expErr) {
				t.Fatalf("expected error: %s, but got: %s", expErr, err.Error())
			}
		})
	})
}

func TestExecutor_Execute_MinMaxCountEqual(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)

	idx, err := hldr.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("x", pilosa.OptFieldTypeDefault()); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("f", pilosa.OptFieldTypeInt(-1100, 1000)); err != nil {
		t.Fatal(err)
	}

	if _, err := idx.CreateField("dec", pilosa.OptFieldTypeDecimal(3)); err != nil {
		t.Fatal(err)
	}

	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: `
			Set(0, f=3)
			Set(1, f=3)
			Set(2, f=4)
			Set(3, f=5)
			Set(4, f=5)
			Set(` + strconv.Itoa(ShardWidth+1) + `, f=3)
			Set(` + strconv.Itoa(ShardWidth+2) + `, f=5)
			Set(` + strconv.Itoa(ShardWidth+3) + `, f=5)
			Set(` + strconv.Itoa(ShardWidth+4) + `, f=5)
			Set(` + strconv.Itoa(ShardWidth+5) + `, f=4)
			Set(` + strconv.Itoa(2*ShardWidth+1) + `, f=3)
			Set(0, x=3)
			Set(1, x=3)
            Set(0, dec=5.122)
            Set(1, dec=12.985)
            Set(2, dec=4.234)
            Set(3, dec=12.985)

		`}); err != nil {
		t.Fatal(err)
	}

	t.Run("Min", func(t *testing.T) {
		tests := []struct {
			filter string
			exp    int64
			cnt    int64
		}{
			{filter: ``, exp: 3, cnt: 4},
			{filter: `Row(x=3)`, exp: 3, cnt: 2},
		}
		for i, tt := range tests {
			var pql string
			if tt.filter == "" {
				pql = `Min(field=f)`
			} else {
				pql = fmt.Sprintf(`Min(%s, field=f)`, tt.filter)
			}
			if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: tt.exp, Count: tt.cnt}) {
				t.Fatalf("unexpected result, test %d: %s", i, spew.Sdump(result))
			}
		}
	})
	t.Run("MinDec", func(t *testing.T) {
		tests := []struct {
			filter string
			exp    pql.Decimal
			cnt    int64
		}{
			{filter: ``, exp: pql.NewDecimal(4234, 3), cnt: 1},
			{filter: `Row(x=3)`, exp: pql.NewDecimal(5122, 3), cnt: 1},
		}
		for i, tt := range tests {
			var pql string
			if tt.filter == "" {
				pql = `Min(field=dec)`
			} else {
				pql = fmt.Sprintf(`Min(%s, field=dec)`, tt.filter)
			}
			if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{DecimalVal: &tt.exp, Count: tt.cnt}) {
				t.Fatalf("unexpected result, test %d: %s", i, spew.Sdump(result.Results[0]))
			}
		}
	})

	t.Run("Max", func(t *testing.T) {
		tests := []struct {
			filter string
			exp    int64
			cnt    int64
		}{
			{filter: ``, exp: 5, cnt: 5},
		}
		for i, tt := range tests {
			var pql string
			if tt.filter == "" {
				pql = `Max(field=f)`
			} else {
				pql = fmt.Sprintf(`Max(%s, field=f)`, tt.filter)
			}
			if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{Val: tt.exp, Count: tt.cnt}) {
				t.Fatalf("unexpected result, test %d: %s", i, spew.Sdump(result))
			}
		}
	})

	t.Run("MaxDec", func(t *testing.T) {
		tests := []struct {
			filter string
			exp    pql.Decimal
			cnt    int64
		}{
			{filter: ``, exp: pql.NewDecimal(12985, 3), cnt: 2},
			{filter: `Row(x=3)`, exp: pql.NewDecimal(12985, 3), cnt: 1},
		}
		for i, tt := range tests {
			var pql string
			if tt.filter == "" {
				pql = `Max(field=dec)`
			} else {
				pql = fmt.Sprintf(`Max(%s, field=dec)`, tt.filter)
			}
			if result, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(result.Results[0], pilosa.ValCount{DecimalVal: &tt.exp, Count: tt.cnt}) {
				t.Fatalf("unexpected result, test %d: %s", i, spew.Sdump(result.Results[0]))
			}
		}
	})

	t.Run("MinMaxRangeError", func(t *testing.T) {
		// Min
		pql := `Set(4, dec=-92233720368547758.08)`
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err == nil {
			t.Fatalf("expected error but got: nil")
		} else if errors.Cause(err) != pilosa.ErrDecimalOutOfRange {
			t.Fatalf("expected error: %s, but got: %s", pilosa.ErrDecimalOutOfRange, err)
		}
		// Max
		pql = `Set(4, dec=92233720368547758.07)`
		if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{Index: "i", Query: pql}); err == nil {
			t.Fatalf("expected error but got: nil")
		} else if errors.Cause(err) != pilosa.ErrDecimalOutOfRange {
			t.Fatalf("expected error: %s, but got: %s", pilosa.ErrDecimalOutOfRange, err)
		}
	})
}

func TestExecutor_Execute_NoIndex(t *testing.T) {
	t.Helper()
	indexOptions := &pilosa.IndexOptions{}
	c := test.MustRunCluster(t, 1)
	defer c.Close()
	hldr := c.GetHolder(0)
	index := hldr.MustCreateIndexIfNotExists("i", *indexOptions)
	_, err := index.CreateField("f")
	if err != nil {
		t.Fatal("should work")
	}

	if _, err := c.GetNode(0).API.Query(context.Background(), &pilosa.QueryRequest{
		Index: "i",
		Query: "Count(Distinct(Row(gpu_tag='GTX'), index=systems, field=jarvis_id))",
	}); errors.Cause(err) != pilosa.ErrIndexNotFound {
		t.Fatal("expecting error: 'index systems does not exist'")
	}
}

func TestExecutor_Execute_CountDistinct(t *testing.T) {
	data, err := ioutil.ReadFile("testdata/schema.json")
	if err != nil {
		t.Fatal(err)
	}

	c := test.MustRunCluster(t, 1)

	defer c.Close()
	api := c.GetNode(0).API

	schema := &pilosa.Schema{}
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(schema); err != nil {
		t.Fatal(err)
	}

	if err := api.ApplySchema(context.TODO(), schema, false); err != nil {
		t.Fatal(err)
	}

	// AntitodePoint == row 1 b/c keys field.
	// Note: type=TwoPoints should match 100/101, and no row in type
	// matches 102, but 102 is present in equip_id at all.
	writeQuery := `
		Set(100, type=AntidotePoint)
		Set(100, type=TwoPoints)
		Set(101, type=TwoPoints)
		Set(100, equip_id=100)
		Set(101, equip_id=101)
		Set(102, equip_id=102)
		Set(100, site_id=100)
		Set(100, id=100)
	`
	for k, i := range schema.Indexes {
		_ = k
		if _, err := api.Query(context.TODO(), &pilosa.QueryRequest{Index: i.Name, Query: writeQuery}); err != nil {
			t.Fatal(err)
		}
	}

	// test query - Distinct of Distincts
	pql := `Distinct(
		Intersect(
			Distinct(
				Intersect(Row(type=AntidotePoint)),
			index=equipment, field=equip_id),
			Distinct(
				Intersect(Row(type=TwoPoints)),
			index=sites, field=equip_id)
		), index=power_ts, field=site_id)`

	// Check if test query gives correct results (one column 100)
	t.Run("Distinct", func(t *testing.T) {
		resp, err := api.Query(context.TODO(), &pilosa.QueryRequest{
			Index: "sites",
			Query: pql,
		})
		if err != nil {
			t.Fatal(err)
		}
		r, ok := resp.Results[0].(pilosa.SignedRow)
		if !ok {
			t.Fatalf("invalid response type, expected: pilosa.SignedRow, got: %T", resp.Results[0])
		}
		if r.Pos.Count() != 1 {
			t.Fatalf("invalid pilosa.SignedRow.Pos.Count, expected: 1, got: %v", r.Pos.Count())

		}
		if r.Pos.Columns()[0] != 100 {
			t.Fatalf("invalid pilosa.SignedRow.Pos.Columns, expected: [100], got: %v", r.Pos.Columns())
		}
	})

	// Following tests check if wrapping Distinct of Distincts query by Count and GroupBy
	// is fixed and does not give an error: 'unknown call: Distinct' error.

	// Check if Count on test query gives correct, exactly 1 result
	t.Run("Count(Distinct)", func(t *testing.T) {
		resp, err := api.Query(context.TODO(), &pilosa.QueryRequest{
			Index: "sites",
			Query: fmt.Sprintf("Count(%s)", pql),
		})
		if err != nil {
			t.Fatal(err)
		}
		cnt, ok := resp.Results[0].(uint64)
		if !ok {
			t.Fatalf("invalid response type, expected: uint64, got: %T", resp.Results[0])
		}
		if cnt != 1 {
			t.Fatalf("invalid result, expected: 1, got: %v", cnt)
		}
	})

	// Check if GroupBy on test query gives correct, exactly 1 result
	t.Run("GroupBy(Distinct)", func(t *testing.T) {
		resp, err := api.Query(context.TODO(), &pilosa.QueryRequest{
			Index: "sites",
			Query: fmt.Sprintf("GroupBy(Rows(type), filter=%s)", pql),
		})
		if err != nil {
			t.Fatal(err)
		}
		gcc, ok := resp.Results[0].(*pilosa.GroupCounts)
		if !ok {
			t.Fatalf("invalid response type, expected: []pilosa.GroupCount, got: %T", resp.Results[0])
		}
		gc := gcc.Groups()
		if len(gc) != 2 {
			t.Fatalf("invalid group count length, expected: 2, got: %v", len(gc))
		}
		if gc[0].Count != 1 {
			t.Fatalf("invalid group-by count for %d, expected: 1, got: %v", gc[0].Group[0].RowID, gc[0].Count)
		}
		if gc[1].Count != 1 {
			t.Fatalf("invalid group-by count for %d, expected: 1, got: %v", gc[1].Group[0].RowID, gc[1].Count)
		}
	})
	t.Run("Store(Distinct)", func(t *testing.T) {
		_, err = api.Query(context.TODO(), &pilosa.QueryRequest{
			Index: "sites",
			Query: `Store(Distinct(field=equip_id), type="a")`,
		})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := api.Query(context.TODO(), &pilosa.QueryRequest{
			Index: "sites",
			Query: `Row(type="a")`,
		})
		if err != nil {
			t.Fatal(err)
		}
		res, ok := resp.Results[0].(*pilosa.Row)
		if !ok {
			t.Fatalf("invalid response type, expected: *pilosa.Row, got: %T", resp.Results[0])
		}
		cols := res.Columns()
		if !eq(cols, []uint64{100, 101, 102}) {
			t.Fatalf("expected [100, 101, 102], got %d", cols)
		}

		_, err = api.Query(context.TODO(), &pilosa.QueryRequest{
			Index: "sites",
			Query: `Store(Distinct(Row(type="TwoPoints"), field=equip_id), type="b")`,
		})
		if err != nil {
			t.Fatal(err)
		}
		resp, err = api.Query(context.TODO(), &pilosa.QueryRequest{
			Index: "sites",
			Query: `Row(type="b")`,
		})
		if err != nil {
			t.Fatal(err)
		}
		res, ok = resp.Results[0].(*pilosa.Row)
		if !ok {
			t.Fatalf("invalid response type, expected: *pilosa.Row, got: %T", resp.Results[0])
		}
		cols = res.Columns()
		if !eq(cols, []uint64{100, 101}) {
			t.Fatalf("expected [100, 101], got %d", cols)
		}
	})
}

// Ensure that a top-level, bare distinct on multiple nodes
// is handled correctly.
func TestExecutor_BareDistinct(t *testing.T) {
	t.Helper()
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	c.CreateField(t, "i", pilosa.IndexOptions{}, "ints",
		pilosa.OptFieldTypeInt(0, math.MaxInt64),
	)
	c.CreateField(t, "i", pilosa.IndexOptions{}, "filter")

	// Populate integer data.
	c.Query(t, "i", fmt.Sprintf(`
			Set(0, ints=1)
			Set(%d, ints=2)
		`, ShardWidth))
	c.Query(t, "i", fmt.Sprintf(`
			 Set(0, filter=1)
			 Set(%d, filter=1)
	        `, 65537))

	for _, pql := range []string{
		`Distinct(field="ints")`,
		`Distinct(index="i", field="ints")`,
	} {
		exp := []uint64{1, 2}
		res := c.Query(t, "i", pql).Results[0].(pilosa.SignedRow)
		if got := res.Pos.Columns(); !reflect.DeepEqual(exp, got) {
			t.Fatalf("expected: %v, but got: %v", exp, got)
		}
	}
}

func TestExecutor_Execute_TopNDistinct(t *testing.T) {
	data, err := ioutil.ReadFile("testdata/schema.json")
	if err != nil {
		t.Fatal(err)
	}

	c := test.MustRunCluster(t, 1)

	defer c.Close()
	api := c.GetNode(0).API

	schema := &pilosa.Schema{}
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(schema); err != nil {
		t.Fatal(err)
	}
	if err := api.ApplySchema(context.TODO(), schema, false); err != nil {
		t.Fatal(err)
	}

	writeQuery := `Set(100, type=AntidotePoint)Set(100, equip_id=100)Set(100, site_id=100)Set(100, id=100)`
	for _, i := range schema.Indexes {
		if _, err := api.Query(context.TODO(), &pilosa.QueryRequest{Index: i.Name, Query: writeQuery}); err != nil {
			t.Fatal(err)
		}
	}

	pql := `TopN(type, Distinct(Row(type=AntidotePoint), index=power_ts, field=equip_id))`

	// Check if test query gives correct results (one column 100)
	t.Run("TopN", func(t *testing.T) {
		resp, err := api.Query(context.TODO(), &pilosa.QueryRequest{
			Index: "equipment",
			Query: pql,
		})
		if err != nil {
			t.Fatal(err)
		}
		pf, ok := resp.Results[0].(*pilosa.PairsField)
		if !ok {
			t.Fatalf("invalid response type, expected: *pilosa.PairsField, got: %T", resp.Results[0])
		}
		if len(pf.Pairs) != 1 {
			t.Fatalf("invalid Pairs length, expected: 1, got: %v", len(pf.Pairs))
		}
		if pf.Pairs[0].Count != 1 {
			t.Fatalf("invalid Pairs count, expected: 1, got: %v", pf.Pairs[0].Count)
		}
	})
}

func Test_Executor_Execute_UnionRows(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	c.CreateField(t, "i", pilosa.IndexOptions{}, "s",
		pilosa.OptFieldTypeSet(pilosa.CacheTypeRanked, 50000),
	)

	// Populate data.
	c.Query(t, "i", `
			Set(0, s=1)
			Set(1, s=2)
			Set(2, s=3)
			Set(3, s=1)
			Set(3, s=5)
		`)

	if res := c.Query(t, "i", `Count(UnionRows(TopN(s, n=1)))`); res.Results[0] != uint64(2) {
		t.Errorf("expected 2 columns, got %v", res.Results[0])
	}
	if res := c.Query(t, "i", `Count(UnionRows(Rows(s)))`); res.Results[0] != uint64(4) {
		t.Errorf("expected 4 columns, got %v", res.Results[0])
	}
}

func TestTimelessClearRegression(t *testing.T) {
	data, err := ioutil.ReadFile("testdata/timeRegressionSchema.json")
	if err != nil {
		t.Fatal(err)
	}

	c := test.MustRunCluster(t, 1)
	defer c.Close()

	api := c.GetNode(0).API

	schema := &pilosa.Schema{}
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(schema); err != nil {
		t.Fatal(err)
	}
	if err := api.ApplySchema(context.TODO(), schema, false); err != nil {
		t.Fatal(err)
	}

	idxName := schema.Indexes[0].Name

	setQuery := `Set(511, stargazer=376)`
	if _, err := api.Query(context.TODO(), &pilosa.QueryRequest{Index: idxName, Query: setQuery}); err != nil {
		t.Fatal(err)
	}

	setQuery = `Set(512, stargazer=300, 2017-05-18T00:00)`
	if _, err := api.Query(context.TODO(), &pilosa.QueryRequest{Index: idxName, Query: setQuery}); err != nil {
		t.Fatal(err)
	}

	clearQuery := `Clear(511, stargazer=376)`
	if res, err := api.Query(context.TODO(), &pilosa.QueryRequest{Index: idxName, Query: clearQuery}); err != nil {
		t.Fatal(err)
	} else if res.Results[0] != true {
		t.Fatal("clear supposedly failed")
	}
}

func TestMissingKeyRegression(t *testing.T) {
	c := test.MustRunCluster(t, 1, []server.CommandOption{server.OptCommandServerOptions(
		pilosa.OptServerStorageConfig(&storage.Config{
			Backend:      "roaring",
			FsyncEnabled: false,
		}))})
	defer c.Close()

	c.CreateField(t, "i", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "f", pilosa.OptFieldKeys())

	tests := []struct {
		name     string
		query    string
		expected []interface{}
	}{
		{
			name:     "RowGarbage",
			query:    `Row(f="garbage")`,
			expected: []interface{}{[]string(nil)},
		},
		{
			name:     "Set",
			query:    `Set("a", f="example")`,
			expected: []interface{}{true},
		},
		{
			name:     "Count",
			query:    `Count(Row(f="example"))`,
			expected: []interface{}{uint64(1)},
		},
		{
			name:     "NotGarbage",
			query:    `Not(Row(f="garbage"))`,
			expected: []interface{}{[]string{"a"}},
		},
		{
			name:     "DifferenceGarbage",
			query:    `Difference(All(), Row(f="garbage"))`,
			expected: []interface{}{[]string{"a"}},
		},
		/*{
			// Key translation works here, but it seems the actual count query is processing stale data.
			// Uncomment it when the bug has been fixed.
			name: "SetAndCount",
			query: `Set("a", f="example")` + "\n" +
				`Count(Row(f="example"))`,
			expected: []interface{}{true, uint64(1)},
		},*/
		{
			name:     "CountNothing",
			query:    `Count(Row(f="garbage"))`,
			expected: []interface{}{uint64(0)},
		},
		{
			name:     "StoreInvertSelf",
			query:    `Store(Not(Row(f="xyzzy")), f="xyzzy")`,
			expected: []interface{}{true},
		},
		{
			name: "SetClear",
			query: `Set("b", f="plugh")` + "\n" +
				`Clear("b", f="plugh")`,
			expected: []interface{}{true, true},
		},
		{
			name: "ClearMix",
			query: `Clear("a", f="garbage")` + "\n" +
				`Clear("a", f="example")`,
			expected: []interface{}{false, true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := c.Query(t, "i", tc.query)
			if len(resp.Results) != len(tc.expected) {
				t.Errorf("expected %d results but got %d", len(resp.Results), len(tc.expected))
				return
			}
			for i, r := range resp.Results {
				if row, ok := r.(*pilosa.Row); ok {
					r = row.Keys
				}
				expect := tc.expected[i]
				if !reflect.DeepEqual(r, expect) {
					t.Errorf("result %d differs: expected %v but got %v", i, expect, r)
				}
			}
		})
	}
}

// TestVariousQueries has originally been written to test out a
// variety of scenarios with Distinct, but it's structure is more
// general purpose. My vision is to eventually have any test which
// needs to test a single query be in here, and have a robust enough
// test data set loaded at the start which covers what we want to
// test.
//
// I'd also like to have it automatically run a matrix of scenarios
// (single and multi-node clusters, different endpoints for the
// queries (HTTP, GRPC, Postgres), etc.).
func TestVariousQueries(t *testing.T) {
	for _, clusterSize := range []int{1, 3, 4, 7} {
		clusterSize := clusterSize
		t.Run(fmt.Sprintf("%d-node", clusterSize), func(t *testing.T) {
			c := test.MustRunCluster(t, clusterSize)
			defer c.Close()

			variousQueries(t, c)
			variousQueriesOnTimeFields(t, c)
			variousQueriesOnPercentiles(t, c)
		})
	}
}

// tests for abbreviating time values in queries
func variousQueriesOnPercentiles(t *testing.T, c *test.Cluster) {
	// todo, make rand more random, 42 isnt the answer to everything
	// however, to make tests reproducible, seed should be printed
	// on failure?
	r := rand.New(rand.NewSource(42))

	// gen Numbers to test percentile query on, shuffle for extra spice
	// size should always be greater than 0
	type testValue struct {
		colKey string
		num    int64
		rowKey string
	}
	size := 100

	testValues := make([]testValue, size)
	rowKeys := [2]string{"foo", "bar"}
	for i := 0; i < size; i++ {
		num := int64(r.Uint32())
		// flip coin to negate
		if r.Uint64()%2 == 0 {
			num = -num
		}
		testValues[i] = testValue{
			colKey: fmt.Sprintf("user%d", i+1),
			num:    num,
			rowKey: rowKeys[r.Uint64()%2], // flip a coin
		}
	}

	// filter out nums that fulfil predicate
	var nums []int64
	for _, v := range testValues {
		if v.rowKey == "foo" {
			nums = append(nums, v.num)
		}
	}

	// get min and max for calculating both expected median
	// and bounds for bsi field
	// get min & max

	// helper function for calculating percentiles to
	// cross-check with Pilosa's results
	getExpectedPercentile := func(nums []int64, nth float64) int64 {
		min, max := nums[0], nums[0]
		for _, num := range nums {
			if num < min {
				min = num
			}
			if num > max {
				max = num
			}
		}
		if nth == 0.0 {
			return min
		}
		k := (100 - nth) / nth

		possibleNthVal := int64(0)
		// bin search
		for min < max {
			possibleNthVal = ((max / 2) + (min / 2)) + (((max % 2) + (min % 2)) / 2)
			leftCount, rightCount := int64(0), int64(0)
			for _, num := range nums {
				if num < possibleNthVal {
					leftCount++
				} else if num > possibleNthVal {
					rightCount++
				}
			}

			leftCountWeighted := int64(math.Round(k * float64(leftCount)))

			if leftCountWeighted > rightCount {
				max = possibleNthVal - 1
			} else if leftCountWeighted < rightCount {
				min = possibleNthVal + 1
			} else { // perfectly balanced, as all things should be
				return possibleNthVal
			}
		}
		return min
	}

	// generate numeric entries for index
	intEntries := make([]test.IntKey, size)
	for i := 0; i < size; i++ {
		key := testValues[i].colKey
		val := testValues[i].num
		intEntries[i] = test.IntKey{Key: key, Val: val}
	}

	// generate string-set entries for index
	var stringEntries [][2]string
	for _, v := range testValues {
		stringEntries = append(stringEntries,
			[2]string{v.rowKey, v.colKey})
	}

	// get min max for bsi bounds
	min, max := testValues[0].num, testValues[0].num
	for _, v := range testValues {
		if v.num < min {
			min = v.num
		}
		if v.num > max {
			max = v.num
		}
	}

	// generic index
	c.CreateField(t, "users2", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "net_worth", pilosa.OptFieldTypeInt(min, max))
	c.ImportIntKey(t, "users2", "net_worth", intEntries)

	c.CreateField(t, "users2", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "val", pilosa.OptFieldKeys())
	c.ImportKeyKey(t, "users2", "val", stringEntries)

	splitSortBackToCSV := func(csvStr string) string {
		ss := strings.Split(csvStr[:len(csvStr)-1], "\n")
		sort.Strings(ss)
		return strings.Join(ss, "\n") + "\n"
	}

	type testCase struct {
		query string
		// qrVerifier  func(t *testing.T, resp pilosa.QueryResponse)
		csvVerifier string
	}

	// generate test cases per each nth argument
	nthsFloat := []float64{0, 10, 25, 50, 75, 90, 99}
	var tests []testCase
	for _, nth := range nthsFloat {
		query := fmt.Sprintf(`Percentile(field="net_worth", filter=Row(val="foo"), nth=%f)`, nth)
		expectedPercentile := getExpectedPercentile(nums, nth)
		tests = append(tests, testCase{
			query:       query,
			csvVerifier: fmt.Sprintf("%d,1\n", expectedPercentile),
		})
	}
	nthsInt := []int64{0, 10, 100}
	for _, nth := range nthsInt {
		query := fmt.Sprintf(`Percentile(field="net_worth", filter=Row(val="foo"), nth=%d)`, nth)
		expectedPercentile := getExpectedPercentile(nums, float64(nth))
		tests = append(tests, testCase{
			query:       query,
			csvVerifier: fmt.Sprintf("%d,1\n", expectedPercentile),
		})
		query2 := fmt.Sprintf(`Percentile(field=net_worth, filter=Row(val="foo"), nth=%d)`, nth)
		tests = append(tests, testCase{
			query:       query2,
			csvVerifier: fmt.Sprintf("%d,1\n", expectedPercentile),
		})
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d-%s", i, tst.query), func(t *testing.T) {
			// resp := c.Query(t, "users2", tst.query)
			tr := c.QueryGRPC(t, "users2", tst.query)
			// if tst.qrVerifier != nil {
			// 	tst.qrVerifier(t, resp)
			// }
			csvString, err := tableResponseToCSVString(tr)
			if err != nil {
				t.Fatal(err)
			}
			// verify everything after header
			got := splitSortBackToCSV(csvString[strings.Index(csvString, "\n")+1:])
			if got != tst.csvVerifier {
				t.Errorf("expected:\n%s\ngot:\n%s", tst.csvVerifier, got)
			}

			// TODO: add HTTP and Postgres and ability to convert
			// those results to CSV to run through CSV verifier
		})
	}
}

// tests for abbreviating time values in queries
func variousQueriesOnTimeFields(t *testing.T, c *test.Cluster) {
	ts := func(t time.Time) int64 {
		return t.Unix() * 1e+9
	}

	// generic index
	// worth noting, since we are using YMDH resolution, both C4 & C5
	// get binned to the same hour
	c.CreateField(t, "t_index", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "f1", pilosa.OptFieldKeys(), pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))
	c.ImportTimeQuantumKey(t, "t_index", "f1", []test.TimeQuantumKey{
		// from edge cases
		{ColKey: "C1", RowKey: "R1", Ts: ts(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC))},
		{ColKey: "C2", RowKey: "R2", Ts: ts(time.Date(2019, 8, 1, 0, 0, 0, 0, time.UTC))},
		{ColKey: "C3", RowKey: "R3", Ts: ts(time.Date(2019, 8, 4, 0, 0, 0, 0, time.UTC))},
		{ColKey: "C4", RowKey: "R4", Ts: ts(time.Date(2019, 8, 4, 14, 0, 0, 0, time.UTC))},
		{ColKey: "C5", RowKey: "R5", Ts: ts(time.Date(2019, 8, 4, 14, 36, 0, 0, time.UTC))},
		// to edge cases
		{ColKey: "C6", RowKey: "R6", Ts: ts(time.Date(2019, 8, 4, 16, 0, 0, 0, time.UTC))},
		{ColKey: "C7", RowKey: "R7", Ts: ts(time.Date(2019, 8, 5, 0, 0, 0, 0, time.UTC))},
		{ColKey: "C8", RowKey: "R8", Ts: ts(time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC))},
		{ColKey: "C9", RowKey: "R9", Ts: ts(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))},
	})

	// in this field, all columns have the same row value to simplify test queries for Row
	c.CreateField(t, "t_index", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "f2", pilosa.OptFieldKeys(), pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YMDH")))
	c.ImportTimeQuantumKey(t, "t_index", "f2", []test.TimeQuantumKey{
		// from
		{ColKey: "C1", RowKey: "R", Ts: ts(time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC))},
		{ColKey: "C2", RowKey: "R", Ts: ts(time.Date(2019, 8, 1, 0, 0, 0, 0, time.UTC))},
		{ColKey: "C3", RowKey: "R", Ts: ts(time.Date(2019, 8, 4, 0, 0, 0, 0, time.UTC))},
		{ColKey: "C4", RowKey: "R", Ts: ts(time.Date(2019, 8, 4, 14, 0, 0, 0, time.UTC))},
		{ColKey: "C5", RowKey: "R", Ts: ts(time.Date(2019, 8, 4, 14, 36, 0, 0, time.UTC))},
		// to
		{ColKey: "C6", RowKey: "R", Ts: ts(time.Date(2019, 8, 4, 16, 0, 0, 0, time.UTC))},
		{ColKey: "C7", RowKey: "R", Ts: ts(time.Date(2019, 8, 5, 0, 0, 0, 0, time.UTC))},
		{ColKey: "C8", RowKey: "R", Ts: ts(time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC))},
		{ColKey: "C9", RowKey: "R", Ts: ts(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))},
	})

	splitSortBackToCSV := func(csvStr string) string {
		ss := strings.Split(csvStr[:len(csvStr)-1], "\n")
		sort.Strings(ss)
		return strings.Join(ss, "\n") + "\n"
	}

	toCSV := func(s string) string {
		return strings.Join(strings.Split(s, " "), "\n") + "\n"
	}

	type testCase struct {
		query       string
		qrVerifier  func(t *testing.T, resp pilosa.QueryResponse)
		csvVerifier string
	}

	tests := []testCase{
		// Rows
		{
			query:       `Rows(f1, from='2019-08-04T14:36', to='2019-08-04T16:00')`,
			csvVerifier: toCSV("R4 R5"),
		},
		{
			query:       `Rows(f1, from='2019-08-04T14', to='2019-08-04T17:00')`,
			csvVerifier: toCSV("R4 R5 R6"),
		},
		{
			query:       `Rows(f1, from='2019-08-04', to='2019-08-05')`,
			csvVerifier: toCSV("R3 R4 R5 R6"),
		},
		{
			query:       `Rows(f1, from='2019-08', to='2019-12')`,
			csvVerifier: toCSV("R2 R3 R4 R5 R6 R7"),
		},
		{
			query:       `Rows(f1, from='2019', to='2020')`,
			csvVerifier: toCSV("R1 R2 R3 R4 R5 R6 R7 R8"),
		},
		// Row
		{
			query:       `Row(f2='R', from='2019-08-04T14:36', to='2019-08-04T16:00')`,
			csvVerifier: toCSV("C4 C5"),
		},
		{
			query:       `Row(f2='R', from='2019-08-04T14', to='2019-08-04T17:00')`,
			csvVerifier: toCSV("C4 C5 C6"),
		},
		{
			query:       `Row(f2='R', from='2019-08-04', to='2019-08-05')`,
			csvVerifier: toCSV("C3 C4 C5 C6"),
		},
		{
			query:       `Row(f2='R', from='2019-08', to='2019-12')`,
			csvVerifier: toCSV("C2 C3 C4 C5 C6 C7"),
		},
		{
			query:       `Row(f2='R', from='2019', to='2020')`,
			csvVerifier: toCSV("C1 C2 C3 C4 C5 C6 C7 C8"),
		},
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d-%s", i, tst.query), func(t *testing.T) {
			resp := c.Query(t, "t_index", tst.query)
			tr := c.QueryGRPC(t, "t_index", tst.query)
			if tst.qrVerifier != nil {
				tst.qrVerifier(t, resp)
			}
			csvString, err := tableResponseToCSVString(tr)
			if err != nil {
				t.Fatal(err)
			}
			// verify everything after header
			got := splitSortBackToCSV(csvString[strings.Index(csvString, "\n")+1:])
			if got != tst.csvVerifier {
				t.Errorf("expected:\n%s\ngot:\n%s", tst.csvVerifier, got)
			}

			// TODO: add HTTP and Postgres and ability to convert
			// those results to CSV to run through CSV verifier
		})
	}
}

func variousQueries(t *testing.T, c *test.Cluster) {
	// Create and populate "likenums" similar to "likes", but without keys on the field.
	c.CreateField(t, "users", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "likenums")
	c.ImportIDKey(t, "users", "likenums", []test.KeyID{
		{ID: 1, Key: "userA"},
		{ID: 2, Key: "userB"},
		{ID: 3, Key: "userC"},
		{ID: 4, Key: "userD"},
		{ID: 5, Key: "userE"},
		{ID: 6, Key: "userF"},
		{ID: 7, Key: "userA"},
		{ID: 7, Key: "userB"},
		{ID: 7, Key: "userC"},
		{ID: 7, Key: "userD"},
		// we intentionally leave user E out because then there is no
		// data for userE's shard for this field, which triggered a
		// "fragment not found" problem
		{ID: 7, Key: "userF"},
	})

	// Create and populate "likes" field.
	c.CreateField(t, "users", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "likes", pilosa.OptFieldKeys())
	c.ImportKeyKey(t, "users", "likes", [][2]string{
		{"molecula", "userA"},
		{"pilosa", "userB"},
		{"pangolin", "userC"},
		{"zebra", "userD"},
		{"toucan", "userE"},
		{"dog", "userF"},
		{"icecream", "userA"},
		{"icecream", "userB"},
		{"icecream", "userC"},
		{"icecream", "userD"},
		{"icecream", "userE"},
		{"icecream", "userF"},
	})

	// Create and populate "dinner" field.
	c.CreateField(t, "users", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "dinner", pilosa.OptFieldKeys())
	c.ImportKeyKey(t, "users", "dinner", [][2]string{
		{"leftovers", "userB"},
		{"pizza", "userA"},
		{"pizza", "userB"},
		{"chinese", "userA"},
		{"chinese", "userB"},
		{"chinese", "userF"},
	})

	// Create and populate "places_visited" time field.
	c.CreateField(t, "users", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "places_visited", pilosa.OptFieldKeys(), pilosa.OptFieldTypeTime(pilosa.TimeQuantum("YM")))
	ts2019Jan01 := int64(1546300800) * 1e+9 // 2019 January 1st 0:00:00
	ts2019Aug01 := int64(1564617600) * 1e+9 // 2019 August  1st 0:00:00
	ts2020Jan01 := int64(1577836800) * 1e+9 // 2020 January 1st 0:00:00
	c.ImportTimeQuantumKey(t, "users", "places_visited", []test.TimeQuantumKey{
		// 2019 January: nairobi, paris, austin, toronto
		{RowKey: "nairobi", ColKey: "userB", Ts: ts2019Jan01},
		{RowKey: "paris", ColKey: "userC", Ts: ts2019Jan01},
		{RowKey: "austin", ColKey: "userF", Ts: ts2019Jan01},
		{RowKey: "toronto", ColKey: "userA", Ts: ts2019Jan01},
		// 2019 August: toronto only
		{RowKey: "toronto", ColKey: "userB", Ts: ts2019Aug01},
		{RowKey: "toronto", ColKey: "userC", Ts: ts2019Aug01},
		// 2020: toronto, mombasa, sydney, nairobi
		{RowKey: "toronto", ColKey: "userB", Ts: ts2020Jan01},
		{RowKey: "toronto", ColKey: "userD", Ts: ts2020Jan01},
		{RowKey: "toronto", ColKey: "userE", Ts: ts2020Jan01},
		{RowKey: "toronto", ColKey: "userF", Ts: ts2020Jan01},
		{RowKey: "mombasa", ColKey: "userA", Ts: ts2020Jan01},
		{RowKey: "sydney", ColKey: "userD", Ts: ts2020Jan01},
		{RowKey: "nairobi", ColKey: "userE", Ts: ts2020Jan01},
	})

	// Create and populate "affinity" int field with negative, positive, zero and null values.
	c.CreateField(t, "users", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "affinity", pilosa.OptFieldTypeInt(-1000, 1000))
	c.ImportIntKey(t, "users", "affinity", []test.IntKey{
		{Val: 10, Key: "userA"},
		{Val: -10, Key: "userB"},
		{Val: 5, Key: "userC"},
		{Val: -5, Key: "userD"},
		{Val: 0, Key: "userE"},
	})

	// Create and populate "net_worth" int field with positive values.
	c.CreateField(t, "users", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "net_worth", pilosa.OptFieldTypeInt(-100000000, 100000000))
	c.ImportIntKey(t, "users", "net_worth", []test.IntKey{
		{Val: 1, Key: "userA"},
		{Val: 10, Key: "userB"},
		{Val: 100, Key: "userC"},
		{Val: 1000, Key: "userD"},
		{Val: 10000, Key: "userE"},
		{Val: 100000, Key: "userF"},
	})

	c.CreateField(t, "users", pilosa.IndexOptions{Keys: true, TrackExistence: true}, "zip_code", pilosa.OptFieldTypeInt(0, 100000))
	c.ImportIntKey(t, "users", "zip_code", []test.IntKey{
		{Val: 78739, Key: "userA"},
		{Val: 78739, Key: "userB"},
		{Val: 19707, Key: "userC"},
		{Val: 19707, Key: "userD"},
		{Val: 86753, Key: "userE"},
		{Val: 78739, Key: "userG"},
	})

	tests := []struct {
		query       string
		qrVerifier  func(t *testing.T, resp pilosa.QueryResponse)
		csvVerifier string
	}{
		{ // 2020 & 2019 All
			query: `GroupBy(Rows(places_visited, from='2019-01-01T00:00', to='2020-12-31T23:59'))`,
			csvVerifier: `nairobi,2
paris,1
austin,1
toronto,6
mombasa,1
sydney,1
`,
		},
		{ // 2019 January only
			query: `GroupBy(Rows(places_visited, from='2019-01-01T00:00', to='2019-02-01T00:00'))`,
			csvVerifier: `nairobi,1
paris,1
austin,1
toronto,1
`,
		},
		{ // 2019 All
			query: `GroupBy(Rows(places_visited, from='2019-01-01T00:00', to='2019-12-31T23:59'))`,
			csvVerifier: `nairobi,1
paris,1
austin,1
toronto,3
`,
		},
		{ // 2019 All, this excludes userC (who likes pangolin & icecream) from the count.
			// UserC visited Paris and Toronto in 2019
			query: `GroupBy(
					Rows(places_visited, from='2019-01-01T00:00', to='2019-12-31T23:59'),
					filter=Not(Intersect(Row(likes='pangolin'), Row(likes='icecream')))
				)`,
			csvVerifier: `nairobi,1
austin,1
toronto,2
`,
		},
		{ // After excluding UserC, this gets the sum of the networth of everyone per cities travelled
			query: `GroupBy(
					Rows(places_visited, from='2019-01-01T00:00', to='2019-12-31T23:59'),
					filter=Not(Intersect(Row(likes='pangolin'), Row(likes='icecream'))),
					aggregate=Sum(field=net_worth)
				)`,
			csvVerifier: `nairobi,1,10
austin,1,100000
toronto,2,11
`,
		},
		{ // 2020 & 2019 All
			query:       `Rows(places_visited, from='2019-01-01T00:00', to='2020-12-31T23:59')`,
			csvVerifier: "nairobi\nparis\naustin\ntoronto\nmombasa\nsydney\n",
		},
		{ // 2019 All
			query:       `Rows(places_visited, from='2019-01-01T00:00', to='2019-12-31T23:59')`,
			csvVerifier: "nairobi\nparis\naustin\ntoronto\n",
		},
		{ // 2019 January only
			query:       `Rows(places_visited, from='2019-01-01T00:00', to='2019-02-01T00:00')`,
			csvVerifier: "nairobi\nparis\naustin\ntoronto\n",
		},
		{
			query: "Count(All())",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if resp.Results[0].(uint64) != 7 {
					t.Errorf("expected 7, got %+v", resp.Results[0])
				}
			},
			csvVerifier: "7\n",
		},
		{
			query: "Count(Distinct(field=likenums))",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if resp.Results[0].(uint64) != 7 {
					t.Errorf("wrong count: %+v", resp.Results[0])
				}
			},
			csvVerifier: "7\n",
		},
		{
			query: "Distinct(field=likenums)",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Columns(), []uint64{1, 2, 3, 4, 5, 6, 7}) {
					t.Errorf("wrong values: %+v %+v", resp.Results[0].(*pilosa.Row).Columns(), resp.Results[0].(*pilosa.Row))
				}
			},
			csvVerifier: "1\n2\n3\n4\n5\n6\n7\n",
		},
		{
			query: "Count(Distinct(field=likes))",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if resp.Results[0].(uint64) != 7 {
					t.Errorf("wrong count: %+v", resp.Results[0])
				}
			},
			csvVerifier: "7\n",
		},
		{
			query: "Distinct(field=affinity)",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if !reflect.DeepEqual(resp.Results[0].(pilosa.SignedRow).Pos.Columns(), []uint64{0, 5, 10}) {
					t.Errorf("wrong positive records: %+v", resp.Results[0].(pilosa.SignedRow).Pos.Columns())
				}
				if !reflect.DeepEqual(resp.Results[0].(pilosa.SignedRow).Neg.Columns(), []uint64{5, 10}) {
					t.Errorf("wrong negative records: %+v", resp.Results[0].(pilosa.SignedRow).Neg.Columns())
				}
			},
			csvVerifier: "-10\n-5\n0\n5\n10\n",
		},
		{
			query: "Count(Distinct(field=affinity))",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if resp.Results[0].(uint64) != 5 {
					t.Errorf("wrong number of values: %+v", resp.Results[0])
				}
			},
			csvVerifier: "5\n",
		},
		{
			query: "Distinct(Row(affinity>=0),field=affinity)",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if !reflect.DeepEqual(resp.Results[0].(pilosa.SignedRow).Pos.Columns(), []uint64{0, 5, 10}) {
					t.Errorf("wrong positive records: %+v", resp.Results[0].(pilosa.SignedRow).Pos.Columns())
				}
				if !reflect.DeepEqual(resp.Results[0].(pilosa.SignedRow).Neg.Columns(), []uint64{}) {
					t.Errorf("wrong negative records: %+v", resp.Results[0].(pilosa.SignedRow).Neg.Columns())
				}
			},
			csvVerifier: "0\n5\n10\n",
		},
		{
			query: "Count(Distinct(Row(affinity>=0),field=affinity))",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if resp.Results[0].(uint64) != 3 {
					t.Errorf("wrong number of values: %+v", resp.Results[0])
				}
			},
			csvVerifier: "3\n",
		},

		// Handling this case properly will require changing the way
		// that precomputed data is stored on Call objects. Currently
		// if a Distinct is at all nested (e.g. within a Count) it
		// gets handled by executor.handlePreCalls which assumes that
		// only the positive values are worthwhile.
		//
		// {
		// 	query: "Count(Distinct(field=affinity))",
		// 	verifier: func(t *testing.T, resp pilosa.QueryResponse) {
		// 		if resp.Results[0].(uint64) != 5 {
		// 			t.Errorf("wrong number of values: %+v", resp.Results[0])
		// 		}
		// 	},
		// },
		{
			query: "Distinct(Row(affinity<0),field=likes)",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Keys, []string{"pilosa", "zebra", "icecream"}) {
					t.Errorf("wrong values: %+v", resp.Results[0])
				}
			},
			csvVerifier: "pilosa\nzebra\nicecream\n",
		},
		{
			query: "Distinct(Row(affinity>0),field=likes)",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Keys, []string{"molecula", "pangolin", "icecream"}) {
					t.Errorf("wrong values: %+v", resp.Results[0])
				}
			},
			csvVerifier: "molecula\npangolin\nicecream\n",
		},
		{
			query: "Distinct(Row(likenums=1),field=likes)",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Keys, []string{"molecula", "icecream"}) {
					t.Errorf("wrong values: %+v", resp.Results[0])
				}
			},
			csvVerifier: "molecula\nicecream\n",
		},
		{
			query: "Distinct(field=likes)",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Keys, []string{"molecula", "pilosa", "pangolin", "zebra", "toucan", "dog", "icecream"}) {
					t.Errorf("wrong values: %+v", resp.Results[0])
				}
			},
			csvVerifier: "molecula\npilosa\npangolin\nzebra\ntoucan\ndog\nicecream\n",
		},
		{
			query: "Distinct(All(),field=likes)",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Keys, []string{"molecula", "pilosa", "pangolin", "zebra", "toucan", "dog", "icecream"}) {
					t.Errorf("wrong values: %+v", resp.Results[0])
				}
			},
			csvVerifier: "molecula\npilosa\npangolin\nzebra\ntoucan\ndog\nicecream\n",
		},
		{
			query: "Distinct(field=likes )",
			qrVerifier: func(t *testing.T, resp pilosa.QueryResponse) {
				if !reflect.DeepEqual(resp.Results[0].(*pilosa.Row).Keys, []string{"molecula", "pilosa", "pangolin", "zebra", "toucan", "dog", "icecream"}) {
					t.Errorf("wrong values: %+v", resp.Results[0])
				}
			},
			csvVerifier: "molecula\npilosa\npangolin\nzebra\ntoucan\ndog\nicecream\n",
		},
		{
			query: "GroupBy(Rows(field=likes))",
			csvVerifier: `molecula,1
pilosa,1
pangolin,1
zebra,1
toucan,1
dog,1
icecream,6
`,
		},
		{
			query: "GroupBy(Rows(field=likes), aggregate=Sum(field=net_worth), limit=2, having=Condition(sum>10))",
			csvVerifier: `pangolin,1,100
zebra,1,1000
`,
		},
		{
			query:       "GroupBy(Rows(field=likes), having=Condition(count>5))",
			csvVerifier: "icecream,6\n",
		},
		{
			query: "GroupBy(Rows(field=likes), filter=Row(affinity>-7))",
			csvVerifier: `molecula,1
pangolin,1
zebra,1
toucan,1
icecream,4
`,
		},
		{
			query: "GroupBy(Rows(field=likes), aggregate=Count(Distinct(field=zip_code)))",
			csvVerifier: `molecula,1,1
pilosa,1,1
pangolin,1,1
zebra,1,1
toucan,1,1
dog,1,0
icecream,6,3
`,
		},
		{
			query:       "GroupBy(Rows(field=likes), aggregate=Count(Distinct(field=zip_code)), having=Condition(sum>2))",
			csvVerifier: "icecream,6,3\n",
		},
		{
			query: "GroupBy(Rows(field=likes), filter=Row(affinity>-11), aggregate=Count(Distinct(field=zip_code)))",
			csvVerifier: `molecula,1,1
pilosa,1,1
pangolin,1,1
zebra,1,1
toucan,1,1
icecream,5,3
`,
		},
		{
			query: "GroupBy(Rows(field=likes), filter=Row(affinity>-11), aggregate=Count(Distinct(Row(affinity>-7), field=zip_code)))",
			csvVerifier: `molecula,1,1
pilosa,1,0
pangolin,1,1
zebra,1,1
toucan,1,1
icecream,5,3
`,
		},
		{
			query: "GroupBy(Rows(field=likes), sort=\"count desc\")",
			csvVerifier: `icecream,6
molecula,1
pilosa,1
pangolin,1
zebra,1
toucan,1
dog,1
`,
		},
		{
			query: "GroupBy(Rows(field=likes), aggregate=Sum(field=net_worth), sort=\"aggregate desc, count asc\")",
			csvVerifier: `icecream,6,111111
dog,1,100000
toucan,1,10000
zebra,1,1000
pangolin,1,100
pilosa,1,10
molecula,1,1
`,
		},
		{
			query: "GroupBy(Rows(field=likes), aggregate=Sum(field=net_worth), sort=\"aggregate desc, count asc\", limit=3)",
			csvVerifier: `icecream,6,111111
dog,1,100000
toucan,1,10000
`,
		},
		{
			query: "GroupBy(Rows(field=likes), aggregate=Sum(field=net_worth),sort=\"aggregate desc, count asc\",limit=3,offset=2)",
			csvVerifier: `toucan,1,10000
zebra,1,1000
pangolin,1,100
`,
		},
		{
			query: "GroupBy(Rows(field=affinity), aggregate=Count(Distinct(field=zip_code)))",
			csvVerifier: `-10,1,1
-5,1,1
0,1,1
5,1,1
10,1,1
`,
		},
		{
			query: "GroupBy(Rows(field=dinner), sort=\"count desc\", limit=2)",
			csvVerifier: `chinese,3
pizza,2
`,
		},
		{
			query: "TopK(dinner)",
			csvVerifier: `chinese,3
pizza,2
leftovers,1
`,
		},
		{
			query: "TopK(field=dinner)",
			csvVerifier: `chinese,3
pizza,2
leftovers,1
`,
		},
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d-%s", i, tst.query), func(t *testing.T) {
			resp := c.Query(t, "users", tst.query)
			tr := c.QueryGRPC(t, "users", tst.query)
			if tst.qrVerifier != nil {
				tst.qrVerifier(t, resp)
			}
			csvString, err := tableResponseToCSVString(tr)
			if err != nil {
				t.Fatal(err)
			}
			// verify everything after header
			got := csvString[strings.Index(csvString, "\n")+1:]
			if got != tst.csvVerifier {
				t.Errorf("expected:\n%s\ngot:\n%s", tst.csvVerifier, got)
			}

			// TODO: add HTTP and Postgres and ability to convert
			// those results to CSV to run through CSV verifier
		})
	}
}

// TestVariousSingleShardQueries tests queries on a dataset which
// consists of an unkeyed index, and data only in the first
// shard. Turns out that there are some interesting failure modes
// which only crop up with one shard. An example is that the mapReduce
// logic does its first reduce call with a nil interface{} value, and
// an actual result value. If this is the only reduce that gets done
// (because there's only one shard), the result might never go through
// normal merge/reduction logic and might e.g. be nil instead of an
// empty struct resulting in an NPE later on.
func TestVariousSingleShardQueries(t *testing.T) {
	for _, clusterSize := range []int{1, 4} {
		t.Run(fmt.Sprintf("%d-node", clusterSize), func(t *testing.T) {
			variousSingleShardQueries(t, clusterSize)
		})
	}
}

func variousSingleShardQueries(t *testing.T, clusterSize int) {
	c := test.MustRunCluster(t, clusterSize)
	defer c.Close()

	// Create and populate "likenums" similar to "likes", but without keys on the field.
	c.CreateField(t, "events", pilosa.IndexOptions{Keys: false, TrackExistence: true}, "lostcount", pilosa.OptFieldTypeInt(0, 1000000000))
	c.ImportIntID(t, "events", "lostcount", []test.IntID{
		{Val: 0, ID: 1},
		{Val: 1, ID: 2},
		{Val: 0, ID: 3},
		{Val: 2, ID: 4},
		{Val: 2, ID: 5},
		{Val: 0, ID: 6},
		{Val: 3, ID: 7},
		{Val: 3, ID: 8},
		{Val: 3, ID: 9},
		{Val: 0, ID: 10},
	})

	c.CreateField(t, "events", pilosa.IndexOptions{Keys: false, TrackExistence: true}, "jittermax", pilosa.OptFieldTypeInt(0, 1000000000))
	c.ImportIntID(t, "events", "jittermax", []test.IntID{
		{Val: 17, ID: 1},
		{Val: 3, ID: 2},
		{Val: 42, ID: 3},
		{Val: 9, ID: 4},
		{Val: 17, ID: 5},
		{Val: 3, ID: 6},
		{Val: 42, ID: 7},
		{Val: 9, ID: 8},
		{Val: 17, ID: 9},
		{Val: 3, ID: 10},
	})

	tests := []struct {
		query       string
		csvVerifier string
	}{
		{
			query: "GroupBy(Rows(lostcount), aggregate=Count(Distinct(field=jittermax)))",
			csvVerifier: `0,4,3
1,1,1
2,2,2
3,3,3
`,
		},
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d-%s", i, tst.query), func(t *testing.T) {
			tr := c.QueryGRPC(t, "events", tst.query)
			csvString, err := tableResponseToCSVString(tr)
			if err != nil {
				t.Fatal(err)
			}
			// verify everything after header
			got := csvString[strings.Index(csvString, "\n")+1:]
			if got != tst.csvVerifier {
				t.Errorf("expected:\n%s\ngot:\n%s", tst.csvVerifier, got)
			}

		})
	}

}

// tableResponseToCSV converts a generic TableResponse to a CSV format
// and writes it to the writer.
func tableResponseToCSV(m *proto.TableResponse, w io.Writer) error {
	writer := csv.NewWriter(w)
	record := make([]string, len(m.Headers))
	for i, h := range m.Headers {
		record[i] = h.Name
	}
	err := writer.Write(record)
	if err != nil {
		return errors.Wrap(err, "writing header")
	}
	for i, row := range m.Rows {
		record = record[:0]
		for colIndex, col := range row.Columns {
			switch m.Headers[colIndex].Datatype {
			case "[]string":
				record = append(record, fmt.Sprintf("%v", col.GetStringArrayVal()))
			case "[]uint64":
				record = append(record, fmt.Sprintf("%v", col.GetUint64ArrayVal()))
			case "string":
				record = append(record, fmt.Sprintf("%v", col.GetStringVal()))
			case "uint64":
				record = append(record, fmt.Sprintf("%v", col.GetUint64Val()))
			case "decimal":
				record = append(record, fmt.Sprintf("%v", col.GetDecimalVal().String()))
			case "bool":
				record = append(record, fmt.Sprintf("%v", col.GetBoolVal()))
			case "int64":
				record = append(record, fmt.Sprintf("%v", col.GetInt64Val()))
			}
		}
		err := writer.Write(record)
		if err != nil {
			return errors.Wrapf(err, "writing row %d", i)
		}
	}
	writer.Flush()
	return errors.Wrap(writer.Error(), "writing or flushing CSV")
}

// tableResponseToCSVString converts a generic TableResponse to a CSV format
// and returns it as a string.
func tableResponseToCSVString(m *proto.TableResponse) (string, error) {
	buf := &bytes.Buffer{}
	err := tableResponseToCSV(m, buf)
	if err != nil {
		return "", errors.Wrap(err, "writing tableResponse CSV to bytes.Buffer")
	}
	return buf.String(), nil
}

var dbDSN string

func init() {
	flag.StringVar(&dbDSN, "externalLookupDSN", "", "SQL DSN to use for external database access")
}

func TestExternalLookup(t *testing.T) {
	// Set up access to a SQL database.
	if dbDSN == "" {
		t.Skip("no database provided")
	}
	db, err := sql.Open("postgres", dbDSN)
	if err != nil {
		t.Fatalf("failed to set up test database: %v", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			t.Errorf("failed to close test database: %v", cerr)
		}
	}()

	// Set up some data to use in the SQL DB.
	// This creates 3 tables:
	// - "lookup" - which stores an id->string mapping
	// - "misc" - which has misc non-nullable fields
	// - "nullable" - to test handling of nullable fields
	func() {
		// Set up a write transaction.
		// This uses a function scope so that the defer is scoped appropriately.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("failed to create transaction: %v", err)
		}
		var ok bool
		defer func() {
			if !ok {
				rerr := tx.Rollback()
				if rerr != nil {
					t.Errorf("failed to roll-back transaction: %v", rerr)
				}
			}
		}()

		// Delete all tables so that we start fresh.
		_, err = tx.Exec(`DROP INDEX IF EXISTS lookupIndex`)
		if err != nil {
			t.Fatalf("deleting lookup index: %v", err)
		}
		_, err = tx.Exec(`DROP TABLE IF EXISTS lookup`)
		if err != nil {
			t.Fatalf("deleting lookup table: %v", err)
		}
		_, err = tx.Exec(`DROP TABLE IF EXISTS misc`)
		if err != nil {
			t.Fatalf("deleting misc table: %v", err)
		}
		_, err = tx.Exec(`DROP TABLE IF EXISTS nullable`)
		if err != nil {
			t.Fatalf("deleting nullable table: %v", err)
		}

		_, err = tx.Exec(`CREATE TABLE lookup (
			id int NOT NULL,
			data text NOT NULL
		)`)
		if err != nil {
			t.Fatalf("failed to create lookup table: %v", err)
		}
		_, err = tx.Exec(`INSERT INTO lookup (id, data) VALUES
			(0, 'h'),
			(1, 'xyzzy'),
			(2, 'plugh'),
			(3, 'wow')
		`)
		if err != nil {
			t.Fatalf("failed to populate lookup table: %v", err)
		}
		_, err = tx.Exec(`CREATE UNIQUE INDEX lookupIndex ON lookup (id);`)
		if err != nil {
			t.Fatalf("failed to index lookup table: %v", err)
		}

		_, err = tx.Exec(`CREATE TABLE misc (
			id int NOT NULL,
			stringval text NOT NULL,
			boolval boolean NOT NULL,
			intval int NOT NULL
		)`)
		if err != nil {
			t.Fatalf("failed to create misc table: %v", err)
		}
		_, err = tx.Exec(`INSERT INTO misc (id, stringval, boolval, intval) VALUES
			(0, 'h', true, 4),
			(1, 'y', false, 11)
		`)
		if err != nil {
			t.Fatalf("failed to populate misc table: %v", err)
		}

		_, err = tx.Exec(`CREATE TABLE nullable (
			id int NOT NULL,
			stringval text,
			boolval boolean,
			intval int
		)`)
		if err != nil {
			t.Fatalf("failed to create nullable table: %v", err)
		}
		_, err = tx.Exec(`INSERT INTO nullable (id, stringval, boolval, intval) VALUES
			(0, 'h', true, 4),
			(1, 'y', false, 11),
			(2, null, null, null),
			(3, null, true, null),
			(4, 'plugh', null, 0)
		;`)
		if err != nil {
			t.Fatalf("failed to populate nullable table: %v", err)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("failed to commit DB setup transaction: %v", err)
		}

		ok = true
	}()

	// Start up a Pilosa cluster with access to the DB.
	c := test.MustRunCluster(t, 3, []server.CommandOption{server.OptCommandServerOptions(pilosa.OptServerLookupDB(dbDSN))})
	defer c.Close()

	// Populate a field with some data that can be used in queries.
	c.CreateField(t, "i", pilosa.IndexOptions{TrackExistence: true}, "f")
	c.ImportBits(t, "i", "f", [][2]uint64{
		{1, 1},
		{1, 3},
		{2, 2},
		{2, 3},
	})

	cases := []struct {
		name   string
		query  string
		expect pilosa.QueryResponse
	}{
		{
			name:  "Empty",
			query: `ExternalLookup(Union(), query="select * from lookup where id = ANY($1)")`,
			expect: pilosa.QueryResponse{
				Results: []interface{}{
					pilosa.ExtractedTable{},
				},
			},
		},
		{
			name:  "ConstRowLookup",
			query: `ExternalLookup(ConstRow(columns=[1, 3]), query="select id, data from lookup where id = ANY($1)")`,
			expect: pilosa.QueryResponse{
				Results: []interface{}{
					pilosa.ExtractedTable{
						Fields: []pilosa.ExtractedTableField{
							{Name: "data", Type: "string"},
						},
						Columns: []pilosa.ExtractedTableColumn{
							{
								Column: pilosa.KeyOrID{ID: 1},
								Rows:   []interface{}{"xyzzy"},
							},
							{
								Column: pilosa.KeyOrID{ID: 3},
								Rows:   []interface{}{"wow"},
							},
						},
					},
				},
			},
		},
		{
			name:  "ComputedFilter",
			query: `ExternalLookup(Intersect(Row(f=1), Row(f=2)), query="select id, data from lookup where id = ANY($1)")`,
			expect: pilosa.QueryResponse{
				Results: []interface{}{
					pilosa.ExtractedTable{
						Fields: []pilosa.ExtractedTableField{
							{Name: "data", Type: "string"},
						},
						Columns: []pilosa.ExtractedTableColumn{
							{
								Column: pilosa.KeyOrID{ID: 3},
								Rows:   []interface{}{"wow"},
							},
						},
					},
				},
			},
		},
		{
			name:  "Misc",
			query: `ExternalLookup(ConstRow(columns=[0, 1]), query="select id, stringval, boolval, intval from misc where id = ANY($1)")`,
			expect: pilosa.QueryResponse{
				Results: []interface{}{
					pilosa.ExtractedTable{
						Fields: []pilosa.ExtractedTableField{
							{Name: "stringval", Type: "string"},
							{Name: "boolval", Type: "bool"},
							{Name: "intval", Type: "int64"},
						},
						Columns: []pilosa.ExtractedTableColumn{
							{
								Column: pilosa.KeyOrID{ID: 0},
								Rows:   []interface{}{"h", true, int64(4)},
							},
							{
								Column: pilosa.KeyOrID{ID: 1},
								Rows:   []interface{}{"y", false, int64(11)},
							},
						},
					},
				},
			},
		},
		{
			name:  "Nullable",
			query: `ExternalLookup(ConstRow(columns=[0, 1, 2, 3, 4]), query="select id, stringval, boolval, intval from nullable where id = ANY($1)")`,
			expect: pilosa.QueryResponse{
				Results: []interface{}{
					pilosa.ExtractedTable{
						Fields: []pilosa.ExtractedTableField{
							{Name: "stringval", Type: "string"},
							{Name: "boolval", Type: "bool"},
							{Name: "intval", Type: "int64"},
						},
						Columns: []pilosa.ExtractedTableColumn{
							{
								Column: pilosa.KeyOrID{ID: 0},
								Rows:   []interface{}{"h", true, int64(4)},
							},
							{
								Column: pilosa.KeyOrID{ID: 1},
								Rows:   []interface{}{"y", false, int64(11)},
							},
							{
								Column: pilosa.KeyOrID{ID: 2},
								Rows:   []interface{}{nil, nil, nil},
							},
							{
								Column: pilosa.KeyOrID{ID: 3},
								Rows:   []interface{}{nil, true, nil},
							},
							{
								Column: pilosa.KeyOrID{ID: 4},
								Rows:   []interface{}{"plugh", nil, int64(0)},
							},
						},
					},
				},
			},
		},
	}
	t.Run("Query", func(t *testing.T) {
		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				result := c.Query(t, "i", tc.query)
				if !reflect.DeepEqual(result, tc.expect) {
					t.Errorf("expected %v but got %v", tc.expect, result)
				}
			})
		}
	})
	t.Run("Delete", func(t *testing.T) {
		c.Query(t, "i", `ExternalLookup(All(), query="delete from lookup where id = ANY($1)", write=true)`)
		res := c.Query(t, "i", `ExternalLookup(All(), query="select id from lookup where id = ANY($1)")`)
		tbl := res.Results[0].(pilosa.ExtractedTable)
		if len(tbl.Columns) != 0 {
			t.Errorf("unexpected remaining records: %v", tbl)
		}
	})
}
func TestToRows(t *testing.T) {
	ids := &pilosa.RowIdentifiers{
		Rows: []uint64{1, 2, 3},
	}
	c := ids.Clone()
	if c == nil {
		t.Fatal("Shouldn't be nil ")
	}
	e := ids.ToRows(func(*proto.RowResponse) error {
		return nil
	})
	if e != nil {
		t.Fatal("Shouldn't be err ", e)
	}

	keys := &pilosa.RowIdentifiers{
		Keys: []string{"a", "b"},
	}
	c = keys.Clone()
	if c == nil {
		t.Fatal("Shouldn't be nil ")
	}
	e = keys.ToRows(func(*proto.RowResponse) error {
		return nil
	})
	if e != nil {
		t.Fatal("Shouldn't be err ", e)
	}
	v := &pilosa.ValCount{
		TimestampVal: time.Now(),
		Count:        1,
	}
	x := v.Clone()
	if x == nil {
		t.Fatal("Shouldn't be nil ")
	}
	e = v.ToRows(func(*proto.RowResponse) error {
		return nil
	})
	if e != nil {
		t.Fatal("Shouldn't be err ", e)
	}
	v.DecimalVal = &pql.Decimal{Value: 1, Scale: 1}
	e = v.ToRows(func(*proto.RowResponse) error {
		return nil
	})
	if e != nil {
		t.Fatal("Shouldn't be err ", e)
	}
	v.DecimalVal = nil
	v.FloatVal = 3.0
	e = v.ToRows(func(*proto.RowResponse) error {
		return nil
	})
	if e != nil {
		t.Fatal("Shouldn't be err ", e)
	}

	pfi := &pilosa.PairField{
		Pair:  pilosa.Pair{ID: 1, Count: 1},
		Field: "f",
	}
	z := pfi.Clone()
	if z.Pair.ID != pfi.Pair.ID {
		t.Fatal("Should be equal ", z, pfi)
	}
	e = pfi.ToRows(func(*proto.RowResponse) error {
		return nil
	})
	if e != nil {
		t.Fatal("Shouldn't be err ", e)
	}
	pfk := &pilosa.PairField{
		Pair:  pilosa.Pair{Key: "a", Count: 1},
		Field: "f",
	}
	o := pfk.Clone()
	if o.Pair.Key != pfk.Pair.Key {
		t.Fatal("Should be equal ")
	}
	e = pfk.ToRows(func(*proto.RowResponse) error {
		return nil
	})
	if e != nil {
		t.Fatal("Shouldn't be err ", e)
	}
	pfs := &pilosa.PairsField{
		Pairs: []pilosa.Pair{{ID: 1, Count: 1}},
		Field: "f",
	}
	f := pfs.Clone()
	if f.Pairs[0].ID != pfs.Pairs[0].ID {
		t.Fatal("Should be equal ")
	}
	e = pfs.ToRows(func(*proto.RowResponse) error {
		return nil
	})
	if e != nil {
		t.Fatal("Shouldn't be err ", e)
	}

	r4 := server.ResultUint64(1)
	e = r4.ToRows(func(*proto.RowResponse) error {
		return nil
	})
	if e != nil {
		t.Fatal("Shouldn't be err ", e)
	}

}
