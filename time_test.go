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
	"reflect"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
)

// Ensure string can be parsed into time quantum.
func TestParseTimeQuantum(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if q, err := pilosa.ParseTimeQuantum("YMDH"); err != nil {
			t.Fatalf("unexpected error: %s", err)
		} else if q != pilosa.TimeQuantum("YMDH") {
			t.Fatalf("unexpected quantum: %#v", q)
		}
	})

	t.Run("ErrInvalidTimeQuantum", func(t *testing.T) {
		if _, err := pilosa.ParseTimeQuantum("BADQUANTUM"); err != pilosa.ErrInvalidTimeQuantum {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

// Ensure generated view name can be returned for a given time unit.
func TestViewByTimeUnit(t *testing.T) {
	ts := time.Date(2000, time.January, 2, 3, 4, 5, 6, time.UTC)

	t.Run("Y", func(t *testing.T) {
		if s := pilosa.ViewByTimeUnit("F", ts, 'Y'); s != "F_2000" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
	t.Run("M", func(t *testing.T) {
		if s := pilosa.ViewByTimeUnit("F", ts, 'M'); s != "F_200001" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
	t.Run("D", func(t *testing.T) {
		if s := pilosa.ViewByTimeUnit("F", ts, 'D'); s != "F_20000102" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
	t.Run("H", func(t *testing.T) {
		if s := pilosa.ViewByTimeUnit("F", ts, 'H'); s != "F_2000010203" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
}

// Ensure all applicable frame names can be generated when mutating a time bit.
func TestViewsByTime(t *testing.T) {
	ts := time.Date(2000, time.January, 2, 3, 4, 5, 6, time.UTC)

	t.Run("YMDH", func(t *testing.T) {
		a := pilosa.ViewsByTime("F", ts, MustParseTimeQuantum("YMDH"))
		if !reflect.DeepEqual(a, []string{"F_2000", "F_200001", "F_20000102", "F_2000010203"}) {
			t.Fatalf("unexpected names: %+v", a)
		}
	})

	t.Run("D", func(t *testing.T) {
		a := pilosa.ViewsByTime("F", ts, MustParseTimeQuantum("D"))
		if !reflect.DeepEqual(a, []string{"F_20000102"}) {
			t.Fatalf("unexpected names: %+v", a)
		}
	})
}

// Ensure sets of frames can be returned for a given time range.
func TestViewsByTimeRange(t *testing.T) {
	t.Run("Y", func(t *testing.T) {
		a := pilosa.ViewsByTimeRange("F", MustParseTime("2000-01-01 00:00"), MustParseTime("2002-01-01 00:00"), MustParseTimeQuantum("Y"))
		if !reflect.DeepEqual(a, []string{"F_2000", "F_2001"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("YM", func(t *testing.T) {
		a := pilosa.ViewsByTimeRange("F", MustParseTime("2000-11-01 00:00"), MustParseTime("2003-03-01 00:00"), MustParseTimeQuantum("YM"))
		if !reflect.DeepEqual(a, []string{"F_200011", "F_200012", "F_2001", "F_2002", "F_200301", "F_200302"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("YMD", func(t *testing.T) {
		a := pilosa.ViewsByTimeRange("F", MustParseTime("2000-11-28 00:00"), MustParseTime("2003-03-02 00:00"), MustParseTimeQuantum("YMD"))
		if !reflect.DeepEqual(a, []string{"F_20001128", "F_20001129", "F_20001130", "F_200012", "F_2001", "F_2002", "F_200301", "F_200302", "F_20030301"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("YMDH", func(t *testing.T) {
		a := pilosa.ViewsByTimeRange("F", MustParseTime("2000-11-28 22:00"), MustParseTime("2002-03-01 03:00"), MustParseTimeQuantum("YMDH"))
		if !reflect.DeepEqual(a, []string{"F_2000112822", "F_2000112823", "F_20001129", "F_20001130", "F_200012", "F_2001", "F_200201", "F_200202", "F_2002030100", "F_2002030101", "F_2002030102"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("M", func(t *testing.T) {
		a := pilosa.ViewsByTimeRange("F", MustParseTime("2000-01-01 00:00"), MustParseTime("2000-03-01 00:00"), MustParseTimeQuantum("M"))
		if !reflect.DeepEqual(a, []string{"F_200001", "F_200002"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("MD", func(t *testing.T) {
		a := pilosa.ViewsByTimeRange("F", MustParseTime("2000-11-29 00:00"), MustParseTime("2002-02-03 00:00"), MustParseTimeQuantum("MD"))
		if !reflect.DeepEqual(a, []string{"F_20001129", "F_20001130", "F_200012", "F_200101", "F_200102", "F_200103", "F_200104", "F_200105", "F_200106", "F_200107", "F_200108", "F_200109", "F_200110", "F_200111", "F_200112", "F_200201", "F_20020201", "F_20020202"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("MDH", func(t *testing.T) {
		a := pilosa.ViewsByTimeRange("F", MustParseTime("2000-11-29 22:00"), MustParseTime("2002-03-02 03:00"), MustParseTimeQuantum("MDH"))
		if !reflect.DeepEqual(a, []string{"F_2000112922", "F_2000112923", "F_20001130", "F_200012", "F_200101", "F_200102", "F_200103", "F_200104", "F_200105", "F_200106", "F_200107", "F_200108", "F_200109", "F_200110", "F_200111", "F_200112", "F_200201", "F_200202", "F_20020301", "F_2002030200", "F_2002030201", "F_2002030202"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("D", func(t *testing.T) {
		a := pilosa.ViewsByTimeRange("F", MustParseTime("2000-01-01 00:00"), MustParseTime("2000-01-04 00:00"), MustParseTimeQuantum("D"))
		if !reflect.DeepEqual(a, []string{"F_20000101", "F_20000102", "F_20000103"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("DH", func(t *testing.T) {
		a := pilosa.ViewsByTimeRange("F", MustParseTime("2000-01-01 22:00"), MustParseTime("2000-03-01 02:00"), MustParseTimeQuantum("DH"))
		if !reflect.DeepEqual(a, []string{"F_2000010122", "F_2000010123", "F_20000102", "F_20000103", "F_20000104", "F_20000105", "F_20000106", "F_20000107", "F_20000108", "F_20000109", "F_20000110", "F_20000111", "F_20000112", "F_20000113", "F_20000114", "F_20000115", "F_20000116", "F_20000117", "F_20000118", "F_20000119", "F_20000120", "F_20000121", "F_20000122", "F_20000123", "F_20000124", "F_20000125", "F_20000126", "F_20000127", "F_20000128", "F_20000129", "F_20000130", "F_20000131", "F_20000201", "F_20000202", "F_20000203", "F_20000204", "F_20000205", "F_20000206", "F_20000207", "F_20000208", "F_20000209", "F_20000210", "F_20000211", "F_20000212", "F_20000213", "F_20000214", "F_20000215", "F_20000216", "F_20000217", "F_20000218", "F_20000219", "F_20000220", "F_20000221", "F_20000222", "F_20000223", "F_20000224", "F_20000225", "F_20000226", "F_20000227", "F_20000228", "F_20000229", "F_2000030100", "F_2000030101"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("H", func(t *testing.T) {
		a := pilosa.ViewsByTimeRange("F", MustParseTime("2000-01-01 00:00"), MustParseTime("2000-01-01 02:00"), MustParseTimeQuantum("H"))
		if !reflect.DeepEqual(a, []string{"F_2000010100", "F_2000010101"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
}

// DefaultTimeLayout is the time layout used by the tests.
const DefaultTimeLayout = "2006-01-02 15:04"

// MustParseTime parses value using DefaultTimeLayout. Panic on error.
func MustParseTime(value string) time.Time {
	v, err := time.Parse(DefaultTimeLayout, value)
	if err != nil {
		panic(err)
	}
	return v
}

// MustParseTimePtr parses value using DefaultTimeLayout. Panic on error.
func MustParseTimePtr(value string) *time.Time {
	v := MustParseTime(value)
	return &v
}

// MustParseTimeQuantum parses v into a time quantum. Panic on error.
func MustParseTimeQuantum(v string) pilosa.TimeQuantum {
	q, err := pilosa.ParseTimeQuantum(v)
	if err != nil {
		panic(err)
	}
	return q
}
