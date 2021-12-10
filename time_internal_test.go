// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// Ensure string can be parsed into time quantum.
func TestParseTimeQuantum(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if q, err := parseTimeQuantum("YMDH"); err != nil {
			t.Fatalf("unexpected error: %s", err)
		} else if q != TimeQuantum("YMDH") {
			t.Fatalf("unexpected quantum: %#v", q)
		}
	})

	t.Run("ErrInvalidTimeQuantum", func(t *testing.T) {
		if _, err := parseTimeQuantum("BADQUANTUM"); err != ErrInvalidTimeQuantum {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

// Ensure generated view name can be returned for a given time unit.
func TestViewByTimeUnit(t *testing.T) {
	ts := time.Date(2000, time.January, 2, 3, 4, 5, 6, time.UTC)

	t.Run("Y", func(t *testing.T) {
		if s := viewByTimeUnit("F", ts, 'Y'); s != "F_2000" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
	t.Run("M", func(t *testing.T) {
		if s := viewByTimeUnit("F", ts, 'M'); s != "F_200001" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
	t.Run("D", func(t *testing.T) {
		if s := viewByTimeUnit("F", ts, 'D'); s != "F_20000102" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
	t.Run("H", func(t *testing.T) {
		if s := viewByTimeUnit("F", ts, 'H'); s != "F_2000010203" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
}

// Ensure all applicable field names can be generated when mutating a time bit.
func TestViewsByTime(t *testing.T) {
	ts := time.Date(2000, time.January, 2, 3, 4, 5, 6, time.UTC)

	t.Run("YMDH", func(t *testing.T) {
		a := viewsByTime("F", ts, mustParseTimeQuantum("YMDH"))
		if !reflect.DeepEqual(a, []string{"F_2000", "F_200001", "F_20000102", "F_2000010203"}) {
			t.Fatalf("unexpected names: %+v", a)
		}
	})

	t.Run("D", func(t *testing.T) {
		a := viewsByTime("F", ts, mustParseTimeQuantum("D"))
		if !reflect.DeepEqual(a, []string{"F_20000102"}) {
			t.Fatalf("unexpected names: %+v", a)
		}
	})
}

func TestViewsByTimeInto(t *testing.T) {
	ts := time.Date(2000, time.January, 2, 3, 4, 5, 6, time.UTC)
	s := []byte("F_YYYYMMDDHH")
	var timeViews [][]byte

	t.Run("YMDH", func(t *testing.T) {
		a := viewsByTime("F", ts, mustParseTimeQuantum("YMDH"))
		b := viewsByTimeInto(s, timeViews, ts, mustParseTimeQuantum("YMDH"))
		if len(a) != len(b) {
			t.Fatalf("mismatch: viewsByTime: %q, viewsByTimeInto: %q", a, b)
		}
		for i := range a {
			if a[i] != string(b[i]) {
				t.Fatalf("mismatch: viewsByTime: %q, viewsByTimeInto: %q", a, b)
			}
		}
	})

	t.Run("D", func(t *testing.T) {
		a := viewsByTime("F", ts, mustParseTimeQuantum("D"))
		b := viewsByTimeInto(s, timeViews, ts, mustParseTimeQuantum("D"))
		if len(a) != len(b) {
			t.Fatalf("mismatch: viewsByTime: %q, viewsByTimeInto: %q", a, b)
		}
		for i := range a {
			if a[i] != string(b[i]) {
				t.Fatalf("mismatch: viewsByTime: %q, viewsByTimeInto: %q", a, b)
			}
		}
	})
}

// Ensure sets of fields can be returned for a given time range.
func TestViewsByTimeRange(t *testing.T) {
	t.Run("Y", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-01-01 00:00"), mustParseTime("2002-01-01 00:00"), mustParseTimeQuantum("Y"))
		if !reflect.DeepEqual(a, []string{"F_2000", "F_2001"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("YM", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-11-01 00:00"), mustParseTime("2003-03-01 00:00"), mustParseTimeQuantum("YM"))
		if !reflect.DeepEqual(a, []string{"F_200011", "F_200012", "F_2001", "F_2002", "F_200301", "F_200302"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("YM31up", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2001-10-31 00:00"), mustParseTime("2003-04-01 00:00"), mustParseTimeQuantum("YM"))
		if !reflect.DeepEqual(a, []string{"F_200110", "F_200111", "F_200112", "F_2002", "F_200301", "F_200302", "F_200303"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("YM31mid", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("1999-12-31 00:00"), mustParseTime("2000-04-01 00:00"), mustParseTimeQuantum("YM"))
		if !reflect.DeepEqual(a, []string{"F_199912", "F_200001", "F_200002", "F_200003"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("YM31down", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-01-31 00:00"), mustParseTime("2001-04-01 00:00"), mustParseTimeQuantum("YM"))
		if !reflect.DeepEqual(a, []string{"F_2000", "F_200101", "F_200102", "F_200103"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("YMD", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-11-28 00:00"), mustParseTime("2003-03-02 00:00"), mustParseTimeQuantum("YMD"))
		if !reflect.DeepEqual(a, []string{"F_20001128", "F_20001129", "F_20001130", "F_200012", "F_2001", "F_2002", "F_200301", "F_200302", "F_20030301"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("YMDH", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-11-28 22:00"), mustParseTime("2002-03-01 03:00"), mustParseTimeQuantum("YMDH"))
		if !reflect.DeepEqual(a, []string{"F_2000112822", "F_2000112823", "F_20001129", "F_20001130", "F_200012", "F_2001", "F_200201", "F_200202", "F_2002030100", "F_2002030101", "F_2002030102"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("M", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-01-01 00:00"), mustParseTime("2000-03-01 00:00"), mustParseTimeQuantum("M"))
		if !reflect.DeepEqual(a, []string{"F_200001", "F_200002"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("MD", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-11-29 00:00"), mustParseTime("2002-02-03 00:00"), mustParseTimeQuantum("MD"))
		if !reflect.DeepEqual(a, []string{"F_20001129", "F_20001130", "F_200012", "F_200101", "F_200102", "F_200103", "F_200104", "F_200105", "F_200106", "F_200107", "F_200108", "F_200109", "F_200110", "F_200111", "F_200112", "F_200201", "F_20020201", "F_20020202"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("MDH", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-11-29 22:00"), mustParseTime("2002-03-02 03:00"), mustParseTimeQuantum("MDH"))
		if !reflect.DeepEqual(a, []string{"F_2000112922", "F_2000112923", "F_20001130", "F_200012", "F_200101", "F_200102", "F_200103", "F_200104", "F_200105", "F_200106", "F_200107", "F_200108", "F_200109", "F_200110", "F_200111", "F_200112", "F_200201", "F_200202", "F_20020301", "F_2002030200", "F_2002030201", "F_2002030202"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("D", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-01-01 00:00"), mustParseTime("2000-01-04 00:00"), mustParseTimeQuantum("D"))
		if !reflect.DeepEqual(a, []string{"F_20000101", "F_20000102", "F_20000103"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("DH", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-01-01 22:00"), mustParseTime("2000-03-01 02:00"), mustParseTimeQuantum("DH"))
		if !reflect.DeepEqual(a, []string{"F_2000010122", "F_2000010123", "F_20000102", "F_20000103", "F_20000104", "F_20000105", "F_20000106", "F_20000107", "F_20000108", "F_20000109", "F_20000110", "F_20000111", "F_20000112", "F_20000113", "F_20000114", "F_20000115", "F_20000116", "F_20000117", "F_20000118", "F_20000119", "F_20000120", "F_20000121", "F_20000122", "F_20000123", "F_20000124", "F_20000125", "F_20000126", "F_20000127", "F_20000128", "F_20000129", "F_20000130", "F_20000131", "F_20000201", "F_20000202", "F_20000203", "F_20000204", "F_20000205", "F_20000206", "F_20000207", "F_20000208", "F_20000209", "F_20000210", "F_20000211", "F_20000212", "F_20000213", "F_20000214", "F_20000215", "F_20000216", "F_20000217", "F_20000218", "F_20000219", "F_20000220", "F_20000221", "F_20000222", "F_20000223", "F_20000224", "F_20000225", "F_20000226", "F_20000227", "F_20000228", "F_20000229", "F_2000030100", "F_2000030101"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
	t.Run("H", func(t *testing.T) {
		a := viewsByTimeRange("F", mustParseTime("2000-01-01 00:00"), mustParseTime("2000-01-01 02:00"), mustParseTimeQuantum("H"))
		if !reflect.DeepEqual(a, []string{"F_2000010100", "F_2000010101"}) {
			t.Fatalf("unexpected fields: %#v", a)
		}
	})
}

func TestMinMaxViews(t *testing.T) {
	t.Run("Combos", func(t *testing.T) {
		tests := []struct {
			views []string
			q     TimeQuantum
			min   string
			max   string
		}{
			{
				[]string{""},
				mustParseTimeQuantum("Y"),
				"",
				"",
			},
			{
				[]string{"std_2019", "std_2020", "std_202002", "std_202002", "std_2022"},
				mustParseTimeQuantum("Y"),
				"std_2019",
				"std_2022",
			},
			{
				[]string{"std_201902", "std_201901"},
				mustParseTimeQuantum("M"),
				"std_201901",
				"std_201902",
			},
			{
				[]string{"std_201902", "std_201901"},
				mustParseTimeQuantum("D"),
				"",
				"",
			},
			{
				[]string{"std_20190201"},
				mustParseTimeQuantum("D"),
				"std_20190201",
				"std_20190201",
			},
			{
				[]string{"foo", "bar"},
				mustParseTimeQuantum("D"),
				"",
				"",
			},
		}
		for i, test := range tests {
			if min, max := minMaxViews(test.views, test.q); min != test.min {
				t.Errorf("test %d expected min: %v, but got: %v", i, test.min, min)
			} else if max != test.max {
				t.Errorf("test %d expected max: %v, but got: %v", i, test.max, max)
			}
		}
	})
}

func TestTimeOfView(t *testing.T) {
	t.Run("Combos", func(t *testing.T) {
		tests := []struct {
			view   string
			exp    time.Time
			expAdj time.Time
			expErr string
		}{
			{
				"std_2019",
				time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				"",
			},
			{
				"std_201902",
				time.Date(2019, 2, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2019, 3, 1, 0, 0, 0, 0, time.UTC),
				"",
			},
			{
				"std_20190203",
				time.Date(2019, 2, 3, 0, 0, 0, 0, time.UTC),
				time.Date(2019, 2, 4, 0, 0, 0, 0, time.UTC),
				"",
			},
			{
				"std_2019020308",
				time.Date(2019, 2, 3, 8, 0, 0, 0, time.UTC),
				time.Date(2019, 2, 3, 9, 0, 0, 0, time.UTC),
				"",
			},
			{
				"foo",
				time.Time{},
				time.Time{},
				"invalid time format on view: foo",
			},
			{
				"std_201902030801",
				time.Time{},
				time.Time{},
				"invalid time format on view: std_201902030801",
			},
		}
		for i, test := range tests {
			// adj: false
			if tm, err := timeOfView(test.view, false); err != nil {
				if err.Error() != test.expErr {
					t.Errorf("test %d got unexpected error: %s", i, err)
				}
			} else if test.expErr != "" {
				t.Errorf("test %d expected error: %s but got none", i, test.expErr)
			} else if tm != test.exp {
				t.Errorf("test %d expected time: %v, but got: %v", i, test.exp, tm)
			}
			// adj: true
			if tm, err := timeOfView(test.view, true); err != nil {
				if err.Error() != test.expErr {
					t.Errorf("test %d got unexpected error: %s", i, err)
				}
			} else if test.expErr != "" {
				t.Errorf("test %d expected error: %s but got none", i, test.expErr)
			} else if tm != test.expAdj {
				t.Errorf("test %d expected time: %v, but got: %v", i, test.expAdj, tm)
			}
		}
	})
}

// defaultTimeLayout is the time layout used by the tests.
const defaultTimeLayout = "2006-01-02 15:04"

// mustParseTime parses value using DefaultTimeLayout. Panic on error.
func mustParseTime(value string) time.Time {
	v, err := time.Parse(defaultTimeLayout, value)
	if err != nil {
		panic(err)
	}
	return v
}

// mustParseTimeQuantum parses v into a time quantum. Panic on error.
func mustParseTimeQuantum(v string) TimeQuantum {
	q, err := parseTimeQuantum(v)
	if err != nil {
		panic(err)
	}
	return q
}

// parseTimeQuantum parses v into a time quantum.
func parseTimeQuantum(v string) (TimeQuantum, error) {
	q := TimeQuantum(strings.ToUpper(v))
	if !q.Valid() {
		return "", ErrInvalidTimeQuantum
	}
	return q, nil
}

func TestParsePartialTime(t *testing.T) {
	// test handling of valud inputs
	testCases := []struct {
		userInput    string
		expectedTime time.Time
	}{
		{
			"2006",
			time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"2006-07",
			time.Date(2006, time.July, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"2006-07-02",
			time.Date(2006, time.July, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			"2006-07-02T15",
			time.Date(2006, time.July, 2, 15, 0, 0, 0, time.UTC),
		},
		{
			"2006-07-02T15:04",
			time.Date(2006, time.July, 2, 15, 4, 0, 0, time.UTC),
		},
	}
	for _, tc := range testCases {
		got, err := parsePartialTime(tc.userInput)
		if err != nil {
			t.Errorf("expected nil error given parsing for '%s'", tc.userInput)
		}
		if got != tc.expectedTime {
			t.Errorf("expected %v, got %v", tc.expectedTime, got)
		}
	}

	// test handling of invalid inputs
	invalidInputs := []string{
		"  2006-01-02 ",
		" foo-bar ",
		"2006-",
		"2006-01-",
		"2006-01-02T",
		"2006T",
		"2006T04",
		"01-02",
		"2006-01T04",
		"2006-01T04:",
		"2006-01T:04",
	}
	for _, invalidInput := range invalidInputs {
		_, err := parsePartialTime(invalidInput)
		if err == nil {
			t.Errorf("for input '%s', error on parse expected", invalidInput)
		}
	}

}
