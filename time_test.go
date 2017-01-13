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

// Ensure generated frame name can be returned for a given time unit.
func TestFrameByTimeUnit(t *testing.T) {
	ts := time.Date(2000, time.January, 2, 3, 4, 5, 6, time.UTC)

	t.Run("Y", func(t *testing.T) {
		if s := pilosa.FrameByTimeUnit("F", ts, 'Y'); s != "F::2000" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
	t.Run("M", func(t *testing.T) {
		if s := pilosa.FrameByTimeUnit("F", ts, 'M'); s != "F::200001" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
	t.Run("D", func(t *testing.T) {
		if s := pilosa.FrameByTimeUnit("F", ts, 'D'); s != "F::20000102" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
	t.Run("H", func(t *testing.T) {
		if s := pilosa.FrameByTimeUnit("F", ts, 'H'); s != "F::2000010203" {
			t.Fatalf("unexpected name: %s", s)
		}
	})
}

// Ensure all applicable frame names can be generated when mutating a time bit.
func TestFramesByTime(t *testing.T) {
	ts := time.Date(2000, time.January, 2, 3, 4, 5, 6, time.UTC)

	t.Run("YMDH", func(t *testing.T) {
		a := pilosa.FramesByTime("F", ts, MustParseTimeQuantum("YMDH"))
		if !reflect.DeepEqual(a, []string{"F::2000", "F::200001", "F::20000102", "F::2000010203"}) {
			t.Fatalf("unexpected names: %+v", a)
		}
	})

	t.Run("D", func(t *testing.T) {
		a := pilosa.FramesByTime("F", ts, MustParseTimeQuantum("D"))
		if !reflect.DeepEqual(a, []string{"F::20000102"}) {
			t.Fatalf("unexpected names: %+v", a)
		}
	})
}

// Ensure sets of frames can be returned for a given time range.
func TestFramesByTimeRange(t *testing.T) {
	t.Run("Y", func(t *testing.T) {
		a := pilosa.FramesByTimeRange("F", MustParseTime("2000-01-01 00:00"), MustParseTime("2002-01-01 00:00"), MustParseTimeQuantum("Y"))
		if !reflect.DeepEqual(a, []string{"F::2000", "F::2001"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("YM", func(t *testing.T) {
		a := pilosa.FramesByTimeRange("F", MustParseTime("2000-11-01 00:00"), MustParseTime("2003-03-01 00:00"), MustParseTimeQuantum("YM"))
		if !reflect.DeepEqual(a, []string{"F::200011", "F::200012", "F::2001", "F::2002", "F::200301", "F::200302"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("YMD", func(t *testing.T) {
		a := pilosa.FramesByTimeRange("F", MustParseTime("2000-11-28 00:00"), MustParseTime("2003-03-02 00:00"), MustParseTimeQuantum("YMD"))
		if !reflect.DeepEqual(a, []string{"F::20001128", "F::20001129", "F::20001130", "F::200012", "F::2001", "F::2002", "F::200301", "F::200302", "F::20030301"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("YMDH", func(t *testing.T) {
		a := pilosa.FramesByTimeRange("F", MustParseTime("2000-11-28 22:00"), MustParseTime("2002-03-01 03:00"), MustParseTimeQuantum("YMDH"))
		if !reflect.DeepEqual(a, []string{"F::2000112822", "F::2000112823", "F::20001129", "F::20001130", "F::200012", "F::2001", "F::200201", "F::200202", "F::2002030100", "F::2002030101", "F::2002030102"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("M", func(t *testing.T) {
		a := pilosa.FramesByTimeRange("F", MustParseTime("2000-01-01 00:00"), MustParseTime("2000-03-01 00:00"), MustParseTimeQuantum("M"))
		if !reflect.DeepEqual(a, []string{"F::200001", "F::200002"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("MD", func(t *testing.T) {
		a := pilosa.FramesByTimeRange("F", MustParseTime("2000-11-29 00:00"), MustParseTime("2002-02-03 00:00"), MustParseTimeQuantum("MD"))
		if !reflect.DeepEqual(a, []string{"F::20001129", "F::20001130", "F::200012", "F::200101", "F::200102", "F::200103", "F::200104", "F::200105", "F::200106", "F::200107", "F::200108", "F::200109", "F::200110", "F::200111", "F::200112", "F::200201", "F::20020201", "F::20020202"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("MDH", func(t *testing.T) {
		a := pilosa.FramesByTimeRange("F", MustParseTime("2000-11-29 22:00"), MustParseTime("2002-03-02 03:00"), MustParseTimeQuantum("MDH"))
		if !reflect.DeepEqual(a, []string{"F::2000112922", "F::2000112923", "F::20001130", "F::200012", "F::200101", "F::200102", "F::200103", "F::200104", "F::200105", "F::200106", "F::200107", "F::200108", "F::200109", "F::200110", "F::200111", "F::200112", "F::200201", "F::200202", "F::20020301", "F::2002030200", "F::2002030201", "F::2002030202"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("D", func(t *testing.T) {
		a := pilosa.FramesByTimeRange("F", MustParseTime("2000-01-01 00:00"), MustParseTime("2000-01-04 00:00"), MustParseTimeQuantum("D"))
		if !reflect.DeepEqual(a, []string{"F::20000101", "F::20000102", "F::20000103"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("DH", func(t *testing.T) {
		a := pilosa.FramesByTimeRange("F", MustParseTime("2000-01-01 22:00"), MustParseTime("2000-03-01 02:00"), MustParseTimeQuantum("DH"))
		if !reflect.DeepEqual(a, []string{"F::2000010122", "F::2000010123", "F::20000102", "F::20000103", "F::20000104", "F::20000105", "F::20000106", "F::20000107", "F::20000108", "F::20000109", "F::20000110", "F::20000111", "F::20000112", "F::20000113", "F::20000114", "F::20000115", "F::20000116", "F::20000117", "F::20000118", "F::20000119", "F::20000120", "F::20000121", "F::20000122", "F::20000123", "F::20000124", "F::20000125", "F::20000126", "F::20000127", "F::20000128", "F::20000129", "F::20000130", "F::20000131", "F::20000201", "F::20000202", "F::20000203", "F::20000204", "F::20000205", "F::20000206", "F::20000207", "F::20000208", "F::20000209", "F::20000210", "F::20000211", "F::20000212", "F::20000213", "F::20000214", "F::20000215", "F::20000216", "F::20000217", "F::20000218", "F::20000219", "F::20000220", "F::20000221", "F::20000222", "F::20000223", "F::20000224", "F::20000225", "F::20000226", "F::20000227", "F::20000228", "F::20000229", "F::2000030100", "F::2000030101"}) {
			t.Fatalf("unexpected frames: %#v", a)
		}
	})
	t.Run("H", func(t *testing.T) {
		a := pilosa.FramesByTimeRange("F", MustParseTime("2000-01-01 00:00"), MustParseTime("2000-01-01 02:00"), MustParseTimeQuantum("H"))
		if !reflect.DeepEqual(a, []string{"F::2000010100", "F::2000010101"}) {
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
