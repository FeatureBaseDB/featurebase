package pilosa_test

import (
	"testing"
	"time"

	"github.com/umbel/pilosa"
)

func TestTimeIDsFromRange_1h_0(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-08-11 14:00"),
		*MustParseTime("2014-08-11 16:00"),
		uint64(1),
	); len(m) != 2 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromRange_1h_1(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-01-02 10:03"),
		*MustParseTime("2014-01-02 11:03"),
		uint64(1),
	); len(m) != 1 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromRange_2h(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-01-02 10:03"),
		*MustParseTime("2014-01-02 12:03"),
		uint64(1),
	); len(m) != 2 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromRange_24h(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-01-02 12:03"),
		*MustParseTime("2014-01-03 12:03"),
		uint64(1),
	); len(m) != 24 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromRange_1d(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-01-02 00:00"),
		*MustParseTime("2014-01-03 00:00"),
		uint64(1),
	); len(m) != 1 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromRange_1d1h(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-01-02 00:00"),
		*MustParseTime("2014-01-03 01:00"),
		uint64(1),
	); len(m) != 2 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromRange_1h1d(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-01-02 23:00"),
		*MustParseTime("2014-01-04 00:00"),
		uint64(1),
	); len(m) != 2 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromRange_1h1d1h(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-01-02 23:00"),
		*MustParseTime("2014-01-04 01:00"),
		uint64(1),
	); len(m) != 3 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromRange_1y(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-01-01 00:00"),
		*MustParseTime("2015-01-01 00:00"),
		uint64(1),
	); len(m) != 1 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromRange_1h1d1m(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-01-30 23:00"),
		*MustParseTime("2014-03-01 00:00"),
		uint64(1),
	); len(m) != 3 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromRange_1h1d1m1d1h(t *testing.T) {
	if m := pilosa.TimeIDsFromRange(
		*MustParseTime("2014-01-30 23:00"),
		*MustParseTime("2014-03-02 01:00"),
		uint64(1),
	); len(m) != 5 {
		t.Fatalf("unexpected range len: %d", len(m))
	}
}

func TestTimeIDsFromQuantum(t *testing.T) {
	_ = pilosa.TimeIDsFromQuantum(pilosa.YMD, *MustParseTime("1970-01-01 00:00"), uint64(15027))
}

// DefaultTimeLayout is the time layout used by the tests.
const DefaultTimeLayout = "2006-01-02 15:04"

// MustParseTime parses value using DefaultTimeLayout. Panic on error.
func MustParseTime(value string) *time.Time {
	v, err := time.Parse(DefaultTimeLayout, value)
	if err != nil {
		panic(err)
	}
	return &v
}
