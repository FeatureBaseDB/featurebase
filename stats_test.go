package pilosa_test

import (
	"context"
	"github.com/pilosa/pilosa"
	"testing"
	"time"
)

func TestStatsCount_TopN(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0).SetBit(0, 0)
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 1).SetBit(0, SliceWidth)
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 1).SetBit(0, SliceWidth+2)

	// Execute query.
	called := false
	e := NewExecutor(idx.Index, NewCluster(1))
	e.Index.Stats = &MockStats{
		mockCountWithTags: func(name string, value int64, tags []string) {
			if name != "TopN" {
				t.Errorf("Expected TopN, Results %s", name)
			}

			if tags[0] != "db:d" {
				t.Errorf("Expected db, Results %s", tags[0])
			}

			called = true
			return
		},
	}
	if _, err := e.Execute(context.Background(), "d", MustParse(`TopN(frame=f, n=2)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("CountWithCustomTags name isn't called")
	}
}

func TestStatsCount_Bitmap(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0).SetBit(0, 0)
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0).SetBit(0, 1)
	called := false
	e := NewExecutor(idx.Index, NewCluster(1))
	e.Index.Stats = &MockStats{
		mockCountWithTags: func(name string, value int64, tags []string) {
			if name != "Bitmap" {
				t.Errorf("Expected Bitmap, Results %s", name)
			}

			if tags[0] != "db:d" {
				t.Errorf("Expected db, Results %s", tags[0])
			}

			called = true
			return
		},
	}
	if _, err := e.Execute(context.Background(), "d", MustParse(`Bitmap(frame=f, id=0)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("CountWithCustomTags name isn't called")
	}
}

func TestStatsCount_SetBitmapAttrs(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0).SetBit(10, 0)
	idx.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0).SetBit(10, 1)

	called := false
	e := NewExecutor(idx.Index, NewCluster(1))
	frame := e.Index.Frame("d", "f")
	if frame == nil {
		t.Fatal("frame not found")
	}

	frame.Stats = &MockStats{
		mockCount: func(name string, value int64) {
			if name != "SetBitmapAttrs" {
				t.Errorf("Expected SetBitmapAttrs, Results %s", name)
			}
			called = true
			return
		},
	}
	if _, err := e.Execute(context.Background(), "d", MustParse(`SetBitmapAttrs(id=10, frame=f, foo="bar")`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("Count isn't called")
	}
}

type MockStats struct {
	mockCount         func(name string, value int64)
	mockCountWithTags func(name string, value int64, tags []string)
}

func (s *MockStats) Count(name string, value int64) {
	if s.mockCount != nil {
		s.mockCount(name, value)
		return
	}
	return
}

func (s *MockStats) CountWithCustomTags(name string, value int64, tags []string) {
	if s.mockCountWithTags != nil {
		s.mockCountWithTags(name, value, tags)
		return
	}
	return
}
func (c *MockStats) Tags() []string {
	return nil
}
func (c *MockStats) WithTags(tags ...string) pilosa.StatsClient {
	return c
}

func (c *MockStats) Gauge(name string, value float64)        {}
func (c *MockStats) Histogram(name string, value float64)    {}
func (c *MockStats) Set(name string, value string)           {}
func (c *MockStats) Timing(name string, value time.Duration) {}
