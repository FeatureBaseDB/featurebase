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
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

// TestMultiStatClient_Expvar run the multistat client with exp var
// since the EXPVAR data is stored in a global we should run these in one test function
func TestMultiStatClient_Expvar(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	c := pilosa.NewExpvarStatsClient()
	ms := make(pilosa.MultiStatsClient, 1)
	ms[0] = c
	hldr.Stats = ms

	hldr.SetBit("d", "f", 0, 0)
	hldr.SetBit("d", "f", 0, 1)
	hldr.SetBit("d", "f", 0, SliceWidth)
	hldr.SetBit("d", "f", 0, SliceWidth+2)
	hldr.ClearBit("d", "f", 0, 1)

	if pilosa.Expvar.String() != `{"index:d": {"field:f": {"view:standard": {"slice:0": {"clearBit": 1, "rows": 0, "setBit": 2}, "slice:1": {"rows": 0, "setBit": 2}}}}}` {
		t.Fatalf("unexpected expvar : %s", pilosa.Expvar.String())
	}

	hldr.Stats.CountWithCustomTags("cc", 1, 1.0, []string{"foo:bar"})
	if pilosa.Expvar.String() != `{"cc": 1, "index:d": {"field:f": {"view:standard": {"slice:0": {"clearBit": 1, "rows": 0, "setBit": 2}, "slice:1": {"rows": 0, "setBit": 2}}}}}` {
		t.Fatalf("unexpected expvar : %s", pilosa.Expvar.String())
	}

	// Gauge creates a unique key, subsequent Gauge calls will overwrite
	hldr.Stats.Gauge("g", 5, 1.0)
	hldr.Stats.Gauge("g", 8, 1.0)
	if pilosa.Expvar.String() != `{"cc": 1, "g": 8, "index:d": {"field:f": {"view:standard": {"slice:0": {"clearBit": 1, "rows": 0, "setBit": 2}, "slice:1": {"rows": 0, "setBit": 2}}}}}` {
		t.Fatalf("unexpected expvar : %s", pilosa.Expvar.String())
	}

	// Set creates a unique key, subsequent sets will overwrite
	hldr.Stats.Set("s", "4", 1.0)
	hldr.Stats.Set("s", "7", 1.0)
	if pilosa.Expvar.String() != `{"cc": 1, "g": 8, "index:d": {"field:f": {"view:standard": {"slice:0": {"clearBit": 1, "rows": 0, "setBit": 2}, "slice:1": {"rows": 0, "setBit": 2}}}}, "s": "7"}` {
		t.Fatalf("unexpected expvar : %s", pilosa.Expvar.String())
	}

	// Record timing duration and a uniquely Set key/value
	dur, _ := time.ParseDuration("123us")
	hldr.Stats.Timing("tt", dur, 1.0)
	if pilosa.Expvar.String() != `{"cc": 1, "g": 8, "index:d": {"field:f": {"view:standard": {"slice:0": {"clearBit": 1, "rows": 0, "setBit": 2}, "slice:1": {"rows": 0, "setBit": 2}}}}, "s": "7", "tt": 123µs}` {
		t.Fatalf("unexpected expvar : %s", pilosa.Expvar.String())
	}

	// Expvar histogram is implemented as a gauge
	hldr.Stats.Histogram("hh", 3, 1.0)
	if pilosa.Expvar.String() != `{"cc": 1, "g": 8, "hh": 3, "index:d": {"field:f": {"view:standard": {"slice:0": {"clearBit": 1, "rows": 0, "setBit": 2}, "slice:1": {"rows": 0, "setBit": 2}}}}, "s": "7", "tt": 123µs}` {
		t.Fatalf("unexpected expvar : %s", pilosa.Expvar.String())
	}

	// Expvar should ignore earlier set tags from setbit
	if hldr.Stats.Tags() != nil {
		t.Fatalf("unexpected tag")
	}
}

func TestStatsCount_TopN(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	hldr.SetBit("d", "f", 0, 0)
	hldr.SetBit("d", "f", 0, 1)
	hldr.SetBit("d", "f", 0, SliceWidth)
	hldr.SetBit("d", "f", 0, SliceWidth+2)

	// Execute query.
	called := false
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	e.Holder.Stats = &MockStats{
		mockCountWithTags: func(name string, value int64, rate float64, tags []string) {
			if name != "TopN" {
				t.Errorf("Expected TopN, Results %s", name)
			}

			if tags[0] != "index:d" {
				t.Errorf("Expected db, Results %s", tags[0])
			}

			called = true
		},
	}
	if _, err := e.Execute(context.Background(), "d", test.MustParse(`TopN(field=f, n=2)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("CountWithCustomTags name isn't called")
	}
}

func TestStatsCount_Bitmap(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	hldr.SetBit("d", "f", 0, 0)
	hldr.SetBit("d", "f", 0, 1)
	called := false
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	e.Holder.Stats = &MockStats{
		mockCountWithTags: func(name string, value int64, rate float64, tags []string) {
			if name != "Bitmap" {
				t.Errorf("Expected Bitmap, Results %s", name)
			}

			if tags[0] != "index:d" {
				t.Errorf("Expected db, Results %s", tags[0])
			}

			called = true
		},
	}
	if _, err := e.Execute(context.Background(), "d", test.MustParse(`Bitmap(field=f, row=0)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("CountWithCustomTags name isn't called")
	}
}

func TestStatsCount_SetColumnAttrs(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	hldr.SetBit("d", "f", 10, 0)
	hldr.SetBit("d", "f", 10, 1)

	called := false
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	field := e.Holder.Field("d", "f")
	if field == nil {
		t.Fatal("field not found")
	}

	field.Stats = &MockStats{
		mockCount: func(name string, value int64, rate float64) {
			if name != "SetRowAttrs" {
				t.Errorf("Expected SetRowAttrs, Results %s", name)
			}
			called = true
		},
	}
	if _, err := e.Execute(context.Background(), "d", test.MustParse(`SetRowAttrs(row=10, field=f, foo="bar")`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("Count isn't called")
	}
}

func TestStatsCount_SetProfileAttrs(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	hldr.SetBit("d", "f", 10, 0)
	hldr.SetBit("d", "f", 10, 1)

	called := false
	e := test.NewExecutor(hldr.Holder, pilosa.NewTestCluster(1))
	idx := e.Holder.Index("d")
	if idx == nil {
		t.Fatal("idex not found")
	}

	idx.Stats = &MockStats{
		mockCount: func(name string, value int64, rate float64) {
			if name != "SetProfileAttrs" {
				t.Errorf("Expected SetProfilepAttrs, Results %s", name)
			}

			called = true
		},
	}
	if _, err := e.Execute(context.Background(), "d", test.MustParse(`SetColumnAttrs(col=10, field=f, foo="bar")`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("Count isn't called")
	}
}

func TestStatsCount_CreateIndex(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()
	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()
	called := false
	s.Handler.API.Holder.Stats = &MockStats{
		mockCount: func(name string, value int64, rate float64) {
			if name != "createIndex" {
				t.Errorf("Expected createIndex, Results %s", name)
			}

			called = true
		},
	}
	http.DefaultClient.Do(test.MustNewHTTPRequest("POST", s.URL+"/index/i", nil))
	if !called {
		t.Error("Count isn't called")
	}
}

func TestStatsCount_DeleteIndex(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	// Create index.
	if _, err := hldr.CreateIndexIfNotExists("i", pilosa.IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	called := false
	s.Handler.API.Holder.Stats = &MockStats{
		mockCount: func(name string, value int64, rate float64) {
			if name != "deleteIndex" {
				t.Errorf("Expected deleteIndex, Results %s", name)
			}

			called = true
		},
	}
	http.DefaultClient.Do(test.MustNewHTTPRequest("DELETE", s.URL+"/index/i", strings.NewReader("")))
	if !called {
		t.Error("Count isn't called")
	}
}

func TestStatsCount_CreateField(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()

	// Create index.
	if _, err := hldr.CreateIndexIfNotExists("i", pilosa.IndexOptions{}); err != nil {
		t.Fatal(err)
	}
	called := false
	s.Handler.API.Holder.Stats = &MockStats{
		mockCountWithTags: func(name string, value int64, rate float64, index []string) {
			if name != "createField" {
				t.Errorf("Expected createField, Results %s", name)
			}
			if index[0] != "index:i" {
				t.Errorf("Expected index:i, Results %s", index)
			}

			called = true
		},
	}
	http.DefaultClient.Do(test.MustNewHTTPRequest("POST", s.URL+"/index/i/field/f", nil))
	if !called {
		t.Error("Count isn't called")
	}
}

func TestStatsCount_DeleteField(t *testing.T) {
	hldr := test.MustOpenHolder()
	defer hldr.Close()

	s := test.NewServer()
	s.Handler.API.Holder = hldr.Holder
	defer s.Close()
	called := false
	// Create index.
	indx, _ := hldr.CreateIndexIfNotExists("i", pilosa.IndexOptions{})
	if _, err := indx.CreateFieldIfNotExists("test", pilosa.FieldOptions{}); err != nil {
		t.Fatal(err)
	}
	s.Handler.API.Holder.Stats = &MockStats{
		mockCountWithTags: func(name string, value int64, rate float64, index []string) {
			if name != "deleteField" {
				t.Errorf("Expected deleteField, Results %s", name)
			}
			if index[0] != "index:i" {
				t.Errorf("Expected index:i, Results %s", index)
			}

			called = true
		},
	}
	http.DefaultClient.Do(test.MustNewHTTPRequest("DELETE", s.URL+"/index/i/field/f", strings.NewReader("")))
	if !called {
		t.Error("Count isn't called")
	}
}

type MockStats struct {
	mockCount         func(name string, value int64, rate float64)
	mockCountWithTags func(name string, value int64, rate float64, tags []string)
}

func (s *MockStats) Count(name string, value int64, rate float64) {
	if s.mockCount != nil {
		s.mockCount(name, value, rate)
	}
}

func (s *MockStats) CountWithCustomTags(name string, value int64, rate float64, tags []string) {
	if s.mockCountWithTags != nil {
		s.mockCountWithTags(name, value, rate, tags)
	}
}

func (c *MockStats) Tags() []string                                        { return nil }
func (c *MockStats) WithTags(tags ...string) pilosa.StatsClient            { return c }
func (c *MockStats) Gauge(name string, value float64, rate float64)        {}
func (c *MockStats) Histogram(name string, value float64, rate float64)    {}
func (c *MockStats) Set(name string, value string, rate float64)           {}
func (c *MockStats) Timing(name string, value time.Duration, rate float64) {}
func (c *MockStats) SetLogger(logger pilosa.Logger)                        {}
func (c *MockStats) Open()                                                 {}
func (c *MockStats) Close() error                                          { return nil }
