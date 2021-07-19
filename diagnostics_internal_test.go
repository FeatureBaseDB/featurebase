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

package pilosa

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/logger"
)

func TestDiagnosticsClient(t *testing.T) {
	// Mock server.
	server := httptest.NewServer(nil)
	defer server.Close()

	// Create a new client.
	d := newDiagnosticsCollector(server.URL)

	d.Set("gg", 10)
	d.Set("ss", "ss")

	data, err := d.encode()
	if err != nil {
		t.Fatal(err)
	}

	// Test the recorded metrics, note that some types are skipped.
	var eq bool
	output1 := []byte(`{"gg":10,"ss":"ss"}`)
	if eq, err = compareJSON(data, output1); err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Fatalf("unexpected diagnostics: %+v", string(data))
	}

	// Test the metrics after a flush.
	d.Flush()
	data, err = d.encode()
	if err != nil {
		t.Fatal(err)
	}

	output2 := []byte(`{"gg":10,"ss":"ss","Uptime":0}`)
	if eq, err = compareJSON(data, output2); err != nil {
		t.Fatal(err)
	}
	if !eq {
		t.Fatalf("unexpected diagnostics after flush: %+v", string(data))
	}
}

func TestDiagnosticsVersion_Parse(t *testing.T) {
	version := "0.1.1"
	vs := versionSegments(version)

	output := []int{0, 1, 1}
	if !reflect.DeepEqual(vs, output) {
		t.Fatalf("unexpected version: %+v", vs)
	}
}

func TestDiagnosticsVersion_Compare(t *testing.T) {
	d := newDiagnosticsCollector("localhost:10101")

	version := "v0.1.1"
	d.SetVersion(version)

	err := d.compareVersion("v1.7.0")
	if !strings.Contains(err.Error(), "a newer version") {
		t.Fatalf("Expected a newer version is available, actual error: %s", err)
	}
	err = d.compareVersion("1.7.0")
	if !strings.Contains(err.Error(), "a newer version") {
		t.Fatalf("Expected a newer version is available, actual error: %s", err)
	}
	err = d.compareVersion("0.7.0")
	if !strings.Contains(err.Error(), "the latest minor release is") {
		t.Fatalf("Expected Minor Version Missmatch, actual error: %s", err)
	}
	err = d.compareVersion("0.1.2")
	if !strings.Contains(err.Error(), "there is a new patch release of Pilosa") {
		t.Fatalf("Expected Patch Version Missmatch, actual error: %s", err)
	}
	err = d.compareVersion("0.1.1")
	if err != nil {
		t.Fatalf("Versions should match")
	}
	d.SetVersion("v1.7.0")
	err = d.compareVersion("0.7.2")
	if err != nil {
		t.Fatalf("Local version is greater")
	}
}

func TestDiagnosticsVersion_Check(t *testing.T) {
	// Mock server.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(versionResponse{
			Version: "1.1.1",
		})
		if err != nil {
			t.Fatalf("couldn't encode version response: %v", err)
		}
	}))
	defer server.Close()

	// Create a new client.
	d := newDiagnosticsCollector("localhost:10101")

	logs := logger.NewCaptureLogger()
	d.Logger = logs

	version := "0.1.1"
	d.SetVersion(version)
	d.VersionURL = server.URL

	err := d.CheckVersion()
	if err != nil {
		t.Fatalf("checking version: %v", err)
	}
	if len(logs.Prints) != 1 {
		t.Fatalf("expected a version upgrade message")
	}
	if !strings.Contains(logs.Prints[0], "a newer version") {
		t.Fatalf("expected version upgrade message, got '%s'", logs.Prints[0])
	}
}

var _ = compareJSON

func compareJSON(a, b []byte) (bool, error) {
	var j1, j2 interface{}
	if err := json.Unmarshal(a, &j1); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j1, j2), nil
}

func BenchmarkDiagnostics(b *testing.B) {

	// Mock server.
	server := httptest.NewServer(nil)
	defer server.Close()

	// Create a new client.
	d := newDiagnosticsCollector(server.URL)

	prev := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(prev)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d.Set("cc", 1)
			d.Set("gg", "test")
		}
	})
}
