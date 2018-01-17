package diagnostics_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/pilosa/pilosa/diagnostics"
)

func TestDiagnosticsClient(t *testing.T) {
	// Mock server.
	server := httptest.NewServer(nil)
	defer server.Close()

	// Create a new client.
	d := diagnostics.New(server.URL)
	d.SetLogger(ioutil.Discard)
	d.Open()
	defer d.Close()

	d.Set("gg", 10)
	d.Set("ss", "ss")

	data, err := d.Encode()
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
	data, err = d.Encode()
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
	vs := diagnostics.VersionSegments(version)

	output := []int{0, 1, 1}
	if !reflect.DeepEqual(vs, output) {
		t.Fatalf("unexpected version: %+v", vs)
	}
}

func TestDiagnosticsVersion_Compare(t *testing.T) {
	d := diagnostics.New("localhost:10101")
	d.Open()
	defer d.Close()

	version := "v0.1.1"
	d.SetVersion(version)

	err := d.CompareVersion("v1.7.0")
	if !strings.Contains(err.Error(), "A newer version") {
		t.Fatalf("Expected a newer version is available, actual error: %s", err)
	}
	err = d.CompareVersion("1.7.0")
	if !strings.Contains(err.Error(), "A newer version") {
		t.Fatalf("Expected a newer version is available, actual error: %s", err)
	}
	err = d.CompareVersion("0.7.0")
	if !strings.Contains(err.Error(), "The latest Minor release is") {
		t.Fatalf("Expected Minor Version Missmatch, actual error: %s", err)
	}
	err = d.CompareVersion("0.1.2")
	if !strings.Contains(err.Error(), "There is a new patch release of Pilosa") {
		t.Fatalf("Expected Patch Version Missmatch, actual error: %s", err)
	}
	err = d.CompareVersion("0.1.1")
	if err != nil {
		t.Fatalf("Versions should match")
	}
	d.SetVersion("v1.7.0")
	err = d.CompareVersion("0.7.2")
	if err != nil {
		t.Fatalf("Local version is greater")
	}
}

func TestDiagnosticsVersion_Check(t *testing.T) {
	// Mock server.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(versionResponse{
			Version: "1.1.1",
		})
	}))
	defer server.Close()

	// Create a new client.
	d := diagnostics.New("localhost:10101")
	defer d.Close()

	version := "0.1.1"
	d.SetVersion(version)
	d.VersionURL = server.URL

	d.CheckVersion()
}

type versionResponse struct {
	Version string `json:"version"`
}

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
	d := diagnostics.New(server.URL)
	d.SetLogger(ioutil.Discard)
	defer d.Close()

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
