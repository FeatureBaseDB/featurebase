package bench_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/pilosa/pilosa/bench"
)

func TestPrettify(t *testing.T) {
	res := make(map[string]interface{}, 1)
	res["0"] = map[string]interface{}{
		"avg":   time.Duration(12345),
		"max":   time.Duration(23456),
		"int":   3456,
		"slice": []time.Duration{123, 234},
	}
	res["runtimes"] = map[string]interface{}{
		"0":     time.Duration(10e9),
		"total": time.Duration(10e9),
	}
	resPretty := bench.Prettify(res)

	jsonString := new(bytes.Buffer)
	enc := json.NewEncoder(jsonString)
	enc.SetIndent("", "  ")

	err := enc.Encode(resPretty)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	expected := `
{
  "0": {
    "avg": "12.345µs",
    "max": "23.456µs",
    "slice": [
      "123ns",
      "234ns"
    ],
    "int": 3456
  },
  "runtimes": {
    "0": "10s",
    "total": "10s"
  }
}
`[1:]

	if jsonString.String() != expected {
		t.Fatalf("failure")
	}
}
