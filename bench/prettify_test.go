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

func prettyEncode(data map[string]interface{}) string {
	pretty := bench.Prettify(data)
	jsonString := new(bytes.Buffer)
	enc := json.NewEncoder(jsonString)
	enc.SetIndent("", "  ")
	err := enc.Encode(pretty)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}

	return jsonString.String()
}

func TestPrettifyString(t *testing.T) {
	res := make(map[string]interface{}, 1)
	res["0"] = "foobar"
	pretty := prettyEncode(res)

	expected := `
{
  "0": "foobar"
}
`[1:]

	if pretty != expected {
		t.Fatalf("Pretty string doesn't match")
	}
}

func TestPrettifyInt(t *testing.T) {
	res := make(map[string]interface{}, 1)
	res["0"] = 234567
	pretty := prettyEncode(res)

	expected := `
{
  "0": 234567
}
`[1:]

	if pretty != expected {
		t.Fatalf("Pretty int doesn't match")
	}
}

func TestPrettifyDuration(t *testing.T) {
	res := make(map[string]interface{}, 1)
	res["0"] = time.Duration(234567)
	pretty := prettyEncode(res)

	expected := `
{
  "0": "234.567µs"
}
`[1:]

	if pretty != expected {
		t.Fatalf("Pretty duration doesn't match")
	}
}

func TestPrettifyDurationSlice(t *testing.T) {
	res := make(map[string]interface{}, 1)
	res["0"] = []time.Duration{123, 234567, 34567890}
	pretty := prettyEncode(res)

	expected := `
{
  "0": [
    "123ns",
    "234.567µs",
    "34.56789ms"
  ]
}
`[1:]

	if pretty != expected {
		t.Fatalf("Pretty duration slice doesn't match")
	}
}
