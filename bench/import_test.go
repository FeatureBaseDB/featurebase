package bench_test

import (
	"bytes"
	"testing"

	"io/ioutil"

	"github.com/pilosa/pilosa/bench"
)

func TestGenerateImportCSVNonRand(t *testing.T) {
	b := bytes.NewBuffer(make([]byte, 0))

	bench.GenerateImportCSV(b, 0, 10, 21, 29, 2, 3, 0, false)

	bytes, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatalf("Error reading buffer: %v", err)
	}

	expected := `
0,21
0,24
1,23
1,28
2,21
2,22
3,21
3,25
4,23
4,21
5,28
5,28
6,25
6,23
7,26
7,23
8,21
8,23
9,25
9,24
`[1:]

	if string(bytes) != expected {
		t.Fatalf("unexpected value for generated csv: \n%v", string(bytes))
	}
}

func TestGenerateImportCSVRand(t *testing.T) {
	b := bytes.NewBuffer(make([]byte, 0))

	bench.GenerateImportCSV(b, 0, 10, 21, 29, 1, 4, 0, true)

	bytes, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatalf("Error reading buffer: %v", err)
	}

	expected := `
8,25
2,23
3,22
3,28
3,28
0,25
0,23
5,26
5,23
7,21
7,23
1,25
6,23
6,27
6,25
9,26
4,22
4,24
`[1:]

	if string(bytes) != expected {
		t.Fatalf("unexpected value for generated csv: \n%v", string(bytes))
	}
}
