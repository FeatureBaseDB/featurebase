package bench_test

import (
	"bytes"
	"log"
	"testing"

	"io/ioutil"

	"os"

	"github.com/pilosa/pilosa/bench"
)

func TestImportInit(t *testing.T) {
	imp := bench.Import{
		BaseBitmapID:      0,
		MaxBitmapID:       10,
		BaseProfileID:     0,
		MaxProfileID:      10,
		RandomBitmapOrder: false,
		MinBitsPerMap:     2,
		MaxBitsPerMap:     3,
		AgentControls:     "width",
		Seed:              0,
	}

	imp.Init([]string{"blah"}, 2)
	f, err := os.Open(imp.Paths[0])
	if err != nil {
		t.Fatalf("Couldn't open file: %v, err: %v", imp.Paths[0], err)
	}
	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatalf("error reading file: %v", err)
	}

	expected := `
0,21
0,22
1,22
1,20
2,22
2,26
3,21
3,23
4,21
4,22
5,20
5,28
6,23
6,27
7,20
7,20
8,29
8,23
9,29
9,23
`[1:]

	if string(bytes) != expected {
		t.Fatalf("unexpected result: %v", string(bytes))
	}

	log.Println(imp)
}

func TestGenerateImportCSVNonRand(t *testing.T) {
	b := bytes.NewBuffer(make([]byte, 0))

	bench.GenerateImportCSV(b, 0, 10, 20, 30, 2, 3, 2, false)

	bytes, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatalf("Error reading buffer: %v", err)
	}

	expected := `
0,21
0,22
1,22
1,20
2,22
2,26
3,21
3,23
4,21
4,22
5,20
5,28
6,23
6,27
7,20
7,20
8,29
8,23
9,29
9,23
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
