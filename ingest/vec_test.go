package ingest

import (
	"errors"
	"testing"
)

type badTranslator struct{}

func (b badTranslator) TranslateKeys(keys ...string) (map[string]uint64, error) {
	if len(keys) == 0 {
		return nil, errors.New("no keys")
	}
	m := make(map[string]uint64)
	skip := true
	for i, k := range keys {
		if skip {
			skip = false
			continue
		}
		m[k] = uint64(i)
	}
	out := make([]uint64, len(keys)-1)
	for i := range out {
		out[i] = uint64(i)
	}
	return nil, nil
}

func (b badTranslator) TranslateIDs(...uint64) (map[uint64]string, error) {
	return nil, nil
}

func TestStringTableErrors(t *testing.T) {
	tbl := NewStringTable()
	btr := badTranslator{}
	_, keyErr := tbl.MakeIDMap(btr)
	if keyErr == nil {
		t.Fatalf("expected error passed up from failed translate, didn't get it")
	}
	a1, err := tbl.ID([]byte("a"))
	if err != nil {
		t.Fatalf("getting translation for key: %v", err)
	}
	b1, err := tbl.ID([]byte("b"))
	if err != nil {
		t.Fatalf("getting translation for key: %v", err)
	}
	_, keyErr = tbl.MakeIDMap(btr)
	if keyErr == nil {
		t.Fatalf("expected error for short translate, didn't get it")
	}
	tr := newStableTranslator()
	_, err = tr.TranslateKeys("c", "d")
	if err != nil {
		t.Fatalf("translating stray keys: %v", err)
	}
	m, err := tbl.MakeIDMap(tr)
	if err != nil {
		t.Fatalf("creating lookup: %v", err)
	}
	var y = []uint64{a1, b1}
	err = translateUnsigned(m, y)
	if err != nil {
		t.Fatalf("unexpected unsigned translation error: %v", err)
	}
	trResults, err := tr.TranslateKeys("a", "b")
	if err != nil {
		t.Fatalf("unexpected translation error: %v", err)
	}
	if y[0] != trResults["a"] {
		t.Fatalf("expected %d, got %d", trResults["a"], y[0])
	}
	if y[1] != trResults["b"] {
		t.Fatalf("expected %d, got %d", trResults["b"], y[1])
	}
	y[0] = a1
	y[1] = (a1 + b1 + 1) // assumed not to be any of them
	err = translateUnsigned(m, y)
	if err == nil {
		t.Fatalf("no error from translating invalid table")
	}
	z := []int64{int64(a1), int64(b1)}
	err = translateSigned(m, z)
	if err != nil {
		t.Fatalf("unexpected unsigned translation error: %v", err)
	}
	if uint64(z[0]) != trResults["a"] {
		t.Fatalf("expected %d, got %d", trResults["a"], z[0])
	}
	if uint64(z[1]) != trResults["b"] {
		t.Fatalf("expected %d, got %d", trResults["b"], z[1])
	}
	z[0] = int64(a1)
	z[1] = int64(a1 + b1 + 1) // assumed not to be any of them
	err = translateSigned(m, z)
	if err == nil {
		t.Fatalf("no error from translating invalid table")
	}
}
