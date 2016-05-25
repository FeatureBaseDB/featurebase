package main_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	main "github.com/umbel/pilosa/cmd/pilosa-bench"
)

// Ensure server count flag can be parsed.
func TestMain_ParseFlags_ServerN(t *testing.T) {
	m := NewMain()
	if err := m.ParseFlags([]string{"-servers", "2"}); err != nil {
		t.Fatal(err)
	} else if m.ServerN != 2 {
		t.Fatalf("unexpected server count: %d", m.ServerN)
	}
}

// Ensure replication factor flag can be parsed.
func TestMain_ParseFlags_ReplicaN(t *testing.T) {
	m := NewMain()
	if err := m.ParseFlags([]string{"-servers=4", "-replicas", "3"}); err != nil {
		t.Fatal(err)
	} else if m.ReplicaN != 3 {
		t.Fatalf("unexpected replica count: %d", m.ReplicaN)
	}
}

// Ensure verbose flag can be parsed.
func TestMain_ParseFlags_Verbose(t *testing.T) {
	m := NewMain()
	if err := m.ParseFlags([]string{"-v"}); err != nil {
		t.Fatal(err)
	} else if !m.Verbose {
		t.Fatalf("expected verbose flag")
	}
}

// Ensure work flag can be parsed.
func TestMain_ParseFlags_Work(t *testing.T) {
	m := NewMain()
	if err := m.ParseFlags([]string{"-work"}); err != nil {
		t.Fatal(err)
	} else if !m.Work {
		t.Fatalf("expected work flag")
	}
}

// Main represents a test wrapper for main.Main.
type Main struct {
	*main.Main

	Stdin  bytes.Buffer
	Stdout bytes.Buffer
	Stderr bytes.Buffer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	m := &Main{Main: main.NewMain()}
	m.Main.Stdin = &m.Stdin
	m.Main.Stdout = &m.Stdout
	m.Main.Stderr = &m.Stderr

	if testing.Verbose() {
		m.Main.Stdout = io.MultiWriter(os.Stdout, m.Main.Stdout)
		m.Main.Stderr = io.MultiWriter(os.Stderr, m.Main.Stderr)
	}

	return m
}
