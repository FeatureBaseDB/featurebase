package cmd_test

import (
	"strings"
	"testing"

	"github.com/pilosa/pilosa/cmd"
)

func TestBenchHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "bench", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") ||
		!strings.Contains(output, "pilosa bench") || err != nil {
		t.Fatalf("Command 'bench --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestBenchConfig(t *testing.T) {
	tests := []commandTest{
		{
			args: []string{"bench", "--operation", "set-bit"},
			env:  map[string]string{"PILOSA_HOST": "localhost:12345"},
			cfgFileContent: `
index = "myindex"
frame = "f1"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Bencher.Host, "localhost:12345")
				v.Check(cmd.Bencher.Index, "myindex")
				v.Check(cmd.Bencher.Frame, "f1")
				v.Check(cmd.Bencher.Op, "set-bit")
				v.Check(cmd.Bencher.N, 0)
				return v.Error()
			},
		},
	}
	executeDry(t, tests)
}
