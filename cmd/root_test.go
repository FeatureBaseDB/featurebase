package cmd_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"time"

	"github.com/pilosa/pilosa/cmd"
	"github.com/spf13/cobra"
)

func tExec(t *testing.T, cmd *cobra.Command, out io.Reader, w io.WriteCloser) (output []byte) {
	done := make(chan struct{})
	go func() {
		var err error
		output, err = ioutil.ReadAll(out)
		if err != nil {
			t.Fatal(err)
		}
		close(done)
	}()
	fmt.Println("executing")
	err := cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing cmd's stdout: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second * 1):
		t.Fatal("Test failed due to command execution timeout")
	}
	return output
}

func ExecNewRootCommand(t *testing.T, args ...string) string {
	out, w := io.Pipe()
	rc := cmd.NewRootCommand(os.Stdin, w, w)
	rc.SetArgs(args)
	output := tExec(t, rc, out, w)
	return string(output)
}

func TestRootCommand(t *testing.T) {
	outStr := ExecNewRootCommand(t, "--help")
	if !strings.Contains(outStr, "Usage:") ||
		!strings.Contains(outStr, "Available Commands:") ||
		!strings.Contains(outStr, "--help") {
		t.Fatalf("Expected standard usage message from RootCommand, but got: %s", outStr)
	}
}
