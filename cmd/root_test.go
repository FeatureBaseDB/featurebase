package cmd_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"time"

	"github.com/pilosa/pilosa/cmd"
	"github.com/spf13/cobra"
)

func failErr(t *testing.T, err error, context ...string) {
	ctx := strings.Join(context, "; ")
	if err != nil {
		t.Fatal(ctx, ": ", err)
	}
}

// tExec executes the given `cmd`, which will be writing its output to `w`, and
// can be read from `out`. It will fail the test if the command does not return
// within 1 second. Useful for testing help messages and such.
func tExec(t *testing.T, cmd *cobra.Command, out io.Reader, w io.WriteCloser) (output []byte, err error) {
	done := make(chan struct{})
	var readErr error
	go func() {
		output, readErr = ioutil.ReadAll(out)
		close(done)
	}()
	err = cmd.Execute()
	if err != nil {
		return output, err
	}
	if err := w.Close(); err != nil {
		return output, fmt.Errorf("closing cmd's stdout: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second * 1):
		t.Fatal("Test failed due to command execution timeout")
	}
	return output, readErr
}

// ExecNewRootCommand executes the pilosa root command with the given arguments
// and returns it's output. It will fail if the command does not complete within
// 1 second.
func ExecNewRootCommand(t *testing.T, args ...string) (string, error) {
	out, w := io.Pipe()
	rc := cmd.NewRootCommand(os.Stdin, w, w)
	rc.SetArgs(args)
	output, err := tExec(t, rc, out, w)
	return string(output), err
}

type validator struct {
	err error
}

func (v *validator) Check(actual, expected interface{}) {
	if v.err != nil {
		return
	}
	if !reflect.DeepEqual(actual, expected) {
		v.err = fmt.Errorf("Actual: '%v' is not equal to '%v'", actual, expected)
	}
}
func (v *validator) Error() error { return v.err }

type commandTest struct {
	args           []string
	env            map[string]string
	cfgFileContent string
	validation     func() error
}

func (ct *commandTest) setupCommand(t *testing.T) *cobra.Command {
	// make config file
	cfgFile, err := ioutil.TempFile("", "")
	failErr(t, err, "making temp file")
	_, err = cfgFile.WriteString(ct.cfgFileContent)
	failErr(t, err, "writing config to temp file")

	// set up config file args/env
	ct.env["PILOSA_CONFIG"] = cfgFile.Name()

	// set up env
	for name, val := range ct.env {
		err = os.Setenv(name, val)
		failErr(t, err, fmt.Sprintf("setting environment variable '%s' to '%s'", name, val))
	}

	// make command and set args
	rc := cmd.NewRootCommand(strings.NewReader(""), ioutil.Discard, ioutil.Discard)
	rc.SetArgs(ct.args)

	err = cfgFile.Close()
	failErr(t, err, "closing config file")

	return rc
}

func (ct *commandTest) reset() {
	for name, _ := range ct.env {
		os.Setenv(name, "")
	}
}

func executeDry(t *testing.T, tests []commandTest) {
	for i, test := range tests {
		test.args = append(test.args[:1], append([]string{"--dry-run"}, test.args[1:]...)...)
		com := test.setupCommand(t)
		err := com.Execute()
		if err.Error() != "dry run" {
			t.Fatalf("Problem with test %d, err: '%v'", i, err)
		}
		if err := test.validation(); err != nil {
			t.Fatalf("Failed test %d due to: %v", i, err)
		}
		test.reset()
	}
}

func TestRootCommand(t *testing.T) {
	outStr, err := ExecNewRootCommand(t, "--help")
	if !strings.Contains(outStr, "Usage:") ||
		!strings.Contains(outStr, "Available Commands:") ||
		!strings.Contains(outStr, "--help") || err != nil {
		t.Fatalf("Expected standard usage message from RootCommand, but err: '%v', output: '%s'", err, outStr)
	}
}
