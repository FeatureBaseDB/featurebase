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

	"github.com/molecula/featurebase/v2/cmd"
	"github.com/molecula/featurebase/v2/testhook"
	"github.com/spf13/cobra"
)

// failErr calls t.Fatal if err != nil and adds the optional context to the
// error message.
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

	// NOTE: if cmd.Execute doesn't return, then this select (and
	// therefore the one-second timeout, won't be reached)
	select {
	case <-done:
	case <-time.After(time.Second * 1):
		t.Fatal("Test failed due to command execution timeout")
	}
	return output, readErr
}

// ExecNewRootCommand executes the pilosa root command with the given arguments
// and returns its output. It will fail if the command does not complete within
// 1 second.
func ExecNewRootCommand(t *testing.T, args ...string) (string, error) {
	out, w := io.Pipe()
	rc := cmd.NewRootCommand(os.Stdin, w, w)
	rc.SetArgs(args)
	output, err := tExec(t, rc, out, w)
	return string(output), err
}

// validator is a simple helper to avoid repeated `if err != nil` checks in
// validation code. One can use it to check that several pairs of things are
// equal, and at the end access an informative error message about the first
// non-equal pair encountered (or nil if all were equal)
type validator struct {
	err error
}

// Check that two things are equal, and if not set v.err to a descriptive error
// message.
func (v *validator) Check(actual, expected interface{}) {
	if v.err != nil {
		return
	}
	if !reflect.DeepEqual(actual, expected) {
		v.err = fmt.Errorf("Actual: '%v' is not equal to '%v'", actual, expected)
	}
}

// Error returns the validator's error value if any v.Check call found an error.
func (v *validator) Error() error { return v.err }

// commandTest represents all possible ways to configure a pilosa command, as
// well as a function for validating whether the command worked as expected.
// args should be set to everything that comes after "pilosa" on the comand
// line.
type commandTest struct {
	args           []string
	env            map[string]string
	cfgFileContent string
	validation     func() error
}

// executeDry sets up and executes each commandTest with the --dry-run flag set
// to true, and then executes the tests validation function. This stops
// execution after PersistentPreRunE (and so before the command's Run or RunE
// function is called). This is useful for verifying that configuration happened
// properly.
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

// setupCommand sets up all the configuration specified in the commandTest so
// that it can be run. This includes setting environment variables, and creating
// a temp config file with the cfgFileContent string as its content.
func (ct *commandTest) setupCommand(t *testing.T) *cobra.Command {
	// make config file
	cfgFile, err := testhook.TempFile(t, "cmdconf")
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
	// address common case where system might have postgres bind
	// setting that an existing running Pilosa is using (causing test
	// failures due to port conflict)
	os.Setenv("PILOSA_POSTGRES_BIND", "")

	// make command and set args
	rc := cmd.NewRootCommand(strings.NewReader(""), ioutil.Discard, ioutil.Discard)
	rc.SetArgs(ct.args)

	err = cfgFile.Close()
	failErr(t, err, "closing config file")

	return rc
}

// reset the environment after setup/run of a commandTest.
func (ct *commandTest) reset() {
	for name := range ct.env {
		os.Setenv(name, "")
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

func TestRootCommand_Config(t *testing.T) {
	file, err := testhook.TempFile(t, "test.conf")
	if err != nil {
		t.Fatalf("creating config file: %v", err)
	}
	config := `data-dir = "/tmp/pil5_0"
bind = "127.0.0.1:10101"

[cluster]
  replicas = 2
  partitions = 128`
	if _, err := file.Write([]byte(config)); err != nil {
		t.Fatalf("writing config file: %v", err)
	}
	file.Close()
	_, err = ExecNewRootCommand(t, "server", "--config", file.Name())
	if err == nil || err.Error() != "invalid option in configuration file: cluster.partitions" {
		t.Fatalf("Expected invalid option in configuration file, but err: '%v'", err)
	}
}
