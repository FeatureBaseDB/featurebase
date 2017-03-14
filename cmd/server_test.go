package cmd_test

import (
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"testing"

	"os"

	"github.com/pilosa/pilosa/cmd"
	"github.com/spf13/cobra"
)

func TestServerHelp(t *testing.T) {
	output := ExecNewRootCommand(t, "server", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") {
		t.Fatalf("Command 'server --help' not working, got: %s", output)
	}
}

type validator struct {
	err error
}

func (v *validator) Check(actual, expected interface{}) {
	if v.err != nil {
		return
	}
	if actual != expected {
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

func TestServerConfig(t *testing.T) {
	actualDataDir, err := ioutil.TempDir("", "")
	failErr(t, err, "making data dir")
	tests := []commandTest{
		{
			args: []string{"server", "--data-dir", actualDataDir},
			env:  map[string]string{"PILOSA_DATA_DIR": "/tmp/myEnvDatadir"},
			cfgFileContent: `
data-dir = "/tmp/myFileDatadir"
host = "localhost:0"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Serve.Config.DataDir, actualDataDir)
				v.Check(cmd.Serve.Config.Host, "localhost:0")
				return v.Error()
			},
		},
	}
	for i, test := range tests {
		com := setupCommand(t, test.args, test.env, test.cfgFileContent)
		wait := sync.Mutex{}
		wait.Lock()
		var execErr error
		go func() {
			execErr = com.Execute()
			wait.Unlock()
		}()
		// Serve.Close automatically waits for Serve.Run() to finish starting
		// the server.
		err := cmd.Serve.Close()
		failErr(t, err, "closing pilosa server command")
		wait.Lock() // make sure com.Execute finishes
		failErr(t, execErr, "executing command")

		if err := test.validation(); err != nil {
			t.Fatalf("Failed test %d due to: %v", i, err)
		}
	}
}

func setupCommand(t *testing.T, args []string, env map[string]string, cfgFileContent string) *cobra.Command {
	// make config file
	cfgFile, err := ioutil.TempFile("", "")
	failErr(t, err, "making temp file")
	_, err = cfgFile.WriteString(cfgFileContent)
	failErr(t, err, "writing config to temp file")

	// set up config file args/env
	env["PILOSA_CONFIG"] = cfgFile.Name()
	args = append(args[:1], append([]string{"--config=" + cfgFile.Name()}, args[1:]...)...)

	// set up env
	for name, val := range env {
		err = os.Setenv(name, val)
		failErr(t, err, fmt.Sprintf("setting environment variable '%s' to '%s'", name, val))
	}

	// make command and set args
	rc := cmd.NewRootCommand(strings.NewReader(""), ioutil.Discard, ioutil.Discard)
	rc.SetArgs(args)

	err = cfgFile.Close()
	failErr(t, err, "closing config file")

	return rc
}
