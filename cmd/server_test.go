package cmd_test

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"os"

	"github.com/pilosa/pilosa"
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

func TestServerConfig(t *testing.T) {
	actualDataDir, err := ioutil.TempDir("", "")
	failErr(t, err, "making data dir")
	tests := []commandTest{
		// TEST 0
		{
			args: []string{"server", "--data-dir", actualDataDir, "--cluster.hosts", "example.com:10101,example.com:10110"},
			env:  map[string]string{"PILOSA_DATA_DIR": "/tmp/myEnvDatadir", "PILOSA_CLUSTER.POLL_INTERVAL": "3m2s"},
			cfgFileContent: `
data-dir = "/tmp/myFileDatadir"
bind = "localhost:0"

[cluster]
  poll-interval = "45s"
  replicas = 2
  hosts = [
   "localhost:19444",
   ]
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Serve.Config.DataDir, actualDataDir)
				v.Check(cmd.Serve.Config.Host, "localhost:0")
				v.Check(cmd.Serve.Config.Cluster.ReplicaN, 2)
				v.Check(cmd.Serve.Config.Cluster.Nodes, []string{"example.com:10101", "example.com:10110"})
				v.Check(cmd.Serve.Config.Cluster.PollingInterval, pilosa.Duration(time.Second*182))
				return v.Error()
			},
		},
		// TEST 1
		{
			args: []string{"server"},
			env:  map[string]string{"PILOSA_CLUSTER.HOSTS": "example.com:1110,example.com:1111"},
			cfgFileContent: `
[cluster]
  hosts = [
   "localhost:19444",
   ]
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Serve.Config.Cluster.Nodes, []string{"example.com:1110", "example.com:1111"})
				return v.Error()
			},
		},
		// TEST 2
		{
			args: []string{"server"},
			env:  map[string]string{},
			cfgFileContent: `
[cluster]
  poll-interval = "2m0s"
  hosts = [
   "localhost:19444",
   ]

`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Serve.Config.Cluster.Nodes, []string{"localhost:19444"})
				v.Check(cmd.Serve.Config.Cluster.PollingInterval, pilosa.Duration(time.Minute*2))
				return v.Error()
			},
		},
	}

	for i, test := range tests {
		com := test.setupCommand(t)
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
		test.reset()
	}
}

func (ct commandTest) setupCommand(t *testing.T) *cobra.Command {
	// make config file
	cfgFile, err := ioutil.TempFile("", "")
	failErr(t, err, "making temp file")
	_, err = cfgFile.WriteString(ct.cfgFileContent)
	failErr(t, err, "writing config to temp file")

	// set up config file args/env
	ct.env["PILOSA_CONFIG"] = cfgFile.Name()
	ct.args = append(ct.args[:1], append([]string{"--config=" + cfgFile.Name()}, ct.args[1:]...)...)

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

func (ct commandTest) reset() {
	for name, _ := range ct.env {
		os.Setenv(name, "")
	}
}
