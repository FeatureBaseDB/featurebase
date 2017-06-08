// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd_test

import (
	"errors"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/cmd"
	_ "github.com/pilosa/pilosa/test"
)

func TestServerHelp(t *testing.T) {
	output, err := ExecNewRootCommand(t, "server", "--help")
	if !strings.Contains(output, "Usage:") ||
		!strings.Contains(output, "Flags:") || err != nil {
		t.Fatalf("Command 'server --help' not working, err: '%v', output: '%s'", err, output)
	}
}

func TestServerConfig(t *testing.T) {
	actualDataDir, err := ioutil.TempDir("", "")
	failErr(t, err, "making data dir")
	profFile, err := ioutil.TempFile("", "")
	failErr(t, err, "making temp file")
	logFile, err := ioutil.TempFile("", "")
	failErr(t, err, "making log file")
	tests := []commandTest{
		// TEST 0
		{
			args: []string{"server", "--data-dir", actualDataDir, "--cluster.hosts", "example.com:10111,example.com:10110", "--bind", "example.com:10111"},
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
				v.Check(cmd.Server.Config.DataDir, actualDataDir)
				v.Check(cmd.Server.Config.Host, "example.com:10111")
				v.Check(cmd.Server.Config.Cluster.ReplicaN, 2)
				v.Check(cmd.Server.Config.Cluster.Hosts, []string{"example.com:10111", "example.com:10110"})
				v.Check(cmd.Server.Config.Cluster.PollingInterval, pilosa.Duration(time.Second*182))
				return v.Error()
			},
		},
		// TEST 1
		{
			args: []string{"server", "--anti-entropy.interval", "9m0s"},
			env:  map[string]string{"PILOSA_CLUSTER.HOSTS": "example.com:1110,example.com:1111", "PILOSA_BIND": "example.com:1110"},
			cfgFileContent: `
	bind = "localhost:0"
	data-dir = "` + actualDataDir + `"
	[cluster]
		hosts = [
			"localhost:19444",
		]
	[plugins]
		path = "/var/sloth"
	`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Server.Config.Cluster.Hosts, []string{"example.com:1110", "example.com:1111"})
				v.Check(cmd.Server.Config.Plugins.Path, "/var/sloth")
				v.Check(cmd.Server.Config.AntiEntropy.Interval, pilosa.Duration(time.Minute*9))
				return v.Error()
			},
		},
		// TEST 2
		{
			args: []string{"server", "--log-path", logFile.Name()},
			env:  map[string]string{"PILOSA_PROFILE.CPU_TIME": "1m"},
			cfgFileContent: `
	bind = "localhost:19444"
	data-dir = "` + actualDataDir + `"
	[cluster]
		poll-interval = "2m0s"
		hosts = [
			"localhost:19444",
		]
	[anti-entropy]
		interval = "11m0s"
	[profile]
		cpu = "` + profFile.Name() + `"
		cpu-time = "35s"
	[metric]
		service = "statsd"
		host = "127.0.0.1:8125"
	`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Server.Config.Cluster.Hosts, []string{"localhost:19444"})
				v.Check(cmd.Server.Config.Cluster.PollingInterval, pilosa.Duration(time.Minute*2))
				v.Check(cmd.Server.Config.AntiEntropy.Interval, pilosa.Duration(time.Minute*11))
				v.Check(cmd.Server.CPUProfile, profFile.Name())
				v.Check(cmd.Server.CPUTime, time.Minute)
				v.Check(cmd.Server.Config.LogPath, logFile.Name())
				v.Check(cmd.Server.Config.Metric.Service, "statsd")
				v.Check(cmd.Server.Config.Metric.Host, "127.0.0.1:8125")
				if v.Error() != nil {
					return v.Error()
				}
				// confirm log file was written
				info, err := logFile.Stat()
				if err != nil || info.Size() == 0 {
					// NOTE: this test assumes that something is being written to the log
					// currently, that is relying on log: "index sync monitor initializing"
					return errors.New("Log file was not written!")
				}
				return nil
			},
		},
	}

	// run server tests
	for i, test := range tests {
		com := test.setupCommand(t)
		executed := make(chan struct{})
		var execErr error
		go func() {
			execErr = com.Execute()
			close(executed)
		}()
		select {
		case <-cmd.Server.Started:
		case <-executed:
		}
		err := cmd.Server.Close()
		failErr(t, err, "closing pilosa server command")
		<-executed
		failErr(t, execErr, "executing command")

		if err := test.validation(); err != nil {
			t.Fatalf("Failed test %d due to: %v", i, err)
		}
		test.reset()
	}
}
