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
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2/cmd"
	_ "github.com/pilosa/pilosa/v2/test"
	"github.com/pilosa/pilosa/v2/toml"
	"github.com/pkg/errors"
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
	logFile, err := ioutil.TempFile("", "")
	failErr(t, err, "making log file")
	tests := []commandTest{
		// TEST 0
		{
			args: []string{"server", "--data-dir", actualDataDir, "--cluster.hosts", "localhost:42454,localhost:10110", "--bind", "localhost:42454", "--bind-grpc", "localhost:30112", "--translation.map-size", "100000"},
			env: map[string]string{
				"PILOSA_DATA_DIR":                "/tmp/myEnvDatadir",
				"PILOSA_LONG_QUERY_TIME":         "1m30s",
				"PILOSA_CLUSTER_LONG_QUERY_TIME": "1m30s",
				"PILOSA_MAX_WRITES_PER_REQUEST":  "2000",
				"PILOSA_PROFILE_BLOCK_RATE":      "9123",
				"PILOSA_PROFILE_MUTEX_FRACTION":  "444",
			},
			cfgFileContent: `
	data-dir = "/tmp/myFileDatadir"
	bind = "localhost:0"
	bind-grpc = "localhost:0"
	max-writes-per-request = 3000
	long-query-time = "1m10s"
	
	[cluster]
		disabled = true
		replicas = 2
		hosts = [
			"localhost:19444",
		]
		long-query-time = "1m10s"
	[profile]
		block-rate = 100
		mutex-fraction = 10
	`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Server.Config.DataDir, actualDataDir)
				v.Check(cmd.Server.Config.Bind, "localhost:42454")
				v.Check(cmd.Server.Config.Cluster.ReplicaN, 2)
				v.Check(cmd.Server.Config.Cluster.Hosts, []string{"localhost:42454", "localhost:10110"})
				v.Check(cmd.Server.Config.LongQueryTime, toml.Duration(time.Second*90))
				v.Check(cmd.Server.Config.Cluster.LongQueryTime, toml.Duration(time.Second*90))
				v.Check(cmd.Server.Config.MaxWritesPerRequest, 2000)
				v.Check(cmd.Server.Config.Translation.MapSize, 100000)
				v.Check(cmd.Server.Config.Profile.BlockRate, 9123)
				v.Check(cmd.Server.Config.Profile.MutexFraction, 444)
				return v.Error()
			},
		},
		// TEST 1
		{
			args: []string{"server",
				"--anti-entropy.interval", "9m0s",
				"--profile.block-rate", "4832",
				"--profile.mutex-fraction", "8290",
			},
			env: map[string]string{
				"PILOSA_CLUSTER_HOSTS":          "localhost:1110,localhost:1111",
				"PILOSA_BIND":                   "localhost:1110",
				"PILOSA_TRANSLATION_MAP_SIZE":   "100000",
				"PILOSA_PROFILE_BLOCK_RATE":     "9123",
				"PILOSA_PROFILE_MUTEX_FRACTION": "444",
			},
			cfgFileContent: `
	bind = "localhost:0"
	bind-grpc = "localhost:0"
	data-dir = "` + actualDataDir + `"
	[cluster]
		disabled = true
		hosts = [
			"localhost:19444",
		]
	[profile]
		block-rate = 100
		mutex-fraction = 10
	`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Server.Config.Cluster.Hosts, []string{"localhost:1110", "localhost:1111"})
				v.Check(cmd.Server.Config.AntiEntropy.Interval, toml.Duration(time.Minute*9))
				v.Check(cmd.Server.Config.Translation.MapSize, 100000)
				v.Check(cmd.Server.Config.Profile.BlockRate, 4832)
				v.Check(cmd.Server.Config.Profile.MutexFraction, 8290)
				return v.Error()
			},
		},
		// TEST 2
		{
			args: []string{"server", "--log-path", logFile.Name(), "--cluster.disabled", "true", "--translation.map-size", "100000"},
			env:  map[string]string{},
			cfgFileContent: `
	bind = "localhost:19444"
	bind-grpc = "localhost:29444"
	data-dir = "` + actualDataDir + `"
	[cluster]
		hosts = [
			"localhost:19444",
		]
	[anti-entropy]
		interval = "11m0s"
	[metric]
		service = "statsd"
		host = "127.0.0.1:8125"
	[profile]
		block-rate = 5352
		mutex-fraction = 91

	`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Server.Config.Cluster.Hosts, []string{"localhost:19444"})
				v.Check(cmd.Server.Config.AntiEntropy.Interval, toml.Duration(time.Minute*11))
				v.Check(cmd.Server.Config.LogPath, logFile.Name())
				v.Check(cmd.Server.Config.Metric.Service, "statsd")
				v.Check(cmd.Server.Config.Metric.Host, "127.0.0.1:8125")
				v.Check(cmd.Server.Config.Profile.BlockRate, 5352)
				v.Check(cmd.Server.Config.Profile.MutexFraction, 91)
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
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
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
			if execErr != nil {
				t.Fatalf("executing server command: %v", execErr)
			}
			err := cmd.Server.Close()
			failErr(t, err, "closing pilosa server command")
			<-executed
			failErr(t, execErr, "executing command")

			if err := test.validation(); err != nil {
				t.Fatalf("Failed test %d due to: %v", i, err)
			}
			test.reset()
		})
	}
}
func TestServerConfig_DeprecateLongQueryTime(t *testing.T) {
	actualDataDir, err := ioutil.TempDir("", "")
	failErr(t, err, "making data dir")

	tests := []commandTest{
		// TEST 0
		{
			args: []string{"server", "--long-query-time", "1m10s"},
			env:  map[string]string{},
			cfgFileContent: `
            	bind = "localhost:0"
            	bind-grpc = "localhost:0"
             	data-dir = "` + actualDataDir + `"
                [gossip]
                  port = "14321"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Server.Config.LongQueryTime, toml.Duration(time.Second*70))
				v.Check(toml.Duration(cmd.Server.API.LongQueryTime()), toml.Duration(time.Second*70))
				return v.Error()
			},
		},
		// TEST 1
		{
			args: []string{"server", "--cluster.long-query-time", "1m20s"},
			env:  map[string]string{},
			cfgFileContent: `
            	bind = "localhost:0"
            	bind-grpc = "localhost:0"
                [gossip]
                  port = "14321"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Server.Config.Cluster.LongQueryTime, toml.Duration(time.Second*80))
				v.Check(toml.Duration(cmd.Server.API.LongQueryTime()), toml.Duration(time.Second*80))
				return v.Error()
			},
		},
		// TEST 2: Use old value if both are provided because it is the simplest implementation
		{
			args: []string{"server", "--long-query-time", "50s", "--cluster.long-query-time", "1m30s"},
			env:  map[string]string{},
			cfgFileContent: `
            	bind = "localhost:0"
            	bind-grpc = "localhost:0"
                [gossip]
                  port = "14321"
`,
			validation: func() error {
				v := validator{}
				v.Check(cmd.Server.Config.LongQueryTime, toml.Duration(time.Second*50))
				v.Check(toml.Duration(cmd.Server.Config.Cluster.LongQueryTime), toml.Duration(time.Second*90))
				v.Check(toml.Duration(cmd.Server.API.LongQueryTime()), toml.Duration(time.Second*90))
				return v.Error()
			},
		},
	}

	// run server tests
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
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
			if execErr != nil {
				t.Fatalf("executing server command: %v", execErr)
			}
			err := cmd.Server.Close()
			failErr(t, err, "closing pilosa server command")
			<-executed
			failErr(t, execErr, "executing command")

			if err := test.validation(); err != nil {
				t.Fatalf("Failed test %d due to: %v", i, err)
			}
			test.reset()
		})
	}
}
