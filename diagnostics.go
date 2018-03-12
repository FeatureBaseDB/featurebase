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

package pilosa

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
	"github.com/sony/gobreaker"
)

// TODO: unique Cluster ID

// Default version check URL.
const (
	defaultVersionCheckURL = "https://diagnostics.pilosa.com/v0/version"
)

type versionResponse struct {
	Version string `json:"version"`
	Message string `json:"message"`
}

// DiagnosticsCollector represents a collector/sender of diagnostics data
type DiagnosticsCollector struct {
	mu          sync.Mutex
	host        string
	VersionURL  string
	version     string
	lastVersion string
	startTime   int64
	start       time.Time

	metrics map[string]interface{}

	client   *http.Client
	interval time.Duration

	cb        *gobreaker.CircuitBreaker
	logOutput io.Writer
}

// New returns a pointer to a new DiagnosticsCollector Client given an addr in the format "hostname:port".
func NewDiagnosticsCollector(host string) *DiagnosticsCollector {

	return &DiagnosticsCollector{
		host:       host,
		VersionURL: defaultVersionCheckURL,
		startTime:  time.Now().Unix(),
		start:      time.Now(),
		client:     http.DefaultClient,
		metrics:    make(map[string]interface{}),
		logOutput:  ioutil.Discard,
	}
}

// SetVersion of locally running Pilosa Cluster to check against master.
func (d *DiagnosticsCollector) SetVersion(v string) {
	d.version = v
	d.Set("Version", v)
}

// SetInterval of the diagnostic go routine and match with the circuit breaker timeout.
func (d *DiagnosticsCollector) SetInterval(i time.Duration) {
	d.interval = i
}

// Flush sends the current metrics.
func (d *DiagnosticsCollector) Flush() error {
	d.mu.Lock()
	d.metrics["Uptime"] = (time.Now().Unix() - d.startTime)
	buf, _ := d.encode()
	d.mu.Unlock()

	_, err := d.cb.Execute(func() (interface{}, error) {
		req, err := http.NewRequest("POST", d.host, bytes.NewReader(buf))
		req.Header.Set("Content-Type", "application/json")
		resp, err := d.client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		// TODO verify response
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return body, nil
	})

	return err
}

// Open configures the circuit breaker used by the HTTP client.
func (d *DiagnosticsCollector) Open() {
	var st gobreaker.Settings
	if d.interval > 0 {
		st.Timeout = d.interval * 2
	}
	d.cb = gobreaker.NewCircuitBreaker(st)

	d.logger().Printf("Pilosa is currently configured to send small diagnostics reports to our team every hour. More information here: https://www.pilosa.com/docs/latest/administration/#diagnostics")
}

// CheckVersion of the local build against Pilosa master.
func (d *DiagnosticsCollector) CheckVersion() error {
	var rsp versionResponse
	req, err := http.NewRequest("GET", d.VersionURL, nil)
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return fmt.Errorf("json decode: %s", err)
	}

	// Same a version as last test
	if rsp.Version == d.lastVersion {
		return nil
	}

	d.lastVersion = rsp.Version
	if err := d.compareVersion(rsp.Version); err != nil {
		d.logger().Printf("%s\n", err.Error())
	}

	return nil
}

// compareVersion check version strings.
func (d *DiagnosticsCollector) compareVersion(value string) error {
	currentVersion := VersionSegments(value)
	localVersion := VersionSegments(d.version)

	if localVersion[0] < currentVersion[0] { //Major
		return fmt.Errorf("Warning: You are running Pilosa %s. A newer version (%s) is available: https://github.com/pilosa/pilosa/releases", d.version, value)
	} else if localVersion[1] < currentVersion[1] && localVersion[0] == currentVersion[0] { // Minor
		return fmt.Errorf("Warning: You are running Pilosa %s. The latest Minor release is %s: https://github.com/pilosa/pilosa/releases", d.version, value)
	} else if localVersion[2] < currentVersion[2] && localVersion[0] == currentVersion[0] && localVersion[1] == currentVersion[1] { // Patch
		return fmt.Errorf("There is a new patch release of Pilosa available: %s: https://github.com/pilosa/pilosa/releases", value)
	}

	return nil
}

// Encode metrics maps into the json message format.
func (d *DiagnosticsCollector) encode() ([]byte, error) {
	return json.Marshal(d.metrics)
}

// Set adds a key value metric.
func (d *DiagnosticsCollector) Set(name string, value interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.metrics[name] = value
}

// SetLogger Set the logger output type.
func (d *DiagnosticsCollector) SetLogger(logger io.Writer) {
	d.logOutput = logger
}

// logger returns a logger that writes to LogOutput.
func (d *DiagnosticsCollector) logger() *log.Logger {
	return log.New(d.logOutput, "", log.LstdFlags)
}

// EnrichWithOSInfo adds OS information to the diagnostics payload.
func (d *DiagnosticsCollector) EnrichWithOSInfo() {
	osInfo, err := host.Info()
	if err != nil {
		d.logOutput.Write([]byte(err.Error()))
	}
	d.Set("HostUptime", osInfo.Uptime)

	platform, family, version, err := host.PlatformInformation()
	if err != nil {
		d.logOutput.Write([]byte(err.Error()))
	}
	d.Set("OSPlatform", platform)
	d.Set("OSFamily", family)
	d.Set("OSVersion", version)

	kernelVersion, err := host.KernelVersion()
	if err != nil {
		d.logOutput.Write([]byte(err.Error()))
	}
	d.Set("OSKernelVersion", kernelVersion)
}

// EnrichWithMemoryInfo adds memory information to the diagnostics payload.
func (d *DiagnosticsCollector) EnrichWithMemoryInfo() {
	memory, err := mem.VirtualMemory()
	if err != nil {
		d.logOutput.Write([]byte(err.Error()))
	}
	d.Set("MemFree", memory.Free)
	d.Set("MemTotal", memory.Total)
	d.Set("MemUsed", memory.Used)

}

// EnrichWithSchemaProperties adds schema info to the diagnostics payload.
func (d *DiagnosticsCollector) EnrichWithSchemaProperties(holder *Holder) {
	var numSlices uint64
	numFrames := 0
	numIndexes := 0
	bsiFieldCount := 0
	timeQuantumEnabled := false

	for _, index := range holder.Indexes() {
		numSlices += index.MaxSlice() + 1
		numIndexes += 1
		for _, frame := range index.Frames() {
			numFrames += 1
			if frame.rangeEnabled {
				if fields, err := frame.GetFields(); err == nil {
					bsiFieldCount += len(fields)
				}
			}
			if frame.TimeQuantum() != "" {
				timeQuantumEnabled = true
			}
		}
	}

	d.Set("NumIndexes", numIndexes)
	d.Set("NumFrames", numFrames)
	d.Set("NumSlices", numSlices)
	d.Set("BSIFieldCount", bsiFieldCount)
	d.Set("TimeQuantumEnabled", timeQuantumEnabled)
}

// VersionSegments returns the numeric segments of the version as a slice of ints.
func VersionSegments(segments string) []int {
	segments = strings.Trim(segments, "v")
	segments = strings.Split(segments, "-")[0]
	s := strings.Split(segments, ".")
	segmentSlice := make([]int, len(s))
	for i, v := range s {
		segmentSlice[i], _ = strconv.Atoi(v)
	}
	return segmentSlice
}
