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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/pilosa/logger"
	"github.com/pkg/errors"
)

// Default version check URL.
const (
	defaultVersionCheckURL = "https://diagnostics.pilosa.com/v0/version"
)

type versionResponse struct {
	Version string `json:"version"`
	Message string `json:"message"`
}

// diagnosticsCollector represents a collector/sender of diagnostics data.
type diagnosticsCollector struct {
	mu          sync.Mutex
	host        string
	VersionURL  string
	version     string
	lastVersion string
	startTime   int64
	start       time.Time

	metrics map[string]interface{}

	client *http.Client

	Logger logger.Logger

	server *Server
}

// newDiagnosticsCollector returns a new DiagnosticsCollector given an addr in the format "hostname:port".
func newDiagnosticsCollector(host string) *diagnosticsCollector { // nolint: unparam
	return &diagnosticsCollector{
		host:       host,
		VersionURL: defaultVersionCheckURL,
		startTime:  time.Now().Unix(),
		start:      time.Now(),
		client:     &http.Client{Timeout: 10 * time.Second},
		metrics:    make(map[string]interface{}),
		Logger:     logger.NopLogger,
	}
}

// SetVersion of locally running Pilosa Cluster to check against master.
func (d *diagnosticsCollector) SetVersion(v string) {
	d.version = v
	d.Set("Version", v)
}

// Flush sends the current metrics.
func (d *diagnosticsCollector) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.metrics["Uptime"] = (time.Now().Unix() - d.startTime)
	buf, err := d.encode()
	if err != nil {
		return errors.Wrap(err, "encoding")
	}
	req, err := http.NewRequest("POST", d.host, bytes.NewReader(buf))
	if err != nil {
		return errors.Wrap(err, "making new request")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "posting")
	}
	// Intentionally ignoring response body, as user does not need to be notified of error.
	defer resp.Body.Close()
	return nil
}

// CheckVersion of the local build against Pilosa master.
func (d *diagnosticsCollector) CheckVersion() error {
	var rsp versionResponse
	req, err := http.NewRequest("GET", d.VersionURL, nil)
	if err != nil {
		return errors.Wrap(err, "making request")
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "getting version")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http: status=%d", resp.StatusCode)
	} else if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		return fmt.Errorf("json decode: %s", err)
	}

	// If version has not changed since the last check, return
	if rsp.Version == d.lastVersion {
		return nil
	}

	d.lastVersion = rsp.Version
	if err := d.compareVersion(rsp.Version); err != nil {
		d.Logger.Printf("%s\n", err.Error())
	}

	return nil
}

// compareVersion check version strings.
func (d *diagnosticsCollector) compareVersion(value string) error {
	currentVersion := versionSegments(value)
	localVersion := versionSegments(d.version)

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
func (d *diagnosticsCollector) encode() ([]byte, error) {
	return json.Marshal(d.metrics)
}

// Set adds a key value metric.
func (d *diagnosticsCollector) Set(name string, value interface{}) {
	switch v := value.(type) {
	case string:
		if v == "" {
			// Do not set empty string
			return
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.metrics[name] = value
}

// logErr logs the error and returns true if an error exists
func (d *diagnosticsCollector) logErr(err error) bool {
	if err != nil {
		d.Logger.Printf("%v", err)
		return true
	}
	return false
}

// EnrichWithCPUInfo adds CPU information to the diagnostics payload.
func (d *diagnosticsCollector) EnrichWithCPUInfo() {
	d.Set("CPUArch", d.server.systemInfo.CPUArch())
}

// EnrichWithOSInfo adds OS information to the diagnostics payload.
func (d *diagnosticsCollector) EnrichWithOSInfo() {
	uptime, err := d.server.systemInfo.Uptime()
	if !d.logErr(err) {
		d.Set("HostUptime", uptime)
	}
	platform, err := d.server.systemInfo.Platform()
	if !d.logErr(err) {
		d.Set("OSPlatform", platform)
	}
	family, err := d.server.systemInfo.Family()
	if !d.logErr(err) {
		d.Set("OSFamily", family)
	}
	version, err := d.server.systemInfo.OSVersion()
	if !d.logErr(err) {
		d.Set("OSVersion", version)
	}
	kernelVersion, err := d.server.systemInfo.KernelVersion()
	if !d.logErr(err) {
		d.Set("OSKernelVersion", kernelVersion)
	}
}

// EnrichWithMemoryInfo adds memory information to the diagnostics payload.
func (d *diagnosticsCollector) EnrichWithMemoryInfo() {
	memFree, err := d.server.systemInfo.MemFree()
	if !d.logErr(err) {
		d.Set("MemFree", memFree)
	}
	memTotal, err := d.server.systemInfo.MemTotal()
	if !d.logErr(err) {
		d.Set("MemTotal", memTotal)
	}
	memUsed, err := d.server.systemInfo.MemUsed()
	if !d.logErr(err) {
		d.Set("MemUsed", memUsed)
	}
}

// EnrichWithSchemaProperties adds schema info to the diagnostics payload.
func (d *diagnosticsCollector) EnrichWithSchemaProperties() {
	var numShards uint64
	numFields := 0
	numIndexes := 0
	bsiFieldCount := 0
	timeQuantumEnabled := false

	for _, index := range d.server.holder.Indexes() {
		numShards += index.AvailableShards().Count()
		numIndexes += 1
		for _, field := range index.Fields() {
			numFields += 1
			if field.Type() == FieldTypeInt {
				bsiFieldCount += 1
			}
			if field.TimeQuantum() != "" {
				timeQuantumEnabled = true
			}
		}
	}

	d.Set("NumIndexes", numIndexes)
	d.Set("NumFields", numFields)
	d.Set("NumShards", numShards)
	d.Set("BSIFieldCount", bsiFieldCount)
	d.Set("TimeQuantumEnabled", timeQuantumEnabled)
}

// versionSegments returns the numeric segments of the version as a slice of ints.
func versionSegments(segments string) []int {
	segments = strings.Trim(segments, "v")
	segments = strings.Split(segments, "-")[0]
	s := strings.Split(segments, ".")
	segmentSlice := make([]int, len(s))
	for i, v := range s {
		segmentSlice[i], _ = strconv.Atoi(v)
	}
	return segmentSlice
}

// SystemInfo collects information about the host OS.
type SystemInfo interface {
	Uptime() (uint64, error)
	Platform() (string, error)
	Family() (string, error)
	OSVersion() (string, error)
	KernelVersion() (string, error)
	MemFree() (uint64, error)
	MemTotal() (uint64, error)
	MemUsed() (uint64, error)
	CPUArch() string
}

// newNopSystemInfo creates a no-op implementation of SystemInfo.
func newNopSystemInfo() *nopSystemInfo {
	return &nopSystemInfo{}
}

// nopSystemInfo is a no-op implementation of SystemInfo.
type nopSystemInfo struct {
}

// Uptime is a no-op implementation of SystemInfo.Uptime.
func (n *nopSystemInfo) Uptime() (uint64, error) {
	return 0, nil
}

// Platform is a no-op implementation of SystemInfo.Platform.
func (n *nopSystemInfo) Platform() (string, error) {
	return "", nil
}

// Family is a no-op implementation of SystemInfo.Family.
func (n *nopSystemInfo) Family() (string, error) {
	return "", nil
}

// OSVersion is a no-op implementation of SystemInfo.OSVersion.
func (n *nopSystemInfo) OSVersion() (string, error) {
	return "", nil
}

// KernelVersion is a no-op implementation of SystemInfo.KernelVersion.
func (n *nopSystemInfo) KernelVersion() (string, error) {
	return "", nil
}

// MemFree is a no-op implementation of SystemInfo.MemFree.
func (n *nopSystemInfo) MemFree() (uint64, error) {
	return 0, nil
}

// MemTotal is a no-op implementation of SystemInfo.MemTotal.
func (n *nopSystemInfo) MemTotal() (uint64, error) {
	return 0, nil
}

// MemUsed is a no-op implementation of SystemInfo.MemUsed.
func (n *nopSystemInfo) MemUsed() (uint64, error) {
	return 0, nil
}

// CPUArch returns the CPU architecture, such as amd64
func (n *nopSystemInfo) CPUArch() string {
	return ""
}
