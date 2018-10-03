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

package gopsutil

import (
	"runtime"

	"github.com/pilosa/pilosa"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
)

var _ pilosa.SystemInfo = NewSystemInfo()

// systemInfo is an implementation of pilosa.systemInfo that uses gopsutil to collect information about the host OS.
type systemInfo struct {
	platform  string
	family    string
	osVersion string
}

// Uptime returns the system uptime in seconds.
func (s *systemInfo) Uptime() (uptime uint64, err error) {
	hostInfo, err := host.Info()
	if err != nil {
		return 0, err
	}
	return hostInfo.Uptime, nil
}

// collectPlatformInfo fetches and caches system platform information.
func (s *systemInfo) collectPlatformInfo() error {
	var err error
	if s.platform == "" {
		s.platform, s.family, s.osVersion, err = host.PlatformInformation()
		if err != nil {
			return err
		}
	}
	return nil
}

// Platform returns the system platform.
func (s *systemInfo) Platform() (string, error) {
	err := s.collectPlatformInfo()
	if err != nil {
		return "", err
	}
	return s.platform, nil
}

// Family returns the system family.
func (s *systemInfo) Family() (string, error) {
	err := s.collectPlatformInfo()
	if err != nil {
		return "", err
	}
	return s.family, err
}

// OSVersion returns the OS Version.
func (s *systemInfo) OSVersion() (string, error) {
	err := s.collectPlatformInfo()
	if err != nil {
		return "", err
	}
	return s.osVersion, err
}

// MemFree returns the amount of free memory in bytes.
func (s *systemInfo) MemFree() (uint64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return memInfo.Free, err
}

// MemTotal returns the amount of total memory in bytes.
func (s *systemInfo) MemTotal() (uint64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return memInfo.Total, err
}

// MemUsed returns the amount of used memory in bytes.
func (s *systemInfo) MemUsed() (uint64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return memInfo.Used, err
}

// KernelVersion returns the kernel version as a string.
func (s *systemInfo) KernelVersion() (string, error) {
	return host.KernelVersion()
}

// CPUArch returns the CPU architecture, such as amd64
func (s *systemInfo) CPUArch() string {
	return runtime.GOARCH
}

// NewSystemInfo is a constructor for the gopsutil implementation of SystemInfo.
func NewSystemInfo() *systemInfo {
	return &systemInfo{}
}
