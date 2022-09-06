// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package gopsutil

import (
	"math"
	"runtime"
	"strings"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
)

var _ pilosa.SystemInfo = NewSystemInfo()

// systemInfo is an implementation of pilosa.systemInfo that uses gopsutil to collect information about the host OS.
type systemInfo struct {
	platform         string
	family           string
	osVersion        string
	cpuModel         string
	cpuPhysicalCores int
	cpuLogicalCores  int
	cpuMHz           int
}

// Uptime returns the system uptime in seconds.
func (s *systemInfo) Uptime() (uptime uint64, err error) {
	hostInfo, err := host.Info()
	if err != nil {
		return 0, err
	}
	return hostInfo.Uptime, nil
}

// cpuFrequencyMultipliers is a lookup table for prefixes on clock speeds.
// This is probably overkill.
var cpuFrequencyMultipliers = [256]int{
	'M': 1,
	'G': 1000,
	'T': 1000 * 1000,
}

// computeHz determines the official rated speed of a CPU from its brand
// string. This insanity is *actually the official documented way to do
// this according to Intel*. There is also a cpuid leaf for the frequency,
// but I am not sure how supported it is, so.
func computeMHz(brandString string) int {
	hz := strings.LastIndex(brandString, "Hz")
	// ' 1MHz'
	if hz < 3 {
		return -1
	}
	multiplier := cpuFrequencyMultipliers[brandString[hz-1]]
	if multiplier == 0 {
		return -1
	}
	freq := 0
	divisor := 0
	decimalShift := 1
	var i int
	for i = hz - 2; i >= 0 && brandString[i] != ' '; i-- {
		if brandString[i] >= '0' && brandString[i] <= '9' {
			freq += int(brandString[i]-'0') * decimalShift
			decimalShift *= 10
		} else if brandString[i] == '.' {
			if divisor != 0 {
				return -1
			}
			divisor = decimalShift
		} else {
			return -1
		}
	}
	// we didn't find a space
	if i < 0 {
		return -1
	}
	if divisor != 0 {
		return (freq * multiplier) / divisor
	}
	return freq * multiplier
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
	if s.cpuModel == "" {
		infos, err := cpu.Info()
		if err != nil || len(infos) == 0 {
			s.cpuModel = "unknown"
			// if err is nil, but we got no infos, we don't
			// have a meaningful error to return.
			return err
		}
		s.cpuModel = infos[0].ModelName
		s.cpuMHz = computeMHz(s.cpuModel)
		if s.cpuMHz < 0 {
			s.cpuMHz = int(math.Round(infos[0].Mhz))
		}
		if s.cpuMHz < 0 {
			// This is supposed to be unsigned.
			s.cpuMHz = 0
		}

		// gopsutil reports core and clock speed info inconsistently
		// by OS
		switch runtime.GOOS {
		case "linux":
			// Each reported "CPU" is a logical core. Some cores may
			// have the same Core ID, which is a strictly numeric
			// value which gopsutil returned as a string, which
			// indicates that they're hyperthreading or similar things
			// on the same physical core.
			uniqueCores := make(map[string]struct{}, len(infos))
			totalCores := 0
			for _, info := range infos {
				uniqueCores[info.CoreID] = struct{}{}
				totalCores += int(info.Cores)
			}
			s.cpuPhysicalCores = len(uniqueCores)
			s.cpuLogicalCores = totalCores
		case "darwin":
			fallthrough
		default: // let's hope other systems give useful core info?
			s.cpuPhysicalCores = int(infos[0].Cores)
			// we have no way to know, let's try runtime
			s.cpuLogicalCores = runtime.NumCPU()
		}
		return nil
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

// CPUModel returns the CPU model string
func (s *systemInfo) CPUModel() string {
	err := s.collectPlatformInfo()
	if err != nil {
		return "unknown"
	}
	return s.cpuModel
}

// CPUMhz returns the CPU clock speed
func (s *systemInfo) CPUMHz() (int, error) {
	err := s.collectPlatformInfo()
	if err != nil {
		return 0, err
	}
	return s.cpuMHz, nil
}

// CPUCores returns the number of (physical or logical) CPU cores
func (s *systemInfo) CPUCores() (physical, logical int, err error) {
	err = s.collectPlatformInfo()
	if err != nil {
		return 0, 0, err
	}
	return s.cpuPhysicalCores, s.cpuLogicalCores, nil
}

// DiskCapacity returns the disk capacity.
func (s *systemInfo) DiskCapacity(path string) (uint64, error) {
	diskInfo, err := disk.Usage(path)

	if err != nil {
		return 0, err
	}
	return diskInfo.Total, nil
}

// NewSystemInfo is a constructor for the gopsutil implementation of SystemInfo.
func NewSystemInfo() *systemInfo {
	return &systemInfo{}
}
