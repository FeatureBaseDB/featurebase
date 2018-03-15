package gopsutil

import (
	"github.com/pilosa/pilosa"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/mem"
)

var _ pilosa.SystemInfo = NewSystemInfo()

// SystemInfo is an implementation of pilosa.SystemInfo that uses gopsutil to collect information about the host OS.
type SystemInfo struct {
	platform  string
	family    string
	osVersion string
}

// Uptime returns the system uptime in seconds.
func (s *SystemInfo) Uptime() (uptime uint64, err error) {
	hostInfo, err := host.Info()
	if err != nil {
		return 0, err
	}
	return hostInfo.Uptime, nil
}

// collectPlatformInfo fetches and caches system platform information.
func (s *SystemInfo) collectPlatformInfo() error {
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
func (s *SystemInfo) Platform() (string, error) {
	err := s.collectPlatformInfo()
	if err != nil {
		return "", err
	}
	return s.platform, nil
}

// Family returns the system family.
func (s *SystemInfo) Family() (string, error) {
	err := s.collectPlatformInfo()
	if err != nil {
		return "", err
	}
	return s.family, err
}

// OSVersion returns the OS Version.
func (s *SystemInfo) OSVersion() (string, error) {
	err := s.collectPlatformInfo()
	if err != nil {
		return "", err
	}
	return s.osVersion, err
}

// MemFree returns the amount of free memory in bytes.
func (s *SystemInfo) MemFree() (uint64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return memInfo.Free, err
}

// MemTotal returns the amount of total memory in bytes.
func (s *SystemInfo) MemTotal() (uint64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return memInfo.Total, err
}

// MemUsed returns the amount of used memory in bytes.
func (s *SystemInfo) MemUsed() (uint64, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return memInfo.Used, err
}

// KernelVersion returns the kernel version as a string.
func (s *SystemInfo) KernelVersion() (string, error) {
	return host.KernelVersion()
}

// NewSystemInfo is a constructor for the gopsutil implementation of SystemInfo.
func NewSystemInfo() *SystemInfo {
	return &SystemInfo{}
}
