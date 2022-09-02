// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package gopsutil_test

import (
	"testing"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/gopsutil"
)

func TestSystemInfo(t *testing.T) {
	var systemInfo pilosa.SystemInfo = gopsutil.NewSystemInfo()

	uptime, err := systemInfo.Uptime()
	if err != nil || uptime == 0 {
		t.Fatalf("Error collecting uptime (error: %v)", err)
	}

	platform, err := systemInfo.Platform()
	if err != nil {
		t.Fatalf("Error getting platform. (platform: %v, error: %v)", platform, err)
	}

	family, err := systemInfo.Family()
	if err != nil {
		t.Fatalf("Error getting OS family. (family: %v, error: %v)", family, err)
	}

	osversion, err := systemInfo.OSVersion()
	if err != nil {
		t.Fatalf("Error getting OS version. (osversion: %v, error: %v)", osversion, err)
	}

	kernelversion, err := systemInfo.KernelVersion()
	if err != nil {
		t.Fatalf("Error getting kernel version. (kernelversion: %v, error: %v)", kernelversion, err)
	}

	memfree, err := systemInfo.MemFree()
	if err != nil {
		t.Fatalf("Error getting memfree. (memfree: %v, error: %v)", memfree, err)
	}

	memused, err := systemInfo.MemUsed()
	if err != nil {
		t.Fatalf("Error getting memused. (memused: %v, error: %v)", memused, err)
	}

	memtotal, err := systemInfo.MemTotal()
	if err != nil {
		t.Fatalf("Error getting memtotal. (memtotal: %v, error: %v)", memtotal, err)
	}

	cpuArch := systemInfo.CPUArch()
	if cpuArch == "" {
		t.Fatalf("Error getting CPU arch.")
	}
}
