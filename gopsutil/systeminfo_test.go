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

package gopsutil_test

import (
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/gopsutil"
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
