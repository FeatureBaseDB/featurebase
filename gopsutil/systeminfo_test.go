package gopsutil_test

import (
	"log"
	"runtime"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/gopsutil"
)

func TestSystemInfo(t *testing.T) {
	var systemInfo pilosa.SystemInfo = gopsutil.NewSystemInfo()

	//	Uptime()(uint64, error)
	//	Platform()(string, error)
	//	Family()(string, error)
	//	OSVersion()(string, error)
	//	KernelVersion()(string, error)
	//	MemFree()(uint64, error)
	//	MemTotal()(uint64, error)
	//	MemUsed()(uint64, error)
	//
	uptime, err := systemInfo.Uptime()
	if err != nil || uptime == 0 {
		t.Fatalf("Error collecting uptime (error: %v)", err)
	}

	platform, err := systemInfo.Platform()
	if err != nil || platform != runtime.GOOS {
		t.Fatalf("Platform must be %s. (error: %v)", runtime.GOOS, err)
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
	log.Println(memtotal)
	if err != nil {
		t.Fatalf("Error getting memtotal. (memtotal: %v, error: %v)", memtotal, err)
	}
}
