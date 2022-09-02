// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package debugstats

import (
	"fmt"
	"testing"
	"time"
)

func TestCallStats(t *testing.T) {

	callStats := NewCallStats()

	for j := 0; j < 4; j++ {
		t0 := time.Now()
		doOperation0()
		callStats.Add("op0", time.Since(t0))

		t1 := time.Now()
		doOperation1()
		callStats.Add("op1", time.Since(t1))

		t2 := time.Now()
		doOperation2()
		callStats.Add("op2", time.Since(t2))
	}

	fmt.Printf("report = \n%v\n", callStats.Report("test"))
}

func doOperation0() {
	time.Sleep(50 * time.Millisecond)
}

func doOperation1() {
	time.Sleep(100 * time.Millisecond)
}

func doOperation2() {
	time.Sleep(200 * time.Millisecond)
}
