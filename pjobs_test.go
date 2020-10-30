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
	"fmt"
	"sync/atomic"
	"testing"
)

func Test_ParallelJobs_EarlyShutdown_WaitsForAllGoro(t *testing.T) {
	const n = 10000 // total jobs to run

	var errLastOne = fmt.Errorf("the last job has run, and returned this error")
	pj := newParallelJobs(100)
	nTotal := int64(0)
	for i := 0; i < n; i++ {
		accepted := pj.run(func(worker int) error {
			highpoint := atomic.AddInt64(&nTotal, 1)
			switch int(highpoint) {
			case n - 1:
				return errLastOne
			}
			return nil
		})
		if !accepted {
			panic("should have been accepted")
		}
	}
	err := pj.waitForFinish()
	tot := atomic.LoadInt64(&nTotal)
	if int(tot) != n {
		panic(fmt.Sprintf("We didn't run them all? tot=%v, n=%v; pj.jobQ len %v; err='%v'", tot, n, len(pj.jobQ), err))
	}
	if err != errLastOne {
		panic("expected to see errLastOne")
	}
	// good: finished cleanly.
}
