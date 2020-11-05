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
	"sync"

	"github.com/glycerine/idem"
)

// parallelJobs runs functions in parallel on a goroutine
// pool that has nGoro goroutines.
type parallelJobs struct {
	nGoro int

	jobQ    chan func(worker int) error
	halters []*idem.Halter

	// err is protected by errmu
	err   error
	errmu sync.Mutex
}

func newParallelJobs(nGoro int) (p *parallelJobs) {
	if nGoro < 1 {
		// 0 really means,
		// "turn it up to 11".
		// same for negative.
		nGoro = 10000
	}
	// maximum 10K goroutines
	if nGoro > 10000 {
		nGoro = 10000
	}

	p = &parallelJobs{
		nGoro:   nGoro,
		jobQ:    make(chan func(worker int) error, 10000),
		halters: make([]*idem.Halter, nGoro),
	}

	for j := 0; j < nGoro; j++ {
		h := idem.NewHalter()
		p.halters[j] = h
	}

	for i, h := range p.halters {
		go func(h *idem.Halter, worker int) {
			defer h.MarkDone()
			for {
				select {
				case <-h.ReqStop.Chan:
					return
				case f, ok := <-p.jobQ:
					if !ok {
						// channel closed, finish up
						return
					}

					err1 := f(worker)
					if err1 != nil {
						p.errmu.Lock()
						if p.err == nil {
							p.err = err1
						}
						p.errmu.Unlock()
						// an error occurred, tell everyone to stop
						for _, h2 := range p.halters {
							h2.RequestStop()
						}
						return
					}
				}
			}
		}(h, i)
	}
	return
}

// return value accepted will be false if we are shutting down
// due to an error.
func (p *parallelJobs) run(fun func(worker int) error) (accepted bool) {
	select {
	case <-p.halters[0].ReqStop.Chan:
		return false
	case p.jobQ <- fun:
		return true
	}
}

func (p *parallelJobs) waitForFinish() error {

	// tell the workers no more jobs.
	close(p.jobQ)

	// wait for everyone to finish
	for i, h := range p.halters {
		_ = i
		<-h.Done.Chan
	}

	return p.err
}
