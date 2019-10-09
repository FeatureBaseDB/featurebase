// Copyright 2019 Pilosa Corp.
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

// +build generationdebug

package pilosa

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"
)

const generationDebug = true

type lifespan struct {
	from, to, finalized time.Time
}

var knownGenerations map[string]lifespan
var knownGenerationLock sync.Mutex

var timeZero time.Time

func registerGeneration(id string) string {
	knownGenerationLock.Lock()
	defer knownGenerationLock.Unlock()
	if knownGenerations == nil {
		knownGenerations = make(map[string]lifespan)
	}
	newSpan := lifespan{from: time.Now()}
	origId := id

	// if you have more than 65k of the same file open, maybe you have bigger
	// problems than this.
	for span, exists := knownGenerations[id]; exists; span, exists = knownGenerations[id] {
		suffix := fmt.Sprintf("::%04x", rand.Int63n(65536))
		if span.finalized != timeZero {
			fmt.Printf("new generation %s: adding %s, previously existed, created %v, died %v, finalized %v\n",
				id, suffix, span.from, span.to, span.finalized)
		} else {
			if span.to != timeZero {
				fmt.Printf("new generation %s: adding %s, previously existed, created %v, died %v\n", id, suffix, span.from, span.to)
			} else {
				fmt.Printf("new generation %s: adding %s, already exists, created %v", id, suffix, span.from)
			}
		}
		id = origId + suffix
	}
	fmt.Printf("new generation %s\n", id)
	knownGenerations[id] = newSpan
	return id
}

func endGeneration(id string) {
	knownGenerationLock.Lock()
	defer knownGenerationLock.Unlock()
	span, exists := knownGenerations[id]
	if !exists {
		oops := fmt.Sprintf("ending generation %s: unknown", id)
		panic(oops)
	}
	if span.finalized != timeZero || span.to != timeZero {
		oops := fmt.Sprintf("ending generation %s: already died at %v, finalized at %v", id, span.to, span.finalized)
		panic(oops)
	}
	span.to = time.Now()
	knownGenerations[id] = span
}

// cancelGeneration marks the generation as finalized. In principle it's
// only used in cases where we just started a generation but something
// went wrong. it's not fancier than this because of the weird cases
// where the same generation shows up again, such as when closing and
// reopening an index so we don't know about previous instances of the
// same files.
func cancelGeneration(id string) {
	knownGenerationLock.Lock()
	defer knownGenerationLock.Unlock()
	span, exists := knownGenerations[id]
	if exists {
		span.finalized = time.Now()
		span.to = span.finalized
		knownGenerations[id] = span
	}
}

func finalizeGeneration(id string) {
	knownGenerationLock.Lock()
	defer knownGenerationLock.Unlock()
	span, exists := knownGenerations[id]
	if !exists {
		oops := fmt.Sprintf("finalizing generation %s: unknown", id)
		panic(oops)
	}
	if span.finalized != timeZero {
		var oops string
		if span.to != timeZero {
			oops = fmt.Sprintf("finalizing generation %s: already finalized at %v, but not dead", id, span.finalized)
		} else {
			oops = fmt.Sprintf("finalizing generation %s: already finalized at %v, dead at %v", id, span.finalized, span.to)
		}
		panic(oops)
	}
	span.finalized = time.Now()
	knownGenerations[id] = span
}

func reportGenerations() []string {
	runtime.GC()
	knownGenerationLock.Lock()
	defer knownGenerationLock.Unlock()
	var surviving []string
	times := make([]int64, 0, len(knownGenerations))
	for id, span := range knownGenerations {
		if span.to == timeZero {
			if span.finalized == timeZero {
				surviving = append(surviving, fmt.Sprintf("%s: %v, not ended or finalized", id, span.from))
			} else {
				surviving = append(surviving, fmt.Sprintf("%s: %v, finalized %v, not ended", id, span.from, span.finalized))
			}
		} else {
			if span.finalized == timeZero {
				surviving = append(surviving, fmt.Sprintf("%s: %v to %v, not finalized", id, span.from, span.to))
			} else {
				times = append(times, int64(span.finalized.Sub(span.to)))
			}
		}
	}
	if len(times) > 0 {
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		var total int64
		for _, d := range times {
			total += d
		}
		var mean, median, p90, p99, worst int64
		mean = total / int64(len(times))
		median = times[len(times)/2]
		p90 = times[(len(times)*9)/10]
		p99 = times[(len(times)*99)/100]
		worst = times[len(times)-1]
		surviving = append(surviving, fmt.Sprintf("%d finalized spans. lag: mean %v, median %v, p90 %v, p99 %v, worst %v",
			len(times), time.Duration(mean), time.Duration(median), time.Duration(p90), time.Duration(p99), time.Duration(worst)))
	}
	return surviving
}
