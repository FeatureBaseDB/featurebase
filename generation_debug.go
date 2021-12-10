// Copyright 2021 Molecula Corp. All rights reserved.
//go:build generationdebug
// +build generationdebug

package pilosa

import (
	"fmt"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

const generationDebug = true

type lifespan struct {
	from, to, finalized time.Time
	stack               []byte
}

var knownGenerations map[string]lifespan
var knownGenerationLock sync.Mutex

var timeZero time.Time

var generationDebugVerbose bool

// History reports the finalized/dead/created status of a span which we think
// is in some way in error. It's shared between a couple of places.
func (span *lifespan) History() string {
	dead := "not dead"
	finalized := "not finalized"
	if span.finalized != timeZero {
		finalized = fmt.Sprintf("finalized at %v", span.finalized)
	}
	if span.to != timeZero {
		dead = fmt.Sprintf("dead at %v", span.to)
	}
	return fmt.Sprintf("%s, %s, created at %v at %s", dead, finalized, span.from, span.stack)
}

func (span *lifespan) reportHistory(reason string, id string) string {
	return fmt.Sprintf("%s %s: %s", id, reason, span.History())
}

func registerGeneration(id string) string {
	knownGenerationLock.Lock()
	defer knownGenerationLock.Unlock()
	if knownGenerations == nil {
		knownGenerations = make(map[string]lifespan)
	}
	newSpan := lifespan{from: time.Now(), stack: debug.Stack()}
	origId := id

	// if you have more than 65k of the same file open, maybe you have bigger
	// problems than this.
	for span, exists := knownGenerations[id]; exists; span, exists = knownGenerations[id] {
		suffix := fmt.Sprintf("::%04x", rand.Int63n(65536))
		if generationDebugVerbose {
			history := span.History()
			fmt.Printf("new generation: adding suffix %s, previous %s\n",
				suffix, history)
		}
		id = origId + suffix
	}
	if generationDebugVerbose {
		fmt.Printf("new generation %s\n", id)
	}
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
		panic(span.reportHistory("ending generation", id))
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
		panic(span.reportHistory("finalizing", id))
	}
	span.finalized = time.Now()
	knownGenerations[id] = span
}

func reportGenerations() (stats string, surviving []string) {
	runtime.GC()
	knownGenerationLock.Lock()
	defer knownGenerationLock.Unlock()
	times := make([]int64, 0, len(knownGenerations))
	for id, span := range knownGenerations {
		if span.to == timeZero || span.finalized == timeZero {
			surviving = append(surviving, span.reportHistory("surviving", id))
		} else {
			times = append(times, int64(span.finalized.Sub(span.to)))
		}
	}
	stats = "no recorded finalized spans"
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
		stats = fmt.Sprintf("%d finalized spans. lag: mean %v, median %v, p90 %v, p99 %v, worst %v",
			len(times), time.Duration(mean), time.Duration(median), time.Duration(p90), time.Duration(p99), time.Duration(worst))
	}
	return stats, surviving
}
