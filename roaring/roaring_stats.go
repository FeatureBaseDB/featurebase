// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
//go:build roaringstats
// +build roaringstats

package roaring

import (
	"github.com/molecula/featurebase/v3/stats"
)

var statsEv = stats.NewExpvarStatsClient()

// statsHit increments the given stat, so we can tell how often we've hit
// that particular event.
func statsHit(name string) {
	statsEv.Count(name, 1, 1)
}
