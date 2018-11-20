// +build roaringstats

package roaring

import (
	"github.com/pilosa/pilosa/stats"
)

var statsEv = stats.NewExpvarStatsClient()

// statsHit increments the given stat, so we can tell how often we've hit
// that particular event.
func statsHit(name string) {
	statsEv.Count(name, 1, 1)
}
