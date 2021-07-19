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

// +build roaringstats

package roaring

import (
	"github.com/molecula/featurebase/v2/stats"
)

var statsEv = stats.NewExpvarStatsClient()

// statsHit increments the given stat, so we can tell how often we've hit
// that particular event.
func statsHit(name string) {
	statsEv.Count(name, 1, 1)
}
