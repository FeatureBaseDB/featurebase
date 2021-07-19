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
//
// +build generationdebug

package pilosa

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/molecula/featurebase/v2/testhook"
)

func examineResults() error {
	runtime.GC()
	stats, results := reportGenerations()
	if len(stats) > 0 {
		fmt.Printf("generation stats: %s\n", stats)
	}
	if len(results) == 0 {
		return nil
	}
	if len(results) > 0 {
		fmt.Printf("generations:\n")
		for _, res := range results {
			fmt.Printf("  %s\n", res)
		}
	}
	return errors.New("outstanding generations detected")
}

func init() {
	testhook.RegisterPostTestHook(examineResults)
}
