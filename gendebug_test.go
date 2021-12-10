// Copyright 2021 Molecula Corp. All rights reserved.
//
//go:build generationdebug
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
