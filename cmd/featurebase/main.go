// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
/*
This is the entrypoint for the Pilosa binary.
*/
package main

import (
	"fmt"
	"os"

	"github.com/featurebasedb/featurebase/v3/cmd"
	"github.com/featurebasedb/featurebase/v3/monitor"
)

func main() {
	defer monitor.CaptureMessage("Session:Ended")
	rootCmd := cmd.NewRootCommand(os.Stderr)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
