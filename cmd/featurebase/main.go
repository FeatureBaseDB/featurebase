// Copyright 2021 Molecula Corp. All rights reserved.
/*
This is the entrypoint for the Pilosa binary.
*/
package main

import (
	"fmt"
	"os"

	"github.com/molecula/featurebase/v3/cmd"
	"github.com/molecula/featurebase/v3/monitor"
)

func main() {
	defer monitor.CaptureMessage("Session:Ended")
	rootCmd := cmd.NewRootCommand(os.Stdin, os.Stdout, os.Stderr)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
