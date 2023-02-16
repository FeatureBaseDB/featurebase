// Copyright 2021 Molecula Corp. All rights reserved.
package main

import (
	"os"

	"github.com/featurebasedb/featurebase/v3/cmd"
)

func main() {
	command := cmd.NewCLICommand(os.Stderr)
	command.Execute()
}
