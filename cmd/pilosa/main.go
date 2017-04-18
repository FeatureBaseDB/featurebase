/*
This is the entrypoint for the Pilosa binary.
*/
package main

import (
	"fmt"
	"os"

	"github.com/pilosa/pilosa/cmd"
)

func main() {
	rootCmd := cmd.NewRootCommand(os.Stdin, os.Stdout, os.Stderr)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
