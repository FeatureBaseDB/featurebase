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

package cmd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var checker *ctl.CheckCommand

func NewCheckCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	checker = ctl.NewCheckCommand(os.Stdin, os.Stdout, os.Stderr)
	checkCmd := &cobra.Command{
		Use:   "check <path> [path2]...",
		Short: "Do a consistency check on a pilosa data file.",
		Long: `
Performs a consistency check on data files.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("path required")
			}
			checker.Paths = args
			if err := checker.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	return checkCmd
}

func init() {
	subcommandFns["check"] = NewCheckCommand
}
