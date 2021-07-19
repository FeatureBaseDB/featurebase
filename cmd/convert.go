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

	"github.com/spf13/cobra"

	"github.com/molecula/featurebase/v2/ctl"
)

var inspector *ctl.InspectCommand

func newInspectCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	inspector = ctl.NewInspectCommand(stdin, stdout, stderr)

	inspectCmd := &cobra.Command{
		Use:   "inspect",
		Short: "Get stats on a FeatureBase data file.",
		Long: `
Inspects a data file and provides stats.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("path required")
			} else if len(args) > 1 {
				return fmt.Errorf("only one path allowed")
			}
			inspector.Path = args[0]
			return inspector.Run(context.Background())
		},
	}
	flags := inspectCmd.Flags()
	flags.BoolVarP(&inspector.Quiet, "quiet", "q", false, "don't list details of containers")
	flags.IntVarP(&inspector.Max, "max", "n", 0, "list at most max items (0 = unlimited)")
	flags.StringVarP(&inspector.InspectOpts.Indexes, "index", "i", "", "filter indexes")
	flags.StringVarP(&inspector.InspectOpts.Views, "view", "v", "", "filter views")
	flags.StringVarP(&inspector.InspectOpts.Fields, "field", "f", "", "filter fields")
	flags.StringVarP(&inspector.InspectOpts.Shards, "shard", "s", "", "filter shards")
	return inspectCmd
}
