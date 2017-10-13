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
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var Restorer *ctl.RestoreCommand

func NewRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Restorer = ctl.NewRestoreCommand(os.Stdin, os.Stdout, os.Stderr)

	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore data to pilosa from a backup file.",
		Long: `
Restores a view to the cluster from a backup file.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := Restorer.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	flags := restoreCmd.Flags()
	flags.StringVarP(&Restorer.Host, "host", "", "localhost:10101", "host:port of Pilosa.")
	flags.StringVarP(&Restorer.Index, "index", "i", "", "Pilosa index to restore into.")
	flags.StringVarP(&Restorer.Frame, "frame", "f", "", "Frame to restore into.")
	flags.StringVarP(&Restorer.View, "view", "v", "", "View to restore into.")
	flags.StringVarP(&Restorer.Path, "input-file", "d", "", "File to restore data from.")
	ctl.SetTLSConfig(flags, &Restorer.TLS.CertificatePath, &Restorer.TLS.CertificateKeyPath, &Restorer.TLS.SkipVerify)

	return restoreCmd
}

func init() {
	subcommandFns["restore"] = NewRestoreCommand
}
