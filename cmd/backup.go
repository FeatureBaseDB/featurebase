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

	"github.com/pilosa/pilosa/ctl"
	"github.com/spf13/cobra"
)

var Backuper *ctl.BackupCommand

func NewBackupCmd(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	Backuper = ctl.NewBackupCommand(os.Stdin, os.Stdout, os.Stderr)
	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup data from pilosa.",
		Long: `
Backs up the view from across the cluster into a single file.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := Backuper.Run(context.Background()); err != nil {
				return err
			}
			return nil
		},
	}
	flags := backupCmd.Flags()
	flags.StringVarP(&Backuper.Host, "host", "", "localhost:10101", "host:port of Pilosa.")
	flags.StringVarP(&Backuper.Index, "index", "i", "", "Pilosa index to backup.")
	flags.StringVarP(&Backuper.Frame, "frame", "f", "", "Frame to backup.")
	flags.StringVarP(&Backuper.View, "view", "v", "", "View to backup.")
	flags.StringVarP(&Backuper.Path, "output-file", "o", "", "File to write backup to - default stdout")
	ctl.SetTLSConfig(flags, &Backuper.TLS.CertificatePath, &Backuper.TLS.CertificateKeyPath, &Backuper.TLS.SkipVerify)

	return backupCmd
}

func init() {
	subcommandFns["backup"] = NewBackupCmd
}
