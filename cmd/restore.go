// Copyright 2020 Pilosa Corp.
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

	"github.com/molecula/featurebase/v2/ctl"
	"github.com/spf13/cobra"
)

func newRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	cmd := ctl.NewRestoreCommand(stdin, stdout, stderr)
	restoreCmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore from a backup",
		Long: `
The Restore command will take a backup archive and restore it to a new, clean cluster.
`,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Run(context.Background())
		},
	}
	flags := restoreCmd.Flags()
	flags.StringVarP(&cmd.Path, "source", "s", "", "pilosa backup file; specify '-' to restore from stdin tar stream")
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of Pilosa.")
	flags.IntVar(&cmd.Concurrency, "concurrency", 1, "number of concurrent uploads")
	ctl.SetTLSConfig(
		flags, "",
		&cmd.TLS.CertificatePath,
		&cmd.TLS.CertificateKeyPath,
		&cmd.TLS.CACertPath,
		&cmd.TLS.SkipVerify,
		&cmd.TLS.EnableClientVerification,
	)

	return restoreCmd
}
