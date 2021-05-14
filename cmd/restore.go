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
	"fmt"
	"io"

	"github.com/pilosa/pilosa/v2/ctl"
	"github.com/spf13/cobra"
)

func newRestoreCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	c := ctl.NewRestoreCommand(stdin, stdout, stderr)
	restoreCmd := &cobra.Command{
		Use:   "restore [flags] PATH ",
		Short: "restore a backup",
		Long: `
		The restore command will take a backup archive and restore it to a new, clean cluster.
`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("data directory path required")
			} else if len(args) > 1 {
				return fmt.Errorf("too many command line arguments")
			}
			c.Path = args[0]
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(context.Background())
		},
	}
	flags := restoreCmd.Flags()
	flags.StringVarP(&c.Host, "host", "", "localhost:10101", "host:port of Pilosa.")
	flags.StringVarP(&c.Path, "source", "s", "", "pilosa backup file")
	ctl.SetTLSConfig(
		flags, "",
		&c.TLS.CertificatePath,
		&c.TLS.CertificateKeyPath,
		&c.TLS.CACertPath,
		&c.TLS.SkipVerify,
		&c.TLS.EnableClientVerification)

	return restoreCmd
}
