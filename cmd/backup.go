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

	"github.com/pilosa/pilosa/v2/ctl"
	"github.com/spf13/cobra"
)

func newBackupCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *cobra.Command {
	cmd := ctl.NewBackupCommand(stdin, stdout, stderr)
	ccmd := &cobra.Command{
		Use:   "backup",
		Short: "Back up FeatureBase server",
		Long: `
Backs up a FeatureBase server to a local, tar-formatted snapshot file.
`,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Run(context.Background())
		},
	}

	flags := ccmd.Flags()
	flags.StringVarP(&cmd.OutputDir, "output", "o", "", "output dir to write to")
	flags.BoolVar(&cmd.NoSync, "no-sync", false, "disable file sync")
	flags.IntVar(&cmd.Concurrency, "concurrency", cmd.Concurrency, "number of concurrent backup goroutines")
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of FeatureBase.")
	ctl.SetTLSConfig(flags, "", &cmd.TLS.CertificatePath, &cmd.TLS.CertificateKeyPath, &cmd.TLS.CACertPath, &cmd.TLS.SkipVerify, &cmd.TLS.EnableClientVerification)
	return ccmd
}
