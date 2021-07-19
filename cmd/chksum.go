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

	"github.com/molecula/featurebase/v2/ctl"
	"github.com/spf13/cobra"
)

func newChkSumCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer) *cobra.Command {
	cmd := ctl.NewChkSumCommand(stdin, stdout, stderr)
	ccmd := &cobra.Command{
		Use:   "chksum",
		Short: "digital signature of pilosa data",
		Long: `
			Generates a digital signature of all the data associated with a provided pilosa server
			WARNING: could be slow if high cardinality fields exist
`,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Run(context.Background())
		},
	}

	flags := ccmd.Flags()
	flags.StringVar(&cmd.Host, "host", "localhost:10101", "host:port of Pilosa.")
	ctl.SetTLSConfig(flags, "", &cmd.TLS.CertificatePath, &cmd.TLS.CertificateKeyPath, &cmd.TLS.CACertPath, &cmd.TLS.SkipVerify, &cmd.TLS.EnableClientVerification)
	return ccmd
}
