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

	"github.com/spf13/cobra"

	"github.com/pilosa/pilosa/ctl"
)

var shardDistributioner *ctl.ShardDistributionCommand

func newShardDistributionCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	shardDistributioner = ctl.NewShardDistributionCommand(stdin, stdout, stderr)
	shardDistributionCmd := &cobra.Command{
		Use:   "shard-distribution",
		Short: "Shard distribution data from pilosa.",
		Long: `
Returns the shard distribution based on the provided arguments, or on a running
Pilosa cluster.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return shardDistributioner.Run(context.Background())
		},
	}
	flags := shardDistributionCmd.Flags()

	flags.StringVarP(&shardDistributioner.Host, "host", "", "", "host:port of Pilosa")
	flags.StringVarP(&shardDistributioner.Index, "index", "i", "", "Pilosa index to consider")
	flags.IntVarP(&shardDistributioner.NumNodes, "num-nodes", "n", 1, "Number of nodes in the cluster")
	flags.IntVarP(&shardDistributioner.MaxShard, "max-shard", "m", 0, "Maximum shard to include (0-indexed)")
	flags.IntVarP(&shardDistributioner.ReplicaN, "replicas", "r", 1, "Number of replicas to include")
	ctl.SetTLSConfig(flags, &shardDistributioner.TLS.CertificatePath, &shardDistributioner.TLS.CertificateKeyPath, &shardDistributioner.TLS.SkipVerify)

	return shardDistributionCmd
}
