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

package ctl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/server"
	"github.com/pkg/errors"
)

// ShardDistributionCommand represents a command for bulk exporting data from a server.
type ShardDistributionCommand struct {
	// Remote host and port.
	Host string

	// Name of the index & field to export from.
	Index string

	// Number of nodes in the cluster.
	NumNodes int

	// Maximum Shard to include.
	MaxShard int

	// Replicas to include in shard distribution.
	ReplicaN int

	// Standard input/output
	*pilosa.CmdIO

	TLS server.TLSConfig
}

// NewShardDistributionCommand returns a new instance of ShardDistributionCommand.
func NewShardDistributionCommand(stdin io.Reader, stdout, stderr io.Writer) *ShardDistributionCommand {
	return &ShardDistributionCommand{
		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),
	}
}

// Run executes the shard location logic.
func (cmd *ShardDistributionCommand) Run(ctx context.Context) error {
	logger := log.New(cmd.Stderr, "", log.LstdFlags)

	// Validate arguments.
	if cmd.Index == "" {
		return pilosa.ErrIndexRequired
	}

	numNodes := cmd.NumNodes
	maxShard := uint64(cmd.MaxShard)

	//nodes := []pilosa.Node{}
	var shards [][]uint64

	// If no host is specified, launch an in-memory cluster.
	if cmd.Host == "" {

		memCluster := make([]*server.Command, numNodes)

		additionalNodes := make([]string, numNodes-1)
		for i := range additionalNodes {
			additionalNodes[i] = fmt.Sprintf("node%d", i+1)
		}

		for i := range memCluster {
			memCluster[i] = server.NewCommand(cmd.Stdin, cmd.Stdout, cmd.Stderr)
			memCluster[i].Config.Bind = ":0"
			memCluster[i].Config.LogPath = "/dev/null"
			memCluster[i].Config.Cluster.Disabled = true
			memCluster[i].Config.Cluster.Coordinator = true
			memCluster[i].Config.Cluster.ReplicaN = cmd.ReplicaN
			memCluster[i].Config.Cluster.Hosts = additionalNodes

			if err := memCluster[i].SetupServer(); err != nil {
				return errors.Wrapf(err, "setting up server %d", i)
			}
		}

		m0 := memCluster[0]
		logger.Printf("in-memory cmd: %v\n", m0.API)

		_, shards = m0.API.ShardDistributionByIndex(ctx, cmd.Index, true, maxShard)
	} else {
		// Create a client to the server.
		client, err := commandClient(cmd)
		if err != nil {
			return errors.Wrap(err, "creating client")
		}

		_, shards, err = client.ShardDistribution(ctx, cmd.Index, maxShard)
		if err != nil {
			return errors.Wrap(err, "getting shard distribution")
		}
	}

	buf, err := json.Marshal(shards)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.Stdout, string(buf))

	return nil
}

func (cmd *ShardDistributionCommand) TLSHost() string {
	return cmd.Host
}

func (cmd *ShardDistributionCommand) TLSConfiguration() server.TLSConfig {
	return cmd.TLS
}
